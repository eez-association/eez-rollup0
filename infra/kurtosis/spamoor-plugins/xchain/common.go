package xchain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/spamoor/scenario"
	"github.com/ethpandaops/spamoor/spamoor"
	"github.com/ethpandaops/spamoor/txbuilder"
	"github.com/ethpandaops/spamoor/utils"
)

// perWalletFundingEth is the per-child-wallet refill; the preflight sizes the
// root key's required balance from it.
const perWalletFundingEth = 5

// validateOutboundKey rejects a malformed key up-front with an actionable error.
func validateOutboundKey(k string) error {
	h := strings.TrimPrefix(strings.TrimPrefix(k, "0x"), "0X")
	if len(h) != 64 {
		return fmt.Errorf("outbound_private_key must be 64 hex chars (32 bytes), with an optional 0x prefix — got %d chars; check for truncation, surrounding quotes, or stray whitespace when pasting", len(h))
	}
	if _, err := crypto.HexToECDSA(h); err != nil {
		return fmt.Errorf("outbound_private_key is not a valid secp256k1 key: %w", err)
	}
	return nil
}

// walletLocker serializes submission per wallet so its nonces reach the front
// in order — the front requires strictly contiguous nonces per sender.
type walletLocker struct{ mu sync.Map }

func (w *walletLocker) acquire(addr common.Address) func() {
	v, _ := w.mu.LoadOrStore(addr, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return func() { m.Unlock() }
}

// Well-formed cross-chain inclusion is observed at up to ~4 min under load.
const defaultInclusionTimeout = 5 * time.Minute

// A per-request timeout keeps a stuck front from pinning a scenario goroutine.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// walletCount auto-sizes the child-wallet pool, capped low (cross-chain is
// drain-limited, and an unbounded count outruns funding). max_wallets overrides.
func walletCount(maxWallets, totalCount, throughput uint64) uint64 {
	if maxWallets > 0 {
		return maxWallets
	}
	const autoMax = 50
	n := throughput * 10
	if c := totalCount / 50; c > n { // ~1 wallet per 50 txs
		n = c
	}
	if n < 10 {
		return 10
	}
	if n > autoMax {
		return autoMax
	}
	return n
}

// buildChainPool builds a standalone pool bound to one chain. `rpc` must be the
// NORMAL RPC, never a front — child wallets fund over it, and a front holds
// every send, so funding would deadlock.
func buildChainPool(ctx context.Context, logger logrus.FieldLogger, poolName, rpc, privkey string, wallets uint64) (*spamoor.WalletPool, error) {
	clients := spamoor.NewClientPool(ctx, logger.WithField("pool", poolName))
	if err := clients.InitClients([]*spamoor.ClientOptions{{RpcHost: rpc}}); err != nil {
		return nil, fmt.Errorf("init client pool: %w", err)
	}
	if err := clients.PrepareClients(); err != nil {
		return nil, fmt.Errorf("prepare client pool: %w", err)
	}

	txpool := spamoor.NewTxPool(&spamoor.TxPoolOptions{
		Context:    ctx,
		Logger:     logger.WithField("pool", poolName),
		ClientPool: clients,
		ChainId:    clients.GetChainId(),
	})
	if err := txpool.InitializeBlockStats(ctx); err != nil {
		return nil, fmt.Errorf("initialize block stats: %w", err)
	}

	rootWallet, err := spamoor.InitRootWallet(ctx, privkey, clients, txpool, logger.WithField("pool", poolName))
	if err != nil {
		return nil, fmt.Errorf("init root wallet: %w", err)
	}

	// Preflight: fail fast if the root can't fund every child wallet.
	required := new(uint256.Int).Mul(utils.EtherToWei(uint256.NewInt(perWalletFundingEth)), uint256.NewInt(wallets))
	if bal := rootWallet.GetWallet().GetBalance(); bal != nil {
		if have, overflow := uint256.FromBig(bal); !overflow && have.Lt(required) {
			return nil, fmt.Errorf("root key %s on %s has %s wei but funding %d child wallets at %d ETH each needs ~%s wei — top up the key (infra/kurtosis/scripts/xchain-provision.sh, raise EEZ_OUT_FUND_ETH) or lower max_wallets before starting",
				rootWallet.GetWallet().GetAddress().Hex(), rpc, have, wallets, perWalletFundingEth, required)
		}
	}

	pool := spamoor.NewWalletPool(ctx, logger.WithField("pool", poolName), rootWallet, clients, txpool)
	pool.SetWalletCount(wallets)
	pool.SetRefillAmount(utils.EtherToWei(uint256.NewInt(perWalletFundingEth)))
	pool.SetRefillBalance(utils.EtherToWei(uint256.NewInt(1)))
	pool.SetRefillInterval(600)
	recovered, err := prepareWalletsWithRetry(ctx, pool, logger.WithField("pool", poolName))
	if err != nil {
		return nil, fmt.Errorf("prepare wallets (funding %d child wallets from root %s over %s): %w — if this is 'txpool is full', the %s mempool is saturated (throttle other spammers or lower throughput/max_wallets and retry); if 'insufficient funds', top up the root key via xchain-provision.sh",
			wallets, rootWallet.GetWallet().GetAddress().Hex(), rpc, err, poolName)
	}
	if recovered {
		// Spamoor starts its native refill loop only when PrepareWallets reaches
		// the end. A partial-funding error returns before that point, so keep the
		// recovered pool supplied with an equivalent context-bound loop.
		go maintainWalletFunding(ctx, pool, logger.WithField("pool", poolName))
	}

	return pool, nil
}

// prepareWalletsWithRetry funds child wallets and explicitly repairs a partial
// PrepareWallets result after a transiently-full mempool. It reports whether
// the caller must provide the refill watcher that Spamoor did not start.
func prepareWalletsWithRetry(ctx context.Context, pool *spamoor.WalletPool, logger logrus.FieldLogger) (bool, error) {
	const attempts = 6
	err := pool.PrepareWallets()
	if err == nil {
		return false, nil
	}
	if !isTransientFundingErr(err) {
		return false, err
	}

	// PrepareWallets installs childWallets before it sends their funding txs.
	// Consequently, calling it again after a partial send failure is a no-op.
	// Refresh and fund the still-short wallets explicitly, one at a time, to
	// avoid filling the RPC txpool again.
	for i := 0; i < attempts; i++ {
		delay := time.Duration(5*(i+1)) * time.Second
		logger.Warnf("wallet funding was incomplete (recovery attempt %d/%d in %s): %v", i+1, attempts, delay, err)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(delay):
		}

		if err = fundShortWallets(ctx, pool); err == nil {
			return true, nil
		}
		if !isTransientFundingErr(err) {
			return false, err
		}
	}
	return false, err
}

// maintainWalletFunding replaces Spamoor's native refill watcher only on the
// partial-funding recovery path, where PrepareWallets returned before starting
// that watcher. It stops with the spammer context.
func maintainWalletFunding(ctx context.Context, pool *spamoor.WalletPool, logger logrus.FieldLogger) {
	const normalInterval = 10 * time.Minute
	const retryInterval = time.Minute
	timer := time.NewTimer(normalInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			interval := normalInterval
			if err := fundShortWallets(ctx, pool); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Warnf("could not check and refill recovered child wallets: %v", err)
				interval = retryInterval
			}
			timer.Reset(interval)
		}
	}
}

// fundShortWallets refreshes every child from the normal RPC and tops up only
// those below the configured 1 ETH working threshold. Requests are sent one by
// one: this is a recovery path entered specifically because a bulk funding send
// filled the txpool.
func fundShortWallets(ctx context.Context, pool *spamoor.WalletPool) error {
	client := pool.GetClient(spamoor.WithClientSelectionMode(spamoor.SelectClientByIndex, 0))
	if client == nil {
		return scenario.ErrNoClients
	}

	threshold := utils.EtherToWei(uint256.NewInt(1))
	refill := utils.EtherToWei(uint256.NewInt(perWalletFundingEth))
	for _, wallet := range pool.GetAllWallets() {
		if err := client.UpdateWallet(ctx, wallet); err != nil {
			return fmt.Errorf("refresh child wallet %s: %w", wallet.GetAddress().Hex(), err)
		}
		balance, overflow := uint256.FromBig(wallet.GetBalance())
		if overflow {
			return fmt.Errorf("child wallet %s balance overflows uint256", wallet.GetAddress().Hex())
		}
		if balance.Cmp(threshold) >= 0 {
			continue
		}

		amount := new(uint256.Int).Set(refill)
		if needed := new(uint256.Int).Sub(threshold, balance); needed.Cmp(amount) > 0 {
			amount = needed
		}
		if err := pool.FundAddresses([]*spamoor.FundingRequest{{
			Wallet:  wallet,
			Amount:  amount,
			IsEmpty: wallet.GetNonce() == 0 && wallet.GetBalance().Sign() == 0,
		}}); err != nil {
			return fmt.Errorf("fund child wallet %s: %w", wallet.GetAddress().Hex(), err)
		}
	}
	return nil
}

// isTransientFundingErr matches mempool-pressure errors that clear as blocks
// are produced (unlike insufficient-funds, which retrying won't fix).
func isTransientFundingErr(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "txpool is full") ||
		strings.Contains(s, "already known") ||
		strings.Contains(s, "replacement transaction underpriced")
}

// callSpec holds the per-transaction fee/gas knobs for submitCall.
type callSpec struct {
	baseFee    float64
	tipFee     float64
	baseFeeWei string
	tipFeeWei  string
	gasLimit   uint64
}

// submitCall signs a tx to target using pool's normal-RPC client, then POSTs it
// to the front. The caller tracks inclusion via waitInclusion.
func submitCall(ctx context.Context, locks *walletLocker, pool *spamoor.WalletPool, frontURL string, target common.Address, value *uint256.Int, calldata []byte, spec callSpec, logger *logrus.Entry, txIdx uint64) (*types.Transaction, *spamoor.Client, *spamoor.Wallet, error) {
	wallet := pool.GetWallet(spamoor.SelectWalletByIndex, int(txIdx))
	if wallet == nil {
		return nil, nil, nil, scenario.ErrNoWallet
	}

	// A still-unfunded wallet (funding tx submitted but not yet confirmed) must
	// not reach BuildDynamicFeeTx below: that call reserves a nonce unconditionally,
	// and MarkSkippedNonce can't reclaim it for a wallet with zero confirmed txs
	// (spamoor no-ops if nonce >= confirmedTxCount) — the front would then be
	// stuck expecting a nonce this wallet can never resend. Skip its turn instead;
	// the pool round-robins back to it once funded, with nonce untouched.
	if wallet.GetBalance().Sign() == 0 {
		return nil, nil, wallet, fmt.Errorf("wallet not yet funded, skipping this turn")
	}

	// Hold the wallet lock across nonce assignment and the POST so its nonces
	// reach the front in order. Released before the caller's waitInclusion.
	release := locks.acquire(wallet.GetAddress())
	defer release()

	client := pool.GetClient(spamoor.WithClientSelectionMode(spamoor.SelectClientByIndex, int(txIdx)))
	if client == nil {
		return nil, client, wallet, scenario.ErrNoClients
	}

	if err := wallet.ResetNoncesIfNeeded(ctx, client); err != nil {
		return nil, client, wallet, err
	}

	baseFeeWei, tipFeeWei := spamoor.ResolveFees(spec.baseFee, spec.tipFee, spec.baseFeeWei, spec.tipFeeWei)
	feeCap, tipCap, err := pool.GetSuggestedFees(client, baseFeeWei, tipFeeWei)
	if err != nil {
		return nil, client, wallet, fmt.Errorf("failed to get suggested fees: %w", err)
	}

	txData, err := txbuilder.DynFeeTx(&txbuilder.TxMetadata{
		GasFeeCap: uint256.MustFromBig(feeCap),
		GasTipCap: uint256.MustFromBig(tipCap),
		Gas:       spec.gasLimit,
		To:        &target,
		Value:     value,
		Data:      calldata,
	})
	if err != nil {
		return nil, client, wallet, fmt.Errorf("failed to build tx: %w", err)
	}

	// Do not reserve a nonce until the front accepts the transaction. Spamoor's
	// MarkSkippedNonce cannot reclaim a freshly reserved nonce when the wallet's
	// confirmed nonce is still zero, which previously produced nonce 1 while the
	// front correctly expected nonce 0. The per-wallet lock makes this explicit
	// peek/sign/submit/advance sequence safe.
	nonce := wallet.GetNonce()
	tx, err := wallet.ReplaceDynamicFeeTx(txData, nonce)
	if err != nil {
		return nil, client, wallet, fmt.Errorf("failed to sign tx: %w", err)
	}

	if err := sendRawTxToFront(ctx, frontURL, tx); err != nil {
		if expected, ok := frontExpectedNonce(err.Error()); ok && nonce < expected {
			// The front already holds earlier nonces, usually after an ambiguous
			// HTTP result or a spammer restart. Catch local accounting up, but do
			// not pretend this newly-built hash was admitted and await it forever.
			for wallet.GetNonce() < expected {
				wallet.GetNextNonce()
			}
			return tx, client, wallet, fmt.Errorf("front already holds nonce %d; advanced local next nonce to %d: %w", nonce, expected, err)
		}
		return tx, client, wallet, fmt.Errorf("front submit: %w", err)
	}

	// Admission succeeded, so consume exactly the nonce we just signed and make
	// the custom front submission visible to Spamoor's block statistics.
	wallet.GetNextNonce()
	wallet.IncrementSubmittedTxCount()
	return tx, client, wallet, nil
}

// reFrontHeldNonce matches the front's nonce-contiguity rejection, e.g.
// "expected 81 (on-chain 79 + 2 held)". Matching the full tail (not just
// "expected N") avoids reading an unrelated rejection as a resubmit.
var reFrontHeldNonce = regexp.MustCompile(`expected (\d+) \(on-chain \d+ \+ \d+ held\)`)

// frontExpectedNonce extracts the front's next contiguous nonce from an
// admission rejection.
func frontExpectedNonce(errMsg string) (uint64, bool) {
	m := reFrontHeldNonce.FindStringSubmatch(errMsg)
	if m == nil {
		return 0, false
	}
	expected, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return expected, true
}

// sendRawTxToFront POSTs a signed tx to a cross-chain front. A JSON-RPC "error"
// means admission was refused (bad nonce, low balance, malformed calldata).
func sendRawTxToFront(ctx context.Context, frontURL string, tx *types.Transaction) error {
	raw, err := tx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal tx: %w", err)
	}
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x%x"]}`, raw)
	resp, err := rpcPost(ctx, frontURL, body)
	if err != nil {
		return err
	}
	if e, ok := resp["error"]; ok && len(e) > 0 && string(e) != "null" {
		return fmt.Errorf("front rejected: %s", string(e))
	}
	var result string
	if r, ok := resp["result"]; !ok || len(r) == 0 || string(r) == "null" {
		return fmt.Errorf("front returned no transaction hash")
	} else if err := json.Unmarshal(r, &result); err != nil {
		return fmt.Errorf("decode front transaction hash: %w", err)
	}
	if !strings.EqualFold(result, tx.Hash().Hex()) {
		return fmt.Errorf("front returned transaction hash %q, want %s", result, tx.Hash().Hex())
	}
	return nil
}

// waitInclusion polls the front (which forwards reads to the source chain) for
// tx's successful receipt until it appears or timeout.
func waitInclusion(ctx context.Context, frontURL string, hash common.Hash, timeout time.Duration) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["%s"]}`, hash.Hex())
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if resp, err := rpcPost(cctx, frontURL, body); err == nil {
			if r, ok := resp["result"]; ok && len(r) > 0 && string(r) != "null" {
				var receipt struct {
					Status string `json:"status"`
				}
				if err := json.Unmarshal(r, &receipt); err != nil {
					return fmt.Errorf("decode receipt for %s: %w", hash.Hex(), err)
				}
				statusText := strings.TrimPrefix(strings.ToLower(receipt.Status), "0x")
				status, err := strconv.ParseUint(statusText, 16, 64)
				if err != nil {
					return fmt.Errorf("decode receipt status %q for %s: %w", receipt.Status, hash.Hex(), err)
				}
				if status != types.ReceiptStatusSuccessful {
					return fmt.Errorf("transaction %s was included but reverted", hash.Hex())
				}
				return nil
			}
		}
		select {
		case <-cctx.Done():
			return fmt.Errorf("timed out waiting for inclusion of %s: %w", hash.Hex(), cctx.Err())
		case <-ticker.C:
		}
	}
}

// rpcPost sends one JSON-RPC request and returns the decoded top-level object.
func rpcPost(ctx context.Context, url, body string) (map[string]json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("rpc returned HTTP %s: %s", resp.Status, string(data))
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode rpc response (%s): %w", string(data), err)
	}
	return out, nil
}

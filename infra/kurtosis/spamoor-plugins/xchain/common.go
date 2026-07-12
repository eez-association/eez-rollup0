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
	return m.Unlock
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
func buildChainPool(logger logrus.FieldLogger, poolName, rpc, privkey string, wallets uint64) (*spamoor.WalletPool, error) {
	ctx := context.Background() // pool lifetime == process lifetime

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
	if err := prepareWalletsWithRetry(pool, logger.WithField("pool", poolName)); err != nil {
		return nil, fmt.Errorf("prepare wallets (funding %d child wallets from root %s over %s): %w — if this is 'txpool is full', the %s mempool is saturated (throttle other spammers or lower throughput/max_wallets and retry); if 'insufficient funds', top up the root key via xchain-provision.sh",
			wallets, rootWallet.GetWallet().GetAddress().Hex(), rpc, err, poolName)
	}

	return pool, nil
}

// prepareWalletsWithRetry funds child wallets, retrying on a transiently-full
// mempool ("txpool is full") that clears as blocks drain. PrepareWallets is
// idempotent, so retries only top up wallets still short; other errors return
// immediately.
func prepareWalletsWithRetry(pool *spamoor.WalletPool, logger logrus.FieldLogger) error {
	const attempts = 6
	var err error
	for i := 0; i < attempts; i++ {
		if err = pool.PrepareWallets(); err == nil {
			return nil
		}
		if !isTransientFundingErr(err) {
			return err
		}
		delay := time.Duration(5*(i+1)) * time.Second
		logger.Warnf("wallet funding hit a transient mempool error (attempt %d/%d), retrying in %s: %v", i+1, attempts, delay, err)
		time.Sleep(delay)
	}
	return err
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

	tx, err := wallet.BuildDynamicFeeTx(txData)
	if err != nil {
		return nil, client, wallet, fmt.Errorf("failed to sign tx: %w", err)
	}

	if err := sendRawTxToFront(ctx, frontURL, tx); err != nil {
		if frontRejectAlreadyHeld(err.Error(), tx.Nonce()) {
			// Already admitted: keep the nonce consumed (skipping breaks
			// contiguity) and report success so the caller awaits the receipt.
			return tx, client, wallet, nil
		}
		// Genuine rejection: release the nonce to keep the chain contiguous.
		wallet.MarkSkippedNonce(tx.Nonce())
		return tx, client, wallet, fmt.Errorf("front submit: %w", err)
	}
	return tx, client, wallet, nil
}

// reFrontHeldNonce matches the front's nonce-contiguity rejection, e.g.
// "expected 81 (on-chain 79 + 2 held)". Matching the full tail (not just
// "expected N") avoids reading an unrelated rejection as a resubmit.
var reFrontHeldNonce = regexp.MustCompile(`expected (\d+) \(on-chain \d+ \+ \d+ held\)`)

// frontRejectAlreadyHeld reports whether a rejection is that contiguity error
// for a nonce already admitted (below the expected next) — a benign resubmit.
func frontRejectAlreadyHeld(errMsg string, txNonce uint64) bool {
	m := reFrontHeldNonce.FindStringSubmatch(errMsg)
	if m == nil {
		return false
	}
	expected, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return false
	}
	return txNonce < expected
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
	return nil
}

// waitInclusion polls the front (which forwards reads to the source chain) for
// tx's receipt until it appears or timeout. Status is not inspected.
func waitInclusion(ctx context.Context, frontURL string, hash common.Hash, timeout time.Duration) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["%s"]}`, hash.Hex())
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if resp, err := rpcPost(cctx, frontURL, body); err == nil {
			if r, ok := resp["result"]; ok && len(r) > 0 && string(r) != "null" {
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
	var out map[string]json.RawMessage
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode rpc response (%s): %w", string(data), err)
	}
	return out, nil
}

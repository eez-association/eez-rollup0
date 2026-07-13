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
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/spamoor/scenario"
	"github.com/ethpandaops/spamoor/spamoor"
	"github.com/ethpandaops/spamoor/txbuilder"
)

const perWalletFundingEth = 5

// walletLocker preserves per-wallet nonce order at the front.
type walletLocker struct{ mu sync.Map }

func (w *walletLocker) acquire(addr common.Address) func() {
	v, _ := w.mu.LoadOrStore(addr, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return func() { m.Unlock() }
}

const defaultInclusionTimeout = 5 * time.Minute

var httpClient = &http.Client{Timeout: 30 * time.Second}

// walletCount auto-sizes the child-wallet pool.
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

	// Avoid reserving a nonce before the wallet's funding confirms.
	if wallet.GetBalance().Sign() == 0 {
		return nil, nil, wallet, fmt.Errorf("wallet not yet funded, skipping this turn")
	}

	// Hold the lock until the front accepts or rejects this nonce.
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

	// Sign the current nonce and advance it only after admission.
	nonce := wallet.GetNonce()
	tx, err := wallet.ReplaceDynamicFeeTx(txData, nonce)
	if err != nil {
		return nil, client, wallet, fmt.Errorf("failed to sign tx: %w", err)
	}

	if err := sendRawTxToFront(ctx, frontURL, tx); err != nil {
		if expected, ok := frontExpectedNonce(err.Error()); ok && nonce < expected {
			// Reconcile local state after an ambiguous response or restart.
			for wallet.GetNonce() < expected {
				wallet.GetNextNonce()
			}
			return tx, client, wallet, fmt.Errorf("front already holds nonce %d; advanced local next nonce to %d: %w", nonce, expected, err)
		}
		return tx, client, wallet, fmt.Errorf("front submit: %w", err)
	}

	// Record the admitted transaction in Spamoor's local state.
	wallet.GetNextNonce()
	wallet.IncrementSubmittedTxCount()
	return tx, client, wallet, nil
}

// Match the front's nonce-contiguity rejection.
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

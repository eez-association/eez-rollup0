package xchain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/spamoor/scenario"
	"github.com/ethpandaops/spamoor/spamoor"
	"github.com/ethpandaops/spamoor/txbuilder"
	"github.com/ethpandaops/spamoor/utils"
)

// Well-formed cross-chain inclusion is observed at up to ~4 min under load.
const defaultInclusionTimeout = 5 * time.Minute

// A per-request timeout keeps a stuck front from pinning a scenario goroutine.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// walletCount derives a child-wallet count from the configured limits,
// mirroring the heuristic native spamoor scenarios use (see calltx/example1).
func walletCount(maxWallets, totalCount, throughput uint64) uint64 {
	if maxWallets > 0 {
		return maxWallets
	}
	if totalCount > 0 {
		count := totalCount / 50 // ~1 wallet per 50 txs
		if count < 10 {
			return 10
		}
		if count > 1000 {
			return 1000
		}
		return count
	}
	if throughput*10 < 1000 {
		return throughput * 10
	}
	return 1000
}

// buildChainPool builds a standalone client+tx+wallet pool bound to one chain,
// the way spamoor's cmd/spamoor/run.go wires its top-level pool.
//
// `rpc` must be the chain's NORMAL RPC, never a cross-chain front: spamoor funds
// child wallets with plain transfers over it, and a front holds every send
// (mining none), so wallet preparation would deadlock.
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

	pool := spamoor.NewWalletPool(ctx, logger.WithField("pool", poolName), rootWallet, clients, txpool)
	pool.SetWalletCount(wallets)
	pool.SetRefillAmount(utils.EtherToWei(uint256.NewInt(5)))
	pool.SetRefillBalance(utils.EtherToWei(uint256.NewInt(1)))
	pool.SetRefillInterval(600)
	if err := pool.PrepareWallets(); err != nil {
		return nil, fmt.Errorf("prepare wallets: %w", err)
	}

	return pool, nil
}

// callSpec holds the per-transaction fee/gas knobs for submitCall.
type callSpec struct {
	baseFee    float64
	tipFee     float64
	baseFeeWei string
	tipFeeWei  string
	gasLimit   uint64
	logName    string
}

// submitCall builds and signs a tx to target using pool's normal-RPC client
// (nonce, fees, funding), then POSTs the signed tx to the cross-chain front at
// frontURL. Receipt tracking is the caller's job via waitInclusion — a held tx
// gets its receipt on the source chain when it lands.
func submitCall(ctx context.Context, pool *spamoor.WalletPool, frontURL string, target common.Address, value *uint256.Int, calldata []byte, spec callSpec, logger *logrus.Entry, txIdx uint64) (*types.Transaction, *spamoor.Client, *spamoor.Wallet, error) {
	wallet := pool.GetWallet(spamoor.SelectWalletByIndex, int(txIdx))
	if wallet == nil {
		return nil, nil, nil, scenario.ErrNoWallet
	}

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
		// A rejected send doesn't consume the nonce; release it so the wallet's
		// chain stays contiguous (the front requires contiguous nonces).
		wallet.MarkSkippedNonce(tx.Nonce())
		return tx, client, wallet, fmt.Errorf("front submit: %w", err)
	}
	return tx, client, wallet, nil
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

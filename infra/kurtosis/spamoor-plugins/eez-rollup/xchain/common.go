package xchain

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/spamoor/scenario"
	"github.com/ethpandaops/spamoor/spamoor"
	"github.com/ethpandaops/spamoor/txbuilder"
	"github.com/ethpandaops/spamoor/utils"
)

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

// buildChainPool constructs a standalone client+tx+wallet pool bound to a
// single chain, the way spamoor's own cmd/spamoor/run.go wires its top-level
// pool. Used for the outbound side — see ScenarioOptions.OutboundRPC for why
// it can't share the native pool.
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
	baseFee     float64
	tipFee      float64
	baseFeeWei  string
	tipFeeWei   string
	gasLimit    uint64
	rebroadcast bool
	logName     string
}

// submitCall builds, signs and submits a single tx carrying arbitrary
// calldata to target via pool — direction-agnostic, the caller passes
// whichever pool matches the target chain. A reverting or rejected tx isn't
// special-cased here; the caller decides whether that's an error.
func submitCall(ctx context.Context, pool *spamoor.WalletPool, target common.Address, value *uint256.Int, calldata []byte, spec callSpec, logger *logrus.Entry, txIdx uint64) (scenario.ReceiptChan, *types.Transaction, *spamoor.Client, *spamoor.Wallet, error) {
	wallet := pool.GetWallet(spamoor.SelectWalletByIndex, int(txIdx))
	if wallet == nil {
		return nil, nil, nil, nil, scenario.ErrNoWallet
	}

	client := pool.GetClient(spamoor.WithClientSelectionMode(spamoor.SelectClientByIndex, int(txIdx)))
	if client == nil {
		return nil, nil, client, wallet, scenario.ErrNoClients
	}

	if err := wallet.ResetNoncesIfNeeded(ctx, client); err != nil {
		return nil, nil, client, wallet, err
	}

	baseFeeWei, tipFeeWei := spamoor.ResolveFees(spec.baseFee, spec.tipFee, spec.baseFeeWei, spec.tipFeeWei)
	feeCap, tipCap, err := pool.GetSuggestedFees(client, baseFeeWei, tipFeeWei)
	if err != nil {
		return nil, nil, client, wallet, fmt.Errorf("failed to get suggested fees: %w", err)
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
		return nil, nil, client, wallet, fmt.Errorf("failed to build tx: %w", err)
	}

	tx, err := wallet.BuildDynamicFeeTx(txData)
	if err != nil {
		return nil, nil, client, wallet, fmt.Errorf("failed to sign tx: %w", err)
	}

	receiptChan := make(scenario.ReceiptChan, 1)
	err = pool.GetTxPool().SendTransaction(ctx, wallet, tx, &spamoor.SendTransactionOptions{
		Client:      client,
		Rebroadcast: spec.rebroadcast,
		OnComplete: func(tx *types.Transaction, receipt *types.Receipt, err error) {
			receiptChan <- receipt
		},
		LogFn: spamoor.GetDefaultLogFn(logger, spec.logName, fmt.Sprintf("%6d", txIdx+1), tx),
	})
	if err != nil {
		wallet.MarkSkippedNonce(tx.Nonce())
		return nil, nil, client, wallet, fmt.Errorf("failed to send transaction: %w", err)
	}

	return receiptChan, tx, client, wallet, nil
}

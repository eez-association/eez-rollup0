package xchain

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/ethpandaops/spamoor/scenario"
	"github.com/ethpandaops/spamoor/spamoor"
)

// Attack modes. Empty means well-formed load; the others send malformed
// traffic for DDoS-resilience testing. Run adversarial load as a separate
// spammer (a second config entry with `attack` set) alongside a healthy one.
const (
	attackNone    = ""                 // well-formed setValue(uint256)
	attackGarbage = "garbage-calldata" // random bytes — front admission/decode path
	attackRevert  = "revert"           // valid selector the target lacks — cross-chain revert path
)

// ScenarioOptions configures the eez-xchain scenario. See infra/kurtosis's
// scripts/wave-test.sh for the reference op semantics this mirrors.
type ScenarioOptions struct {
	// Attack selects one of the modes above. ValueMax only applies when unset.
	Attack string `yaml:"attack"`

	// TotalCount, if non-zero, stops the scenario after exactly this many
	// cross-chain txs (split across inbound/outbound by weight) instead of
	// running forever. Throughput still governs the per-slot rate either way.
	TotalCount uint64 `yaml:"total_count"`
	Throughput uint64 `yaml:"throughput"`

	// Mode is shorthand for the weight pair below — "inbound", "outbound",
	// or "mixed" (1:1). Ignored if either weight is set explicitly.
	Mode           string `yaml:"mode"`
	InboundWeight  uint64 `yaml:"inbound_weight"`
	OutboundWeight uint64 `yaml:"outbound_weight"`

	MaxPending  uint64  `yaml:"max_pending"`
	MaxWallets  uint64  `yaml:"max_wallets"`
	Rebroadcast uint64  `yaml:"rebroadcast"`
	BaseFee     float64 `yaml:"base_fee"`
	TipFee      float64 `yaml:"tip_fee"`
	BaseFeeWei  string  `yaml:"base_fee_wei"`
	TipFeeWei   string  `yaml:"tip_fee_wei"`
	GasLimit    uint64  `yaml:"gas_limit"`
	Timeout     string  `yaml:"timeout"`

	// Outbound (L2->L1) side. The native WalletPool spamoor hands this
	// scenario via Init() is bound to whichever chain --rpchost was launched
	// against (we run with --rpchost at eez-node's l1-xchain front, so that
	// pool is the INBOUND side). The L2 rollup has a distinct chain id, which
	// spamoor's single-chain-id pool can't share — buildChainPool (common.go)
	// builds the outbound pool from these instead.
	OutboundRPC        string `yaml:"outbound_rpc"`
	OutboundPrivateKey string `yaml:"outbound_private_key"`

	// Pre-created cross-chain proxies. This scenario drives load against them —
	// it does not provision them. Create them the way wave-test.sh does
	// (create_l1_proxy / create_l2_proxy) before starting this scenario.
	InboundProxy  string `yaml:"inbound_proxy"`  // L1 CrossChainProxy -> L2 target
	OutboundProxy string `yaml:"outbound_proxy"` // L2 CrossChainProxy -> L1 target

	ValueMax uint64 `yaml:"value_max"`
	LogTxs   bool   `yaml:"log_txs"`
}

var ScenarioName = "eez-xchain"

var ScenarioDefaultOptions = ScenarioOptions{
	Attack:     attackNone,
	Throughput: 10,
	// Mode/weights left unset here on purpose: applyModeShorthand() only
	// fills them in when both are still zero, so it can tell "not
	// configured" apart from an explicit inbound_weight/outbound_weight.
	Rebroadcast: 1,
	BaseFee:     20,
	TipFee:      2,
	GasLimit:    600000,
	ValueMax:    1_000_000,
}

var ScenarioDescriptor = scenario.Descriptor{
	Name:           ScenarioName,
	Description:    "Cross-chain load (L1<->L2 setValue), optionally adversarial (malformed/reverting calldata) for DDoS-resilience testing of the EEZ fronts",
	DefaultOptions: ScenarioDefaultOptions,
	NewScenario:    newScenario,
}

type Scenario struct {
	options ScenarioOptions
	logger  *logrus.Entry

	inboundPool  *spamoor.WalletPool // native pool from spamoor, bound to --rpchost (the L1 front)
	outboundPool *spamoor.WalletPool // built by buildChainPool, bound to OutboundRPC (the L2 front)

	numWallets    uint64
	inboundProxy  common.Address
	outboundProxy common.Address

	setValueSelector []byte
	// revertSelector is a 4-byte selector for a function the target Value
	// contract does not implement, so an attack=revert call reverts on
	// execution (Value has no matching fallback).
	revertSelector []byte
}

func newScenario(logger logrus.FieldLogger) scenario.Scenario {
	return &Scenario{
		options: ScenarioDefaultOptions,
		logger:  logger.WithField("scenario", ScenarioName),
	}
}

func (s *Scenario) Flags(flags *pflag.FlagSet) error {
	flags.StringVar(&s.options.Attack, "attack", ScenarioDefaultOptions.Attack, "Adversarial mode: '' (well-formed), 'garbage-calldata', or 'revert' — run as a separate spammer for DDoS-resilience testing")
	flags.Uint64VarP(&s.options.TotalCount, "count", "c", ScenarioDefaultOptions.TotalCount, "Total number of cross-chain transactions to send, then stop (0 = unlimited, split inbound/outbound by weight)")
	flags.Uint64VarP(&s.options.Throughput, "throughput", "t", ScenarioDefaultOptions.Throughput, "Cross-chain transactions to send per slot (split inbound/outbound by weight)")
	flags.StringVar(&s.options.Mode, "mode", "mixed", "Shorthand for inbound-weight/outbound-weight: 'inbound', 'outbound', or 'mixed' (1:1). Ignored if either weight is set explicitly.")
	flags.Uint64Var(&s.options.InboundWeight, "inbound-weight", ScenarioDefaultOptions.InboundWeight, "Relative weight of inbound (L1->L2) ops (overrides --mode)")
	flags.Uint64Var(&s.options.OutboundWeight, "outbound-weight", ScenarioDefaultOptions.OutboundWeight, "Relative weight of outbound (L2->L1) ops (overrides --mode)")
	flags.Uint64Var(&s.options.MaxPending, "max-pending", ScenarioDefaultOptions.MaxPending, "Maximum number of pending transactions")
	flags.Uint64Var(&s.options.MaxWallets, "max-wallets", ScenarioDefaultOptions.MaxWallets, "Maximum number of child wallets to use per side")
	flags.Uint64Var(&s.options.Rebroadcast, "rebroadcast", ScenarioDefaultOptions.Rebroadcast, "Enable reliable rebroadcast system")
	flags.Float64Var(&s.options.BaseFee, "basefee", ScenarioDefaultOptions.BaseFee, "Max fee per gas (gwei)")
	flags.Float64Var(&s.options.TipFee, "tipfee", ScenarioDefaultOptions.TipFee, "Max tip per gas (gwei)")
	flags.StringVar(&s.options.BaseFeeWei, "basefee-wei", "", "Max fee per gas in wei (overrides --basefee for L2 sub-gwei fees)")
	flags.StringVar(&s.options.TipFeeWei, "tipfee-wei", "", "Max tip per gas in wei (overrides --tipfee for L2 sub-gwei fees)")
	flags.Uint64Var(&s.options.GasLimit, "gas-limit", ScenarioDefaultOptions.GasLimit, "Gas limit for cross-chain proxy calls")
	flags.StringVar(&s.options.Timeout, "timeout", ScenarioDefaultOptions.Timeout, "Timeout for the scenario (e.g. '1h') - empty means no timeout")
	flags.StringVar(&s.options.OutboundRPC, "outbound-rpc", "", "RPC/front URL for the outbound (L2->L1) side, e.g. eez-node's l2-xchain front")
	flags.StringVar(&s.options.OutboundPrivateKey, "outbound-private-key", "", "Funded private key for the outbound-side wallet pool (L2 chain)")
	flags.StringVar(&s.options.InboundProxy, "inbound-proxy", "", "Address of the pre-created L1 CrossChainProxy to target")
	flags.StringVar(&s.options.OutboundProxy, "outbound-proxy", "", "Address of the pre-created L2 CrossChainProxy to target")
	flags.Uint64Var(&s.options.ValueMax, "value-max", ScenarioDefaultOptions.ValueMax, "Upper bound for the random setValue() argument (well-formed load only)")
	flags.BoolVar(&s.options.LogTxs, "log-txs", ScenarioDefaultOptions.LogTxs, "Log every submitted transaction")
	return nil
}

func (s *Scenario) Init(options *scenario.Options) error {
	s.inboundPool = options.WalletPool

	if options.Config != "" {
		if err := scenario.ParseAndValidateConfig(&ScenarioDescriptor, options.Config, &s.options, s.logger); err != nil {
			return err
		}
	}

	switch s.options.Attack {
	case attackNone, attackGarbage, attackRevert:
	default:
		return fmt.Errorf("invalid attack %q (want '', %q or %q)", s.options.Attack, attackGarbage, attackRevert)
	}

	applyModeShorthand(&s.options)

	if s.options.InboundWeight == 0 && s.options.OutboundWeight == 0 {
		return fmt.Errorf("at least one of inbound_weight/outbound_weight must be non-zero (or set mode: inbound|outbound|mixed)")
	}
	if s.options.TotalCount == 0 && s.options.Throughput == 0 {
		return fmt.Errorf("neither total_count nor throughput is set, must define at least one (see --help for flags)")
	}

	// Each side's config/pool is only required if that side actually has
	// weight — inbound-only and outbound-only runs shouldn't need to supply
	// the other direction's RPC/key/proxy at all.
	if s.options.InboundWeight > 0 && s.options.InboundProxy == "" {
		return fmt.Errorf("inbound_proxy is required when inbound_weight > 0 — this scenario drives load against a pre-created cross-chain proxy, it does not provision one (see infra/kurtosis/scripts/wave-test.sh create_l1_proxy)")
	}
	if s.options.OutboundWeight > 0 {
		if s.options.OutboundRPC == "" {
			return fmt.Errorf("outbound_rpc is required when outbound_weight > 0 (the L2->L1 front, e.g. eez-node's l2-xchain endpoint)")
		}
		if s.options.OutboundPrivateKey == "" {
			return fmt.Errorf("outbound_private_key is required when outbound_weight > 0 (funded private key for the outbound/L2 wallet pool)")
		}
		if s.options.OutboundProxy == "" {
			return fmt.Errorf("outbound_proxy is required when outbound_weight > 0 — this scenario drives load against a pre-created cross-chain proxy, it does not provision one (see infra/kurtosis/scripts/wave-test.sh create_l2_proxy)")
		}
	}

	s.setValueSelector = crypto.Keccak256([]byte("setValue(uint256)"))[:4]
	s.revertSelector = crypto.Keccak256([]byte("eezFuzzNoSuchFunction()"))[:4]
	s.numWallets = walletCount(s.options.MaxWallets, s.options.TotalCount, s.options.Throughput)

	if s.options.InboundWeight > 0 {
		s.inboundProxy = common.HexToAddress(s.options.InboundProxy)
		// The native (inbound) pool is prepared by spamoor's runner after
		// Init returns; we only size it here.
		s.inboundPool.SetWalletCount(s.numWallets)
	}

	if s.options.OutboundWeight > 0 {
		s.outboundProxy = common.HexToAddress(s.options.OutboundProxy)
		pool, err := buildChainPool(s.logger, "outbound", s.options.OutboundRPC, s.options.OutboundPrivateKey, s.numWallets)
		if err != nil {
			return fmt.Errorf("failed to init outbound (L2) pool: %w", err)
		}
		s.outboundPool = pool
	}

	return nil
}

// applyModeShorthand lets "mode: inbound|outbound|mixed" set the weight pair
// as a convenience over specifying inbound_weight/outbound_weight directly.
// Explicit weights (if either is already non-zero) take precedence.
func applyModeShorthand(opts *ScenarioOptions) {
	if opts.InboundWeight != 0 || opts.OutboundWeight != 0 {
		return
	}
	switch opts.Mode {
	case "inbound":
		opts.InboundWeight, opts.OutboundWeight = 1, 0
	case "outbound":
		opts.InboundWeight, opts.OutboundWeight = 0, 1
	case "mixed", "":
		opts.InboundWeight, opts.OutboundWeight = 1, 1
	}
}

func (s *Scenario) Run(ctx context.Context) error {
	s.logger.Infof("starting scenario: %s (attack=%q, inbound proxy=%s, outbound proxy=%s)", ScenarioName, s.options.Attack, s.inboundProxy.Hex(), s.outboundProxy.Hex())
	defer s.logger.Infof("scenario %s finished", ScenarioName)

	// Cap pending against the configured wallet count (10 per wallet), using
	// the count both pools were sized to. Reading it off a specific pool would
	// be wrong for single-direction runs where the other pool is unsized.
	maxPending := s.options.MaxPending
	if maxPending == 0 {
		maxPending = s.options.Throughput * 10
		if maxPending == 0 {
			maxPending = 4000 // pure total_count run with no throughput cap
		}
		if walletCap := s.numWallets * 10; walletCap > 0 && maxPending > walletCap {
			maxPending = walletCap
		}
	}

	var timeout time.Duration
	if s.options.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(s.options.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout value: %v", err)
		}
	}

	totalWeight := s.options.InboundWeight + s.options.OutboundWeight

	// RunTransactionScenario uses WalletPool only for per-block throughput
	// stats — point it at an active pool (outbound if inbound is disabled) so
	// single-direction runs don't report an empty one. Each op still picks its
	// own pool in ProcessNextTxFn below.
	statsPool := s.inboundPool
	if s.options.InboundWeight == 0 {
		statsPool = s.outboundPool
	}

	return scenario.RunTransactionScenario(ctx, scenario.TransactionScenarioOptions{
		TotalCount: s.options.TotalCount,
		Throughput: s.options.Throughput,
		MaxPending: maxPending,
		Timeout:    timeout,
		WalletPool: statsPool,
		Logger:     s.logger,
		ProcessNextTxFn: func(ctx context.Context, params *scenario.ProcessNextTxParams) error {
			inbound := (params.TxIdx % totalWeight) < s.options.InboundWeight

			pool, target, side := s.outboundPool, s.outboundProxy, "outbound"
			if inbound {
				pool, target, side = s.inboundPool, s.inboundProxy, "inbound"
			}

			receiptChan, tx, client, wallet, err := submitCall(ctx, pool, target, uint256.NewInt(0), s.buildCalldata(), callSpec{
				baseFee:     s.options.BaseFee,
				tipFee:      s.options.TipFee,
				baseFeeWei:  s.options.BaseFeeWei,
				tipFeeWei:   s.options.TipFeeWei,
				gasLimit:    s.options.GasLimit,
				rebroadcast: s.options.Rebroadcast > 0,
				logName:     ScenarioName,
			}, s.logger, params.TxIdx)

			logger := s.logger.WithField("side", side)
			if s.options.Attack != attackNone {
				logger = logger.WithField("attack", s.options.Attack)
			}
			if client != nil {
				logger = logger.WithField("rpc", client.GetName())
			}
			if tx != nil {
				logger = logger.WithField("nonce", tx.Nonce())
			}
			if wallet != nil {
				logger = logger.WithField("wallet", wallet.GetAddress().Hex())
			}

			params.NotifySubmitted()
			params.OrderedLogCb(func() {
				switch {
				case err != nil && s.options.Attack != attackNone:
					// Rejections are an EXPECTED outcome when attacking, not a
					// failure — log at debug and let the loop carry on.
					logger.Debugf("attack tx #%d rejected at submit: %v", params.TxIdx+1, err)
				case err != nil:
					logger.Warnf("could not send %s tx #%d: %v", side, params.TxIdx+1, err)
				case s.options.LogTxs:
					logger.Infof("sent %s tx #%6d: %v", side, params.TxIdx+1, tx.Hash().String())
				default:
					logger.Debugf("sent %s tx #%6d: %v", side, params.TxIdx+1, tx.Hash().String())
				}
			})

			if err != nil {
				// Returning the error just increments the counter and keeps the
				// loop running (see RunTransactionScenario) — which is what we
				// want both for transient healthy failures and attack rejections.
				return err
			}

			// A reverted tx still yields a receipt; status isn't inspected —
			// for attacks reverting is the point, and healthy setValue against
			// a live proxy is expected to succeed anyway.
			_, err = receiptChan.Wait(ctx)
			return err
		},
	})
}

// buildCalldata returns the payload for the configured attack mode (or a
// well-formed setValue(uint256) call when not attacking). No abigen bindings
// are needed — this mirrors the raw-calldata pattern in wave-test.sh.
func (s *Scenario) buildCalldata() []byte {
	switch s.options.Attack {
	case attackGarbage:
		// Random-length (4..68) random bytes: no valid selector, exercises the
		// front's admission/decode path and the target's fallback.
		data := make([]byte, 4+rand.Intn(65))
		rand.Read(data)
		return data
	case attackRevert:
		// Valid 4-byte selector for a function the target lacks + 32 bytes of
		// junk args, so it decodes as a normal call but reverts on execution.
		return append(append([]byte{}, s.revertSelector...), make([]byte, 32)...)
	default: // attackNone — well-formed setValue
		value := uint64(1)
		if s.options.ValueMax > 0 {
			value = uint64(rand.Intn(int(s.options.ValueMax))) + 1
		}
		return append(append([]byte{}, s.setValueSelector...), common.LeftPadBytes(uint256.NewInt(value).ToBig().Bytes(), 32)...)
	}
}

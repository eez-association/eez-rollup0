package xchain

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/ethpandaops/spamoor/scenario"
	"github.com/ethpandaops/spamoor/spamoor"
	"github.com/ethpandaops/spamoor/utils"
)

// Attack modes send malformed traffic for DDoS-resilience testing.
const (
	attackNone    = ""                 // well-formed load
	attackGarbage = "garbage-calldata" // random bytes
	attackRevert  = "revert"           // valid selector the target lacks
)

// Cross-chain operation types supported by both directions.
const (
	opSet     = "set"
	opNoRet   = "noret"
	opValue   = "value"
	opWrapper = "wrapper"
)

// Default enclave-internal endpoints (see infra/kurtosis/main.star ports).
const (
	defaultInboundFront  = "http://eez-node:18999" // L1 xchain front
	defaultOutboundFront = "http://eez-node:18998" // L2 xchain front
	minWrapperGasLimit   = 800_000
)

const valueWei = 10_000_000_000_000 // 1e13 wei

// ScenarioOptions configures the eez-xchain scenario.
type ScenarioOptions struct {
	Attack string `yaml:"attack"` // when set, ops are ignored (malformed setter calls)

	TotalCount uint64 `yaml:"total_count"`
	Throughput uint64 `yaml:"throughput"`

	// Mode selects the source chain; Ops selects transaction types.
	Mode           string `yaml:"mode"`
	InboundWeight  uint64 `yaml:"inbound_weight"`
	OutboundWeight uint64 `yaml:"outbound_weight"`

	// Ops to cycle through per direction; empty = [set]. Ignored when attacking.
	Ops []string `yaml:"ops"`

	MaxPending uint64  `yaml:"max_pending"`
	MaxWallets uint64  `yaml:"max_wallets"`
	BaseFee    float64 `yaml:"base_fee"`
	TipFee     float64 `yaml:"tip_fee"`
	BaseFeeWei string  `yaml:"base_fee_wei"`
	TipFeeWei  string  `yaml:"tip_fee_wei"`
	GasLimit   uint64  `yaml:"gas_limit"`
	Timeout    string  `yaml:"timeout"`

	InboundFront  string `yaml:"inbound_front"`
	OutboundFront string `yaml:"outbound_front"`

	// Pre-created cross-chain resources per direction (from xchain-provision.sh,
	// not this scenario). Only the ones the configured ops use are required:
	// set→proxy, noret→noret_proxy, value→deposit/withdraw proxy, wrapper→wrapper.
	InboundProxy        string `yaml:"inbound_proxy"`
	InboundNoRetProxy   string `yaml:"inbound_noret_proxy"`
	InboundDepositProxy string `yaml:"inbound_deposit_proxy"`
	InboundWrapper      string `yaml:"inbound_wrapper"`

	OutboundProxy         string `yaml:"outbound_proxy"`
	OutboundNoRetProxy    string `yaml:"outbound_noret_proxy"`
	OutboundWithdrawProxy string `yaml:"outbound_withdraw_proxy"`
	OutboundWrapper       string `yaml:"outbound_wrapper"`

	ValueMax uint64 `yaml:"value_max"` // upper bound for the random setValue() arg
	LogTxs   bool   `yaml:"log_txs"`
}

var ScenarioName = "eez-xchain"

var ScenarioDefaultOptions = ScenarioOptions{
	Attack:        attackNone,
	Throughput:    10,
	BaseFee:       20,
	TipFee:        2,
	GasLimit:      600000,
	ValueMax:      1_000_000,
	InboundFront:  defaultInboundFront,
	OutboundFront: defaultOutboundFront,
}

var ScenarioDescriptor = scenario.Descriptor{
	Name:           ScenarioName,
	Description:    "Cross-chain load (L1<->L2 set/noret/value/wrapper ops), optionally adversarial (malformed/reverting calldata) for DDoS-resilience testing of the EEZ fronts",
	DefaultOptions: ScenarioDefaultOptions,
	NewScenario:    newScenario,
}

// opTargets holds one direction's resolved op addresses.
type opTargets struct {
	setter  common.Address
	noret   common.Address
	value   common.Address // deposit (inbound) / withdraw (outbound)
	wrapper common.Address
}

type Scenario struct {
	options ScenarioOptions
	logger  *logrus.Entry

	inboundPool  *spamoor.WalletPool // native pool of the L1-bound inbound daemon
	outboundPool *spamoor.WalletPool // native pool of the L2-bound outbound daemon

	numWallets    uint64
	inboundFront  string
	outboundFront string

	inTargets  opTargets
	outTargets opTargets
	ops        []string

	setValueSelector    []byte
	setViaProxySelector []byte
	revertSelector      []byte // selector the target lacks, so attack=revert reverts on execution

	submitLocks walletLocker // serializes front submission per wallet
}

func newScenario(logger logrus.FieldLogger) scenario.Scenario {
	return &Scenario{
		options: ScenarioDefaultOptions,
		logger:  logger.WithField("scenario", ScenarioName),
	}
}

func (s *Scenario) Flags(flags *pflag.FlagSet) error {
	flags.StringVar(&s.options.Attack, "attack", ScenarioDefaultOptions.Attack, "Adversarial mode: '' (well-formed), 'garbage-calldata', or 'revert' — run as a separate spammer for DDoS-resilience testing")
	flags.Uint64VarP(&s.options.TotalCount, "count", "c", ScenarioDefaultOptions.TotalCount, "Total number of cross-chain transactions to send, then stop (0 = unlimited)")
	flags.Uint64VarP(&s.options.Throughput, "throughput", "t", ScenarioDefaultOptions.Throughput, "Cross-chain transactions to send per slot")
	flags.StringVar(&s.options.Mode, "mode", ScenarioDefaultOptions.Mode, "Required source direction: 'inbound' on the L1 daemon or 'outbound' on the L2 daemon")
	flags.Uint64Var(&s.options.InboundWeight, "inbound-weight", ScenarioDefaultOptions.InboundWeight, "Legacy alias for inbound mode; cannot be combined with outbound mode or weight")
	flags.Uint64Var(&s.options.OutboundWeight, "outbound-weight", ScenarioDefaultOptions.OutboundWeight, "Legacy alias for outbound mode; cannot be combined with inbound mode or weight")
	flags.StringSliceVar(&s.options.Ops, "ops", nil, "Cross-chain op kinds to cycle through per direction: set,noret,value,wrapper (default set)")
	flags.Uint64Var(&s.options.MaxPending, "max-pending", ScenarioDefaultOptions.MaxPending, "Maximum number of pending transactions")
	flags.Uint64Var(&s.options.MaxWallets, "max-wallets", ScenarioDefaultOptions.MaxWallets, "Maximum number of child wallets to use per side")
	flags.Float64Var(&s.options.BaseFee, "basefee", ScenarioDefaultOptions.BaseFee, "Max fee per gas (gwei)")
	flags.Float64Var(&s.options.TipFee, "tipfee", ScenarioDefaultOptions.TipFee, "Max tip per gas (gwei)")
	flags.StringVar(&s.options.BaseFeeWei, "basefee-wei", "", "Max fee per gas in wei (overrides --basefee for L2 sub-gwei fees)")
	flags.StringVar(&s.options.TipFeeWei, "tipfee-wei", "", "Max tip per gas in wei (overrides --tipfee for L2 sub-gwei fees)")
	flags.Uint64Var(&s.options.GasLimit, "gas-limit", ScenarioDefaultOptions.GasLimit, "Gas limit for cross-chain proxy calls")
	flags.StringVar(&s.options.Timeout, "timeout", ScenarioDefaultOptions.Timeout, "Timeout for the scenario (e.g. '1h') - empty means no timeout")
	flags.StringVar(&s.options.InboundFront, "inbound-front", ScenarioDefaultOptions.InboundFront, "L1 cross-chain front URL where inbound txs are submitted (held)")
	flags.StringVar(&s.options.OutboundFront, "outbound-front", ScenarioDefaultOptions.OutboundFront, "L2 cross-chain front URL where outbound txs are submitted (held)")
	flags.StringVar(&s.options.InboundProxy, "inbound-proxy", "", "Pre-created L1 setter CrossChainProxy (op: set)")
	flags.StringVar(&s.options.InboundNoRetProxy, "inbound-noret-proxy", "", "Pre-created L1 ValueNoRet CrossChainProxy (op: noret)")
	flags.StringVar(&s.options.InboundDepositProxy, "inbound-deposit-proxy", "", "Pre-created L1 recipient CrossChainProxy for value transfers (op: value)")
	flags.StringVar(&s.options.InboundWrapper, "inbound-wrapper", "", "Pre-created L1 wrapper contract over the setter proxy (op: wrapper)")
	flags.StringVar(&s.options.OutboundProxy, "outbound-proxy", "", "Pre-created L2 setter CrossChainProxy (op: set)")
	flags.StringVar(&s.options.OutboundNoRetProxy, "outbound-noret-proxy", "", "Pre-created L2 ValueNoRet CrossChainProxy (op: noret)")
	flags.StringVar(&s.options.OutboundWithdrawProxy, "outbound-withdraw-proxy", "", "Pre-created L2 recipient CrossChainProxy for value transfers (op: value)")
	flags.StringVar(&s.options.OutboundWrapper, "outbound-wrapper", "", "Pre-created L2 wrapper contract over the setter proxy (op: wrapper)")
	flags.Uint64Var(&s.options.ValueMax, "value-max", ScenarioDefaultOptions.ValueMax, "Upper bound for the random setValue() argument (well-formed load only)")
	flags.BoolVar(&s.options.LogTxs, "log-txs", ScenarioDefaultOptions.LogTxs, "Log every submitted transaction")
	return nil
}

func (s *Scenario) Init(options *scenario.Options) error {
	if options == nil || options.WalletPool == nil {
		return fmt.Errorf("wallet pool is required")
	}
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

	if err := resolveDirection(&s.options); err != nil {
		return err
	}

	ops, err := resolveOps(s.options.Ops, s.options.Attack)
	if err != nil {
		return err
	}
	s.ops = ops

	// Backfill endpoints if a config explicitly blanked them.
	if s.options.InboundFront == "" {
		s.options.InboundFront = defaultInboundFront
	}
	if s.options.OutboundFront == "" {
		s.options.OutboundFront = defaultOutboundFront
	}

	if s.options.TotalCount == 0 && s.options.Throughput == 0 {
		return fmt.Errorf("neither total_count nor throughput is set, must define at least one (see --help for flags)")
	}
	if err := validateTransactionOptions(&s.options); err != nil {
		return err
	}

	s.setValueSelector = crypto.Keccak256([]byte("setValue(uint256)"))[:4]
	s.setViaProxySelector = crypto.Keccak256([]byte("setViaProxy(uint256)"))[:4]
	s.revertSelector = crypto.Keccak256([]byte("eezFuzzNoSuchFunction()"))[:4]
	s.numWallets = walletCount(s.options.MaxWallets, s.options.TotalCount, s.options.Throughput)
	s.inboundFront = s.options.InboundFront
	s.outboundFront = s.options.OutboundFront

	// Spamoor prepares and funds the daemon-native pool after Init.
	pool := options.WalletPool
	// Isolate child wallets when a daemon runs multiple spammers.
	pool.SetWalletSeed(fmt.Sprintf("%s-%d", ScenarioName, pool.GetSpammerID()))
	pool.SetWalletCount(s.numWallets)
	pool.SetRefillAmount(utils.EtherToWei(uint256.NewInt(perWalletFundingEth)))
	pool.SetRefillBalance(utils.EtherToWei(uint256.NewInt(1)))
	pool.SetRefillInterval(600)

	if s.options.InboundWeight > 0 {
		t, err := s.resolveTargets(map[string]opAddr{
			opSet:     {s.options.InboundProxy, "inbound_proxy"},
			opNoRet:   {s.options.InboundNoRetProxy, "inbound_noret_proxy"},
			opValue:   {s.options.InboundDepositProxy, "inbound_deposit_proxy"},
			opWrapper: {s.options.InboundWrapper, "inbound_wrapper"},
		})
		if err != nil {
			return err
		}
		s.inTargets = t
		s.inboundPool = pool
	}

	if s.options.OutboundWeight > 0 {
		t, err := s.resolveTargets(map[string]opAddr{
			opSet:     {s.options.OutboundProxy, "outbound_proxy"},
			opNoRet:   {s.options.OutboundNoRetProxy, "outbound_noret_proxy"},
			opValue:   {s.options.OutboundWithdrawProxy, "outbound_withdraw_proxy"},
			opWrapper: {s.options.OutboundWrapper, "outbound_wrapper"},
		})
		if err != nil {
			return err
		}
		s.outTargets = t
		s.outboundPool = pool
	}

	return nil
}

func validateTransactionOptions(opts *ScenarioOptions) error {
	if opts.GasLimit == 0 {
		return fmt.Errorf("gas_limit must be greater than zero")
	}
	if math.IsNaN(opts.BaseFee) || math.IsInf(opts.BaseFee, 0) || opts.BaseFee < 0 {
		return fmt.Errorf("base_fee must be a finite non-negative number")
	}
	if math.IsNaN(opts.TipFee) || math.IsInf(opts.TipFee, 0) || opts.TipFee < 0 {
		return fmt.Errorf("tip_fee must be a finite non-negative number")
	}
	for field, value := range map[string]string{
		"base_fee_wei": opts.BaseFeeWei,
		"tip_fee_wei":  opts.TipFeeWei,
	} {
		if value == "" {
			continue
		}
		parsed, ok := new(big.Int).SetString(value, 10)
		if !ok || parsed.Sign() < 0 {
			return fmt.Errorf("%s must be a non-negative base-10 integer", field)
		}
	}
	if opts.Timeout != "" {
		if _, err := time.ParseDuration(opts.Timeout); err != nil {
			return fmt.Errorf("invalid timeout value: %v", err)
		}
	}
	return nil
}

// resolveDirection maps a scenario to one source-chain daemon.
func resolveDirection(opts *ScenarioOptions) error {
	if opts.InboundWeight > 0 && opts.OutboundWeight > 0 {
		return fmt.Errorf("one spammer cannot use both source chains; run inbound and outbound spammers separately (each supports all ops)")
	}
	switch opts.Mode {
	case "inbound":
		if opts.OutboundWeight > 0 {
			return fmt.Errorf("mode inbound conflicts with outbound_weight")
		}
	case "outbound":
		if opts.InboundWeight > 0 {
			return fmt.Errorf("mode outbound conflicts with inbound_weight")
		}
	case "":
		switch {
		case opts.InboundWeight > 0:
			opts.Mode = "inbound"
		case opts.OutboundWeight > 0:
			opts.Mode = "outbound"
		default:
			return fmt.Errorf("set mode to inbound or outbound")
		}
	default:
		return fmt.Errorf("invalid mode %q (mode selects inbound or outbound source chain; ops selects transaction types)", opts.Mode)
	}
	if opts.Mode == "inbound" {
		opts.InboundWeight, opts.OutboundWeight = 1, 0
	} else {
		opts.InboundWeight, opts.OutboundWeight = 0, 1
	}
	return nil
}

// resolveOps validates and deduplicates the operation list.
func resolveOps(ops []string, attack string) ([]string, error) {
	if attack != attackNone {
		return []string{opSet}, nil
	}
	if len(ops) == 0 {
		return []string{opSet}, nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(ops))
	for _, raw := range ops {
		op := strings.TrimSpace(strings.ToLower(raw))
		switch op {
		case opSet, opNoRet, opValue, opWrapper:
		default:
			return nil, fmt.Errorf("invalid op %q (want any of %s, %s, %s, %s)", raw, opSet, opNoRet, opValue, opWrapper)
		}
		if !seen[op] {
			seen[op] = true
			out = append(out, op)
		}
	}
	return out, nil
}

// opAddr is one op's configured address and its config field (for errors).
type opAddr struct{ value, field string }

// resolveTargets parses the addresses the enabled ops need for one direction.
func (s *Scenario) resolveTargets(fields map[string]opAddr) (opTargets, error) {
	var t opTargets
	dst := map[string]*common.Address{opSet: &t.setter, opNoRet: &t.noret, opValue: &t.value, opWrapper: &t.wrapper}
	for _, op := range s.ops {
		f := fields[op]
		if err := setAddr(dst[op], f.value, f.field, op); err != nil {
			return t, err
		}
	}
	return t, nil
}

// setAddr parses a required hex address into dst, naming the missing field/op.
func setAddr(dst *common.Address, hexAddr, field, op string) error {
	if strings.TrimSpace(hexAddr) == "" {
		return fmt.Errorf("%s is required for op %q — this scenario drives load against pre-created cross-chain resources, it does not provision them (run infra/kurtosis/scripts/spammers.sh, or create them manually the way wave-test.sh does)", field, op)
	}
	if !common.IsHexAddress(hexAddr) {
		return fmt.Errorf("%s is not a valid address: %q", field, hexAddr)
	}
	address := common.HexToAddress(hexAddr)
	if address == (common.Address{}) {
		return fmt.Errorf("%s cannot be the zero address", field)
	}
	*dst = address
	return nil
}

func (s *Scenario) Run(ctx context.Context) error {
	maxPending := s.options.MaxPending
	if maxPending == 0 {
		maxPending = saturatingMul10(s.options.Throughput)
		if maxPending == 0 {
			maxPending = 4000 // pure total_count run with no throughput cap
		}
		if walletCap := saturatingMul10(s.numWallets); walletCap > 0 && maxPending > walletCap {
			maxPending = walletCap
		}
	}

	s.logger.Infof("starting scenario: %s (mode=%s attack=%q ops=%v throughput=%d total_count=%d max_pending=%d wallets=%d)", ScenarioName, s.options.Mode, s.options.Attack, s.ops, s.options.Throughput, s.options.TotalCount, maxPending, s.numWallets)
	defer s.logger.Infof("scenario %s finished", ScenarioName)

	var timeout time.Duration
	if s.options.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(s.options.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout value: %v", err)
		}
	}

	totalWeight := s.options.InboundWeight + s.options.OutboundWeight

	// Use the active pool for Spamoor's per-block statistics.
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

			pool, front, side := s.outboundPool, s.outboundFront, "outbound"
			if inbound {
				pool, front, side = s.inboundPool, s.inboundFront, "inbound"
			}

			op := s.ops[operationIndex(params.TxIdx, s.numWallets, uint64(len(s.ops)))]
			target, value, calldata := s.resolveTx(side, op)
			gasLimit := gasLimitForOp(s.options.GasLimit, op)

			tx, client, wallet, err := submitCall(ctx, &s.submitLocks, pool, front, target, value, calldata, callSpec{
				baseFee:    s.options.BaseFee,
				tipFee:     s.options.TipFee,
				baseFeeWei: s.options.BaseFeeWei,
				tipFeeWei:  s.options.TipFeeWei,
				gasLimit:   gasLimit,
			}, s.logger, params.TxIdx)

			logger := s.logger.WithField("side", side).WithField("op", op)
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
					// Rejection is the expected outcome when attacking.
					logger.Debugf("attack tx #%d rejected at submit: %v", params.TxIdx+1, err)
				case err != nil:
					logger.Warnf("could not send %s %s tx #%d: %v", side, op, params.TxIdx+1, err)
				case s.options.LogTxs:
					logger.Infof("sent %s %s tx #%6d: %v", side, op, params.TxIdx+1, tx.Hash().String())
				default:
					logger.Debugf("sent %s %s tx #%6d: %v", side, op, params.TxIdx+1, tx.Hash().String())
				}
			})

			if err != nil {
				// Just counts the failure and keeps the loop running.
				return err
			}

			if s.options.Attack != attackNone {
				// Attack txs never mine (poison-evicted); admission is the observable.
				return nil
			}

			// Well-formed: wait for the held tx to land (source-chain receipt).
			return waitInclusion(ctx, front, tx.Hash(), defaultInclusionTimeout)
		},
	})
}

func operationIndex(txIdx, wallets, operations uint64) uint64 {
	if wallets == 0 || operations == 0 {
		return 0
	}
	return (txIdx%wallets + txIdx/wallets) % operations
}

func gasLimitForOp(configured uint64, op string) uint64 {
	if op == opWrapper && configured < minWrapperGasLimit {
		return minWrapperGasLimit
	}
	return configured
}

// resolveTx maps a direction+op to (target, value, calldata). Attacks always
// hit the setter proxy with malformed calldata.
func (s *Scenario) resolveTx(side, op string) (common.Address, *uint256.Int, []byte) {
	t := s.inTargets
	if side == "outbound" {
		t = s.outTargets
	}

	if s.options.Attack != attackNone {
		return t.setter, uint256.NewInt(0), s.attackCalldata()
	}

	switch op {
	case opNoRet:
		return t.noret, uint256.NewInt(0), s.setValueCalldata()
	case opValue:
		return t.value, uint256.NewInt(valueWei), nil
	case opWrapper:
		return t.wrapper, uint256.NewInt(0), s.setViaProxyCalldata()
	default: // opSet
		return t.setter, uint256.NewInt(0), s.setValueCalldata()
	}
}

// randValue returns the random setter argument (1..value_max; 0 => fixed 1).
func (s *Scenario) randValue() uint64 {
	if s.options.ValueMax > 0 {
		return rand.Uint64()%s.options.ValueMax + 1
	}
	return 1
}

// setValueCalldata builds a well-formed setValue(uint256) call.
func (s *Scenario) setValueCalldata() []byte {
	return append(append([]byte{}, s.setValueSelector...), common.LeftPadBytes(uint256.NewInt(s.randValue()).ToBig().Bytes(), 32)...)
}

// setViaProxyCalldata builds a well-formed setViaProxy(uint256) call.
func (s *Scenario) setViaProxyCalldata() []byte {
	return append(append([]byte{}, s.setViaProxySelector...), common.LeftPadBytes(uint256.NewInt(s.randValue()).ToBig().Bytes(), 32)...)
}

// attackCalldata returns the malformed payload for the configured attack mode.
func (s *Scenario) attackCalldata() []byte {
	switch s.options.Attack {
	case attackGarbage:
		// Random 4..68 bytes: no valid selector, exercises the admission/decode path.
		data := make([]byte, 4+rand.Intn(65))
		rand.Read(data)
		return data
	case attackRevert:
		// Valid selector the target lacks + junk args: decodes but reverts.
		return append(append([]byte{}, s.revertSelector...), make([]byte, 32)...)
	default:
		return s.setValueCalldata()
	}
}

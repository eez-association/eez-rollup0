# Base-fee collection

Rollup0 follows the [fee-collector rule in Chapter 11 of the specification](https://github.com/eez-association/specs-draft/blob/2c6d346bea0b3736f7453c35174291495694d1fc/docs/rollup0-spec/11-gas-economics.md).
Set `EEZ_L2_FEE_RECIPIENT` before running `make deploy-protocol`. The deployment
script writes that address into the generated genesis as `config.feeCollector`
and into `deployments.env` for the composer's priority fees. If the variable is
unset, it uses the collector from the base genesis. The composer also defaults
its priority-fee recipient to the genesis collector when the variable is absent.
This extends PR #192's configuration of regular, Sync, empty, and recovery blocks.

For example, a deployment using the development recipient has this genesis field:

```json
{
  "config": {
    "chainId": 6290,
    "feeCollector": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
  }
}
```

`feeCollector` is an ordinary account. The specification leaves the production
Rollup0 address to the deployment. The checked-in development genesis files use
the first Anvil development account, `0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266`.
Replace it when preparing a deployment. All composers, followers, and proof
signers must use the same chain configuration. Standalone stateless proof signers
also accept this parameter in a bare chain-config document.

The specification permits both fees to go to the same account; it does not
require it. Priority fees follow the composer-selected block beneficiary.
Base fees follow the fixed genesis collector. A different composer can choose a
different beneficiary without changing where base fees go. After deployment,
changing `EEZ_L2_FEE_RECIPIENT` at composer startup changes only its priority fees;
it cannot override the genesis collector during import or proof replay.

At the end of each ordinary transaction, execution credits the collector with
`baseFeePerGas * gasUsed`, using the receipt's final gas usage after refunds.
Reverted and halted transactions still pay fees. Priority fees continue to go to
the block header's `beneficiary`. If the accounts are the same, it receives both.
The credit executes no collector code and changes no nonce; the next transaction
in the block observes the increased balance. Credits preserve the ordinary EVM
account lifecycle, including deletion by `SELFDESTRUCT`. Sender deductions, fee-cap and
balance checks, gas costs, `BASEFEE`, `GASPRICE`, and receipt effective gas prices
retain Ethereum's behavior. Native EEZ transactions and block-level system calls
credit no fees.

Rollup0 rejects blob transactions during block construction and replay. There is
no L2 blob-fee collection path. The shared raw EVM still supports Ethereum L1
simulation using the L1's chain configuration and ordinary fee accounting.

Omitting `feeCollector` retains the historical Ethereum burn behavior. This
supports existing chain configurations and Ethereum L1 simulations. An explicitly
configured value must be a valid 20-byte address; malformed values are rejected.
The parameter is read from chain configuration, never from Composer environment
variables or the suggested block beneficiary. Changing it on an existing chain
changes consensus state transitions and requires a hardfork; the current
implementation enables collection from genesis and defines no activation schedule.

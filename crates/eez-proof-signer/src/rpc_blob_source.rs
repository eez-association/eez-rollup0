//! Canonical execution-RPC source for blob-settlement authorization.

use std::{env, fs, sync::Arc};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_genesis::Genesis;
use alloy_primitives::{Address, B256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_sol_types::sol;
use async_trait::async_trait;
use eez_protocol::Rollup0Deployment;
use reth_chainspec::{ChainSpec, EthChainSpec as _};
use url::Url;

use crate::{
    BlobContextAuthority, BlobContextConfig, BlobContextError, L1PostingContext, Rollup0L1Source,
    SettlementState,
};

sol! {
    #[sol(rpc)]
    interface IEEZRoots {
        function rollups(uint64 rollupId)
            external
            view
            returns (address rollupContract, bytes32 root, uint256 etherBalance);
    }

    #[sol(rpc)]
    interface IRollup0Manager {
        function activeSettlementWrapper() external view returns (address);
    }
}

/// L1 source used by the proof signer, independent of Composer-supplied data.
#[derive(Debug)]
pub struct RpcRollup0L1Source {
    provider: RootProvider,
    eez: Address,
    rollup_id: u64,
}

impl BlobContextAuthority {
    /// `None` keeps the calldata profile. The L2 chain id comes from the
    /// backend's chain configuration; the rest from the node's `EEZ_*` names.
    ///
    /// # Errors
    ///
    /// A partially configured or inconsistent candidate-V1 profile.
    pub(crate) fn from_env(
        expected_rollup_id: u64,
        l2_chain_id: u64,
    ) -> eyre::Result<Option<Self>> {
        if !Rollup0Deployment::is_configured() {
            return Ok(None);
        }
        let required = |name: &'static str| {
            env::var(name).map_err(|_| eyre::eyre!("{name} is required for candidate-V1 signing"))
        };
        let number = |name: &'static str| -> eyre::Result<u64> {
            required(name)?
                .trim()
                .parse()
                .map_err(|error| eyre::eyre!("{name} is invalid: {error}"))
        };
        let l1_chain_id = number("EEZ_L1_CHAIN_ID")?;
        let deployment =
            Rollup0Deployment::from_env(l1_chain_id, l2_chain_id)?.ok_or_else(|| {
                eyre::eyre!("candidate-V1 deployment disappeared from the environment")
            })?;
        eyre::ensure!(
            deployment.eez_rollup_id == expected_rollup_id,
            "EEZ_ROLLUP_ID {} differs from the signer's rollup {expected_rollup_id}",
            deployment.eez_rollup_id,
        );
        // The same ratio RollupTiming derives K from: Sync heights are multiples of it.
        let (l1_block_ms, l2_block_ms) = (
            number("EEZ_L1_BLOCK_TIME_MS")?,
            number("EEZ_L2_BLOCK_TIME_MS")?,
        );
        eyre::ensure!(
            l2_block_ms > 0 && l1_block_ms.is_multiple_of(l2_block_ms),
            "EEZ_L1_BLOCK_TIME_MS {l1_block_ms} must be a multiple of EEZ_L2_BLOCK_TIME_MS {l2_block_ms}",
        );
        let slot_seconds = l1_block_ms / 1000;
        eyre::ensure!(
            slot_seconds > 0 && slot_seconds.saturating_mul(1000) == l1_block_ms,
            "EEZ_L1_BLOCK_TIME_MS {l1_block_ms} must be a positive whole number of seconds",
        );
        let chain_spec = load_l1_chain_spec(l1_chain_id)?;
        let rpc_url = Url::parse(&required("EEZ_L1_RPC_URL")?)?;
        let source = Arc::new(RpcRollup0L1Source::new(
            rpc_url,
            deployment.eez,
            expected_rollup_id,
        ));
        let authority = BlobContextConfig {
            deployment,
            chain_spec,
            slot_seconds,
            sync_interval: l1_block_ms / l2_block_ms,
        };
        Ok(Some(Self::new(source, authority)?))
    }
}

fn load_l1_chain_spec(expected_chain_id: u64) -> eyre::Result<Arc<ChainSpec>> {
    if expected_chain_id == 10200
        && env::var("EEZ_L1_CHAIN_CONFIG")
            .ok()
            .is_none_or(|path| path.trim().is_empty())
    {
        let spec = reth_gnosis::spec::gnosis_spec::GnosisChainSpec::from(
            reth_gnosis::spec::chains::CHIADO_GENESIS.clone(),
        );
        return Ok(Arc::new(spec.inner.clone()));
    }
    let path = env::var("EEZ_L1_CHAIN_CONFIG")
        .map_err(|_| eyre::eyre!("EEZ_L1_CHAIN_CONFIG is required for candidate V1"))?;
    let genesis: Genesis = serde_json::from_str(&fs::read_to_string(path.trim())?)
        .map_err(|error| eyre::eyre!("EEZ_L1_CHAIN_CONFIG: {error}"))?;
    let spec = ChainSpec::from_genesis(genesis);
    eyre::ensure!(
        spec.chain_id() == expected_chain_id,
        "EEZ_L1_CHAIN_CONFIG chain {} differs from EEZ_L1_CHAIN_ID {expected_chain_id}",
        spec.chain_id(),
    );
    Ok(Arc::new(spec))
}

impl RpcRollup0L1Source {
    #[must_use]
    pub fn new(rpc_url: Url, eez: Address, rollup_id: u64) -> Self {
        Self {
            provider: ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(rpc_url),
            eez,
            rollup_id,
        }
    }
}

#[async_trait]
impl Rollup0L1Source for RpcRollup0L1Source {
    async fn chain_id(&self) -> Result<u64, BlobContextError> {
        self.provider
            .get_chain_id()
            .await
            .map_err(|error| BlobContextError::Unavailable(format!("eth_chainId: {error}")))
    }

    async fn canonical_header(&self, number: u64) -> Result<Option<(B256, u64)>, BlobContextError> {
        self.provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map(|block| block.map(|block| (block.header.hash, block.header.timestamp)))
            .map_err(|error| {
                BlobContextError::Unavailable(format!("canonical L1 block {number}: {error}"))
            })
    }

    async fn settlement_state(
        &self,
        l1_parent: L1PostingContext,
    ) -> Result<SettlementState, BlobContextError> {
        // State after the parent is what the target block's wrapper prechecks.
        let block = BlockId::hash(l1_parent.parent_hash);
        let unavailable = |what: &str, error: alloy_contract::Error| {
            BlobContextError::Unavailable(format!(
                "{what} at L1 block {}: {error}",
                l1_parent.parent_number
            ))
        };
        let rollup = IEEZRoots::new(self.eez, &self.provider)
            .rollups(self.rollup_id)
            .block(block)
            .call()
            .await
            .map_err(|error| unavailable("EEZ rollup entry", error))?;
        let settlement_wrapper = IRollup0Manager::new(rollup.rollupContract, &self.provider)
            .activeSettlementWrapper()
            .block(block)
            .call()
            .await
            .map_err(|error| unavailable("active settlement wrapper", error))?;
        Ok(SettlementState {
            root: rollup.root,
            manager: rollup.rollupContract,
            settlement_wrapper,
        })
    }
}

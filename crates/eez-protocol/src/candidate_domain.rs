//! Rollup0 deployment values bound by every blob-settlement signature.

use std::{env, str::FromStr};

use alloy_primitives::{Address, B256, Bytes, U256, b256};
use alloy_sol_types::SolValue as _;

/// Domain separator for Rollup0 blob settlement.
pub const ROLLUP0_CANDIDATE_V1_TAG: B256 =
    b256!("ba3ba53e31274af0c3f0f70c908597cbb1bc9a6a33006333feea258597f384b7");

const ENV_EEZ: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_MANAGER: &str = "EEZ_ROLLUP_MANAGER_ADDRESS";
const ENV_WRAPPER: &str = "EEZ_SETTLEMENT_WRAPPER_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";

/// Invalid or incomplete blob-settlement deployment configuration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeploymentError {
    /// A required environment variable is absent.
    #[error("{0} is required when {ENV_WRAPPER} selects candidate V1")]
    Missing(&'static str),
    /// An environment variable does not contain a valid nonzero value.
    #[error("{name} is invalid: {reason}")]
    Invalid {
        /// Environment variable name.
        name: &'static str,
        /// Validation failure.
        reason: String,
    },
}

/// The deployment `Rollup0Manager.getCustomData` names. Every field comes from
/// trusted configuration; only the L1 block varies per candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rollup0Deployment {
    /// Chain where the settlement transaction executes.
    pub l1_chain_id: u64,
    /// EEZ registry contract.
    pub eez: Address,
    /// Rollup manager registered with EEZ.
    pub manager: Address,
    /// Active wrapper that validates blob settlements.
    pub settlement_wrapper: Address,
    /// Rollup identifier assigned by EEZ.
    pub eez_rollup_id: u64,
    /// Rollup0 execution chain identifier.
    pub l2_chain_id: u64,
}

impl Rollup0Deployment {
    /// Whether `EEZ_SETTLEMENT_WRAPPER_ADDRESS` selects candidate V1. A
    /// calldata deployment writes it empty.
    #[must_use]
    pub fn is_configured() -> bool {
        env::var(ENV_WRAPPER).is_ok_and(|wrapper| !wrapper.trim().is_empty())
    }

    /// Candidate V1 is configured by its settlement wrapper; without one the
    /// deployment keeps the calldata profile and this returns `None`.
    ///
    /// # Errors
    ///
    /// [`DeploymentError`] for a wrapper without the rest of the deployment,
    /// or any malformed or zero value.
    pub fn from_env(l1_chain_id: u64, l2_chain_id: u64) -> Result<Option<Self>, DeploymentError> {
        if !Self::is_configured() {
            return Ok(None);
        }
        let parsed = |name: &'static str| {
            let raw = env::var(name).map_err(|_| DeploymentError::Missing(name))?;
            Ok::<_, DeploymentError>((name, raw))
        };
        let address = |name: &'static str| {
            let (name, raw) = parsed(name)?;
            Address::from_str(raw.trim())
                .ok()
                .filter(|address| !address.is_zero())
                .ok_or_else(|| DeploymentError::Invalid {
                    name,
                    reason: format!("{raw:?} is not a nonzero address"),
                })
        };
        let (name, raw) = parsed(ENV_ROLLUP_ID)?;
        let eez_rollup_id = raw
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|id| *id != 0)
            .ok_or_else(|| DeploymentError::Invalid {
                name,
                reason: format!("{raw:?} is not a nonzero rollup id"),
            })?;
        Ok(Some(Self {
            l1_chain_id,
            eez: address(ENV_EEZ)?,
            manager: address(ENV_MANAGER)?,
            settlement_wrapper: address(ENV_WRAPPER)?,
            eez_rollup_id,
            l2_chain_id,
        }))
    }

    /// Exact manager custom data for an L1 block at `timestamp` with the given
    /// parent hash.
    #[must_use]
    pub fn custom_data(&self, timestamp: u64, parent_hash: B256) -> Bytes {
        (
            ROLLUP0_CANDIDATE_V1_TAG,
            U256::from(self.l1_chain_id),
            self.eez,
            self.manager,
            self.settlement_wrapper,
            U256::from(self.eez_rollup_id),
            U256::from(self.l2_chain_id),
            U256::from(timestamp),
            parent_hash,
        )
            .abi_encode()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex};

    use super::*;

    #[test]
    fn candidate_v1_custom_data_matches_solidity_known_answer() {
        let deployment = Rollup0Deployment {
            l1_chain_id: 1,
            eez: address!("1111111111111111111111111111111111111111"),
            manager: address!("2222222222222222222222222222222222222222"),
            settlement_wrapper: address!("3333333333333333333333333333333333333333"),
            eez_rollup_id: 7,
            l2_chain_id: 10,
        };
        // `cast abi-encode "f(bytes32,uint256,address,address,address,uint256,uint256,uint256,bytes32)"`.
        let expected = hex::decode(concat!(
            "ba3ba53e31274af0c3f0f70c908597cbb1bc9a6a33006333feea258597f384b7",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "0000000000000000000000001111111111111111111111111111111111111111",
            "0000000000000000000000002222222222222222222222222222222222222222",
            "0000000000000000000000003333333333333333333333333333333333333333",
            "0000000000000000000000000000000000000000000000000000000000000007",
            "000000000000000000000000000000000000000000000000000000000000000a",
            "0000000000000000000000000000000000000000000000000102030405060708",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ))
        .unwrap();
        let encoded = deployment.custom_data(0x0102_0304_0506_0708, B256::repeat_byte(0xaa));
        assert_eq!(encoded.as_ref(), expected);
    }

    #[test]
    fn tag_is_the_hash_of_its_name() {
        assert_eq!(
            alloy_primitives::keccak256("EEZ_ROLLUP0_CANDIDATE_V1"),
            ROLLUP0_CANDIDATE_V1_TAG,
        );
    }
}

//! Rollup0 blob-settlement domain.

use std::{env, str::FromStr};

use alloy_primitives::{Address, B256, Bytes, U256, b256};
use alloy_sol_types::SolValue as _;

const CANDIDATE_V1_TAG: B256 =
    b256!("ba3ba53e31274af0c3f0f70c908597cbb1bc9a6a33006333feea258597f384b7");

const ENV_PROFILE: &str = "EEZ_SETTLEMENT_PROFILE";
const ENV_L1_CHAIN_ID: &str = "EEZ_L1_CHAIN_ID";
const ENV_EEZ: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_MANAGER: &str = "EEZ_ROLLUP_MANAGER_ADDRESS";
const ENV_WRAPPER: &str = "EEZ_SETTLEMENT_WRAPPER_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";

/// L1 target proposed by the composer and independently authenticated by a
/// blob-enabled proof signer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1PostingContext {
    /// Chain id of the L1 the batch is posted to.
    pub chain_id: u64,
    /// Timestamp of the L1 block the batch targets.
    pub target_timestamp: u64,
    /// Number of the L1 parent the batch anchors to.
    pub parent_number: u64,
    /// Hash of the L1 parent the batch anchors to.
    pub parent_hash: B256,
}

/// Which settlement carrier this deployment posts and proves.
///
/// The deployed rollup manager accepts only one carrier, so this matches the
/// deployment: `blob` needs the candidate-V1 manager and its settlement
/// wrapper, `calldata` the standard manager. Unset is `calldata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettlementProfile {
    /// Type-2 `postAndVerifyBatch` with the batch stream in `callData`.
    Calldata,
    /// Type-3 post through the settlement wrapper, with an empty `callData`.
    Blob,
}

impl SettlementProfile {
    /// Reads [`ENV_PROFILE`] and checks it against the configured wrapper.
    ///
    /// # Errors
    ///
    /// [`DeploymentError::Invalid`] for a value other than `calldata` or
    /// `blob`, or for `calldata` with a wrapper address set.
    /// [`DeploymentError::Missing`] for `blob` without a wrapper address.
    fn from_env() -> Result<Self, DeploymentError> {
        let raw = match env::var(ENV_PROFILE) {
            Ok(raw) => Some(raw),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(DeploymentError::Invalid {
                    name: ENV_PROFILE,
                    reason: "contains non-UTF-8 bytes".into(),
                });
            }
        };
        let wrapper = env::var(ENV_WRAPPER).is_ok_and(|wrapper| !wrapper.trim().is_empty());
        Self::parse(raw.as_deref(), wrapper)
    }

    fn parse(raw: Option<&str>, wrapper: bool) -> Result<Self, DeploymentError> {
        let profile = match raw.map(str::trim) {
            None | Some("" | "calldata") => Self::Calldata,
            Some("blob") => Self::Blob,
            Some(other) => {
                return Err(DeploymentError::Invalid {
                    name: ENV_PROFILE,
                    reason: format!("{other:?} is not calldata or blob"),
                });
            }
        };
        match (profile, wrapper) {
            (Self::Blob, false) => Err(DeploymentError::Missing(ENV_WRAPPER)),
            // The candidate-V1 manager reverts every calldata post.
            (Self::Calldata, true) => Err(DeploymentError::Invalid {
                name: ENV_PROFILE,
                reason: format!(
                    "calldata cannot settle on a deployment with {ENV_WRAPPER}; set blob"
                ),
            }),
            _ => Ok(profile),
        }
    }
}

/// Invalid or incomplete blob-settlement deployment configuration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeploymentError {
    /// A required environment variable is absent.
    #[error("{0} is required when {ENV_PROFILE}=blob")]
    Missing(&'static str),
    /// An environment value is malformed or inconsistent with the deployment.
    #[error("{name} is invalid: {reason}")]
    Invalid {
        /// Environment variable name.
        name: &'static str,
        /// Validation failure.
        reason: String,
    },
}

/// Values encoded by `Rollup0Manager.getCustomData`.
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
    /// Reads the blob-settlement deployment when the profile is `blob`;
    /// `calldata` returns `Ok(None)`.
    ///
    /// # Errors
    ///
    /// [`DeploymentError`] for an invalid or mismatched profile, for `blob`
    /// without the rest of the deployment, or any malformed or zero value.
    pub fn from_env(l1_chain_id: u64, l2_chain_id: u64) -> Result<Option<Self>, DeploymentError> {
        Self::read_from_env(|| Ok(l1_chain_id), l2_chain_id)
    }

    /// Reads the blob-settlement deployment and its L1 chain id from the
    /// environment; `calldata` returns `Ok(None)` without requiring the id.
    ///
    /// # Errors
    ///
    /// As [`Self::from_env`], or when `EEZ_L1_CHAIN_ID` is missing or malformed.
    pub fn from_process_env(l2_chain_id: u64) -> Result<Option<Self>, DeploymentError> {
        Self::read_from_env(
            || {
                let raw = env::var(ENV_L1_CHAIN_ID)
                    .map_err(|_| DeploymentError::Missing(ENV_L1_CHAIN_ID))?;
                raw.trim()
                    .parse()
                    .map_err(|error| DeploymentError::Invalid {
                        name: ENV_L1_CHAIN_ID,
                        reason: format!("{raw:?} is not a chain id: {error}"),
                    })
            },
            l2_chain_id,
        )
    }

    fn read_from_env(
        l1_chain_id: impl FnOnce() -> Result<u64, DeploymentError>,
        l2_chain_id: u64,
    ) -> Result<Option<Self>, DeploymentError> {
        if SettlementProfile::from_env()? == SettlementProfile::Calldata {
            return Ok(None);
        }
        let l1_chain_id = l1_chain_id()?;
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

    /// Encodes the manager custom data for an L1 block.
    #[must_use]
    pub fn custom_data(&self, timestamp: u64, parent_hash: B256) -> Bytes {
        (
            CANDIDATE_V1_TAG,
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
            CANDIDATE_V1_TAG,
        );
    }

    #[test]
    fn settlement_profile_must_match_the_deployed_wrapper() {
        use SettlementProfile::{Blob, Calldata};
        for (raw, wrapper, expected) in [
            (None, false, Ok(Calldata)),
            (Some(""), false, Ok(Calldata)),
            (Some(" calldata "), false, Ok(Calldata)),
            (Some("blob"), true, Ok(Blob)),
        ] {
            assert_eq!(SettlementProfile::parse(raw, wrapper), expected);
        }
        assert_eq!(
            SettlementProfile::parse(Some("blob"), false),
            Err(DeploymentError::Missing(ENV_WRAPPER))
        );
        for (raw, wrapper) in [
            (None, true),
            (Some("calldata"), true),
            (Some("blobs"), false),
        ] {
            assert!(matches!(
                SettlementProfile::parse(raw, wrapper),
                Err(DeploymentError::Invalid {
                    name: ENV_PROFILE,
                    ..
                })
            ));
        }
    }
}

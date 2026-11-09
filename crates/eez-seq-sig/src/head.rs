use alloy_primitives::{Address, B256};
use alloy_sol_types::{Eip712Domain, SolStruct, eip712_domain, sol};

sol! {
    struct SignedHead {
        uint64 rollupId;
        uint64 number;
        bytes32 blockHash;
        bytes32 parentHash;
        uint64 epochMs;
        uint64 counter;
    }
}

/// EIP-712 domain binding a signature to one L2 chain, registry and rollup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadDomain {
    pub l2_chain_id: u64,
    pub registry: Address,
    pub rollup_id: u64,
}

impl HeadDomain {
    pub fn eip712(&self) -> Eip712Domain {
        eip712_domain! {
            name: "EEZ Sequencer",
            version: "0",
            chain_id: self.l2_chain_id,
            verifying_contract: self.registry,
        }
    }
}

/// Orders claims: a new `epoch_ms` (signer restart) beats any older counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    pub epoch_ms: u64,
    pub counter: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeadClaim {
    #[serde(with = "alloy_serde::quantity")]
    pub rollup_id: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub number: u64,
    pub block_hash: B256,
    pub parent_hash: B256,
    #[serde(with = "alloy_serde::quantity")]
    pub epoch_ms: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub counter: u64,
}

impl HeadClaim {
    pub fn version(&self) -> Version {
        Version {
            epoch_ms: self.epoch_ms,
            counter: self.counter,
        }
    }

    pub fn signing_hash(&self, domain: &HeadDomain) -> B256 {
        SignedHead {
            rollupId: self.rollup_id,
            number: self.number,
            blockHash: self.block_hash,
            parentHash: self.parent_hash,
            epochMs: self.epoch_ms,
            counter: self.counter,
        }
        .eip712_signing_hash(&domain.eip712())
    }
}

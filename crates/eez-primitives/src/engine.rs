//! Engine API adapters for EEZ blocks. Payload formats and fork rules are Ethereum's;
//! transaction bytes inside those payloads are decoded using the EEZ envelope.
use crate::{Block, EezPrimitives};
use alloy_eips::eip7685::Requests;
use alloy_primitives::{Bytes, U256};
use alloy_rpc_types_engine::{
    BlobsBundleV1, BlobsBundleV2, CancunPayloadFields, ExecutionData, ExecutionPayload,
    ExecutionPayloadEnvelopeV2, ExecutionPayloadEnvelopeV3, ExecutionPayloadEnvelopeV4,
    ExecutionPayloadEnvelopeV5, ExecutionPayloadEnvelopeV6, ExecutionPayloadFieldV2,
    ExecutionPayloadSidecar, ExecutionPayloadV1, ExecutionPayloadV3, ExecutionPayloadV4,
    PayloadAttributes, PraguePayloadFields,
};
use reth_engine_primitives::EngineTypes;
use reth_ethereum_engine_primitives::{BlobSidecars, BuiltPayloadConversionError, EthBuiltPayload};
use reth_payload_primitives::{BuiltPayload, PayloadTypes};
use reth_primitives_traits::SealedBlock;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EezBuiltPayload(EthBuiltPayload<EezPrimitives>);
impl EezBuiltPayload {
    pub fn new(
        block: Arc<SealedBlock<Block>>,
        fees: U256,
        requests: Option<Requests>,
        bal: Option<Bytes>,
    ) -> Self {
        Self(EthBuiltPayload::new(block, fees, requests, bal))
    }
    #[must_use]
    pub fn with_sidecars(mut self, sidecars: impl Into<BlobSidecars>) -> Self {
        self.0 = self.0.with_sidecars(sidecars);
        self
    }
    pub fn block(&self) -> &SealedBlock<Block> {
        self.0.block()
    }
    pub fn block_arc(&self) -> &Arc<SealedBlock<Block>> {
        self.0.block_arc()
    }
    pub fn fees(&self) -> U256 {
        self.0.fees()
    }
    fn blobs_v2(&self) -> Result<BlobsBundleV2, BuiltPayloadConversionError> {
        match self.0.sidecars().clone() {
            BlobSidecars::Empty => Ok(BlobsBundleV2::empty()),
            BlobSidecars::Eip7594(sidecars) => Ok(sidecars.into()),
            BlobSidecars::Eip4844(_) => Err(BuiltPayloadConversionError::UnexpectedEip4844Sidecars),
        }
    }
}
impl BuiltPayload for EezBuiltPayload {
    type Primitives = EezPrimitives;
    fn block(&self) -> &SealedBlock<Block> {
        self.0.block()
    }
    fn fees(&self) -> U256 {
        self.0.fees()
    }
    fn block_access_list(&self) -> Option<&Bytes> {
        self.0.block_access_list()
    }
    fn requests(&self) -> Option<Requests> {
        self.0.requests()
    }
}
impl From<EezBuiltPayload> for ExecutionPayloadV1 {
    fn from(value: EezBuiltPayload) -> Self {
        Self::from_block_unchecked(value.block().hash(), &value.block().clone_block())
    }
}
impl From<EezBuiltPayload> for ExecutionPayloadEnvelopeV2 {
    fn from(value: EezBuiltPayload) -> Self {
        Self {
            block_value: value.fees(),
            execution_payload: ExecutionPayloadFieldV2::from_block_unchecked(
                value.block().hash(),
                &value.block().clone_block(),
            ),
        }
    }
}
impl TryFrom<EezBuiltPayload> for ExecutionPayloadEnvelopeV3 {
    type Error = BuiltPayloadConversionError;
    fn try_from(value: EezBuiltPayload) -> Result<Self, Self::Error> {
        let blobs_bundle = match value.0.sidecars().clone() {
            BlobSidecars::Empty => BlobsBundleV1::empty(),
            BlobSidecars::Eip4844(sidecars) => sidecars.into(),
            BlobSidecars::Eip7594(_) => {
                return Err(BuiltPayloadConversionError::UnexpectedEip7594Sidecars);
            }
        };
        Ok(Self {
            execution_payload: ExecutionPayloadV3::from_block_unchecked(
                value.block().hash(),
                &value.block().clone_block(),
            ),
            block_value: value.fees(),
            should_override_builder: false,
            blobs_bundle,
        })
    }
}
impl TryFrom<EezBuiltPayload> for ExecutionPayloadEnvelopeV4 {
    type Error = BuiltPayloadConversionError;
    fn try_from(value: EezBuiltPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            execution_requests: value.requests().unwrap_or_default(),
            envelope_inner: value.try_into()?,
        })
    }
}
impl TryFrom<EezBuiltPayload> for ExecutionPayloadEnvelopeV5 {
    type Error = BuiltPayloadConversionError;
    fn try_from(value: EezBuiltPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            execution_payload: ExecutionPayloadV3::from_block_unchecked(
                value.block().hash(),
                &value.block().clone_block(),
            ),
            block_value: value.fees(),
            should_override_builder: false,
            blobs_bundle: value.blobs_v2()?,
            execution_requests: value.requests().unwrap_or_default(),
        })
    }
}
impl TryFrom<EezBuiltPayload> for ExecutionPayloadEnvelopeV6 {
    type Error = BuiltPayloadConversionError;
    fn try_from(value: EezBuiltPayload) -> Result<Self, Self::Error> {
        let bal = value
            .block_access_list()
            .cloned()
            .ok_or(BuiltPayloadConversionError::MissingBlockAccessList)?;
        Ok(Self {
            execution_payload: ExecutionPayloadV4::from_block_unchecked_with_bal(
                value.block().hash(),
                &value.block().clone_block(),
                bal,
            ),
            block_value: value.fees(),
            should_override_builder: false,
            blobs_bundle: value.blobs_v2()?,
            execution_requests: value.requests().unwrap_or_default(),
        })
    }
}
impl From<EezBuiltPayload> for ExecutionData {
    fn from(value: EezBuiltPayload) -> Self {
        let block = value.block();
        let (payload, sidecar) = ExecutionPayload::from_block_unchecked_with_extras(
            block.hash(),
            &block.clone_block(),
            value.block_access_list().cloned(),
        );
        let sidecar = if let (Some(requests), Some(parent_beacon_block_root)) =
            (value.requests(), block.header().parent_beacon_block_root)
        {
            ExecutionPayloadSidecar::v4(
                CancunPayloadFields {
                    parent_beacon_block_root,
                    versioned_hashes: block.body().blob_versioned_hashes_iter().copied().collect(),
                },
                PraguePayloadFields::new(requests),
            )
        } else {
            sidecar
        };
        Self { payload, sidecar }
    }
}
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct EezEngineTypes;
impl PayloadTypes for EezEngineTypes {
    type BuiltPayload = EezBuiltPayload;
    type PayloadAttributes = PayloadAttributes;
    type ExecutionData = ExecutionData;
    fn block_to_payload(block: SealedBlock<Block>, bal: Option<Bytes>) -> ExecutionData {
        let (payload, sidecar) = ExecutionPayload::from_block_unchecked_with_extras(
            block.hash(),
            &block.clone_block(),
            bal,
        );
        ExecutionData { payload, sidecar }
    }
}
impl EngineTypes for EezEngineTypes {
    type ExecutionPayloadEnvelopeV1 = ExecutionPayloadV1;
    type ExecutionPayloadEnvelopeV2 = ExecutionPayloadEnvelopeV2;
    type ExecutionPayloadEnvelopeV3 = ExecutionPayloadEnvelopeV3;
    type ExecutionPayloadEnvelopeV4 = ExecutionPayloadEnvelopeV4;
    type ExecutionPayloadEnvelopeV5 = ExecutionPayloadEnvelopeV5;
    type ExecutionPayloadEnvelopeV6 = ExecutionPayloadEnvelopeV6;
}

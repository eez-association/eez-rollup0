//! Signed canonical-head publication: the signer task and the RPC that serves it.

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use alloy_consensus::Header;
use alloy_primitives::{B256, Bytes};
use eez_seq_sig::{Envelope, HeadSigner};
use jsonrpsee::{
    RpcModule,
    types::{ErrorObject, ErrorObjectOwned},
};
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::RpcContext;
use reth_primitives_traits::SealedHeader;
use reth_rpc_eth_api::FullEthApiServer;
use reth_storage_api::BlockReader;
use tokio::sync::watch;
use tracing::{Level, event};

pub(crate) const GET_SIGNED_HEAD: &str = "eez_getSignedHead";
pub(crate) const GET_RAW_BLOCK: &str = "eez_getRawBlock";

/// A head becomes visible on the watch before its block is always readable.
const BLOCK_READ_RETRY: Duration = Duration::from_millis(50);
const BLOCK_READ_DEADLINE: Duration = Duration::from_secs(1);

/// Latest signed head, shared between the signer task and the RPC.
#[derive(Clone, Default)]
pub(crate) struct SignedHeadStore(Arc<RwLock<Option<Envelope>>>);

impl SignedHeadStore {
    pub(crate) fn set(&self, envelope: Envelope) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(envelope);
    }

    pub(crate) fn get(&self) -> Option<Envelope> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Install the head methods on every configured L2 RPC transport.
pub(crate) fn install_head_rpc<Node, EthApi>(
    ctx: &mut RpcContext<'_, Node, EthApi>,
    store: SignedHeadStore,
) -> eyre::Result<()>
where
    Node: FullNodeComponents,
    Node::Provider: BlockReader<Block = eez_primitives::Block> + Clone,
    EthApi: FullEthApiServer + Clone + Send + Sync + 'static,
{
    let module = head_rpc_module(store, ctx.provider().clone())?;
    ctx.modules.merge_configured(module)?;
    event!(
        name: "eez.node.head_rpc.installed",
        Level::INFO,
        "eez_getSignedHead and eez_getRawBlock RPC installed",
    );
    Ok(())
}

fn head_rpc_module<P>(
    store: SignedHeadStore,
    provider: P,
) -> eyre::Result<RpcModule<(SignedHeadStore, P)>>
where
    P: BlockReader<Block = eez_primitives::Block> + Clone + Send + Sync + 'static,
{
    let mut module = RpcModule::new((store, provider));
    module.register_method(GET_SIGNED_HEAD, |params, (store, _), _ext| {
        // Reject arguments instead of silently ignoring a client mistake.
        if let Some(raw) = params.as_str() {
            let value: serde_json::Value = params.parse()?;
            let empty = matches!(&value, serde_json::Value::Array(v) if v.is_empty())
                || matches!(&value, serde_json::Value::Object(v) if v.is_empty());
            if !empty {
                return Err(ErrorObject::owned(
                    -32602,
                    "eez_getSignedHead takes no parameters",
                    Some(raw),
                ));
            }
        }
        Ok::<_, ErrorObjectOwned>(store.get())
    })?;
    module.register_method(GET_RAW_BLOCK, |params, (_, provider), _ext| {
        let (hash,): (B256,) = params.parse()?;
        let block = provider.block_by_hash(hash).map_err(|e| {
            ErrorObject::owned(-32603, format!("block read failed: {e}"), None::<()>)
        })?;
        Ok::<_, ErrorObjectOwned>(block.map(|b| Bytes::from(alloy_rlp::encode(&b))))
    })?;
    Ok(module)
}

/// Sign every change of the canonical head, starting with the head current at
/// launch. Returns when the committer is gone; never panics.
pub(crate) async fn run_head_signer<P>(
    mut heads: watch::Receiver<SealedHeader<Header>>,
    blocks: P,
    mut signer: HeadSigner,
    store: SignedHeadStore,
) where
    P: BlockReader<Block = eez_primitives::Block>,
{
    let mut last_signed: Option<B256> = None;
    loop {
        // The boot head is already marked seen, so it is signed here rather than on a change.
        let header = heads.borrow_and_update().clone();
        if last_signed != Some(header.hash())
            && let Some(envelope) = sign_head(&blocks, &mut signer, &header).await
        {
            last_signed = Some(header.hash());
            store.set(envelope);
        }
        if heads.changed().await.is_err() {
            return;
        }
    }
}

async fn sign_head<P>(
    blocks: &P,
    signer: &mut HeadSigner,
    header: &SealedHeader<Header>,
) -> Option<Envelope>
where
    P: BlockReader<Block = eez_primitives::Block>,
{
    let deadline = tokio::time::Instant::now() + BLOCK_READ_DEADLINE;
    let block = loop {
        match blocks.block_by_hash(header.hash()) {
            Ok(Some(block)) => break block,
            Ok(None) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(BLOCK_READ_RETRY).await;
            }
            Ok(None) => {
                event!(
                    name: "eez.node.head_signer.block_unreadable",
                    Level::WARN,
                    number = header.number,
                    hash = %header.hash(),
                    "head block not readable; not signing it",
                );
                return None;
            }
            Err(err) => {
                event!(
                    name: "eez.node.head_signer.read_failed",
                    Level::ERROR,
                    number = header.number,
                    hash = %header.hash(),
                    %err,
                    "reading head block failed; not signing it",
                );
                return None;
            }
        }
    };
    match signer.sign(header, Bytes::from(alloy_rlp::encode(&block))) {
        Ok(envelope) => Some(envelope),
        Err(err) => {
            event!(
                name: "eez.node.head_signer.sign_failed",
                Level::ERROR,
                number = header.number,
                %err,
                "signing head failed",
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer_local::PrivateKeySigner;
    use eez_primitives::{BlockBody, EezPrimitives};
    use eez_seq_sig::{HeadClaim, HeadDomain};
    use reth_provider::test_utils::MockEthProvider;

    type Mock = MockEthProvider<EezPrimitives>;

    fn block(number: u64) -> eez_primitives::Block {
        let header = Header {
            number,
            ..Default::default()
        };
        eez_primitives::Block {
            header,
            body: BlockBody::default(),
        }
    }

    fn sealed(block: &eez_primitives::Block) -> SealedHeader<Header> {
        SealedHeader::seal_slow(block.header.clone())
    }

    fn envelope(hash: B256) -> Envelope {
        Envelope {
            head: HeadClaim {
                rollup_id: 1,
                number: 1,
                block_hash: hash,
                parent_hash: B256::ZERO,
                epoch_ms: 1,
                counter: 1,
            },
            signature: Bytes::from_static(&[1; 65]),
            block: None,
        }
    }

    async fn call(module: &RpcModule<(SignedHeadStore, Mock)>, req: &str) -> serde_json::Value {
        let (response, _) = module.raw_json_request(req, 1).await.unwrap();
        serde_json::from_str(response.get()).unwrap()
    }

    #[tokio::test]
    async fn get_signed_head_is_null_until_set_then_returns_envelope() {
        let store = SignedHeadStore::default();
        let module = head_rpc_module(store.clone(), Mock::new()).unwrap();
        let req = r#"{"jsonrpc":"2.0","method":"eez_getSignedHead","params":[],"id":1}"#;
        assert_eq!(call(&module, req).await["result"], serde_json::Value::Null);

        let hash = B256::repeat_byte(7);
        store.set(envelope(hash));
        let response = call(&module, req).await;
        assert_eq!(
            response["result"]["head"]["blockHash"],
            serde_json::json!(format!("{hash:#x}"))
        );
    }

    #[tokio::test]
    async fn get_signed_head_rejects_params() {
        let module = head_rpc_module(SignedHeadStore::default(), Mock::new()).unwrap();
        let req = r#"{"jsonrpc":"2.0","method":"eez_getSignedHead","params":[1],"id":1}"#;
        assert_eq!(call(&module, req).await["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn get_raw_block_returns_rlp_or_null() {
        let provider = Mock::new();
        let b = block(3);
        let hash = b.header.hash_slow();
        provider.add_block(hash, b.clone());
        let module = head_rpc_module(SignedHeadStore::default(), provider).unwrap();

        let req = format!(
            r#"{{"jsonrpc":"2.0","method":"eez_getRawBlock","params":["{hash:#x}"],"id":1}}"#
        );
        let expected = Bytes::from(alloy_rlp::encode(&b));
        assert_eq!(
            call(&module, &req).await["result"],
            serde_json::json!(expected.to_string())
        );

        let missing = format!(
            r#"{{"jsonrpc":"2.0","method":"eez_getRawBlock","params":["{:#x}"],"id":1}}"#,
            B256::repeat_byte(9)
        );
        assert_eq!(
            call(&module, &missing).await["result"],
            serde_json::Value::Null
        );
    }

    #[tokio::test(start_paused = true)]
    async fn signer_skips_unreadable_head() {
        let provider = Mock::new();
        let a = block(1);
        let b = block(2);
        let (ha, hb) = (sealed(&a), sealed(&b));
        let (tx, rx) = watch::channel(ha.clone());
        let store = SignedHeadStore::default();
        let domain = HeadDomain {
            l2_chain_id: 1,
            registry: Default::default(),
            rollup_id: 1,
        };
        let signer = HeadSigner::new(PrivateKeySigner::random(), domain, 1);
        let task = tokio::spawn(run_head_signer(rx, provider.clone(), signer, store.clone()));

        // A's block is absent: the startup pass exhausts its retry window.
        // Step in retry-sized slices so the task runs between timer wake-ups.
        for _ in 0..30 {
            tokio::time::advance(BLOCK_READ_RETRY).await;
        }
        assert!(store.get().is_none());

        provider.add_block(hb.hash(), b);
        tx.send(hb.clone()).unwrap();
        for _ in 0..4 {
            tokio::time::advance(BLOCK_READ_RETRY).await;
        }
        let got = store.get().expect("B signed");
        assert_eq!(got.head.block_hash, hb.hash());
        assert_eq!(got.head.counter, 1);
        assert!(!task.is_finished());
    }
}

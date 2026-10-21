//! Signed canonical-head publication: the signer task and the RPC that serves it.

use std::time::Duration;

use crate::composer_rpc::reject_params;
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

/// A head can reach the watch before its block is readable.
const BLOCK_READ_RETRY: Duration = Duration::from_millis(50);
const BLOCK_READ_DEADLINE: Duration = Duration::from_secs(1);

/// Latest signed head, shared between the signer task and the RPC.
pub(crate) type SignedHeadStore = watch::Sender<Option<Envelope>>;

/// Install the head methods on every configured L2 RPC transport.
pub(crate) fn install_head_rpc<Node, EthApi>(
    ctx: &mut RpcContext<'_, Node, EthApi>,
    store: SignedHeadStore,
) -> eyre::Result<()>
where
    Node: FullNodeComponents,
    Node::Provider: BlockReader<Block = eez_primitives::Block>,
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
    P: BlockReader<Block = eez_primitives::Block> + Send + Sync + 'static,
{
    let mut module = RpcModule::new((store, provider));
    module.register_method(GET_SIGNED_HEAD, |params, (store, _), _ext| {
        reject_params(&params, GET_SIGNED_HEAD)?;
        Ok::<_, ErrorObjectOwned>(store.borrow().clone())
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
            && let Some(envelope) = sign_head(&blocks, &heads, &mut signer, &header).await
        {
            last_signed = Some(header.hash());
            store.send_replace(Some(envelope));
        }
        if heads.changed().await.is_err() {
            return;
        }
    }
}

async fn sign_head<P>(
    blocks: &P,
    heads: &watch::Receiver<SealedHeader<Header>>,
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
            // A replaced head is never readable (canonical-only lookup), so stop waiting.
            Ok(None) if heads.has_changed().unwrap_or(true) => {
                event!(
                    name: "eez.node.head_signer.superseded",
                    Level::INFO,
                    number = header.number,
                    hash = %header.hash(),
                    "head replaced before its block was readable; skipping it",
                );
                return None;
            }
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
        store.send_replace(Some(envelope(hash)));
        let response = call(&module, req).await;
        assert_eq!(
            response["result"]["head"]["blockHash"],
            serde_json::json!(format!("{hash:#x}"))
        );
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
        let (a, b, c) = (block(1), block(2), block(3));
        let (ha, hb, hc) = (sealed(&a), sealed(&b), sealed(&c));
        let (tx, rx) = watch::channel(ha.clone());
        let store = SignedHeadStore::default();
        let domain = HeadDomain {
            l2_chain_id: 1,
            registry: Default::default(),
            rollup_id: 1,
        };
        let signer = HeadSigner::new(PrivateKeySigner::random(), domain, 1);
        tokio::spawn(run_head_signer(rx, provider.clone(), signer, store.clone()));
        // Step in retry-sized slices so the task runs between timer wake-ups.
        let advance = |steps: u32| async move {
            for _ in 0..steps {
                tokio::time::advance(BLOCK_READ_RETRY).await;
            }
        };

        // The startup pass retries while A's block is absent, then signs it.
        advance(10).await;
        assert!(store.borrow().is_none());
        provider.add_block(ha.hash(), a);
        advance(2).await;
        let got = store.borrow().clone().expect("A signed");
        assert_eq!((got.head.block_hash, got.head.counter), (ha.hash(), 1));

        // B's block never appears: the retry window ends and A stays.
        tx.send(hb.clone()).unwrap();
        advance(30).await;
        assert_eq!(store.borrow().as_ref().unwrap().head.block_hash, ha.hash());

        // The skipped B consumed no counter value.
        provider.add_block(hc.hash(), c);
        tx.send(hc.clone()).unwrap();
        advance(2).await;
        let got = store.borrow().clone().unwrap();
        assert_eq!((got.head.block_hash, got.head.counter), (hc.hash(), 2));
    }
}

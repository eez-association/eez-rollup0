//! jsonrpsee middleware on the L2 node's RPC that rejects cross-chain
//! txs, pointing callers at the dedicated cross-chain ingress.
//!
//! This endpoint (`:18688`) is the L2 mempool: vanilla L2 txs pass
//! through to reth. A tx touching a registered cross-chain proxy is NOT
//! an L2 tx — it must execute on L1 via the composer's held pool — so it
//! is rejected here with a pointer to [`crate::crosschain_ingress`]
//! rather than silently mined on L2 against the proxy contract
//! (`invariant 7`: loud, not surprising).
//!
//! Classification is delegated to an [`IngressClassifier`]. An empty
//! classifier (no cross-chain proxies configured) ⇒ every tx passes
//! through; the middleware is a no-op on the hot path.

use std::sync::Arc;

use alloy_consensus::{Transaction as _, TxEnvelope};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::Bytes;
use eez_composer::{Classification, IngressClassifier};
use jsonrpsee::core::middleware::{Batch, Notification, RpcServiceT};
use jsonrpsee::core::server::MethodResponse;
use jsonrpsee::types::{ErrorObject, Request};
use tower::Layer;
use tracing::{Level, event};

const METHOD: &str = "eth_sendRawTransaction";

/// Reusable layer for the L2-endpoint reject-guard. Cheaply [`Clone`]able
/// — the classifier is an `Arc`.
#[derive(Clone)]
pub struct IngressLayer {
    classifier: Arc<IngressClassifier>,
}

impl IngressLayer {
    #[must_use]
    pub fn new(classifier: Arc<IngressClassifier>) -> Self {
        Self { classifier }
    }
}

impl<S> Layer<S> for IngressLayer {
    type Service = IngressService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        IngressService {
            inner,
            classifier: Arc::clone(&self.classifier),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    classifier: Arc<IngressClassifier>,
}

impl<S> RpcServiceT for IngressService<S>
where
    S: RpcServiceT<
            MethodResponse = MethodResponse,
            BatchResponse = MethodResponse,
            NotificationResponse = MethodResponse,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    type MethodResponse = S::MethodResponse;
    type NotificationResponse = S::NotificationResponse;
    type BatchResponse = S::BatchResponse;

    fn call<'a>(&self, req: Request<'a>) -> impl Future<Output = Self::MethodResponse> + Send + 'a {
        let inner = self.inner.clone();
        let classifier = Arc::clone(&self.classifier);
        async move {
            // Fast path: not our method, or no cross-chain proxies
            // configured. Either way → vanilla reth handling.
            if req.method.as_ref() != METHOD || classifier.is_empty() {
                return inner.call(req).await;
            }

            // Classify by the raw tx's `to` + `chain_id`. On any decode
            // hiccup, fall through to reth's standard handling/error.
            let Some(params) = req.params.as_deref() else {
                return inner.call(req).await;
            };
            let Ok((raw_tx,)) = serde_json::from_str::<(Bytes,)>(params.get()) else {
                return inner.call(req).await;
            };
            let Ok(envelope) = TxEnvelope::decode_2718(&mut raw_tx.as_ref()) else {
                return inner.call(req).await;
            };

            match classifier.classify(envelope.to().as_ref(), envelope.chain_id()) {
                Classification::L2Only => inner.call(req).await,
                Classification::CrossChain => {
                    event!(
                        name: "eez.ingress.cross_chain.rejected",
                        Level::WARN,
                        "cross-chain tx sent to the L2 mempool endpoint; use the cross-chain ingress",
                    );
                    MethodResponse::error(
                        req.id,
                        ErrorObject::owned(
                            -32000,
                            "cross-chain tx rejected: this is the L2 mempool endpoint; submit \
                             cross-chain txs to the cross-chain ingress \
                             (EEZ_CROSSCHAIN_INGRESS_PORT, default 18699)",
                            None::<()>,
                        ),
                    )
                }
            }
        }
    }

    fn batch<'a>(&self, req: Batch<'a>) -> impl Future<Output = Self::BatchResponse> + Send + 'a {
        // Batches pass through unintercepted — `eth_sendRawTransaction`
        // is rarely batched, and matching a method inside a batch needs
        // per-call dispatch. Deferred until a workload needs it.
        self.inner.batch(req)
    }

    fn notification<'a>(
        &self,
        n: Notification<'a>,
    ) -> impl Future<Output = Self::NotificationResponse> + Send + 'a {
        // Notifications are fire-and-forget; no response. Passthrough.
        self.inner.notification(n)
    }
}

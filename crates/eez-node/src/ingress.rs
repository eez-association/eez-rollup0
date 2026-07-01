//! jsonrpsee middleware that intercepts `eth_sendRawTransaction` and
//! routes cross-chain txs to a per-rollup
//! [`HeldPool`](eez_composer::HeldPool) — vanilla L2 txs pass through
//! to the standard reth pool path.
//!
//! The classification decision is delegated to an
//! [`IngressClassifier`](eez_composer::IngressClassifier). Empty
//! classifier (no cross-chain proxies configured) ⇒ every tx
//! passes through; the middleware is a no-op on the hot path.
//!
//! See `docs/plans/IMPLEMENTATION.md` §5.4.5.

use std::sync::{Arc, OnceLock};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use eez_composer::{Classification, Direction, HeldPool, HeldTx, IngressClassifier};
use eez_protocol::executor::EntryChainClient;
use eez_protocol::rollup_id::RollupId;
use jsonrpsee::core::middleware::{Batch, Notification, RpcServiceT};
use jsonrpsee::core::server::{MethodResponse, ResponsePayload};
use jsonrpsee::types::{ErrorObject, Request};
use reth_storage_api::{StateProvider, StateProviderFactory};
use tower::Layer;
use tracing::{Level, event};

const METHOD: &str = "eth_sendRawTransaction";

/// Max JSON-RPC response body size we ever construct directly (we
/// only write a single 32-byte hash so this is plenty).
const MAX_RESPONSE_SIZE: usize = 1024;

/// Late-filled handle to the node's OWN in-process L2 `StateProviderFactory`.
/// The provider only exists AFTER `launch`, but the middleware (and so this
/// layer) is constructed BEFORE launch, so it is threaded in as a `OnceLock`
/// and populated post-launch (see `main.rs`). Used by the OUTBOUND classifier
/// to read `authorizedProxies[to]` via an in-process committed-state SLOAD —
/// the same handle the composer reads — instead of a re-entrant HTTP self-call.
/// `+ Sync` is spelled explicitly: `StateProviderFactory: ... + Send` only, but
/// the `RpcServiceT` service must be `Send + Sync`.
type L2StateCell = Arc<OnceLock<Arc<dyn StateProviderFactory + Send + Sync>>>;

type IndirectOutboundProbeCell = Arc<OnceLock<Arc<IndirectOutboundProbe>>>;

/// Late-filled admission probe for L2 transactions whose top-level `to` is not
/// itself a registered proxy, but whose execution may call one indirectly.
///
/// The probe reuses the same EVM composer and L2 entry client used by Sync-slot
/// composition. It is therefore an admission-only preview of the authoritative
/// path: if source simulation records outbound target entries, the tx must be
/// held for a Sync block; otherwise it remains a vanilla L2 tx.
pub struct IndirectOutboundProbe {
    composer: eez_evm_inspector::EvmComposer,
    l2_entry: Arc<dyn EntryChainClient<Protocol = eez_evm::EvmProtocol> + Send + Sync>,
    rollup_id: u64,
}

impl IndirectOutboundProbe {
    #[must_use]
    pub fn new(
        composer: eez_evm_inspector::EvmComposer,
        l2_entry: Arc<dyn EntryChainClient<Protocol = eez_evm::EvmProtocol> + Send + Sync>,
        rollup_id: u64,
    ) -> Self {
        Self {
            composer,
            l2_entry,
            rollup_id,
        }
    }

    async fn detects_outbound(&self, raw_tx: &Bytes, tx_hash: B256) -> bool {
        match self
            .composer
            .simulate_and_resolve_recorded_for(
                RollupId(self.rollup_id),
                self.l2_entry.as_ref(),
                raw_tx.as_ref(),
            )
            .await
        {
            Ok((composition, recorded)) => {
                let target_entries: usize = composition
                    .targets
                    .iter()
                    .map(|target| target.batch.entries().len())
                    .sum();
                if target_entries == 0 {
                    event!(
                        name: "eez.ingress.outbound.indirect_probe.empty",
                        Level::DEBUG,
                        tx_hash = %tx_hash,
                        recorded_actions = recorded.len(),
                        "indirect outbound probe recorded no target entries; treating tx as vanilla L2",
                    );
                    return false;
                }
                event!(
                    name: "eez.ingress.outbound.indirect_probe.accepted",
                    Level::INFO,
                    tx_hash = %tx_hash,
                    recorded_actions = recorded.len(),
                    target_entries,
                    "indirect outbound probe found L1 settlement entries; holding tx for Sync slot",
                );
                true
            }
            Err(err) => {
                event!(
                    name: "eez.ingress.outbound.indirect_probe.miss",
                    Level::DEBUG,
                    tx_hash = %tx_hash,
                    error = %err,
                    "indirect outbound probe did not produce a composable outbound; treating tx as vanilla L2",
                );
                false
            }
        }
    }
}

/// Reusable layer for the ingress middleware. Cheaply [`Clone`]able
/// — both the `HeldPool` handle and the classifier are `Arc`s.
#[derive(Clone)]
pub struct IngressLayer {
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
    /// In-process L2 state handle for the DYNAMIC outbound classification.
    /// Populated post-launch; read with `.get()` at classify time.
    l2_state: L2StateCell,
    /// In-process source-simulation probe for indirect L2→L1 calls
    /// (`L2 tx → L2 contract → registered proxy`). Populated only in
    /// composer mode after the EVM composer and L2 entry client exist.
    indirect_outbound_probe: IndirectOutboundProbeCell,
    /// L1 provider for INBOUND (L1→L2) admission validation. An inbound
    /// held tx is an L1 tx (executed on L1), so its nonce/balance are
    /// validated against the L1 tip. `None` ⇒ validation skipped
    /// (standalone / no-L1 modes).
    l1_provider: Option<RootProvider>,
    /// L2 provider for OUTBOUND (L2→L1) admission validation. An outbound
    /// held tx is an L2 tx (`to` = an L2 cross-chain proxy) that EXECUTES
    /// ON L2 in the Sync block with the sender's L2 nonce + L2 balance —
    /// it is NEVER submitted to L1, so validating it against L1 is wrong.
    /// `None` ⇒ validation skipped. Also used for the DYNAMIC outbound
    /// classification (`authorizedProxies[to]` read on `ccm_l2_address`).
    l2_provider: Option<RootProvider>,
    /// The L2 CCM (EEZL2) address whose `authorizedProxies` mapping is the
    /// authoritative, on-chain registry of cross-chain proxies on this
    /// rollup. `Some` ⇒ outbound is detected DYNAMICALLY by reading
    /// `authorizedProxies[to]` live (no static env list to drift). `None`
    /// (no `EEZ_CCM_L2_ADDRESS`) ⇒ outbound disabled (non-cross-chain node).
    ccm_l2_address: Option<Address>,
}

impl IngressLayer {
    #[must_use]
    pub fn new(
        held_pool: Arc<HeldPool>,
        classifier: Arc<IngressClassifier>,
        l2_state: L2StateCell,
        indirect_outbound_probe: IndirectOutboundProbeCell,
    ) -> Self {
        // INBOUND admission reads the sender's nonce + balance from L1.
        // Prefer the canonical-tip RPC (the embedded node can lag 2-3
        // blocks); fall back to the embedded RPC; absent both, no
        // validation (dev/standalone).
        let l1_provider = std::env::var("EEZ_L1_TARGET_RPC_URL")
            .or_else(|_| std::env::var("EEZ_L1_RPC_URL"))
            .ok()
            .and_then(|u| u.parse().ok())
            .map(|u| {
                ProviderBuilder::new()
                    .disable_recommended_fillers()
                    .connect_http(u)
            });
        // OUTBOUND admission reads the sender's nonce + balance from L2 —
        // the held outbound tx executes on L2, not L1. Defaults to the
        // node's own L2 HTTP RPC (the live compose serves L2 on
        // `:18688`, reachable in-container via `network_mode: host`);
        // override with `EEZ_L2_RPC_URL`. Absent a parseable URL, no
        // validation (skip, like the L1 path).
        let l2_provider = std::env::var("EEZ_L2_RPC_URL")
            .ok()
            .filter(|u| !u.is_empty())
            .or_else(|| Some("http://127.0.0.1:18688".to_string()))
            .and_then(|u| u.parse().ok())
            .map(|u| {
                ProviderBuilder::new()
                    .disable_recommended_fillers()
                    .connect_http(u)
            });
        // The L2 CCM (EEZL2) address — the deploy sets EEZ_CCM_L2_ADDRESS on
        // every cross-chain deployment. `Some` enables dynamic outbound
        // classification (authorizedProxies read); absent ⇒ no outbound (a
        // plain L2 node). NOT a hardcoded constant — read from config.
        let ccm_l2_address = std::env::var("EEZ_CCM_L2_ADDRESS")
            .ok()
            .and_then(|s| s.parse::<Address>().ok());
        Self {
            held_pool,
            classifier,
            l2_state,
            indirect_outbound_probe,
            l1_provider,
            l2_provider,
            ccm_l2_address,
        }
    }
}

impl<S> Layer<S> for IngressLayer {
    type Service = IngressService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        IngressService {
            inner,
            held_pool: self.held_pool.clone(),
            classifier: self.classifier.clone(),
            l1_provider: self.l1_provider.clone(),
            l2_provider: self.l2_provider.clone(),
            ccm_l2_address: self.ccm_l2_address,
            l2_state: self.l2_state.clone(),
            indirect_outbound_probe: self.indirect_outbound_probe.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
    l1_provider: Option<RootProvider>,
    l2_provider: Option<RootProvider>,
    ccm_l2_address: Option<Address>,
    l2_state: L2StateCell,
    indirect_outbound_probe: IndirectOutboundProbeCell,
}

/// Reject an `eth_sendRawTransaction` at the door with a clear error.
fn reject(id: jsonrpsee::types::Id<'_>, msg: String) -> MethodResponse {
    event!(
        name: "eez.ingress.tx.rejected",
        Level::WARN,
        reason = %msg,
        "raw tx rejected at ingress",
    );
    let payload = ResponsePayload::<B256>::error(ErrorObject::owned(-32000, msg, None::<()>));
    MethodResponse::response(id, payload, MAX_RESPONSE_SIZE)
}

pub(crate) fn is_reserved_system_sender(sender: Address) -> bool {
    sender == eez_evm::SYSTEM_ADDRESS
}

pub(crate) fn reserved_system_sender_error(sender: Address) -> String {
    format!("reserved system sender {sender} cannot submit transactions through public RPC")
}

/// Conservative lower bound for an L1->L2 proxy consumption tx.
///
/// A plain ETH transfer to the proxy is not a 21k/60k L1 transfer once it is
/// bundled: after `postBatch`, the proxy calls into EEZ to consume the deferred
/// entry, apply state deltas, and emit execution events. Live Chiado traces show
/// a simple value deposit uses ~112k gas; admitting a lower limit makes the
/// builder simulation reject the whole `[postBatch, user_tx]` bundle forever.
const MIN_INBOUND_CROSS_CHAIN_GAS_LIMIT: u64 = 150_000;

/// Outcome of admitting an already-classified cross-chain `eth_sendRawTransaction`.
pub enum Admission {
    /// Admitted + pushed to the HeldPool; return this tx hash to the caller.
    Held(B256),
    /// Rejected at the door (signature / nonce / balance) — return this error.
    Rejected(String),
}

/// THE admission gate for a cross-chain tx — invariant 7, run identically by the
/// L2-RPC jsonrpsee ingress AND the L1 interceptor front (B0), so the two fronts
/// CANNOT drift on the rule: a cross-chain tx rides an all-or-nothing L1 bundle,
/// so one poison tx fails the whole bundle in builder sim and evicts innocent
/// bundle-mates, breaking their nonce chains — validate like a mempool would
/// (`nonce == on-chain + held`, `balance >= value + gas_limit*max_fee`) and
/// reject precisely. On admit, push a raw `HeldTx{direction}` (the composer
/// drains+composes it at the next Sync slot — same for either front). The caller
/// has ALREADY decided this is cross-chain + which `direction` (the L2 RPC by the
/// chain-id-mismatch classifier signal, the interceptor by an `authorizedProxies`
/// lookup).
///
/// The provider to validate against is chosen BY DIRECTION, because the two
/// directions execute on different chains with independent nonce/balance:
///
/// - [`Direction::Inbound`] (L1→L2): the held tx IS an L1 tx, executed on L1 —
///   validate against `l1_provider` (L1 nonce + L1 balance).
/// - [`Direction::Outbound`] (L2→L1): the held tx is an L2 tx (`to` = an L2
///   cross-chain proxy) that EXECUTES ON L2 in the Sync block with the sender's
///   L2 nonce + L2 balance — it is NEVER submitted to L1 — validate against
///   `l2_provider`. The value is burned on L2 and L2 gas is paid there, so the
///   balance check uses the L2 balance.
///
/// `held_count_for` is keyed by `(sender, direction)`, so the held offset is the
/// per-direction chain's pending count and composes with the selected provider's
/// on-chain nonce. The selected provider being `None` ⇒ validation skipped
/// (dev/standalone only).
pub async fn gate_and_hold(
    envelope: &TxEnvelope,
    raw_tx: &Bytes,
    direction: Direction,
    held_pool: &HeldPool,
    l1_provider: Option<&RootProvider>,
    l2_provider: Option<&RootProvider>,
) -> Admission {
    let Ok(sender) = envelope.recover_signer() else {
        return Admission::Rejected("signature recovery failed".into());
    };
    if is_reserved_system_sender(sender) {
        return Admission::Rejected(reserved_system_sender_error(sender));
    }
    if direction == Direction::Inbound && envelope.gas_limit() < MIN_INBOUND_CROSS_CHAIN_GAS_LIMIT {
        return Admission::Rejected(format!(
            "inbound cross-chain tx gas limit {} is below minimum {}; set a higher gas limit for the L1 proxy consumption path",
            envelope.gas_limit(),
            MIN_INBOUND_CROSS_CHAIN_GAS_LIMIT,
        ));
    }
    let nonce = envelope.nonce();
    // Select the validation chain by direction: inbound rides an L1 tx
    // (L1 nonce/balance), outbound rides an L2 tx (L2 nonce/balance).
    let (provider, chain) = match direction {
        Direction::Inbound => (l1_provider, "L1"),
        Direction::Outbound => (l2_provider, "L2"),
    };
    if let Some(provider) = provider {
        let on_chain = match provider.get_transaction_count(sender).await {
            Ok(n) => n,
            Err(e) => {
                return Admission::Rejected(format!(
                    "{chain} validation unavailable (nonce lookup): {e}"
                ));
            }
        };
        let held = held_pool.held_count_for(sender, direction) as u64;
        let expected = on_chain + held;
        if nonce != expected {
            return Admission::Rejected(format!(
                "invalid nonce {nonce} for {sender}: expected {expected} (on-chain {on_chain} + {held} held)"
            ));
        }
        let balance = match provider.get_balance(sender).await {
            Ok(b) => b,
            Err(e) => {
                return Admission::Rejected(format!(
                    "{chain} validation unavailable (balance lookup): {e}"
                ));
            }
        };
        let cost = U256::from(envelope.value())
            + U256::from(envelope.gas_limit()) * U256::from(envelope.max_fee_per_gas());
        if balance < cost {
            return Admission::Rejected(format!(
                "insufficient {chain} balance for {sender}: have {balance}, need {cost} (value + gas_limit * max_fee)"
            ));
        }
    }

    // tx_hash = keccak256(EIP-2718 envelope bytes) — canonical for both legacy
    // (envelope IS the RLP) and typed (`type ‖ rlp(body)`) txs.
    let hash: B256 = keccak256(raw_tx.as_ref());
    event!(
        name: "eez.ingress.cross_chain.push",
        Level::INFO,
        tx_hash = %hash,
        sender = %sender,
        nonce,
        chain_id = ?envelope.chain_id(),
        to = ?envelope.to(),
        gas_limit = envelope.gas_limit(),
        max_fee_per_gas = envelope.max_fee_per_gas(),
        max_priority_fee_per_gas = ?envelope.max_priority_fee_per_gas(),
        value = %envelope.value(),
        ?direction,
        "cross-chain tx held for next Sync slot",
    );
    held_pool.push(HeldTx {
        raw_tx: raw_tx.clone(),
        hash,
        attempts: 0,
        sender,
        nonce,
        direction,
    });
    Admission::Held(hash)
}

/// Dynamic OUTBOUND classification: is `to` a registered cross-chain proxy on
/// the L2 CCM (EEZL2)? Reads `authorizedProxies[to]` LIVE from L2 state — the
/// protocol's own on-chain identity mechanism — so the node never drifts from
/// a hand-maintained env list (the footgun: a proxy created on L2 but absent
/// from a static set is mis-routed `L2Only`, mines, REVERTS, and the L2→L1
/// effect is lost with no recovery). Mirrors the inbound B0 interceptor's L1
/// `authorizedProxies` lookup, targeting the L2 CCM instead of the L1 registry.
///
/// Reads `authorizedProxies[to]` IN-PROCESS against the node's OWN latest
/// committed L2 state (`StateProviderFactory::latest().storage(...)`) — the same
/// handle + storage primitives the composer's `SessionInspector` reads, so both
/// paths agree by construction. NO network hop and NO re-entrant HTTP self-call
/// — the bug `b4f1ecb` introduced (a `get_storage_at` to the node's OWN RPC made
/// FROM INSIDE the ingress RPC handler, which failed under live conditions and
/// silently dropped outbounds to L2Only). Synchronous: it opens and drops a
/// committed-state snapshot within the call, never held across an await
/// (`StateProviderBox` is `Send` but not `Sync`).
///
/// On a state read error → `false` (L2Only): the node erroring on its OWN
/// committed state means it is already unhealthy; this path is now near-
/// unreachable (the failing HTTP self-call is gone).
fn is_authorized_proxy_l2(state: &dyn StateProviderFactory, ccm_l2: Address, to: Address) -> bool {
    // `StateProvider::storage` takes the slot as `B256` directly — pass the
    // mapping key straight through (no U256 round-trip).
    let key: B256 = eez_evm::authorized_proxies::proxy_mapping_key(
        to,
        eez_evm::authorized_proxies::CCM_AUTHORIZED_PROXIES_SLOT,
    );
    let provider = match state.latest() {
        Ok(p) => p,
        Err(e) => {
            event!(
                name: "eez.ingress.outbound.proxy_lookup_failed",
                Level::WARN,
                to = %to,
                error = %e,
                "in-process latest() failed; treating tx as L2Only",
            );
            return false;
        }
    };
    match provider.storage(ccm_l2, key) {
        Ok(opt) => {
            eez_evm::authorized_proxies::decode_proxy_value(opt.unwrap_or(U256::ZERO)).is_some()
        }
        Err(e) => {
            event!(
                name: "eez.ingress.outbound.proxy_lookup_failed",
                Level::WARN,
                to = %to,
                error = %e,
                "in-process storage read failed; treating tx as L2Only",
            );
            false
        }
    }
}

/// HTTP-RPC fallback for the ONLY window the in-process `l2_state` cell is not
/// yet populated: between node `launch` and the post-launch `OnceLock::set`.
/// This is the OLD `b4f1ecb` read (a re-entrant self-call); kept ONLY so outbound
/// detection in that brief startup window is never WORSE than the prior baseline.
/// In steady state the cell is set and this is never reached.
async fn is_authorized_proxy_l2_http(
    provider: &RootProvider,
    ccm_l2: Address,
    to: Address,
) -> bool {
    let key = U256::from_be_bytes(
        eez_evm::authorized_proxies::proxy_mapping_key(
            to,
            eez_evm::authorized_proxies::CCM_AUTHORIZED_PROXIES_SLOT,
        )
        .0,
    );
    match provider.get_storage_at(ccm_l2, key).await {
        Ok(value) => eez_evm::authorized_proxies::decode_proxy_value(value).is_some(),
        Err(_) => false,
    }
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
        let held_pool = self.held_pool.clone();
        let classifier = self.classifier.clone();
        let l1_provider = self.l1_provider.clone();
        let l2_provider = self.l2_provider.clone();
        let ccm_l2_address = self.ccm_l2_address;
        let l2_state = self.l2_state.clone();
        let indirect_outbound_probe = self.indirect_outbound_probe.clone();
        async move {
            // Fast path: not our method.
            // Cross-chain is ON if EITHER inbound (foreign source chain ids
            // configured) OR outbound (the L2 CCM address is configured for the
            // dynamic `authorizedProxies` lookup). A plain L2 node (neither) is
            // a hot-path no-op.
            if req.method.as_ref() != METHOD {
                return inner.call(req).await;
            }

            // Try to extract the raw-tx hex string from the params.
            // On any decode hiccup, fall through to reth — reth's
            // own error path will produce the standard JSON-RPC
            // error for malformed input.
            let Some(params) = req.params.as_deref() else {
                return inner.call(req).await;
            };
            let Ok((raw_tx,)) = serde_json::from_str::<(Bytes,)>(params.get()) else {
                return inner.call(req).await;
            };
            let Ok(envelope) = TxEnvelope::decode_2718(&mut raw_tx.as_ref()) else {
                return inner.call(req).await;
            };
            if let Ok(sender) = envelope.recover_signer() {
                if is_reserved_system_sender(sender) {
                    return reject(req.id, reserved_system_sender_error(sender));
                }
            }

            if classifier.is_empty() && ccm_l2_address.is_none() {
                return inner.call(req).await;
            }
            let to = envelope.to();
            let chain_id = envelope.chain_id();
            let tx_hash: B256 = keccak256(raw_tx.as_ref());

            // Resolve the cross-chain direction (or None = vanilla L2):
            //   1. INBOUND — static, cheap: a foreign source chain id (an
            //      L1-bound raw tx POSTed to L2's RPC). Checked first so an
            //      inbound tx never pays the outbound L2 read.
            //   2. DIRECT OUTBOUND — DYNAMIC: `to` is a registered cross-chain
            //      proxy, resolved by a live `authorizedProxies[to]` read on the
            //      L2 CCM (no static env list to drift).
            //   3. INDIRECT OUTBOUND — if the cheap top-level proxy lookup
            //      misses, source-sim the tx through the same EVM composer used
            //      by Sync-slot composition. If the simulation records target
            //      entries, a nested proxy call exists and the tx must also be
            //      held. Misses fall through to the standard L2 pool.
            let direction = if let Classification::CrossChain(d) = classifier.classify(chain_id) {
                Some(d)
            } else {
                // DYNAMIC outbound: read `authorizedProxies[to]` IN-PROCESS against
                // the node's OWN committed state (no HTTP self-call). The cell is
                // populated post-launch; in the brief startup window before it is
                // set, fall back to the old HTTP read so outbound detection is never
                // worse than the prior baseline.
                let direct_outbound = match (ccm_l2_address, to) {
                    (Some(ccm), Some(addr)) => match l2_state.get() {
                        Some(factory) => is_authorized_proxy_l2(factory.as_ref(), ccm, addr),
                        None => match l2_provider.as_ref() {
                            Some(l2p) => is_authorized_proxy_l2_http(l2p, ccm, addr).await,
                            None => false,
                        },
                    },
                    _ => false,
                };
                if direct_outbound {
                    Some(Direction::Outbound)
                } else if let Some(probe) = indirect_outbound_probe.get() {
                    probe
                        .detects_outbound(&raw_tx, tx_hash)
                        .await
                        .then_some(Direction::Outbound)
                } else {
                    None
                }
            };

            match direction {
                // Vanilla L2 → standard reth pool path.
                None => inner.call(req).await,
                // Cross-chain → the HeldPool; the matched direction is recorded
                // on the HeldTx for per-(sender, direction) nonce-contiguity
                // keying. The admission gate + push is the SHARED `gate_and_hold`
                // (identical to the L1 interceptor front, B0).
                Some(direction) => {
                    match gate_and_hold(
                        &envelope,
                        &raw_tx,
                        direction,
                        &held_pool,
                        l1_provider.as_ref(),
                        l2_provider.as_ref(),
                    )
                    .await
                    {
                        Admission::Held(hash) => {
                            let payload = ResponsePayload::<B256>::success(hash);
                            MethodResponse::response(req.id, payload, MAX_RESPONSE_SIZE)
                        }
                        Admission::Rejected(msg) => reject(req.id, msg),
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_network::TxSignerSync;
    use alloy_primitives::{TxKind, address, b256};
    use alloy_signer_local::PrivateKeySigner;
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes as HyperBytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use serde_json::Value;
    use tokio::net::TcpListener;

    const PROXY: Address = address!("00000000000000000000000000000000000000bb");
    const ANVIL0_KEY: alloy_primitives::B256 =
        b256!("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");

    /// A minimal mock JSON-RPC node that answers `eth_getTransactionCount`
    /// and `eth_getBalance` with FIXED values, so a test can assert which
    /// chain `gate_and_hold` validated against. `nonce` is returned for
    /// every sender; `balance_wei` likewise. Returns the node's URL.
    async fn spawn_mock_node(nonce: u64, balance_wei: u128) -> String {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| async move {
                        let body = req.collect().await.unwrap().to_bytes();
                        let j: Value = serde_json::from_slice(&body).unwrap_or_default();
                        let method = j.get("method").and_then(Value::as_str).unwrap_or("");
                        let id = j.get("id").cloned().unwrap_or(Value::Null);
                        let result = match method {
                            "eth_getTransactionCount" => format!("0x{nonce:x}"),
                            "eth_getBalance" => format!("0x{balance_wei:x}"),
                            _ => "0x0".to_string(),
                        };
                        let resp =
                            serde_json::json!({ "jsonrpc": "2.0", "result": result, "id": id });
                        Ok::<_, hyper::Error>(Response::new(Full::new(HyperBytes::from(
                            resp.to_string(),
                        ))))
                    });
                    let io = TokioIo::new(stream);
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        url
    }

    fn provider_for(url: &str) -> RootProvider {
        RootProvider::new_http(url.parse().unwrap())
    }

    /// Sign an EIP-1559 tx to `to` with the given `nonce` (value 0, 1 gwei
    /// fee, gas limit above the inbound cross-chain minimum).
    fn signed_tx(signer: &PrivateKeySigner, to: Address, nonce: u64) -> (TxEnvelope, Bytes) {
        signed_tx_with_gas(signer, to, nonce, 200_000)
    }

    fn signed_tx_with_gas(
        signer: &PrivateKeySigner,
        to: Address,
        nonce: u64,
        gas_limit: u64,
    ) -> (TxEnvelope, Bytes) {
        use alloy_eips::eip2718::Encodable2718;
        let mut tx = TxEip1559 {
            chain_id: 31337,
            nonce,
            gas_limit,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(to),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let signed = tx.into_signed(sig);
        let mut buf = Vec::new();
        signed.encode_2718(&mut buf);
        let raw = Bytes::from(buf);
        let envelope = TxEnvelope::Eip1559(signed);
        (envelope, raw)
    }

    const ONE_ETH: u128 = 1_000_000_000_000_000_000;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reserved_system_sender_is_rejected_before_hold() {
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::from_bytes(&ANVIL0_KEY).unwrap();
        assert_eq!(signer.address(), eez_evm::SYSTEM_ADDRESS);
        let (env, raw) = signed_tx(&signer, PROXY, 0);

        let admission = gate_and_hold(&env, &raw, Direction::Inbound, &pool, None, None).await;
        match admission {
            Admission::Rejected(msg) => assert!(
                msg.contains("reserved system sender"),
                "expected reserved-sender rejection, got: {msg}",
            ),
            Admission::Held(_) => panic!("SYSTEM_ADDRESS tx must not enter the held pool"),
        }
        assert_eq!(
            pool.held_count_for(eez_evm::SYSTEM_ADDRESS, Direction::Inbound),
            0,
            "SYSTEM_ADDRESS tx must not be held",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbound_below_proxy_consumption_gas_floor_is_rejected() {
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::random();
        let (env, raw) =
            signed_tx_with_gas(&signer, PROXY, 0, MIN_INBOUND_CROSS_CHAIN_GAS_LIMIT - 1);

        let admission = gate_and_hold(&env, &raw, Direction::Inbound, &pool, None, None).await;
        match admission {
            Admission::Rejected(msg) => assert!(
                msg.contains("below minimum"),
                "expected gas-floor rejection, got: {msg}",
            ),
            Admission::Held(_) => panic!("low-gas inbound tx must not enter the held pool"),
        }
        assert_eq!(
            pool.held_count_for(signer.address(), Direction::Inbound),
            0,
            "low-gas inbound tx must not be held",
        );
    }

    /// OUTBOUND validates against the L2 provider: an L2-nonce-2 tx is
    /// admitted when L2 reports nonce 2, even though the L1 provider
    /// reports nonce 1 (the live bug — an L2 tx wrongly checked against L1
    /// was rejected `invalid nonce 2 ... expected 1`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outbound_validates_against_l2_not_l1() {
        // L1 sees nonce 1; L2 sees nonce 2. The outbound tx is L2-nonce-2.
        let l1 = provider_for(&spawn_mock_node(1, ONE_ETH).await);
        let l2 = provider_for(&spawn_mock_node(2, ONE_ETH).await);
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::random();
        let (env, raw) = signed_tx(&signer, PROXY, 2);

        let admission =
            gate_and_hold(&env, &raw, Direction::Outbound, &pool, Some(&l1), Some(&l2)).await;
        assert!(
            matches!(admission, Admission::Held(_)),
            "outbound tx with L2 nonce 2 must validate against L2 (nonce 2), not L1 (nonce 1)",
        );
        assert_eq!(
            pool.held_count_for(signer.address(), Direction::Outbound),
            1,
            "the admitted outbound tx must be held",
        );
    }

    /// If outbound were (wrongly) validated against L1, the L2-nonce-2 tx
    /// would be rejected. Prove the OLD behavior by passing the L1
    /// provider in the L2 slot — the gate must then reject (nonce 2 vs
    /// on-chain 1). This pins the direction→provider mapping.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outbound_against_l1_provider_rejects_l2_nonce() {
        let l1 = provider_for(&spawn_mock_node(1, ONE_ETH).await);
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::random();
        let (env, raw) = signed_tx(&signer, PROXY, 2);

        // L2 slot deliberately fed the L1 provider (nonce 1).
        let admission =
            gate_and_hold(&env, &raw, Direction::Outbound, &pool, None, Some(&l1)).await;
        match admission {
            Admission::Rejected(msg) => assert!(
                msg.contains("invalid nonce 2") && msg.contains("expected 1"),
                "expected nonce-1 rejection, got: {msg}",
            ),
            Admission::Held(_) => {
                panic!("L2-nonce-2 tx must be rejected against a nonce-1 provider")
            }
        }
    }

    /// INBOUND validates against the L1 provider — byte-unchanged from
    /// before the fix: an L1-nonce-1 tx is admitted when L1 reports nonce
    /// 1, regardless of what L2 reports.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbound_validates_against_l1_not_l2() {
        // L1 sees nonce 1; L2 sees nonce 2 (must be ignored for inbound).
        let l1 = provider_for(&spawn_mock_node(1, ONE_ETH).await);
        let l2 = provider_for(&spawn_mock_node(2, ONE_ETH).await);
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::random();
        let (env, raw) = signed_tx(&signer, PROXY, 1);

        let admission =
            gate_and_hold(&env, &raw, Direction::Inbound, &pool, Some(&l1), Some(&l2)).await;
        assert!(
            matches!(admission, Admission::Held(_)),
            "inbound tx with L1 nonce 1 must validate against L1, ignoring L2 nonce 2",
        );
        assert_eq!(
            pool.held_count_for(signer.address(), Direction::Inbound),
            1,
            "the admitted inbound tx must be held",
        );
    }

    /// The held offset is per-(sender, direction): after one outbound tx
    /// is held, the next valid outbound nonce is `on-chain + 1`. The L1
    /// (inbound) chain of the SAME sender is untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outbound_held_offset_is_per_direction() {
        let l2 = provider_for(&spawn_mock_node(5, ONE_ETH).await);
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::random();

        // First outbound: on-chain L2 nonce 5, 0 held → expect 5. Admit.
        let (e0, r0) = signed_tx(&signer, PROXY, 5);
        assert!(matches!(
            gate_and_hold(&e0, &r0, Direction::Outbound, &pool, None, Some(&l2)).await,
            Admission::Held(_)
        ));
        // Second outbound: on-chain 5 + 1 held → expect 6. Admit.
        let (e1, r1) = signed_tx(&signer, PROXY, 6);
        assert!(
            matches!(
                gate_and_hold(&e1, &r1, Direction::Outbound, &pool, None, Some(&l2)).await,
                Admission::Held(_)
            ),
            "the second outbound tx must validate at on-chain L2 nonce + held",
        );
        assert_eq!(
            pool.held_count_for(signer.address(), Direction::Outbound),
            2,
        );
        assert_eq!(
            pool.held_count_for(signer.address(), Direction::Inbound),
            0,
            "the inbound chain must be untouched by outbound holds",
        );
    }

    /// A `None` selected provider keeps today's skip-validation behavior:
    /// an outbound tx with no L2 provider is admitted without a nonce
    /// check (dev/standalone). Mirrors the L1 path's `None` semantics.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outbound_with_no_l2_provider_skips_validation() {
        let pool = HeldPool::new();
        let signer = PrivateKeySigner::random();
        // An arbitrary nonce — no provider means no check.
        let (env, raw) = signed_tx(&signer, PROXY, 999);
        assert!(matches!(
            gate_and_hold(&env, &raw, Direction::Outbound, &pool, None, None).await,
            Admission::Held(_)
        ));
    }

    /// Minimal mock answering `eth_getStorageAt` with a FIXED word — exercises
    /// the DYNAMIC outbound `authorizedProxies` read in isolation.
    async fn spawn_storage_mock(storage_word_hex: &'static str) -> String {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| async move {
                        let body = req.collect().await.unwrap().to_bytes();
                        let j: Value = serde_json::from_slice(&body).unwrap_or_default();
                        let method = j.get("method").and_then(Value::as_str).unwrap_or("");
                        let id = j.get("id").cloned().unwrap_or(Value::Null);
                        let result = match method {
                            "eth_getStorageAt" => storage_word_hex.to_string(),
                            _ => "0x0".to_string(),
                        };
                        let resp =
                            serde_json::json!({ "jsonrpc": "2.0", "result": result, "id": id });
                        Ok::<_, hyper::Error>(Response::new(Full::new(HyperBytes::from(
                            resp.to_string(),
                        ))))
                    });
                    let io = TokioIo::new(stream);
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        url
    }

    /// The DYNAMIC outbound signal (replacing the static env set): a non-zero
    /// `authorizedProxies[to]` slot ⇒ registered cross-chain proxy ⇒ outbound;
    /// a zero slot ⇒ not a proxy ⇒ L2Only. Proves the read decodes the on-chain
    /// registry, so the node can't drift from a hand-maintained list.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_outbound_authorized_proxy_lookup() {
        let ccm = address!("4200000000000000000000000000000000000007");
        // Non-zero word (originalAddress in the low bytes) → registered proxy.
        let yes = provider_for(
            &spawn_storage_mock(
                "0x0000000000000000000000000000000000000000000000000000000000000abc",
            )
            .await,
        );
        assert!(
            is_authorized_proxy_l2_http(&yes, ccm, PROXY).await,
            "a non-zero authorizedProxies slot must classify `to` as an outbound proxy",
        );
        // Zero word (unset mapping entry) → not a proxy.
        let no = provider_for(
            &spawn_storage_mock(
                "0x0000000000000000000000000000000000000000000000000000000000000000",
            )
            .await,
        );
        assert!(
            !is_authorized_proxy_l2_http(&no, ccm, PROXY).await,
            "a zero authorizedProxies slot must NOT be classified outbound",
        );
    }
}

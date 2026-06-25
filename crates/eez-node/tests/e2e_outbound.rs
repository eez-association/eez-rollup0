//! Outbound (L2→L1) cross-chain E2E — bring-up against an EMBEDDED L1.
//!
//! This is **step S1** of the P-1 plan (`docs/p1-outbound-e2e-design.md`
//! §5): prove the cross-chain composer node comes up against the
//! in-process embedded reth `--dev` L1, that the protocol (EEZ + proof
//! system + Rollup + registerRollup) deploys onto that embedded L1, and
//! that the composer reads EEZ and settles ≥1 `postBatch` (an empty /
//! anchor batch is fine — no outbound user tx yet; that is S2–S5).
//!
//! Topology differs from `e2e.rs` (external anvil): here the L1 is the
//! node's OWN in-process reth, the only path that wires the cross-chain
//! `EvmComposer` (`main.rs:449`) — and, in the current code, the only
//! path that submits `postBatch` at all (the `postbatch_sink` is fed
//! exclusively by `compose_via_evm_composer` → `dispatch_minimal_post-
//! batch`; a plain-anvil composer builds Sync blocks but never emits a
//! batch).
//!
//! Deploy ordering is **placeholder-then-restart** (design §2): the
//! embedded L1 boots empty *with* the node, but the node needs
//! `EEZ_REGISTRY_ADDRESS` at startup. Phase A starts with a placeholder
//! registry (codeless → composer reads nothing, retries, no crash); we
//! deploy the protocol onto the now-live embedded L1; Phase B restarts
//! the node (same datadirs, same pinned L1 ports) with the real
//! addresses so the composer reads EEZ and starts posting.

use std::time::Duration;

use alloy_primitives::{Address, U256, address};

mod common;
use common::{
    ANVIL_KEY, ANVIL_KEY_1, ANVIL_KEY_2, ANVIL_KEY_3, ANVIL_KEY_4, CCM_L2_ADDRESS, EmbeddedL1,
    L1Chain, NodeConfig, NodeHandle, PLACEHOLDER_ADDRESS, block_state_root_at,
    create_l2_cross_chain_proxy, cross_chain_env, deploy_contracts_with_initial, deploy_value,
    fresh_cross_chain_genesis, l1_block_number, proxy_original_address, read_value, remove_env,
    safe_block_number_and_root, safe_block_state_root, send_l2_value_transfer,
    send_outbound_set_value, state_root, wait_for, wait_for_l1_blocks, wait_for_l2_rpc,
    wait_for_l2_tx_receipt, wait_for_rpc_down,
};

/// Embedded reth bring-up + a fresh genesis materialization can take a
/// while; be generous (design: 120–180s).
const L1_BOOT_TIMEOUT: Duration = Duration::from_secs(180);
const BATCH_TIMEOUT: Duration = Duration::from_secs(180);

/// S1 — cross-chain composer comes up against an embedded L1, the
/// protocol deploys onto it, and ≥1 `postBatch` settles (empty/anchor
/// batch). No outbound user tx.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_cross_chain_node_settles_anchor_batch() {
    // ── Cross-chain L2 genesis (EEZL2 predeploy at 0x42..07) with a
    // CURRENT timestamp — without it the L1-anchored scheduler never
    // closes a sync slot (see `fresh_genesis`). State root is unchanged
    // by the timestamp rewrite, so it == the `initialState` we register.
    let l2_genesis = fresh_cross_chain_genesis().expect("fresh cross-chain genesis");
    let cfg = NodeConfig {
        genesis_path: Some(l2_genesis.path.as_path()),
        ..Default::default()
    };

    // ── Embedded L1: alloc pinned ports + datadir (persist across restart).
    let l1 = EmbeddedL1::alloc().expect("alloc embedded L1");

    // Shared L2 datadir so the Phase-A node's L2 state survives the
    // Phase-B restart.
    let l2_datadir = tempfile::tempdir().expect("l2 datadir");

    // ── Phase A — placeholder registry. The embedded L1 boots WITH the
    // node; the composer can't read a codeless registry, so it retries
    // without posting. We only need the embedded L1's HTTP RPC up.
    // Poster = anvil#1, DISTINCT from the deploy/proof-signer key
    // (anvil#0): even against a placeholder (codeless) registry the
    // composer still drives the poster EOA's nonce, which would race the
    // protocol deploy if they shared a key (`nonce too low`). The proof
    // signer stays anvil#0 (== deploy key) so the mock PS accepts the
    // self-signed proof.
    let rollup_id = 1u64;
    let phase_a_env = cross_chain_env(
        &l1,
        ANVIL_KEY_1, // poster = anvil#1 (≠ deploy/proof-signer anvil#0)
        PLACEHOLDER_ADDRESS,
        PLACEHOLDER_ADDRESS,
        0,
        rollup_id,
        None, // no proxy env in S1
    );
    let node_a = NodeHandle::spawn_with("composer-a", l2_datadir.path(), &cfg, &phase_a_env)
        .expect("spawn phase-A node");

    // ── Wait for the embedded L1 HTTP RPC to answer eth_blockNumber.
    // `wait_for_l2_rpc` tolerates connection-refused while the embedded
    // reth boots (it maps RPC errors to "not ready yet"); it's URL-
    // generic, so it serves the L1 RPC equally.
    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("embedded L1 RPC did not come up");

    // ── Deploy the protocol onto the embedded L1 from anvil#0. The
    // deployer key MUST equal the composer's proof signer
    // (`EEZ_PROOF_SIGNER_KEY` = ANVIL_KEY in `cross_chain_env`):
    // `deploy_contracts_with_initial` bakes the deployer address into the
    // `MockECDSAProofSystem` ctor (its `signer`) AND the Rollup vkey, and
    // the contract recovers the proof signer against exactly that
    // address. A mismatch makes every postBatch's `verify` reject →
    // `_applyStateDeltas` skipped → `state_applied: false`, never
    // `BatchPosted`. The poster (anvil#1) is a different EOA, so this
    // deploy never races the composer's postBatch nonces. `initialState`
    // = cross-chain genesis state root.
    let dep = deploy_contracts_with_initial(&l1.rpc_url, ANVIL_KEY, l2_genesis.state_root)
        .await
        .expect("deploy protocol onto embedded L1");

    // ── Durably persist the deploy before the restart. reth's engine
    // keeps the most recent `DEFAULT_PERSISTENCE_THRESHOLD` (= 2) blocks
    // in MEMORY and only flushes to the datadir once the head advances
    // past `last_persisted + 2`. The deploy's tail txs (Rollup deploy +
    // registerRollup) land in the latest blocks, so killing Phase A
    // immediately loses them — Phase B's embedded reth re-opens the
    // datadir with EEZ present but the rollup UNregistered (`rollup-
    // Counter()==0`), and every postBatch reverts (`state_applied:
    // false`). Wait for several more auto-mined L1 blocks so the deploy
    // is buried deep enough to flush.
    let after_deploy = l1_block_number(&l1.rpc_url).await;
    wait_for_l1_blocks(
        &l1.rpc_url,
        after_deploy + 6,
        Duration::from_secs(120),
    )
    .await
    .expect("embedded L1 did not advance to persist the deploy");

    // ── Phase B — restart the node (same L2 datadir + same embedded L1
    // datadir/ports) with the REAL registry / proof system / deploy
    // block. Now the composer reads EEZ and starts posting.
    //
    // Kill Phase A, then WAIT for its embedded L1 RPC to stop answering:
    // reth holds an exclusive datadir lock + the pinned L1 ports, and the
    // OS releases them a beat after the process dies. Binding them before
    // release makes Phase B's embedded reth fail to start.
    drop(node_a);
    wait_for_rpc_down(&l1.rpc_url, Duration::from_secs(60))
        .await
        .expect("Phase A embedded L1 did not shut down");

    let phase_b_env = cross_chain_env(
        &l1,
        ANVIL_KEY_1, // poster = anvil#1 (proof signer stays anvil#0)
        dep.eez_address,
        dep.mock_ps_address,
        dep.deploy_block,
        dep.rollup_id,
        None,
    );
    let node_b = NodeHandle::spawn_with("composer-b", l2_datadir.path(), &cfg, &phase_b_env)
        .expect("spawn phase-B node");
    // Phase B brings the embedded L1 + EEZ back up; wait for its own L2
    // RPC, then for the embedded L1 RPC, before reading batch events.
    wait_for_l2_rpc(&node_b.l2_rpc_url(), L1_BOOT_TIMEOUT)
        .await
        .expect("Phase B L2 RPC did not come up");
    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase B embedded L1 RPC did not come up");

    // ── Assert ≥1 postBatch settles on the embedded L1.
    let l1_chain = L1Chain::new(&l1.rpc_url, &dep);
    let n = l1_chain
        .wait_for_batches(1, BATCH_TIMEOUT)
        .await
        .expect("composer did not settle any batch against the embedded L1");
    assert!(n >= 1, "expected ≥1 BatchPosted, got {n}");
}

/// `MAINNET` rollup id — the source rollup id for an L2→L1 (outbound)
/// proxy on EEZL2 (`EEZL2.createCrossChainProxy(L1Value, MAINNET=0)`).
const MAINNET_ROLLUP_ID: u64 = 0;

/// The OUTBOUND user tx sets `Value` to this on L1.
const OUTBOUND_VALUE: u64 = 42;

/// The L2 fixture genesis (`reorg_genesis_path`) pins `chainId = 1`.
/// Sanity-asserted at runtime against `eth_chainId`.
const L2_CHAIN_ID: u64 = 1;

/// anvil#2 (`0x3C44…`) — creates the L2 proxy. Distinct from the L2
/// SYSTEM_ADDRESS (anvil#0) so the proxy-creation tx never interleaves
/// with the composer's `loadExecutionTable` system-tx nonce stream, and
/// distinct from the outbound sender (anvil#3) so their L2 nonces never
/// collide.
const PROXY_CREATOR_ADDR: Address = address!("0x3C44Cdddb6a900fa2b585dD299E03D12FA4293bC");

/// Outbound settle round-trip (L2 sim → loadExecutionTable + user tx in
/// the Sync block → postBatch on L1 → `_processNCalls` executes
/// `setValue` on L1) can take a while behind the slot scheduler; be
/// generous.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(150);

/// A fresh-datadir follower must boot reth, subscribe to the embedded
/// L1, catch_up from the deploy block (re-deriving every settled batch
/// incl. the outbound one), and land its safe head on the attested
/// settled root — all while the composer stays alive serving its
/// embedded L1. Generous per the brief (60-120s).
const FOLLOWER_TIMEOUT: Duration = Duration::from_secs(180);

/// A NON-PROXY EOA on the L2 — the destination of S5's negative-control
/// plain value transfer (anvil#3, `0x90F7…`). Distinct from the proxy
/// `P`, so the ingress classifier (keyed on
/// `to ∈ EEZ_CROSS_CHAIN_PROXY_ADDRESSES`) must NOT tag the transfer
/// outbound — it must mine as a normal L2 tx. Sent from anvil#4
/// (`ANVIL_KEY_4`), an EOA used by no other phase of this test, so its
/// L2 nonce can't collide.
const NEGATIVE_CONTROL_RECIPIENT: Address =
    address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906");

/// S2+S3 — the full OUTBOUND (L2→L1) acceptance:
///
/// **S2** (in the Phase-A window): deploy `Value(0)` on the embedded L1,
/// create the L2 cross-chain proxy `P =
/// EEZL2.createCrossChainProxy(L1Value, MAINNET=0)`, restart the node
/// (Phase B) with `EEZ_CROSS_CHAIN_PROXY_ADDRESSES=P`. Assert `P` is
/// non-zero and `authorizedProxies(P).originalAddress == L1Value`.
///
/// **S3**: send the OUTBOUND user tx (L2-chain-id, `to=P`,
/// `setValue(42)`) to the L2 ingress, wait for it to settle, and assert
/// (a) `Value.value() == 42` on the embedded L1 (the cross-chain
/// `setValue` executed during `postBatch`) and (b) `rollups[1].stateRoot`
/// on L1 EEZ == the L2 safe-block state root (the user-tx-inclusive
/// settled root).
///
/// REGRESSION GUARD for the OUTBOUND settlement-entry `destinationRollupId`
/// bug (found by this very test): the L2→L1 settlement entry was composed with
/// `destinationRollupId = MAINNET(0)` (the call's target), which
/// `assert_batch_registry_native` (CORRECTLY) rejected, degrading every
/// outbound slot to an empty postBatch so the cross-chain `setValue` never
/// reached L1 `_processNCalls`. The protocol's canonical structure
/// (`sync-rollups-protocol/test/IntegrationTestBridge.t.sol`: an immediate
/// entry carrying an L1-targeted `l2ToL1Call` sets
/// `destinationRollupId = L2_ROLLUP_ID`) requires the SOURCE rollup id —
/// `EEZ.sol`'s `_validateStructure` membership-checks the same field and
/// MAINNET(0) can never be a registered member, while `_processNCalls` routes
/// execution off each call's `sourceRollupId`, never the entry's destination.
/// Fixed in `prepare_post_batch_raw` (composer.rs) by setting the spliced
/// outbound entry's `destinationRollupId = rid`; the gate stays UNCHANGED. No
/// contract/protocol change. This test asserts the full outbound round-trip
/// settles: `Value.value() == 42` on L1 + L1/L2 root reconciliation.
///
/// **S4** (the core P-1 exit assertion — exercises the A2.4 deriver
/// reconstruction): with the COMPOSER still ALIVE (its in-process embedded
/// L1 keeps serving), spawn a FRESH-datadir FOLLOWER (proof-signer key
/// removed → `Mode::Follower`; `EEZ_L1_RPC_URL` → the composer's embedded
/// L1 HTTP). The follower re-derives the settled state from L1 ALONE — its
/// deriver fetches the outbound `postBatch` entries, partitions the outbound
/// immediate by `proxyEntryHash==0`, rebuilds the `loadExecutionTable`
/// system tx via the SHARED `build_l2_outbound_entry`, interleaves the user
/// tx from the DA slot, and replays. Assert the follower's safe-block state
/// root lands on the L1-attested settled root == the S3 settled root (the
/// user-tx-inclusive outbound root). This exact-root re-derivation IS the
/// byte-equality proof: the deriver's `build_sync_block` == the composer's
/// by construction, so an identical re-derived root means the reconstructed
/// `loadExecutionTable` + interleaving were byte-identical.
///
/// **S5** (negative control): a PLAIN L2 value transfer to a non-proxy EOA
/// is NOT classified outbound — it mines as a normal L2 tx (gets a receipt)
/// without producing a cross-chain settlement entry (`Value.value()`
/// unchanged by it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_s3_s4_s5_outbound_set_value_settles_and_follower_rederives() {
    let l2_genesis = fresh_cross_chain_genesis().expect("fresh cross-chain genesis");
    let cfg = NodeConfig {
        genesis_path: Some(l2_genesis.path.as_path()),
        ..Default::default()
    };

    let l1 = EmbeddedL1::alloc().expect("alloc embedded L1");
    let l2_datadir = tempfile::tempdir().expect("l2 datadir");

    let rollup_id = 1u64;

    // ── Phase A — placeholder registry (see S1 for the rationale). ──
    let phase_a_env = cross_chain_env(
        &l1,
        ANVIL_KEY_1, // poster = anvil#1 (≠ deploy/proof-signer anvil#0)
        PLACEHOLDER_ADDRESS,
        PLACEHOLDER_ADDRESS,
        0,
        rollup_id,
        None, // no proxy env yet — added in Phase B
    );
    let node_a = NodeHandle::spawn_with("composer-a", l2_datadir.path(), &cfg, &phase_a_env)
        .expect("spawn phase-A node");

    // Embedded L1 RPC up.
    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("embedded L1 RPC did not come up");

    // ── Deploy the protocol onto the embedded L1 (anvil#0 = proof
    // signer; see S1). `initialState` = cross-chain genesis state root.
    let dep = deploy_contracts_with_initial(&l1.rpc_url, ANVIL_KEY, l2_genesis.state_root)
        .await
        .expect("deploy protocol onto embedded L1");

    // ── S2.a — deploy `Value(0)` on the SAME embedded L1 (the L1
    // settlement target). Deploy from anvil#0 too — sequentially after
    // the protocol deploy, so no nonce race.
    let l1_value_addr = deploy_value(&l1.rpc_url, ANVIL_KEY, 0)
        .await
        .expect("deploy Value(0) on embedded L1");
    assert_ne!(
        l1_value_addr,
        Address::ZERO,
        "Value deploy returned the zero address"
    );
    // Sanity: fresh Value reads 0.
    let v0 = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read Value.value() post-deploy");
    assert_eq!(v0, U256::ZERO, "fresh Value(0) should read 0, got {v0}");
    eprintln!("[S2] Value deployed @ {l1_value_addr:#x}  value()={v0}");

    // ── S2.b — create the L2 cross-chain proxy on EEZL2 (0x42..07).
    // The L2 RPC is the Phase-A node's own (it serves the L2 even with a
    // placeholder L1 registry — only the composer's postBatch path is
    // idle). Creator = anvil#2 (≠ SYSTEM_ADDRESS anvil#0, ≠ outbound
    // sender anvil#3).
    let l2_rpc = node_a.l2_rpc_url();
    wait_for_l2_rpc(&l2_rpc, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase A L2 RPC did not come up");
    let proxy = create_l2_cross_chain_proxy(
        &l2_rpc,
        ANVIL_KEY_2,
        CCM_L2_ADDRESS,
        l1_value_addr,
        MAINNET_ROLLUP_ID,
    )
    .await
    .expect("createCrossChainProxy on L2");
    assert_ne!(proxy, Address::ZERO, "proxy P is the zero address");
    eprintln!("[S2] L2 cross-chain proxy P = {proxy:#x}");

    // ── S2 assertion — the proxy is registered against the L1 Value.
    let registered = proxy_original_address(&l2_rpc, CCM_L2_ADDRESS, proxy)
        .await
        .expect("authorizedProxies(P).originalAddress");
    assert_eq!(
        registered, l1_value_addr,
        "authorizedProxies({proxy:#x}).originalAddress = {registered:#x}, expected the L1 Value {l1_value_addr:#x}",
    );
    eprintln!("[S2] authorizedProxies(P).originalAddress == L1 Value ✓");

    // ── Bury the deploys + proxy creation deep enough to flush before
    // the Phase-A kill (reth 2-block persistence window; see S1).
    let after_deploy = l1_block_number(&l1.rpc_url).await;
    wait_for_l1_blocks(&l1.rpc_url, after_deploy + 6, Duration::from_secs(120))
        .await
        .expect("embedded L1 did not advance to persist the deploy");

    // ── Phase B — restart with the REAL addresses AND the proxy env so
    // the ingress classifier tags `to=P` txs as OUTBOUND.
    drop(node_a);
    wait_for_rpc_down(&l1.rpc_url, Duration::from_secs(60))
        .await
        .expect("Phase A embedded L1 did not shut down");

    let phase_b_env = cross_chain_env(
        &l1,
        ANVIL_KEY_1,
        dep.eez_address,
        dep.mock_ps_address,
        dep.deploy_block,
        dep.rollup_id,
        Some(&[proxy]), // EEZ_CROSS_CHAIN_PROXY_ADDRESSES = P
    );
    let node_b = NodeHandle::spawn_with("composer-b", l2_datadir.path(), &cfg, &phase_b_env)
        .expect("spawn phase-B node");
    let l2_rpc_b = node_b.l2_rpc_url();
    wait_for_l2_rpc(&l2_rpc_b, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase B L2 RPC did not come up");
    wait_for_l2_rpc(&l1.rpc_url, L1_BOOT_TIMEOUT)
        .await
        .expect("Phase B embedded L1 RPC did not come up");

    // The proxy survived the restart (same L2 datadir): re-assert.
    let registered_b = proxy_original_address(&l2_rpc_b, CCM_L2_ADDRESS, proxy)
        .await
        .expect("Phase B authorizedProxies(P)");
    assert_eq!(
        registered_b, l1_value_addr,
        "proxy registration lost across the Phase-A→B restart",
    );

    // ── Wait for the Phase-B composer to start settling (≥1 batch)
    // before firing the outbound tx — proves the embedded L1 + EEZ are
    // live and the composer's postBatch stream is flowing.
    let l1_chain = L1Chain::new(&l1.rpc_url, &dep);
    let batches_before = l1_chain
        .wait_for_batches(1, BATCH_TIMEOUT)
        .await
        .expect("Phase B composer did not settle any anchor batch");
    eprintln!("[S3] Phase B settling; batches so far = {batches_before}");

    // ── S3 — the OUTBOUND user tx. L2-chain-id (fixture = 1), to=P,
    // data = setValue(42), from anvil#3 (≠ proxy creator, ≠ system).
    let chain_id = {
        use alloy_provider::{Provider, ProviderBuilder};
        ProviderBuilder::new()
            .connect_http(l2_rpc_b.parse().unwrap())
            .get_chain_id()
            .await
            .expect("eth_chainId on L2")
    };
    assert_eq!(
        chain_id, L2_CHAIN_ID,
        "L2 fixture chainId changed; outbound tx must be signed for the L2 chain",
    );
    // anvil#3 must not collide with the proxy creator (anvil#2) at the
    // same address — they are distinct EOAs, asserted here for clarity.
    assert_ne!(
        PROXY_CREATOR_ADDR,
        address!("0x90F79bf6EB2c4f870365E785982E1f101E93b906"),
        "proxy creator and outbound sender must be distinct EOAs",
    );

    let tx_hash = send_outbound_set_value(&l2_rpc_b, ANVIL_KEY_3, proxy, OUTBOUND_VALUE, U256::ZERO)
        .await
        .expect("submit outbound setValue(42) to L2 ingress");
    eprintln!(
        "[S3] outbound user tx submitted: chain_id={chain_id} to={proxy:#x} \
         data=setValue({OUTBOUND_VALUE}) hash={tx_hash:#x}"
    );

    // ── S3 acceptance (a) — Value.value() == 42 on the embedded L1.
    // Poll: the round-trip (drain → loadExecutionTable + user tx in the
    // Sync block → postBatch → L1 _processNCalls) takes a few slots.
    let settled = wait_for(SETTLE_TIMEOUT, || async {
        let v = read_value(&l1.rpc_url, l1_value_addr).await?;
        Ok((v == U256::from(OUTBOUND_VALUE)).then_some(v))
    })
    .await;
    let final_value = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read final Value.value()");
    assert!(
        settled.is_ok(),
        "OUTBOUND setValue(42) did not settle on L1: Value.value() = {final_value} \
         (expected {OUTBOUND_VALUE}). The outbound drain / postBatch splice did not \
         execute the cross-chain call. Inspect the composer log for \
         eez.composer.cc_compose.outbound_* events.",
    );
    eprintln!("[S3] ACCEPTANCE (a): L1 Value.value() == {final_value} ✓");

    // ── S3 acceptance (c) — the OUTBOUND user tx's own L2 receipt is SUCCESS
    // (regression pin for the RollingHashMismatch fix). Before the lean
    // `build_l2_outbound_entry` shape, the loaded entry re-delivered the call
    // to the codeless L1 target ON L2, so `_consumeAndExecute` reverted
    // `RollingHashMismatch` (EEZL2.sol:422) and the user tx mined with
    // status=0 — even though the L1 effect (a) still landed (settlement is
    // tx-based, revert-agnostic, so acceptance (a)/(b) passed regardless).
    // The lean entry (callCount=0, incomingCalls=[], rollingHash=0) makes
    // `_processNCalls(0)` a no-op so the consume passes and the tx succeeds.
    let outbound_status = wait_for_l2_tx_receipt(&l2_rpc_b, tx_hash, SETTLE_TIMEOUT)
        .await
        .expect("outbound user tx never got an L2 receipt");
    assert!(
        outbound_status,
        "OUTBOUND user tx {tx_hash:#x} reverted on L2 (receipt status=0) — expected SUCCESS. \
         A populated outbound entry re-delivers the call to a codeless L1 target on L2 → \
         RollingHashMismatch (EEZL2.sol:422). The lean build_l2_outbound_entry shape must make \
         the on-chain consume a no-op so the user tx mines successfully.",
    );
    eprintln!("[S3] ACCEPTANCE (c): outbound user tx L2 receipt status == success ✓");

    // ── S3 acceptance (b) — L1 rollups[1].stateRoot == L2 safe-block
    // state root (the user-tx-inclusive settled root). Poll until the
    // contract's tracked root matches a settled L2 safe head — both
    // advance asynchronously, so wait for convergence. Capture the safe
    // block's (number, root): the block height is PINNED here for the S4
    // follower comparison — the composer's safe head keeps advancing past
    // this block, so S4 must compare the follower at THIS height, not at
    // its (later) current head.
    let reconciled = wait_for(SETTLE_TIMEOUT, || async {
        let l1_root = state_root(&l1.rpc_url, dep.eez_address, dep.rollup_id).await?;
        let l2 = safe_block_number_and_root(&l2_rpc_b).await?;
        Ok(match l2 {
            Some((num, root))
                if root != alloy_primitives::B256::ZERO && root == l1_root =>
            {
                Some((l1_root, num, root))
            }
            _ => None,
        })
    })
    .await;
    let (settled_block_number, settled_root) = match reconciled {
        Ok((l1_root, num, l2_root)) => {
            assert_eq!(l1_root, l2_root, "reconciled roots must be equal");
            eprintln!(
                "[S3] ACCEPTANCE (b): L1 rollups[{}].stateRoot == L2 safe root == {l1_root:#x} \
                 (L2 block #{num}) ✓",
                dep.rollup_id
            );
            (num, l2_root)
        }
        Err(e) => {
            let l1_root = state_root(&l1.rpc_url, dep.eez_address, dep.rollup_id)
                .await
                .unwrap_or_default();
            let l2_root = safe_block_state_root(&l2_rpc_b)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            panic!(
                "L1↔L2 root reconciliation failed ({e}): \
                 L1 rollups[{}].stateRoot = {l1_root:#x}, L2 safe root = {l2_root:#x}. \
                 setValue(42) executed on L1 (acceptance a passed) but the tracked \
                 root never matched the L2 safe head — a settled-root mismatch.",
                dep.rollup_id,
            );
        }
    };
    assert_ne!(
        settled_root,
        alloy_primitives::B256::ZERO,
        "S3 settled root is zero — nothing for the follower to re-derive",
    );
    eprintln!(
        "[S4] S3 settled root (the follower's target) = {settled_root:#x} \
         @ L2 block #{settled_block_number}"
    );

    // ─── S5 — NEGATIVE CONTROL ────────────────────────────────────────
    // A PLAIN L2 value transfer (EIP-1559, `to` = a non-proxy EOA, signed
    // for the L2 chain) must be classified NORMAL, not OUTBOUND: the
    // ingress classifier keys on `to ∈ EEZ_CROSS_CHAIN_PROXY_ADDRESSES`
    // (= {P}); the recipient here is NOT P, so the tx is neither held nor
    // turned into a cross-chain settlement entry. Robust + deterministic
    // assertion: (1) it MINES (gets an L2 receipt) — proving it was NOT
    // held in the cross-chain HeldPool; (2) the proxy's L1 `Value.value()`
    // stays 42 — proving the transfer produced NO outbound settlement.
    // Run BEFORE spawning the follower so it lands in an already-settled
    // batch the follower will also re-derive (and ignore as a plain tx).
    let value_before_ctrl = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read Value before negative control");
    assert_eq!(
        value_before_ctrl,
        U256::from(OUTBOUND_VALUE),
        "precondition: Value == 42 before the negative control",
    );
    assert_ne!(
        NEGATIVE_CONTROL_RECIPIENT, proxy,
        "negative-control recipient must NOT be the cross-chain proxy P",
    );
    let ctrl_hash = send_l2_value_transfer(
        &l2_rpc_b,
        ANVIL_KEY_4,
        NEGATIVE_CONTROL_RECIPIENT,
        U256::from(1u64),
    )
    .await
    .expect("submit negative-control plain L2 value transfer");
    eprintln!(
        "[S5] negative-control plain transfer submitted: to={NEGATIVE_CONTROL_RECIPIENT:#x} \
         (≠ proxy P) hash={ctrl_hash:#x}"
    );
    let ctrl_status = wait_for_l2_tx_receipt(&l2_rpc_b, ctrl_hash, SETTLE_TIMEOUT)
        .await
        .expect(
            "negative-control tx never mined — a plain non-proxy L2 transfer must NOT be \
             held by the cross-chain ingress classifier",
        );
    assert!(
        ctrl_status,
        "negative-control plain L2 transfer reverted; expected a successful normal-tx receipt",
    );
    // Give the composer a couple of L1 blocks to settle whatever batch the
    // control tx landed in, then prove it produced NO outbound settlement:
    // the L1 Value is still 42 (the control tx is a pure L2 value transfer
    // with no cross-chain effect).
    let after_ctrl = l1_block_number(&l1.rpc_url).await;
    wait_for_l1_blocks(&l1.rpc_url, after_ctrl + 4, Duration::from_secs(60))
        .await
        .expect("embedded L1 did not advance after the negative control");
    let value_after_ctrl = read_value(&l1.rpc_url, l1_value_addr)
        .await
        .expect("read Value after negative control");
    assert_eq!(
        value_after_ctrl,
        U256::from(OUTBOUND_VALUE),
        "negative control changed L1 Value to {value_after_ctrl} — a plain L2 transfer \
         was (wrongly) classified outbound and settled a cross-chain call",
    );
    eprintln!(
        "[S5] ACCEPTANCE: plain transfer mined on L2 (receipt ok) and L1 Value unchanged \
         (== {value_after_ctrl}); not classified outbound ✓"
    );

    // ─── S4 — FRESH FOLLOWER RE-DERIVES THE SETTLED ROOT (CORE P-1) ────
    // Build the FOLLOWER env from the composer's Phase-B env, then DEMOTE
    // it to a follower:
    //   - REMOVE `EEZ_PROOF_SIGNER_KEY` → `Mode::from_env` returns Follower
    //     (main.rs:73-80: L1 RPC set + proof signer UNSET). With it kept,
    //     the node would boot as a Composer and spawn its OWN empty
    //     embedded L1 (wrong L1, never re-derives the settled batch).
    //   - REMOVE the embedded-L1 knobs that would make it try to bind the
    //     composer's pinned ports / datadir. A follower does NOT spawn an
    //     embedded L1; it reads L1 over `EEZ_L1_RPC_URL`. Pin
    //     `EEZ_L1_EMBEDDED=0` and point both L1 URLs at the COMPOSER's
    //     embedded L1 HTTP (the one the live composer node is serving).
    //   - KEEP the cross-chain system-tx context (`EEZ_L2_SYSTEM_KEY`,
    //     `EEZ_CCM_L2_ADDRESS`, `EEZ_ROLLUP_ID`) so the deriver rebuilds
    //     the outbound `loadExecutionTable` system tx (main.rs:766-772 /
    //     build_follower_system_tx_cfg), and the L1Watcher env
    //     (`EEZ_REGISTRY_ADDRESS`, `EEZ_REGISTRY_DEPLOY_BLOCK`,
    //     `EEZ_MOCK_PROOF_SYSTEM_ADDRESS`) so it reads postBatch events.
    let follower_env = {
        let mut e = cross_chain_env(
            &l1,
            ANVIL_KEY_1,
            dep.eez_address,
            dep.mock_ps_address,
            dep.deploy_block,
            dep.rollup_id,
            Some(&[proxy]),
        );
        e = remove_env(e, "EEZ_PROOF_SIGNER_KEY"); // → Mode::Follower
        // A follower reads L1 over RPC; it must NOT spawn an embedded L1
        // (which would race the live composer for the pinned ports). The
        // L1 URLs already point at the composer's embedded L1 (cross_chain_env
        // → l1_env sets both to l1.rpc_url == http://127.0.0.1:<http_port>).
        e = common::override_env(e, "EEZ_L1_EMBEDDED", "0");
        // The OUTBOUND-reconstruction proof scrapes `eez.deriver.reconcile.*`
        // (INFO-level). `cross_chain_env` defaults RUST_LOG to `warn`
        // (unless EEZ_TEST_LOG is set), which would hide them. Force the
        // deriver target to `info` here so the markers are always emitted —
        // composed with EEZ_TEST_LOG if the runner set a richer filter.
        let base_log = std::env::var("EEZ_TEST_LOG").unwrap_or_else(|_| "warn".to_string());
        e = common::override_env(e, "RUST_LOG", &format!("{base_log},eez_deriver=info"));
        e
    };
    // Sanity: the follower env must NOT carry a proof signer (else it boots
    // as a composer), and its L1 RPC must be the composer's embedded L1.
    assert!(
        !follower_env.iter().any(|(k, _)| *k == "EEZ_PROOF_SIGNER_KEY"),
        "follower env still carries EEZ_PROOF_SIGNER_KEY → would boot as composer",
    );
    let follower_l1_rpc = follower_env
        .iter()
        .find(|(k, _)| *k == "EEZ_L1_RPC_URL")
        .map(|(_, v)| v.clone())
        .expect("follower env missing EEZ_L1_RPC_URL");
    assert_eq!(
        follower_l1_rpc, l1.rpc_url,
        "follower EEZ_L1_RPC_URL must point at the composer's embedded L1",
    );
    eprintln!(
        "[S4] follower env: Mode=Follower (EEZ_PROOF_SIGNER_KEY removed), \
         EEZ_L1_RPC_URL={follower_l1_rpc} (composer's embedded L1), \
         EEZ_L1_EMBEDDED=0"
    );

    // Fresh datadir (NOT the composer's). Same cross-chain L2 genesis so
    // the follower's genesis state root == the registered initialState.
    // `clean_cwd: true` runs the node from a `.env`-free working dir so
    // the repo's `.env` doesn't silently re-add `EEZ_PROOF_SIGNER_KEY`
    // via dotenvy (which would re-promote the follower to a composer —
    // see `NodeConfig::clean_cwd`).
    let follower_cfg = NodeConfig {
        genesis_path: cfg.genesis_path,
        clean_cwd: true,
    };
    let follower_datadir = tempfile::tempdir().expect("follower datadir");
    let follower = NodeHandle::spawn_with(
        "follower",
        follower_datadir.path(),
        &follower_cfg,
        &follower_env,
    )
    .expect("spawn fresh follower");
    wait_for_l2_rpc(&follower.l2_rpc_url(), L1_BOOT_TIMEOUT)
        .await
        .expect("follower L2 RPC did not come up");

    // The follower re-derives from L1 alone. Prove it reproduced the exact
    // OUTBOUND-settled L2 block. Two complementary, equally-valid signals
    // (the composer + its embedded L1 are STILL ALIVE here — node_b / l1
    // not dropped — so the follower's safe head keeps advancing PAST the
    // outbound block):
    //   (A) HEIGHT-PINNED: the follower's canonical block at the outbound
    //       height `settled_block_number` has state root == `settled_root`.
    //   (B) SAFE-HEAD MATCH: the follower's `safe` head root == settled_root
    //       at some poll (the outbound root WAS the follower's safe head).
    // Either proves byte-identical re-derivation. We accept whichever fires
    // first (robust against reth transiently not serving a freshly-derived
    // block by number while it's still optimistically reconciling). If the
    // height-pinned block is PRESENT but its root DIFFERS from settled_root,
    // that is a HARD failure (a real divergence) — surfaced immediately.
    let follower_outcome = wait_for(FOLLOWER_TIMEOUT, || async {
        // (A) height-pinned
        if let Ok(Some(r)) =
            block_state_root_at(&follower.l2_rpc_url(), settled_block_number).await
        {
            if r == settled_root {
                return Ok(Some(format!(
                    "height-pinned: block #{settled_block_number} root == settled root"
                )));
            }
            if r != alloy_primitives::B256::ZERO {
                // Present but divergent — a real re-derivation gap. Fail hard.
                return Err(anyhow::anyhow!(
                    "follower's re-derived block #{settled_block_number} root {r:#x} != \
                     composer's settled root {settled_root:#x}: the deriver's outbound \
                     reconstruction (loadExecutionTable rebuild / user-tx interleave) is NOT \
                     byte-identical to the composer's. This is a real A2.4 derivation gap."
                ));
            }
        }
        // (B) safe-head match
        if let Ok(Some(root)) = safe_block_state_root(&follower.l2_rpc_url()).await {
            if root == settled_root {
                return Ok(Some(
                    "safe-head: follower safe root == settled root".to_string(),
                ));
            }
        }
        Ok(None)
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "FOLLOWER did not re-derive the OUTBOUND-settled root {settled_root:#x} \
             (outbound height #{settled_block_number}) within {FOLLOWER_TIMEOUT:?}: {e}. \
             A follower that cannot reconstruct an OUTBOUND batch from L1 alone is a real \
             A2.4 deriver bug. Inspect the follower log for the deriver reconcile messages \
             ('reconcile_batch_blocks entered' / 'fetching postBatch entries' / \
             'built outbound load + inbound delivery system txs' with outbound≥1).",
        )
    });

    follower.assert_no_process_death();

    // The follower independently reproduced the user-tx-inclusive
    // OUTBOUND-settled L2 root from L1 alone. The exact-root match IS the
    // byte-equality proof: the deriver's reconstructed `loadExecutionTable`
    // system tx + the user-tx interleaving (from the DA slot) were
    // byte-identical to the composer's emit — same root ⇒ same block ⇒
    // same system txs by construction. A single differing byte (wrong
    // nonce, calldata, or tx order) would yield a different root.
    eprintln!(
        "[S4] ACCEPTANCE: fresh FOLLOWER re-derived the user-tx-inclusive OUTBOUND-settled \
         root {settled_root:#x} from L1 ALONE ({follower_outcome}) ✓"
    );

    // Belt-and-suspenders: prove the follower's deriver actually walked the
    // OUTBOUND reconstruction path (not some unrelated re-convergence).
    // The custom `event!(name: …)` tracing events render via reth's default
    // formatter as their MESSAGE strings (the `name:` field is not printed),
    // so scrape the messages emitted by `Deriver::reconcile_batch_blocks`
    // (eez_deriver::deriver):
    //   - "reconcile_batch_blocks entered" + "cross_chain=true" — the
    //     cross-chain reconcile ran;
    //   - "fetching postBatch entries via L1 RPC" — it fetched the batch's
    //     entries from L1 (codec-v1 fallback, l2_entries empty);
    //   - "built outbound load + inbound delivery system txs" with
    //     "outbound=" ≥ 1 — it partitioned the OUTBOUND immediate
    //     (proxyEntryHash==0) and rebuilt its loadExecutionTable system tx.
    // The exact-root match above is the real proof; these guard against a
    // future regression where the root matched for an unintended reason.
    // reth's tracing layer writes ANSI-colored output even to a file, so
    // field renders as `name<ESC…>=<ESC…>value` — a literal `key=value`
    // substring never matches. Strip CSI escape sequences first, then
    // collapse whitespace so `cross_chain = true` (now bare `=` after the
    // strip) matches `cross_chain=true`.
    let raw_log = std::fs::read_to_string(&follower.log_path).unwrap_or_default();
    let log = strip_ansi_and_normalize(&raw_log);
    let reconcile_ran = log
        .lines()
        .any(|l| l.contains("reconcile_batch_blocks entered") && l.contains("cross_chain=true"));
    let fetched_entries = log.contains("fetching postBatch entries via L1 RPC");
    let built_outbound = log.lines().any(|l| {
        l.contains("built outbound load + inbound delivery system txs")
            && l.contains("outbound=")
            && !l.contains("outbound=0 ")
    });
    eprintln!(
        "[S4] follower deriver markers: reconcile(cross_chain)={reconcile_ran} \
         fetch_postBatch_entries={fetched_entries} system_txs_built(outbound>0)={built_outbound}"
    );
    assert!(
        reconcile_ran && fetched_entries && built_outbound,
        "follower's deriver log lacks the OUTBOUND reconstruction markers \
         (reconcile_batch_blocks entered+cross_chain / fetching postBatch entries / \
         built outbound load … outbound>0). The matching root may have re-converged via \
         an unintended path. Set EEZ_TEST_LOG_DIR + EEZ_TEST_LOG=info and inspect {}.",
        follower.log_path.display(),
    );
}

/// Strip ANSI CSI escape sequences (`ESC [ … <final-byte>`) from a log
/// dump so structured-log field renders like
/// `key<ESC[0m><ESC[2m>=<ESC[0m>value` collapse to a bare `key=value`
/// the marker scrape can substring-match. reth's tracing layer emits
/// color codes even when writing to a file. Pure std (no extra dep): a
/// tiny state machine that drops bytes from `ESC` through the first
/// alphabetic terminator (`m`, `K`, …).
fn strip_ansi_and_normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // Consume `[` then the parameter/intermediate bytes up to and
            // including the final byte (an ASCII letter, e.g. `m`).
            if chars.peek() == Some(&'[') {
                chars.next();
                for cc in chars.by_ref() {
                    if cc.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            // A lone ESC not starting a CSI: just drop it.
        } else {
            out.push(c);
        }
    }
    out
}

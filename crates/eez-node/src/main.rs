//! Legacy compatibility entrypoint.
//!
//! New deployments should invoke `eez-composer`, `eez-follower`, or
//! `eez-dev-node` directly. This wrapper retains the historical
//! environment-derived role selection while launchers and tests migrate.

use eez_node::NodeRole;

fn main() -> eyre::Result<()> {
    let l1_enabled = std::env::var_os("EEZ_L1_RPC_URL").is_some();
    let can_attest = std::env::var_os("EEZ_PROVER_URL").is_some()
        || std::env::var_os("EEZ_PROOF_SIGNER_KEY").is_some();
    let role = match (l1_enabled, can_attest) {
        (false, _) => NodeRole::Standalone,
        (true, false) => NodeRole::Follower,
        (true, true) => NodeRole::Composer,
    };
    eez_node::run(role)
}

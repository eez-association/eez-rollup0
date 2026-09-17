//! Standalone stateless proof-signer daemon.

#[tokio::main]
async fn main() -> eyre::Result<()> {
    eez_prover_stateless::run().await
}

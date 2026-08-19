//! Unanchored interval-sequencer entrypoint for local development.

fn main() -> eyre::Result<()> {
    eez_node::run_dev_node()
}

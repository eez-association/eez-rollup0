//! Unanchored interval-sequencer entrypoint for local development.

fn main() -> eyre::Result<()> {
    eez_node::run(eez_node::NodeRole::Standalone)
}

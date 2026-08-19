//! Production composer node entrypoint.

fn main() -> eyre::Result<()> {
    eez_node::run(eez_node::NodeRole::Composer)
}

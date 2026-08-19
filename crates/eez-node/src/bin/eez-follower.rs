//! L1-derived follower node entrypoint.

fn main() -> eyre::Result<()> {
    eez_node::run(eez_node::NodeRole::Follower)
}

use anyhow::{Result, anyhow};
use tracing_subscriber::EnvFilter;

/// Initializes the global tracing subscriber with stderr output.
pub fn init_tracing(filter: EnvFilter) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))
}

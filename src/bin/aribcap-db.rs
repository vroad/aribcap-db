use anyhow::Result;
use clap::Parser;

use aribcap_db::cli::{DbArgs, DbCommand};

#[tokio::main]
async fn main() -> Result<()> {
    match DbArgs::parse().command {
        DbCommand::Serve(args) => aribcap_db::serve::run(args).await,
    }
}

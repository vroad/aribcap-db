use anyhow::Result;
use clap::Parser;

use aribcap_db::cli::{DbArgs, DbCommand};

#[tokio::main]
async fn main() -> Result<()> {
    match DbArgs::parse().command {
        DbCommand::Serve(args) => aribcap_db::serve::run(args).await,
        DbCommand::SearchRebuild(args) => {
            aribcap_db::search_db::run_rebuild(&args.data_dir).await?;
            println!("Search index rebuilt at {}", args.data_dir.display());
            Ok(())
        }
    }
}

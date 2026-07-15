use anyhow::{Context, Result};
use clap::Parser;

use aribcap_db::cli::{DbArgs, DbCommand};
use aribcap_db::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    match DbArgs::parse().command {
        DbCommand::Serve(args) => aribcap_db::serve::run(args).await,
        DbCommand::SearchRebuild(args) => {
            let config = Config::load(&args.config)?;
            let data_dir = config
                .serve
                .and_then(|serve| serve.data_dir)
                .context("set [serve].data_dir in the config file")?;
            aribcap_db::search_db::run_rebuild(&data_dir).await?;
            println!("Search index rebuilt at {}", data_dir.display());
            Ok(())
        }
    }
}

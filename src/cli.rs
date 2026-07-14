use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Normal,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ColorOption {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Parser)]
#[command(
    name = "aribcap-tail",
    about = "Tail JSONL streams produced by aribcap-dump"
)]
pub struct Args {
    #[arg(long, value_name = "PATH", help = "Path to the TOML config file")]
    pub config: PathBuf,

    #[arg(
        long = "stream",
        alias = "target",
        value_name = "NAME",
        help = "Stream name from [streams.<NAME>]; repeat to tail multiple streams"
    )]
    pub streams: Vec<String>,

    #[arg(long, help = "Tail every stream defined in the config")]
    pub all: bool,

    #[arg(
        long,
        value_enum,
        default_value = "normal",
        value_name = "FORMAT",
        help = "Output format"
    )]
    pub format: OutputFormat,

    #[arg(
        long,
        value_enum,
        default_value = "auto",
        value_name = "WHEN",
        help = "When to emit ANSI colors"
    )]
    pub color: ColorOption,

    #[arg(long, help = "Print diagnostics to stderr")]
    pub verbose: bool,
}

#[derive(Debug, Parser)]
#[command(name = "aribcap-db", about = "Store aribcap JSONL records")]
pub struct DbArgs {
    #[command(subcommand)]
    pub command: DbCommand,
}

#[derive(Debug, Subcommand)]
pub enum DbCommand {
    #[command(about = "Store JSONL streams as per-program archive files")]
    Serve(ServeArgs),
}

#[derive(Debug, Parser)]
pub struct ServeArgs {
    #[arg(long, value_name = "PATH", help = "Path to the TOML config file")]
    pub config: PathBuf,

    #[arg(long, value_name = "PATH", help = "Directory for stored JSONL records")]
    pub data_dir: Option<PathBuf>,

    #[arg(
        long,
        value_name = "DURATION",
        help = "How long to keep archived JSONL files, for example 30d"
    )]
    pub retention: Option<String>,

    #[arg(
        long,
        default_value = "info",
        value_name = "FILTER",
        help = "tracing-subscriber EnvFilter, for example info or aribcap_db=debug"
    )]
    pub log_level: String,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::{DbArgs, DbCommand};

    #[test]
    fn serve_accepts_config() {
        let args =
            DbArgs::try_parse_from(["aribcap-db", "serve", "--config", "config.toml"]).unwrap();

        let DbCommand::Serve(args) = args.command;
        assert_eq!(args.config, PathBuf::from("config.toml"));
    }
}

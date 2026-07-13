use std::path::PathBuf;

use clap::{Parser, ValueEnum};

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

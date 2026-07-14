use std::{
    io::{IsTerminal as _, Write as _},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use aribcap_db::{
    cli::{Args, ColorOption, OutputFormat},
    config::Config,
    logging, render, tail,
};

fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    logging::init_tracing(filter)?;

    let args = Args::parse();
    let config = Config::load(&args.config)?;
    let targets = tail::resolve_targets(&config, &args.streams, args.all)?;

    let stdout = std::io::stdout();
    let stdout_is_terminal = stdout.is_terminal();
    let use_color = match args.color {
        ColorOption::Auto => stdout_is_terminal,
        ColorOption::Always => true,
        ColorOption::Never => false,
    };
    let color_choice = if use_color {
        anstream::ColorChoice::AlwaysAnsi
    } else {
        anstream::ColorChoice::Never
    };

    // Serialize rendering and stdout writes from per-stream consumers so that output lines cannot
    // interleave.
    let output = Arc::new(Mutex::new(anstream::AutoStream::new(stdout, color_choice)));
    let format = args.format;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start tokio runtime")?;
    let result = runtime.block_on(tail::tail_targets(
        targets,
        move |event| {
            let output = output.clone();
            async move {
                // If a program consuming aribcap-tail's stdout stops reading, a synchronous
                // stdout write can block indefinitely. `spawn_blocking` keeps Tokio worker
                // threads available to handle Ctrl-C.
                tokio::task::spawn_blocking(move || {
                    let mut output = output
                        .lock()
                        .map_err(|_| anyhow!("stdout mutex poisoned"))?;
                    match format {
                        OutputFormat::Normal => render::write_normal_line(
                            &mut *output,
                            &event.label,
                            &event.line,
                            use_color,
                            render::terminal_width(),
                        ),
                        OutputFormat::Jsonl => render::write_jsonl_line(
                            &mut *output,
                            &event.label,
                            &event.line,
                            use_color,
                        ),
                    }
                    .context("failed to write output")
                    .and_then(|()| output.flush().context("failed to flush stdout"))
                })
                .await
                .context("stdout writer task failed")?
            }
        },
        async {
            tokio::signal::ctrl_c()
                .await
                .context("failed to listen for Ctrl-C")
        },
    ));
    // Let the process exit even when a task is still blocked in a synchronous
    // stdout write. A plain runtime drop waits for every task to finish and
    // would never return in that state.
    runtime.shutdown_background();

    if args.verbose {
        match &result {
            Ok(()) => eprintln!("received Ctrl-C; exiting"),
            Err(error) => eprintln!("stream stopped: {error:#}"),
        }
    }

    result
}

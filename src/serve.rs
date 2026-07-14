use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

use crate::{
    archive::{self, ArchiveEvent, ArchiveStore},
    cli::ServeArgs,
    config::{Config, ServeConfig},
    logging, tail,
};

const GC_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(15);

pub async fn run(args: ServeArgs) -> Result<()> {
    // -------------------------------------------------------------------------
    // Configuration and validation
    // -------------------------------------------------------------------------
    let filter = EnvFilter::try_new(&args.log_level).context("invalid --log-level filter")?;
    logging::init_tracing(filter)?;
    let config = Config::load(&args.config)?;
    let targets = config.resolve_all_streams()?;
    let data_dir = resolve_data_dir(&args, config.serve.as_ref())?;
    let retention = resolve_retention(&args, config.serve.as_ref())?;
    archive::validate_retention(retention)?;
    std::fs::create_dir_all(archive::records_root(&data_dir)).with_context(|| {
        format!(
            "failed to create archive directory in {}",
            data_dir.display()
        )
    })?;

    // -------------------------------------------------------------------------
    // Start ingest and the garbage-collection dry run
    // -------------------------------------------------------------------------
    let store = Arc::new(Mutex::new(ArchiveStore::new(&data_dir)));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal = shutdown_signal();
    tokio::pin!(signal);
    let mut ingest_task = tokio::spawn(ingest_loop(targets, store.clone(), shutdown_rx.clone()));
    let gc_dry_run_store = store.clone();
    let mut gc_dry_run_task = tokio::task::spawn_blocking(move || {
        archive::dry_run_garbage_collection(&gc_dry_run_store, retention)
    });

    // -------------------------------------------------------------------------
    // Wait for the garbage-collection dry run to finish
    // -------------------------------------------------------------------------
    let gc_dry_run_result = tokio::select! {
        signal_result = &mut signal => {
            let _ = shutdown_tx.send(true);
            gc_dry_run_task.abort();
            join(ingest_task, "JSONL ingest").await;
            return signal_result;
        }
        result = &mut ingest_task => {
            let error = unexpected_service_exit("JSONL ingest", result);
            tracing::error!(%error, "Service task stopped");
            let _ = shutdown_tx.send(true);
            gc_dry_run_task.abort();
            return Err(error);
        }
        result = &mut gc_dry_run_task => result,
    };
    log_gc_dry_run(gc_dry_run_result, retention, &data_dir);

    // -------------------------------------------------------------------------
    // Run and supervise the long-lived services
    // -------------------------------------------------------------------------
    let mut gc_task = tokio::spawn(gc_loop(store.clone(), retention, shutdown_rx.clone()));
    loop {
        tokio::select! {
            signal_result = &mut signal => {
                let _ = shutdown_tx.send(true);
                join(ingest_task, "JSONL ingest").await;
                join(gc_task, "archive garbage collection").await;
                return signal_result;
            }
            result = &mut ingest_task => {
                let error = unexpected_service_exit("JSONL ingest", result);
                tracing::error!(%error, "Service task stopped");
                let _ = shutdown_tx.send(true);
                join(gc_task, "archive garbage collection").await;
                return Err(error);
            }
            result = &mut gc_task => {
                let error = unexpected_service_exit("archive garbage collection", result);
                tracing::error!(%error, "Service task stopped; restarting");
                gc_task = tokio::spawn(gc_loop(
                    store.clone(),
                    retention,
                    shutdown_rx.clone(),
                ));
            }
        }
    }
}

fn log_gc_dry_run(
    result: std::result::Result<Result<archive::GarbageCollectionDryRun>, tokio::task::JoinError>,
    retention: Duration,
    data_dir: &std::path::Path,
) {
    let records_root = archive::records_root(data_dir);

    match result {
        Ok(Ok(dry_run)) => tracing::info!(
            eligible_files = dry_run.eligible_files,
            retention = %humantime::format_duration(retention),
            cutoff = %dry_run.cutoff,
            interval = %humantime::format_duration(GC_INTERVAL),
            records_root = %records_root.display(),
            "Archive garbage collection dry run finished",
        ),
        Ok(Err(error)) => tracing::warn!(
            %error,
            retention = %humantime::format_duration(retention),
            interval = %humantime::format_duration(GC_INTERVAL),
            records_root = %records_root.display(),
            "Archive garbage collection dry run failed",
        ),
        Err(error) => tracing::warn!(
            %error,
            retention = %humantime::format_duration(retention),
            interval = %humantime::format_duration(GC_INTERVAL),
            records_root = %records_root.display(),
            "Archive garbage collection dry run task failed",
        ),
    }
}

async fn ingest_loop(
    targets: Vec<crate::config::ResolvedStream>,
    store: Arc<Mutex<ArchiveStore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let connect_store = store.clone();
        let line_store = store.clone();
        let mut attempt_shutdown = shutdown.clone();
        let result = tail::tail_targets_isolated(
            targets.clone(),
            move |stream_name| {
                let store = connect_store.clone();
                async move {
                    tokio::task::block_in_place(|| {
                        archive::deactivate_stream(&store, &stream_name).with_context(|| {
                            format!("failed to reset archive state for stream '{stream_name}'")
                        })
                    })
                }
            },
            move |event| {
                let store = line_store.clone();
                async move {
                    let archive_event = tokio::task::block_in_place(|| {
                        archive::handle_line(&store, &event.stream_name, &event.line)
                    })?;
                    match archive_event {
                        Some(ArchiveEvent::ProgramStarted(path)) => tracing::info!(stream = event.stream_name, path = %path.display(), "Program started"),
                        Some(ArchiveEvent::SkippedNoProgram) => tracing::trace!(stream = event.stream_name, "Skipped line before present EIT"),
                        Some(ArchiveEvent::SkippedInvalidJson) => tracing::debug!(stream = event.stream_name, "Skipped invalid JSON line"),
                        None => {}
                    }
                    Ok(())
                }
            },
            async move {
                wait_for_shutdown(&mut attempt_shutdown).await;
                Ok(())
            },
        ).await;

        if requested(&shutdown) {
            return;
        }
        match result {
            Ok(()) => {
                tracing::warn!(retry_in = ?RETRY_INTERVAL, "JSONL ingest stopped unexpectedly")
            }
            Err(error) => tracing::warn!(%error, retry_in = ?RETRY_INTERVAL, "JSONL ingest failed"),
        }
        if sleep_or_shutdown(RETRY_INTERVAL, &mut shutdown).await {
            return;
        }
    }
}

async fn gc_loop(
    store: Arc<Mutex<ArchiveStore>>,
    retention: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if sleep_or_shutdown(GC_INTERVAL, &mut shutdown).await {
            return;
        }
        let result = tokio::task::block_in_place(|| archive::collect_garbage(&store, retention));
        match result {
            Ok(0) => {}
            Ok(files_removed) => {
                tracing::info!(files_removed, "Archive garbage collection finished")
            }
            Err(error) => tracing::warn!(%error, "Archive garbage collection failed"),
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = wait_for_shutdown(shutdown) => true,
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !requested(shutdown) {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

fn resolve_data_dir(args: &ServeArgs, serve: Option<&ServeConfig>) -> Result<PathBuf> {
    args.data_dir
        .clone()
        .or_else(|| serve.and_then(|serve| serve.data_dir.clone()))
        .context("set --data-dir or [serve].data_dir")
}

fn resolve_retention(args: &ServeArgs, serve: Option<&ServeConfig>) -> Result<Duration> {
    let value = args
        .retention
        .as_deref()
        .or_else(|| serve.and_then(|serve| serve.retention.as_deref()))
        .context("set --retention or [serve].retention")?;
    humantime::parse_duration(value).context("invalid retention duration")
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate()).context("failed to listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("failed to listen for Ctrl-C"),
        result = terminate.recv() => result.context("SIGTERM listener closed").map(|_| ()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for Ctrl-C")
}

async fn join(task: tokio::task::JoinHandle<()>, service: &str) {
    if let Err(error) = task.await {
        tracing::error!(service, %error, "Service task panicked during shutdown");
    }
}

fn unexpected_service_exit(
    service: &str,
    result: std::result::Result<(), tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(()) => anyhow!("{service} stopped unexpectedly"),
        Err(error) => anyhow!("{service} task failed: {error}"),
    }
}

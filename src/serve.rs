use std::{
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

use crate::{
    archive::{self, ArchiveEvent, ArchiveStore},
    cli::ServeArgs,
    config::{Config, ServeConfig},
    live::LiveBroadcaster,
    logging, search_db, server, tail,
};

const DEFAULT_LISTEN: &str = "127.0.0.1:40773";
const GC_INTERVAL: Duration = Duration::from_secs(60 * 60);
const SEARCH_INDEX_INTERVAL: Duration = Duration::from_secs(10);
const RETRY_INTERVAL: Duration = Duration::from_secs(15);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(args: ServeArgs) -> Result<()> {
    // -------------------------------------------------------------------------
    // Configuration and validation
    // -------------------------------------------------------------------------
    let filter = EnvFilter::try_new(&args.log_level).context("invalid --log-level filter")?;
    logging::init_tracing(filter)?;
    let config = Config::load(&args.config)?;
    let targets = config.resolve_all_streams()?;
    let data_dir = resolve_data_dir(&args, config.serve.as_ref())?;
    let listen = resolve_listen(&args, config.serve.as_ref())?;
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
    let live_broadcaster = Arc::new(LiveBroadcaster::new(
        targets.iter().map(|target| target.name.clone()),
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal = shutdown_signal();
    tokio::pin!(signal);
    let mut ingest_task = tokio::spawn(ingest_loop(
        targets,
        store.clone(),
        live_broadcaster.clone(),
        shutdown_rx.clone(),
    ));
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
    let search_db_path = search_db::search_db_path(&data_dir);
    let search_db_ready = Arc::new(AtomicBool::new(false));
    let (maintenance_shutdown_tx, maintenance_shutdown_rx) = watch::channel(false);
    let mut maintenance_task = tokio::spawn(archive_maintenance_loop(
        search_db_path.clone(),
        archive::records_root(&data_dir),
        store.clone(),
        retention,
        search_db_ready.clone(),
        maintenance_shutdown_rx,
    ));
    let app = server::router(
        data_dir.clone(),
        search_db_path.clone(),
        live_broadcaster,
        search_db_ready.clone(),
        shutdown_rx.clone(),
    );
    // Keep ingest and garbage collection running while HTTP is unavailable. After
    // a bind or serve error, the HTTP task waits 15 seconds and tries to bind and
    // serve again. If the HTTP task exits unexpectedly, `run()` restarts it.
    let mut http_task = tokio::spawn(http_server_loop(listen, app.clone(), shutdown_rx.clone()));
    loop {
        tokio::select! {
            signal_result = &mut signal => {
                let _ = shutdown_tx.send(true);
                tokio::join!(
                    join(ingest_task, "JSONL ingest"),
                    join_http_with_timeout(http_task, HTTP_SHUTDOWN_TIMEOUT),
                );
                let _ = maintenance_shutdown_tx.send(true);
                join(maintenance_task, "archive maintenance").await;
                return signal_result;
            }
            result = &mut ingest_task => {
                let error = unexpected_service_exit("JSONL ingest", result);
                tracing::error!(%error, "Service task stopped");
                let _ = shutdown_tx.send(true);
                tokio::join!(
                    join_http_with_timeout(http_task, HTTP_SHUTDOWN_TIMEOUT),
                );
                let _ = maintenance_shutdown_tx.send(true);
                join(maintenance_task, "archive maintenance").await;
                return Err(error);
            }
            result = &mut http_task => {
                let error = unexpected_service_exit("HTTP server", result);
                tracing::error!(%error, "Service task stopped; restarting");
                http_task = tokio::spawn(http_server_loop(
                    listen,
                    app.clone(),
                    shutdown_rx.clone(),
                ));
            }
            result = &mut maintenance_task => {
                let error = unexpected_service_exit("archive maintenance", result);
                tracing::error!(%error, "Service task stopped; restarting");
                maintenance_task = tokio::spawn(archive_maintenance_loop(
                    search_db_path.clone(),
                    archive::records_root(&data_dir),
                    store.clone(),
                    retention,
                    search_db_ready.clone(),
                    maintenance_shutdown_tx.subscribe(),
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
    live_broadcaster: Arc<LiveBroadcaster>,
    mut shutdown: watch::Receiver<bool>,
) {
    retry_until_shutdown(RETRY_INTERVAL, &mut shutdown, move |attempt_shutdown| {
        run_ingest_once(
            targets.clone(),
            store.clone(),
            live_broadcaster.clone(),
            attempt_shutdown,
        )
    })
    .await;
}

async fn run_ingest_once(
    targets: Vec<crate::config::ResolvedStream>,
    store: Arc<Mutex<ArchiveStore>>,
    live_broadcaster: Arc<LiveBroadcaster>,
    shutdown: watch::Receiver<bool>,
) {
    let connect_store = store.clone();
    let line_store = store;
    let mut attempt_shutdown = shutdown.clone();
    let result = tail::tail_targets_isolated(
        targets,
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
            let live_broadcaster = live_broadcaster.clone();
            async move {
                live_broadcaster.publish(&event.stream_name, &event.line);
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
    )
    .await;

    if requested(&shutdown) {
        return;
    }
    match result {
        Ok(()) => tracing::warn!(
            retry_in = ?RETRY_INTERVAL,
            "JSONL ingest stopped unexpectedly"
        ),
        Err(error) => {
            tracing::warn!(%error, retry_in = ?RETRY_INTERVAL, "JSONL ingest failed")
        }
    }
}

async fn http_server_loop(
    listen: SocketAddr,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) {
    retry_until_shutdown(RETRY_INTERVAL, &mut shutdown, move |attempt_shutdown| {
        run_http_server_once(listen, app.clone(), attempt_shutdown)
    })
    .await;
}

async fn run_http_server_once(
    listen: SocketAddr,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) {
    let listener = tokio::select! {
        _ = wait_for_shutdown(&mut shutdown) => return,
        result = tokio::net::TcpListener::bind(listen) => match result {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(
                    %listen,
                    %error,
                    retry_in = ?RETRY_INTERVAL,
                    "Failed to bind HTTP server; HTTP API is unavailable"
                );
                return;
            }
        },
    };

    tracing::info!(%listen, "Start aribcap-db HTTP server");

    let mut server_shutdown = shutdown.clone();
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            wait_for_shutdown(&mut server_shutdown).await;
        })
        .await;
    if requested(&shutdown) {
        return;
    }
    match result {
        Ok(()) => tracing::warn!(
            retry_in = ?RETRY_INTERVAL,
            "HTTP server stopped unexpectedly"
        ),
        Err(error) => tracing::warn!(
            %error,
            retry_in = ?RETRY_INTERVAL,
            "HTTP server failed"
        ),
    }
}

/// Runs service attempts until shutdown, waiting `retry_interval` between attempts.
///
/// The first attempt starts immediately. Each attempt must observe the provided
/// shutdown receiver and finish when shutdown is requested.
async fn retry_until_shutdown<RunOnce, Run>(
    retry_interval: Duration,
    shutdown: &mut watch::Receiver<bool>,
    mut run_once: RunOnce,
) where
    RunOnce: FnMut(watch::Receiver<bool>) -> Run,
    Run: Future<Output = ()>,
{
    loop {
        if requested(shutdown) {
            return;
        }
        run_once(shutdown.clone()).await;
        if requested(shutdown) {
            return;
        }
        if sleep_or_shutdown(retry_interval, shutdown).await {
            return;
        }
    }
}

async fn archive_maintenance_loop(
    db_path: PathBuf,
    records_root: PathBuf,
    store: Arc<Mutex<ArchiveStore>>,
    retention: Duration,
    search_db_ready: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let result = search_db::run_archive_maintenance(
            db_path.clone(),
            records_root.clone(),
            search_db::ArchiveMaintenanceConfig {
                index_interval: SEARCH_INDEX_INTERVAL,
                gc_interval: GC_INTERVAL,
                retention,
            },
            store.clone(),
            search_db_ready.clone(),
            shutdown.clone(),
        )
        .await;

        if requested(&shutdown) {
            if let Err(error) = result {
                tracing::warn!(%error, "Archive maintenance shutdown processing failed");
            }
            return;
        }

        match result {
            Ok(()) => tracing::warn!(
                retry_in = ?RETRY_INTERVAL,
                "Archive maintenance stopped unexpectedly"
            ),
            Err(error) => tracing::warn!(
                %error,
                retry_in = ?RETRY_INTERVAL,
                "Archive maintenance failed"
            ),
        }

        if sleep_or_shutdown(RETRY_INTERVAL, &mut shutdown).await {
            return;
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

fn resolve_listen(args: &ServeArgs, serve: Option<&ServeConfig>) -> Result<SocketAddr> {
    if let Some(listen) = args.listen {
        return Ok(listen);
    }
    serve
        .and_then(|serve| serve.listen.as_deref())
        .unwrap_or(DEFAULT_LISTEN)
        .parse()
        .context("invalid listen address")
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

async fn join_http_with_timeout(mut task: tokio::task::JoinHandle<()>, timeout: Duration) {
    match tokio::time::timeout(timeout, &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::error!(%error, "HTTP server task failed during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                ?timeout,
                "HTTP server did not stop gracefully; aborting connections"
            );
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                tracing::error!(%error, "HTTP server task failed while aborting");
            }
        }
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;

    fn serve_args(listen: Option<SocketAddr>) -> ServeArgs {
        ServeArgs {
            config: PathBuf::from("config.toml"),
            data_dir: None,
            listen,
            retention: None,
            log_level: "info".to_owned(),
        }
    }

    #[test]
    fn listen_uses_cli_then_config_then_default() {
        let cli_listen = "127.0.0.1:40800".parse().unwrap();
        let args = serve_args(Some(cli_listen));
        let config = ServeConfig {
            data_dir: None,
            listen: Some("127.0.0.1:40801".to_owned()),
            retention: None,
        };

        assert_eq!(resolve_listen(&args, Some(&config)).unwrap(), cli_listen);
        assert_eq!(
            resolve_listen(&serve_args(None), Some(&config)).unwrap(),
            "127.0.0.1:40801".parse().unwrap()
        );
        assert_eq!(
            resolve_listen(&serve_args(None), None).unwrap(),
            DEFAULT_LISTEN.parse().unwrap()
        );
    }

    #[test]
    fn listen_rejects_invalid_config_value() {
        let config = ServeConfig {
            data_dir: None,
            listen: Some("not-an-address".to_owned()),
            retention: None,
        };

        assert_eq!(
            resolve_listen(&serve_args(None), Some(&config))
                .unwrap_err()
                .to_string(),
            "invalid listen address"
        );
    }

    #[tokio::test]
    async fn retry_policy_runs_again_after_an_attempt_finishes() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let run_attempts = attempts.clone();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        retry_until_shutdown(Duration::ZERO, &mut shutdown_rx, move |_| {
            let attempt = run_attempts.fetch_add(1, Ordering::SeqCst);
            let shutdown_tx = shutdown_tx.clone();
            async move {
                if attempt == 1 {
                    shutdown_tx.send(true).unwrap();
                }
            }
        })
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http_shutdown_aborts_task_after_timeout() {
        let task = tokio::spawn(std::future::pending());

        join_http_with_timeout(task, Duration::ZERO).await;
    }
}

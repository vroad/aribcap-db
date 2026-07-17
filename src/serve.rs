use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::{
    archive::{self, ArchiveEvent, ArchiveStore},
    cli::ServeArgs,
    config::{Config, ListenAddr, ServeConfig},
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
    let client = config.build_http_client()?;
    let data_dir = resolve_data_dir(config.serve.as_ref())?;
    let addrs = resolve_listen_addrs(config.serve.as_ref())?;
    let retention = resolve_retention(config.serve.as_ref())?;
    let mcp_enabled = config.serve.as_ref().is_some_and(|serve| serve.mcp);
    archive::validate_retention(retention)?;
    std::fs::create_dir_all(archive::archive_root(&data_dir)).with_context(|| {
        format!(
            "failed to create archive directory in {}",
            data_dir.display()
        )
    })?;
    // Hold the data-directory lock for the lifetime of `run` to prevent
    // concurrent `serve` or `search-rebuild` processes for the same directory.
    let _data_dir_lock = search_db::acquire_data_dir_lock(&data_dir).await?;

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
        client,
        targets,
        store.clone(),
        live_broadcaster.clone(),
        shutdown_rx.clone(),
    ));

    // -------------------------------------------------------------------------
    // Wait for the garbage-collection dry run to finish
    // -------------------------------------------------------------------------
    let gc_dry_run_result = {
        let gc_dry_run = archive::dry_run_garbage_collection(&store, retention);
        tokio::pin!(gc_dry_run);
        tokio::select! {
            signal_result = &mut signal => {
                let _ = shutdown_tx.send(true);
                join(ingest_task, "JSONL ingest").await;
                return signal_result;
            }
            result = &mut ingest_task => {
                let error = unexpected_service_exit("JSONL ingest", result);
                tracing::error!(%error, "Service task stopped");
                let _ = shutdown_tx.send(true);
                return Err(error);
            }
            result = &mut gc_dry_run => result,
        }
    };
    log_gc_dry_run(gc_dry_run_result, retention, &data_dir);

    // -------------------------------------------------------------------------
    // Run and supervise the long-lived services
    // -------------------------------------------------------------------------
    let search_db_path = search_db::search_db_path(&data_dir);
    let search_db_ready = Arc::new(AtomicBool::new(false));
    let mcp_cancellation = cancellation_token_on_shutdown(shutdown_rx.clone());
    let (maintenance_shutdown_tx, _) = watch::channel(false);
    let spawn_archive_maintenance = |shutdown| {
        tokio::spawn(search_db::run_archive_maintenance(
            search_db_path.clone(),
            archive::archive_root(&data_dir),
            search_db::ArchiveMaintenanceConfig {
                index_interval: SEARCH_INDEX_INTERVAL,
                gc_interval: GC_INTERVAL,
                retention,
            },
            store.clone(),
            search_db_ready.clone(),
            shutdown,
        ))
    };
    let mut maintenance_task = spawn_archive_maintenance(maintenance_shutdown_tx.subscribe());
    let app = server::router(
        data_dir.clone(),
        search_db_path.clone(),
        live_broadcaster,
        search_db_ready.clone(),
        shutdown_rx.clone(),
        mcp_enabled,
        mcp_cancellation.clone(),
    );
    // Keep ingest and garbage collection running while HTTP is unavailable. Each
    // configured listener (TCP or Unix socket) is retried independently inside
    // `run_http_listeners`; if that supervisor task itself exits unexpectedly
    // (e.g. a panic), `run()` restarts it along with every listener.
    let mut http_task = tokio::spawn(run_http_listeners(
        addrs.clone(),
        app.clone(),
        shutdown_rx.clone(),
    ));
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
                let error = unexpected_service_exit("HTTP listeners", result);
                tracing::error!(%error, "Service task stopped; restarting");
                http_task = tokio::spawn(run_http_listeners(
                    addrs.clone(),
                    app.clone(),
                    shutdown_rx.clone(),
                ));
            }
            result = &mut maintenance_task => {
                let error = unexpected_service_exit("archive maintenance", result);
                tracing::error!(%error, "Service task stopped; restarting");
                maintenance_task =
                    spawn_archive_maintenance(maintenance_shutdown_tx.subscribe());
            }
        }
    }
}

fn log_gc_dry_run(
    result: Result<archive::GarbageCollectionDryRun>,
    retention: Duration,
    data_dir: &std::path::Path,
) {
    let archive_root = archive::archive_root(data_dir);

    match result {
        Ok(dry_run) => tracing::info!(
            eligible_files = dry_run.eligible_files,
            retention = %humantime::format_duration(retention),
            cutoff = %dry_run.cutoff,
            interval = %humantime::format_duration(GC_INTERVAL),
            archive_root = %archive_root.display(),
            "Archive garbage collection dry run finished",
        ),
        Err(error) => tracing::warn!(
            %error,
            retention = %humantime::format_duration(retention),
            interval = %humantime::format_duration(GC_INTERVAL),
            archive_root = %archive_root.display(),
            "Archive garbage collection dry run failed",
        ),
    }
}

async fn ingest_loop(
    client: reqwest::Client,
    targets: Vec<crate::config::ResolvedStream>,
    store: Arc<Mutex<ArchiveStore>>,
    live_broadcaster: Arc<LiveBroadcaster>,
    mut shutdown: watch::Receiver<bool>,
) {
    retry_until_shutdown(RETRY_INTERVAL, &mut shutdown, move |attempt_shutdown| {
        run_ingest_once(
            client.clone(),
            targets.clone(),
            store.clone(),
            live_broadcaster.clone(),
            attempt_shutdown,
        )
    })
    .await;
}

async fn run_ingest_once(
    client: reqwest::Client,
    targets: Vec<crate::config::ResolvedStream>,
    store: Arc<Mutex<ArchiveStore>>,
    live_broadcaster: Arc<LiveBroadcaster>,
    shutdown: watch::Receiver<bool>,
) {
    let connect_store = store.clone();
    let line_store = store;
    let mut attempt_shutdown = shutdown.clone();
    let result = tail::tail_targets_isolated(
        client,
        targets,
        move |stream_name| {
            let store = connect_store.clone();
            async move {
                tokio::task::block_in_place(|| archive::deactivate_stream(&store, &stream_name));
                Ok(())
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

/// Runs every configured listener (any mix of TCP and Unix sockets)
/// concurrently, restarting an individual listener in place if its task
/// exits unexpectedly, without affecting the others. Returns only on
/// shutdown.
async fn run_http_listeners(
    addrs: Vec<ListenAddr>,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut set = tokio::task::JoinSet::new();
    let mut addr_by_id = std::collections::HashMap::new();

    for addr in addrs {
        let handle = set.spawn(run_listener(addr.clone(), app.clone(), shutdown.clone()));
        addr_by_id.insert(handle.id(), addr);
    }

    loop {
        tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => {
                drain_after_shutdown(&mut set, &mut addr_by_id).await;
                return;
            }
            result = set.join_next_with_id(), if !set.is_empty() => {
                let Some(result) = result else { continue };
                if requested(&shutdown) {
                    continue;
                }
                let (id, task_result) = match result {
                    Ok((id, ())) => (id, Ok(())),
                    Err(join_error) => (join_error.id(), Err(join_error)),
                };
                let addr = addr_by_id
                    .remove(&id)
                    .expect("every spawned listener task is tracked in addr_by_id");
                let error = unexpected_service_exit(&addr.to_string(), task_result);
                tracing::error!(%error, "Service task stopped; restarting");
                let handle = set.spawn(run_listener(addr.clone(), app.clone(), shutdown.clone()));
                addr_by_id.insert(handle.id(), addr);
            }
        }
    }
}

/// Waits for every listener task in `set` to finish shutting down
/// gracefully, letting in-flight HTTP responses drain.
async fn drain_after_shutdown(
    set: &mut tokio::task::JoinSet<()>,
    addr_by_id: &mut std::collections::HashMap<tokio::task::Id, ListenAddr>,
) {
    while let Some(result) = set.join_next_with_id().await {
        let (id, task_result) = match result {
            Ok((id, ())) => (id, Ok(())),
            Err(join_error) => (join_error.id(), Err(join_error)),
        };
        addr_by_id.remove(&id);
        if let Err(join_error) = task_result {
            tracing::error!(%join_error, "HTTP listener task panicked during shutdown");
        }
    }
}

/// Dispatches to the transport-specific supervised server loop for one
/// configured address.
async fn run_listener(addr: ListenAddr, app: axum::Router, shutdown: watch::Receiver<bool>) {
    match addr {
        ListenAddr::Tcp(listen) => spawn_tcp_http_server(listen, app, shutdown).await,
        #[cfg(unix)]
        ListenAddr::UnixSocket(path) => spawn_unix_http_server(path, app, shutdown).await,
        #[cfg(not(unix))]
        ListenAddr::UnixSocket(_) => {
            unreachable!(
                "ListenAddr::UnixSocket is rejected during config resolution on this platform"
            )
        }
    }
}

/// Spawns the supervised TCP HTTP server task; the returned future never
/// resolves except on shutdown or a rare unhandled panic.
async fn spawn_tcp_http_server(
    listen: SocketAddr,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) {
    retry_until_shutdown(RETRY_INTERVAL, &mut shutdown, move |attempt_shutdown| {
        run_http_server_once(
            listen.to_string(),
            move || tokio::net::TcpListener::bind(listen),
            app.clone(),
            attempt_shutdown,
        )
    })
    .await;
}

/// Spawns the supervised Unix-socket HTTP server task; same restart-in-place
/// policy as [`spawn_tcp_http_server`], serving the same router.
#[cfg(unix)]
async fn spawn_unix_http_server(
    path: PathBuf,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) {
    let description = format!("unix:{}", path.display());
    retry_until_shutdown(RETRY_INTERVAL, &mut shutdown, move |attempt_shutdown| {
        let path = path.clone();
        run_http_server_once(
            description.clone(),
            move || {
                let path = path.clone();
                async move { bind_unix_listener(&path).await }
            },
            app.clone(),
            attempt_shutdown,
        )
    })
    .await;
}

/// A bound Unix-socket listener paired with the advisory lock file that
/// proves this process is the sole owner of the socket path. The lock is
/// held for as long as this value is alive. It releases automatically,
/// freeing the path for the next process, whether the listener shuts down
/// cleanly or the process dies unexpectedly.
#[cfg(unix)]
#[derive(Debug)]
struct LockedUnixListener {
    listener: tokio::net::UnixListener,
    _lock: std::fs::File,
}

#[cfg(unix)]
impl axum::serve::Listener for LockedUnixListener {
    type Io = <tokio::net::UnixListener as axum::serve::Listener>::Io;
    type Addr = <tokio::net::UnixListener as axum::serve::Listener>::Addr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        axum::serve::Listener::accept(&mut self.listener).await
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        axum::serve::Listener::local_addr(&self.listener)
    }
}

/// Appends `.lock` to a socket path without disturbing its existing
/// extension (a plain `with_extension` would replace `.sock` instead).
#[cfg(unix)]
fn lock_path_for(path: &Path) -> PathBuf {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".lock");
    PathBuf::from(lock_path)
}

/// Binds a Unix socket listener at `path` after acquiring the associated lock
/// and removing any stale socket file left by a previous run.
///
/// The returned [`LockedUnixListener`] holds the lock for its entire lifetime.
#[cfg(unix)]
async fn bind_unix_listener(path: &Path) -> io::Result<LockedUnixListener> {
    use std::os::unix::fs::FileTypeExt;

    let lock_path = lock_path_for(path);
    let lock_file = tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!(
                    "{} is already in use by another process",
                    lock_path.display()
                ),
            )),
            Err(std::fs::TryLockError::Error(error)) => Err(error),
        }
    })
    .await
    .expect("lock acquisition task should not panic")?;

    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "{} exists and is not a socket; refusing to remove it",
                        path.display()
                    ),
                ));
            }
            match tokio::net::UnixStream::connect(path).await {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("{} is already in use by another process", path.display()),
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    tokio::fs::remove_file(path).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    Ok(LockedUnixListener {
        listener,
        _lock: lock_file,
    })
}

async fn run_http_server_once<L, Bind, BindFut>(
    description: String,
    bind: Bind,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) where
    L: axum::serve::Listener,
    L::Addr: std::fmt::Debug,
    Bind: FnOnce() -> BindFut,
    BindFut: Future<Output = io::Result<L>>,
{
    let listener = tokio::select! {
        _ = wait_for_shutdown(&mut shutdown) => return,
        result = bind() => match result {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(
                    listen = %description,
                    %error,
                    retry_in = ?RETRY_INTERVAL,
                    "Failed to bind HTTP server; HTTP API is unavailable"
                );
                return;
            }
        },
    };

    tracing::info!(listen = %description, "Start aribcap-db HTTP server");

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

/// Returns a token that is cancelled when shutdown is requested.
fn cancellation_token_on_shutdown(mut shutdown: watch::Receiver<bool>) -> CancellationToken {
    let token = CancellationToken::new();
    let task_token = token.clone();
    tokio::spawn(async move {
        wait_for_shutdown(&mut shutdown).await;
        task_token.cancel();
    });
    token
}

fn requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

fn resolve_data_dir(serve: Option<&ServeConfig>) -> Result<PathBuf> {
    serve
        .and_then(|serve| serve.data_dir.clone())
        .context("set [serve].data_dir in the config file")
}

/// Resolves the addresses to listen on: `[serve].addrs` as configured,
/// falling back to the default TCP address when it is empty or absent.
fn resolve_listen_addrs(serve: Option<&ServeConfig>) -> Result<Vec<ListenAddr>> {
    let addrs = match serve.map(|serve| serve.addrs.as_slice()) {
        Some(addrs) if !addrs.is_empty() => addrs.to_vec(),
        _ => vec![ListenAddr::Tcp(
            DEFAULT_LISTEN
                .parse()
                .expect("valid default listen address"),
        )],
    };

    #[cfg(not(unix))]
    if let Some(path) = addrs.iter().find_map(|addr| match addr {
        ListenAddr::UnixSocket(path) => Some(path),
        ListenAddr::Tcp(_) => None,
    }) {
        anyhow::bail!(
            "unix listen addresses are not supported on this platform: {}",
            path.display()
        );
    }

    if let Some(duplicate) = first_duplicate(&addrs) {
        anyhow::bail!("duplicate [serve].addrs entry: {duplicate}");
    }

    Ok(addrs)
}

/// Returns the first entry in `addrs` that also appears earlier in the slice.
fn first_duplicate(addrs: &[ListenAddr]) -> Option<&ListenAddr> {
    let mut seen = std::collections::HashSet::with_capacity(addrs.len());
    addrs.iter().find(|addr| !seen.insert(*addr))
}

fn resolve_retention(serve: Option<&ServeConfig>) -> Result<Duration> {
    let value = serve
        .and_then(|serve| serve.retention.as_deref())
        .context("set [serve].retention in the config file")?;
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

    fn serve_config_with_addrs(addrs: Vec<ListenAddr>) -> ServeConfig {
        ServeConfig {
            data_dir: None,
            addrs,
            retention: None,
            mcp: false,
        }
    }

    #[test]
    fn listen_addrs_uses_config_addrs() {
        let config = serve_config_with_addrs(vec![
            ListenAddr::Tcp("0.0.0.0:40801".parse().unwrap()),
            ListenAddr::UnixSocket(PathBuf::from("/run/aribcap-db/config.sock")),
        ]);

        assert_eq!(resolve_listen_addrs(Some(&config)).unwrap(), config.addrs);
    }

    #[test]
    fn listen_addrs_falls_back_to_default_tcp_address() {
        assert_eq!(
            resolve_listen_addrs(None).unwrap(),
            vec![ListenAddr::Tcp(DEFAULT_LISTEN.parse().unwrap())]
        );
    }

    #[test]
    fn listen_addrs_rejects_duplicate_tcp_addrs() {
        let config = serve_config_with_addrs(vec![
            ListenAddr::Tcp("127.0.0.1:40773".parse().unwrap()),
            ListenAddr::Tcp("127.0.0.1:40773".parse().unwrap()),
        ]);

        let error = resolve_listen_addrs(Some(&config)).unwrap_err();
        assert!(
            error.to_string().contains("127.0.0.1:40773"),
            "unexpected error: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn listen_addrs_rejects_duplicate_unix_socket_paths() {
        let config = serve_config_with_addrs(vec![
            ListenAddr::UnixSocket(PathBuf::from("/run/aribcap-db/aribcap-db.sock")),
            ListenAddr::UnixSocket(PathBuf::from("/run/aribcap-db/aribcap-db.sock")),
        ]);

        let error = resolve_listen_addrs(Some(&config)).unwrap_err();
        assert!(
            error.to_string().contains("aribcap-db.sock"),
            "unexpected error: {error}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tcp_and_unix_listeners_serve_the_same_app_simultaneously() {
        let app = axum::Router::new().route("/ping", axum::routing::get(|| async { "pong" }));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();
        let tcp_task = tokio::spawn(run_http_server_once(
            tcp_addr.to_string(),
            move || std::future::ready(io::Result::Ok(tcp_listener)),
            app.clone(),
            shutdown_rx.clone(),
        ));

        let socket_path = std::env::temp_dir().join(format!(
            "aribcap-db-serve-test-{}-{}.sock",
            std::process::id(),
            tcp_addr.port()
        ));
        let _ = std::fs::remove_file(&socket_path);
        let unix_listener = bind_unix_listener(&socket_path).await.unwrap();
        let unix_task = tokio::spawn(run_http_server_once(
            format!("unix:{}", socket_path.display()),
            move || std::future::ready(io::Result::Ok(unix_listener)),
            app,
            shutdown_rx,
        ));

        let tcp_response = reqwest::get(format!("http://{tcp_addr}/ping"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(tcp_response, "pong");

        let unix_client = reqwest::Client::builder()
            .unix_socket(socket_path.as_path())
            .build()
            .unwrap();
        let unix_response = unix_client
            .get("http://localhost/ping")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(unix_response, "pong");

        tcp_task.abort();
        unix_task.abort();
        std::fs::remove_file(&socket_path).unwrap();
    }

    #[cfg(unix)]
    fn unique_socket_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "aribcap-db-serve-test-{label}-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_unix_listener_removes_stale_socket_and_rebinds() {
        let socket_path = unique_socket_path("stale");
        let first_listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        drop(first_listener);

        let second_listener = bind_unix_listener(&socket_path).await.unwrap();
        drop(second_listener);
        std::fs::remove_file(&socket_path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_unix_listener_refuses_regular_file() {
        let path = unique_socket_path("regular-file");
        std::fs::write(&path, b"not a socket").unwrap();

        let error = bind_unix_listener(&path).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"not a socket");

        std::fs::remove_file(&path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_unix_listener_refuses_socket_in_use() {
        let socket_path = unique_socket_path("live");
        let live_listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

        let error = bind_unix_listener(&socket_path).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);

        // The original listener must still be intact and accepting connections.
        let client_task = tokio::spawn({
            let socket_path = socket_path.clone();
            async move { tokio::net::UnixStream::connect(&socket_path).await }
        });
        let (accepted, connected) = tokio::join!(live_listener.accept(), client_task);
        accepted.unwrap();
        connected.unwrap().unwrap();

        drop(live_listener);
        std::fs::remove_file(&socket_path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_unix_listener_race_does_not_orphan_the_winning_listener() {
        let socket_path = unique_socket_path("race");
        // Leave a stale socket file behind, like a crashed prior instance would.
        let stale = tokio::net::UnixListener::bind(&socket_path).unwrap();
        drop(stale);

        // Two processes racing to clean up and rebind the same stale path:
        // without the lock, both would see it as stale and the loser would
        // unlink the winner's freshly bound socket out from under it.
        let (first, second) = tokio::join!(
            bind_unix_listener(&socket_path),
            bind_unix_listener(&socket_path)
        );

        let (winner, loser) = match (first, second) {
            (Ok(winner), Err(loser)) => (winner, loser),
            (Err(loser), Ok(winner)) => (winner, loser),
            other => panic!("expected exactly one winner and one AddrInUse loser, got {other:?}"),
        };
        assert_eq!(loser.kind(), io::ErrorKind::AddrInUse);

        // The winner must still be reachable: the loser must not have
        // unlinked and rebound the path out from under it.
        let client_task = tokio::spawn({
            let socket_path = socket_path.clone();
            async move { tokio::net::UnixStream::connect(&socket_path).await }
        });
        let (accepted, connected) = tokio::join!(winner.listener.accept(), client_task);
        accepted.unwrap();
        connected.unwrap().unwrap();

        drop(winner);
        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_file(lock_path_for(&socket_path)).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_http_listeners_finishes_in_flight_request_before_returning() {
        let started = Arc::new(tokio::sync::Notify::new());
        let started_signal = started.clone();
        let app = axum::Router::new().route(
            "/slow",
            axum::routing::get(move || {
                let started_signal = started_signal.clone();
                async move {
                    started_signal.notify_one();
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    "done"
                }
            }),
        );

        let socket_path = unique_socket_path("drain");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listeners_task = tokio::spawn(run_http_listeners(
            vec![ListenAddr::UnixSocket(socket_path.clone())],
            app,
            shutdown_rx,
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !socket_path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("socket file should be created within 1s");

        let client_task = tokio::spawn({
            let socket_path = socket_path.clone();
            async move {
                let client = reqwest::Client::builder()
                    .unix_socket(socket_path.as_path())
                    .build()
                    .unwrap();
                client
                    .get("http://localhost/slow")
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap()
            }
        });

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should have started");
        // Shutdown fires while the handler above is still sleeping. Measure
        // `listeners_task`'s elapsed time separately from `client_task`'s.
        // Joining them would let `client_task`'s own ~150ms wait for the
        // response mask a `listeners_task` that returned instantly by
        // aborting the connection.
        shutdown_tx.send(true).unwrap();

        let (client_result, listeners_elapsed) = tokio::join!(
            async {
                tokio::time::timeout(Duration::from_secs(1), client_task)
                    .await
                    .expect("client task should finish")
            },
            async {
                let started = std::time::Instant::now();
                tokio::time::timeout(Duration::from_secs(1), listeners_task)
                    .await
                    .expect("run_http_listeners should return once draining completes")
                    .unwrap();
                started.elapsed()
            }
        );
        assert_eq!(client_result.unwrap(), "done");
        assert!(
            listeners_elapsed >= Duration::from_millis(100),
            "run_http_listeners returned before the in-flight handler could have finished \
            ({listeners_elapsed:?} elapsed), meaning it aborted rather than drained the request"
        );

        std::fs::remove_file(&socket_path).unwrap();
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

    #[tokio::test]
    async fn cancellation_token_follows_shutdown_watch() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let token = cancellation_token_on_shutdown(shutdown_rx);

        assert!(!token.is_cancelled());
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
            .await
            .expect("token should cancel shortly after shutdown is requested");
    }
}

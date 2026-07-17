use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use sqlx::SqliteConnection;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::archive::{self, ArchiveStore};

use super::db::{open_and_migrate, search_db_path};
use super::ingest::{cleanup_index_for_deleted_files, ingest_once, ingest_paths};

/// Waits for up to `interval`, waking early if `shutdown` changes or its
/// sender is dropped. Returns `true` if shutdown was signaled.
async fn sleep_or_shutdown(interval: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(interval) => false,
        _ = shutdown.changed() => true,
    }
}

/// Indexes archive files modified since the previous pass.
/// Indexing errors are logged.
async fn apply_pending_archive_changes(
    conn: &mut SqliteConnection,
    archive_root: &Path,
    store: &Arc<Mutex<ArchiveStore>>,
    phase: &str,
) {
    let dirty_paths = archive::lock_store(store).snapshot_dirty_paths();
    for (path, generation) in dirty_paths {
        match ingest_paths(conn, archive_root, [path.clone()]).await {
            Ok(()) => {
                archive::lock_store(store).clear_dirty_path_if_unchanged(&path, generation);
            }
            Err(error) => {
                tracing::warn!(%error, phase, path = %path.display(), "search ingest pass failed");
            }
        }
    }
}

async fn apply_search_changes(
    conn: &mut Option<SqliteConnection>,
    db_path: &Path,
    archive_root: &Path,
    store: &Arc<Mutex<ArchiveStore>>,
    search_db_ready: &AtomicBool,
    cleanup_pending: &mut bool,
    phase: &str,
) {
    if conn.is_none() {
        search_db_ready.store(false, Ordering::Release);
        let mut new_conn = match open_and_migrate(db_path).await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(%error, phase, "failed to open search database");
                return;
            }
        };
        search_db_ready.store(true, Ordering::Release);
        if let Err(error) = ingest_once(&mut new_conn, archive_root).await {
            tracing::warn!(%error, phase, "search full ingest pass failed");
        }
        *conn = Some(new_conn);
    }

    let conn = conn.as_mut().expect("search connection was initialized");
    apply_pending_archive_changes(conn, archive_root, store, phase).await;
    apply_search_cleanup(conn, cleanup_pending, phase).await;
}

async fn apply_search_cleanup(
    conn: &mut SqliteConnection,
    cleanup_pending: &mut bool,
    phase: &str,
) {
    if *cleanup_pending {
        match cleanup_index_for_deleted_files(conn).await {
            Ok(_) => *cleanup_pending = false,
            Err(error) => tracing::warn!(%error, phase, "search cleanup pass failed"),
        }
    }
}

async fn collect_garbage(
    store: Arc<Mutex<ArchiveStore>>,
    retention: Duration,
) -> std::result::Result<Result<usize>, tokio::task::JoinError> {
    tokio::task::spawn_blocking(move || archive::collect_garbage(&store, retention)).await
}

fn gc_is_due(last_gc_finished_at: Instant, now: Instant, interval: Duration) -> bool {
    now.duration_since(last_gc_finished_at) >= interval
}

pub(crate) struct ArchiveMaintenanceConfig {
    pub(crate) index_interval: Duration,
    pub(crate) gc_interval: Duration,
    pub(crate) retention: Duration,
}

/// Keeps the search database synchronized and runs archive garbage collection.
///
/// The first search pass runs immediately. Later passes run after
/// `index_interval`; a due garbage-collection pass runs after the search pass.
/// Before exiting, the task makes one final attempt to index pending changes.
pub(crate) async fn run_archive_maintenance(
    db_path: PathBuf,
    archive_root: PathBuf,
    config: ArchiveMaintenanceConfig,
    store: Arc<Mutex<ArchiveStore>>,
    search_db_ready: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut conn = None;
    let mut cleanup_pending = true;
    let mut last_gc_finished_at = Instant::now();

    apply_search_changes(
        &mut conn,
        &db_path,
        &archive_root,
        &store,
        &search_db_ready,
        &mut cleanup_pending,
        "startup",
    )
    .await;

    while !sleep_or_shutdown(config.index_interval, &mut shutdown).await {
        apply_search_changes(
            &mut conn,
            &db_path,
            &archive_root,
            &store,
            &search_db_ready,
            &mut cleanup_pending,
            "steady-state",
        )
        .await;

        let now = Instant::now();
        if !gc_is_due(last_gc_finished_at, now, config.gc_interval) {
            continue;
        }

        match collect_garbage(store.clone(), config.retention).await {
            Ok(Ok(0)) => {}
            Ok(Ok(files_removed)) => {
                tracing::info!(files_removed, "Archive garbage collection finished")
            }
            Ok(Err(error)) => tracing::warn!(%error, "Archive garbage collection failed"),
            Err(error) => tracing::warn!(%error, "Archive garbage collection task failed"),
        }
        last_gc_finished_at = Instant::now();
        cleanup_pending = true;

        if let Some(conn) = conn.as_mut() {
            apply_search_cleanup(conn, &mut cleanup_pending, "post-gc").await;
        }
    }

    apply_search_changes(
        &mut conn,
        &db_path,
        &archive_root,
        &store,
        &search_db_ready,
        &mut cleanup_pending,
        "shutdown",
    )
    .await;
}

/// Rebuilds the search index by deleting the search database and re-ingesting
/// all existing JSONL archive files from offset 0. The JSONL archive files are
/// not modified.
///
/// Do not run this concurrently with a `serve` process against the same
/// `data_dir`: this deletes and recreates the search database file, which a
/// running server's reader pool and indexer do not expect.
pub async fn run_rebuild(data_dir: &Path) -> Result<()> {
    let db_path = search_db_path(data_dir);
    let archive_root = crate::archive::archive_root(data_dir);

    for suffix in ["", "-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{suffix}", db_path.display()));
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", path.display()));
            }
        }
    }

    let mut conn = open_and_migrate(&db_path).await?;
    ingest_once(&mut conn, &archive_root).await?;
    cleanup_index_for_deleted_files(&mut conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::ArchiveEvent;

    use super::super::test_support::{eit_line, temp_dir};

    #[test]
    fn garbage_collection_is_due_after_interval() {
        let interval = Duration::from_secs(60 * 60);
        let last_gc_finished_at = Instant::now();

        assert!(!gc_is_due(
            last_gc_finished_at,
            last_gc_finished_at + interval - Duration::from_secs(1),
            interval
        ));
        assert!(gc_is_due(
            last_gc_finished_at,
            last_gc_finished_at + interval,
            interval
        ));
    }

    #[tokio::test]
    async fn failed_archive_path_is_retried_by_the_next_pass() {
        let data_dir = temp_dir();
        let archive_root = archive::archive_root(&data_dir);
        let store = Arc::new(Mutex::new(ArchiveStore::new(&data_dir)));
        let line = eit_line(1, "ニュース", "");
        let Some(ArchiveEvent::ProgramStarted(path)) =
            archive::handle_line(&store, "nhk", &line).unwrap()
        else {
            panic!("expected a new archive file");
        };
        archive::deactivate_stream(&store, "nhk");
        fs::remove_file(&path).unwrap();
        let mut conn = open_and_migrate(&search_db_path(&data_dir)).await.unwrap();

        apply_pending_archive_changes(&mut conn, &archive_root, &store, "test").await;
        assert!(
            archive::lock_store(&store)
                .snapshot_dirty_paths()
                .contains_key(&path)
        );

        fs::write(&path, format!("{line}\n")).unwrap();
        apply_pending_archive_changes(&mut conn, &archive_root, &store, "test").await;

        assert!(
            archive::lock_store(&store)
                .snapshot_dirty_paths()
                .is_empty()
        );
        let programs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM programs")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(programs, 1);
    }
}

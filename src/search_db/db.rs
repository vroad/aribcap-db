use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use sqlx::{
    Connection,
    migrate::Migrator,
    sqlite::{
        SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePoolOptions,
        SqliteSynchronous,
    },
};

const SEARCH_DB_FILENAME: &str = "search.sqlite3";
static MIGRATOR: Migrator = sqlx::migrate!();

/// Maximum number of concurrent search-reader connections in the pool (and
/// thus the OS worker threads SQLx spawns per SQLite connection). It bounds
/// resource usage from a burst of requests.
const SEARCH_POOL_MAX_CONNECTIONS: u32 = 32;

pub fn search_db_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SEARCH_DB_FILENAME)
}

/// Opens or creates the search database, then applies the SQLite pragmas and
/// pending migrations.
pub async fn open_and_migrate(db_path: &Path) -> Result<SqliteConnection> {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_millis(5000))
        .foreign_keys(true);
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    MIGRATOR
        .run_direct(None, &mut conn, false)
        .await
        .context("failed to apply search database migrations")?;
    Ok(conn)
}

/// Builds a bounded pool of read-only connections for search queries. The
/// writer connection (owned by the indexer) must create and migrate the
/// database. Connections are established lazily, so this can be called
/// before the database file exists; the first `acquire()` after that point
/// opens it.
pub fn open_reader_pool(db_path: &Path) -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .read_only(true)
        .busy_timeout(Duration::from_millis(5000))
        .foreign_keys(true);
    SqlitePoolOptions::new()
        .max_connections(SEARCH_POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy_with(options)
}

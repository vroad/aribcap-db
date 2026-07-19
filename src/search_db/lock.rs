use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

const LOCK_FILENAME: &str = "search.sqlite3.lock";

/// Process-scoped lock for a data directory's search database.
///
/// The OS releases the lock when this value is dropped or the process exits.
#[derive(Debug)]
pub(crate) struct DataDirLock(#[allow(dead_code)] std::fs::File);

fn lock_path(data_dir: &Path) -> PathBuf {
    data_dir.join(LOCK_FILENAME)
}

/// Acquires the `data_dir` lock, failing immediately if it is already held.
///
/// This prevents `search-rebuild` from replacing `search.sqlite3` while
/// `aribcap-db serve` is still using it. Without this lock, existing SQLite
/// connections on POSIX would continue using the old database even after
/// `search-rebuild` replaces the database file.
pub(crate) async fn acquire_data_dir_lock(data_dir: &Path) -> Result<DataDirLock> {
    let path = lock_path(data_dir);
    tokio::task::spawn_blocking(move || {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(DataDirLock(file)),
            Err(std::fs::TryLockError::WouldBlock) => Err(anyhow::anyhow!(
                "the data directory lock is held by another aribcap-db process: {}",
                path.display()
            )),
            Err(std::fs::TryLockError::Error(error)) => {
                Err(error).with_context(|| format!("failed to lock {}", path.display()))
            }
        }
    })
    .await
    .context("lock acquisition task failed")?
}

#[cfg(test)]
mod tests {
    use super::super::test_support::TEST_DIR_PREFIX;
    use super::*;
    use crate::test_support::TestDir;

    #[tokio::test]
    async fn second_lock_attempt_is_rejected() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);

        let first = acquire_data_dir_lock(&data_dir).await.unwrap();

        let error = acquire_data_dir_lock(&data_dir).await.unwrap_err();
        assert!(error.to_string().contains("held by another"));

        drop(first);
        acquire_data_dir_lock(&data_dir)
            .await
            .expect("lock must be free again after the holder is dropped");
    }
}

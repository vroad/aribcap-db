use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt as _, stream};
use serde_json::Value;
use sqlx::{Connection, SqliteConnection};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, SeekFrom};

use crate::archive::{ArchiveWalkTarget, parse_recording_started_at, walk_archive_paths};

use super::record::{
    CaptionRecord, EitPresent, caption_from_value, eit_present_from_value, stream_month_filename,
};
use super::text::search_index_text;

struct IndexedFileRow {
    program_id: Option<i64>,
    indexed_offset: i64,
    indexed_lines: i64,
    status: String,
}

struct ArchiveFileMetadata {
    size_bytes: i64,
    mtime: i64,
}

async fn stat_archive_file(path: &Path) -> Result<ArchiveFileMetadata> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("failed to stat {}", path.display()))?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    Ok(ArchiveFileMetadata {
        size_bytes: metadata.len() as i64,
        mtime,
    })
}

async fn read_archive_tail(path: &Path, start_offset: i64) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.seek(SeekFrom::Start(start_offset as u64)).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

async fn load_indexed_file(
    conn: &mut SqliteConnection,
    path: &str,
) -> Result<Option<IndexedFileRow>> {
    sqlx::query_as!(
        IndexedFileRow,
        "
        SELECT program_id, indexed_offset, indexed_lines, status
        FROM indexed_files
        WHERE path = ?1
        ",
        path,
    )
    .fetch_optional(conn)
    .await
    .context("failed to load indexed_files row")
}

/// Marks `path` as a permanent indexing error: a later ingest pass skips it
/// (see the `status == "error"` check in `ingest_file`) until a rebuild
/// clears `indexed_files`. `program_id` links to a program row already
/// committed this pass, if any; on conflict with an existing row, the prior
/// `program_id`/`indexed_offset`/`indexed_lines` are left untouched.
async fn mark_indexing_error(
    conn: &mut SqliteConnection,
    path: &str,
    program_id: Option<i64>,
    size_bytes: i64,
    mtime: i64,
    last_error: &str,
) -> Result<()> {
    sqlx::query!(
        "
        INSERT INTO indexed_files (
            path, program_id, size_bytes, mtime, indexed_offset, indexed_lines, status, last_error
        ) VALUES (?1, ?2, ?3, ?4, 0, 0, 'error', ?5)
        ON CONFLICT(path) DO UPDATE SET
            status = 'error',
            size_bytes = excluded.size_bytes,
            mtime = excluded.mtime,
            last_error = excluded.last_error
        ",
        path,
        program_id,
        size_bytes,
        mtime,
        last_error,
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Marks an indexed archive path as a duplicate of `winner_path`.
async fn demote_to_duplicate(
    conn: &mut SqliteConnection,
    path: &str,
    winner_path: &str,
) -> Result<()> {
    let last_error =
        format!("superseded by (stream, recording_started_at) collision winner {winner_path}");
    sqlx::query!(
        "
        UPDATE indexed_files
        SET program_id = NULL, status = 'duplicate', last_error = ?2
        WHERE path = ?1
        ",
        path,
        last_error,
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Marks `path` `duplicate` and returns `true` if `path` loses the
/// collision, i.e. if `path` is lexicographically smaller than the path
/// already on file for this `(stream, recording_started_at)`.
///
/// This function does not handle the case where `path` wins the collision
/// (i.e., is lexicographically greater); that case is handled in
/// `upsert_program`.
async fn mark_recording_started_at_collision_loser(
    conn: &mut SqliteConnection,
    stream: &str,
    recording_started_at: &str,
    path: &str,
    size_bytes: i64,
    mtime: i64,
) -> Result<bool> {
    let existing_path: Option<String> = sqlx::query_scalar!(
        "
        SELECT path FROM programs
        WHERE stream = ?1 AND recording_started_at = ?2
        ",
        stream,
        recording_started_at,
    )
    .fetch_optional(&mut *conn)
    .await?;
    let Some(existing_path) = existing_path else {
        return Ok(false);
    };
    if path >= existing_path.as_str() {
        return Ok(false);
    }

    // Mark `path` as duplicate to avoid querying `programs` for the collision
    // check when the same file is processed again.
    let last_error = format!("lost (stream, recording_started_at) collision to {existing_path}");
    sqlx::query!(
        "
        INSERT INTO indexed_files (
            path, program_id, size_bytes, mtime, indexed_offset, indexed_lines, status, last_error
        ) VALUES (?1, NULL, ?2, ?3, 0, 0, 'duplicate', ?4)
        ON CONFLICT(path) DO UPDATE SET
            program_id = NULL,
            size_bytes = excluded.size_bytes,
            mtime = excluded.mtime,
            indexed_offset = 0,
            indexed_lines = 0,
            status = 'duplicate',
            last_error = excluded.last_error
        ",
        path,
        size_bytes,
        mtime,
        last_error,
    )
    .execute(conn)
    .await?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_indexed_file(
    conn: &mut SqliteConnection,
    path: &str,
    program_id: Option<i64>,
    size_bytes: i64,
    mtime: i64,
    indexed_offset: i64,
    indexed_lines: i64,
) -> Result<()> {
    sqlx::query!(
        "
        INSERT INTO indexed_files (
            path, program_id, size_bytes, mtime, indexed_offset, indexed_lines, status, last_error
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'ok', NULL)
        ON CONFLICT(path) DO UPDATE SET
            program_id = excluded.program_id,
            size_bytes = excluded.size_bytes,
            mtime = excluded.mtime,
            indexed_offset = excluded.indexed_offset,
            indexed_lines = excluded.indexed_lines,
            status = 'ok',
            last_error = NULL
        ",
        path,
        program_id,
        size_bytes,
        mtime,
        indexed_offset,
        indexed_lines,
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Inserts or updates the program for `path`.
#[allow(clippy::too_many_arguments)]
async fn upsert_program(
    conn: &mut SqliteConnection,
    stream: &str,
    month: &str,
    filename: &str,
    path: &str,
    recording_started_at: &str,
    eit: &EitPresent,
) -> Result<i64> {
    // Find the previous winner, then delete its captions and mark its path as duplicate.
    let existing = sqlx::query!(
        "
        SELECT id, path FROM programs
        WHERE stream = ?1 AND recording_started_at = ?2
        ",
        stream,
        recording_started_at,
    )
    .fetch_optional(&mut *conn)
    .await?;
    if let Some(existing) = existing
        && existing.path != path
    {
        sqlx::query!(
            "DELETE FROM caption_lines WHERE program_id = ?1",
            existing.id
        )
        .execute(&mut *conn)
        .await?;
        demote_to_duplicate(conn, &existing.path, path).await?;
    }

    let normalized_title = search_index_text(&eit.title);
    let normalized_description = search_index_text(&eit.description);

    let program_id: i64 = sqlx::query_scalar::<_, i64>(
        "
        INSERT INTO programs (
            stream, month, filename, path, recording_started_at, start_time, duration_sec,
            title, description, version,
            service_id, transport_stream_id, original_network_id, event_id,
            normalized_title, normalized_description
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
        ON CONFLICT(path) DO UPDATE SET
            start_time = excluded.start_time,
            duration_sec = excluded.duration_sec,
            title = excluded.title,
            description = excluded.description,
            version = excluded.version,
            service_id = excluded.service_id,
            transport_stream_id = excluded.transport_stream_id,
            original_network_id = excluded.original_network_id,
            event_id = excluded.event_id,
            normalized_title = excluded.normalized_title,
            normalized_description = excluded.normalized_description
        ON CONFLICT(stream, recording_started_at) DO UPDATE SET
            month = excluded.month,
            filename = excluded.filename,
            path = excluded.path,
            start_time = excluded.start_time,
            duration_sec = excluded.duration_sec,
            title = excluded.title,
            description = excluded.description,
            version = excluded.version,
            service_id = excluded.service_id,
            transport_stream_id = excluded.transport_stream_id,
            original_network_id = excluded.original_network_id,
            event_id = excluded.event_id,
            normalized_title = excluded.normalized_title,
            normalized_description = excluded.normalized_description
        RETURNING id
        ",
    )
    .bind(stream)
    .bind(month)
    .bind(filename)
    .bind(path)
    .bind(recording_started_at)
    .bind(&eit.start_time)
    .bind(eit.duration_sec)
    .bind(&eit.title)
    .bind(&eit.description)
    .bind(eit.version)
    .bind(eit.service_id)
    .bind(eit.transport_stream_id)
    .bind(eit.original_network_id)
    .bind(eit.event_id)
    .bind(normalized_title)
    .bind(normalized_description)
    .fetch_one(&mut *conn)
    .await?;

    sqlx::query("DELETE FROM program_genres WHERE program_id = ?1")
        .bind(program_id)
        .execute(&mut *conn)
        .await?;
    for genre in &eit.genres {
        sqlx::query(
            "
            INSERT OR IGNORE INTO program_genres (
                program_id, content_nibble_level1, content_nibble_level2,
                user_nibble1, user_nibble2
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ",
        )
        .bind(program_id)
        .bind(genre.content_nibble_level1)
        .bind(genre.content_nibble_level2)
        .bind(genre.user_nibble1)
        .bind(genre.user_nibble2)
        .execute(&mut *conn)
        .await?;
    }

    Ok(program_id)
}

async fn insert_caption_line(
    conn: &mut SqliteConnection,
    program_id: i64,
    line_no: i64,
    byte_offset: i64,
    caption: &CaptionRecord,
) -> Result<()> {
    let normalized_text = search_index_text(&caption.text);

    let _line_id: i64 = sqlx::query_scalar::<_, i64>(
        "
        INSERT INTO caption_lines (
            program_id, line_no, byte_offset,
            time, text, color, pid, caption_type, language_code, duration_ms, clear_screen,
            normalized_text
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(program_id, line_no) DO UPDATE SET
            byte_offset = excluded.byte_offset,
            time = excluded.time,
            text = excluded.text,
            color = excluded.color,
            pid = excluded.pid,
            caption_type = excluded.caption_type,
            language_code = excluded.language_code,
            duration_ms = excluded.duration_ms,
            clear_screen = excluded.clear_screen,
            normalized_text = excluded.normalized_text
        RETURNING id
        ",
    )
    .bind(program_id)
    .bind(line_no)
    .bind(byte_offset)
    .bind(&caption.time)
    .bind(&caption.text)
    .bind(&caption.color)
    .bind(caption.pid)
    .bind(&caption.caption_type)
    .bind(&caption.language_code)
    .bind(caption.duration_ms)
    .bind(caption.clear_screen)
    .bind(normalized_text)
    .fetch_one(&mut *conn)
    .await?;
    Ok(())
}

async fn ingest_file(conn: &mut SqliteConnection, archive_root: &Path, path: &Path) -> Result<()> {
    let Some((stream, month, filename)) = stream_month_filename(archive_root, path) else {
        return Ok(());
    };

    let Some(recording_started_at) = parse_recording_started_at(&filename).map(str::to_owned)
    else {
        tracing::debug!(path = %path.display(), "Skip archive file with unparseable recording start time in filename");
        return Ok(());
    };

    let path_str = path.to_string_lossy().into_owned();
    let metadata = stat_archive_file(path).await?;
    let current_size = metadata.size_bytes;
    let mtime = metadata.mtime;

    let existing = load_indexed_file(conn, &path_str).await?;

    // Skip files already marked as duplicate or a permanent indexing error;
    // a rebuild is required to retry the latter.
    if matches!(&existing, Some(row) if row.status == "duplicate" || row.status == "error") {
        return Ok(());
    }

    // Mark a new collision loser as duplicate before reading it. A potential
    // winner is resolved by `upsert_program` after its EIT present record is read.
    if existing.is_none()
        && mark_recording_started_at_collision_loser(
            conn,
            &stream,
            &recording_started_at,
            &path_str,
            current_size,
            mtime,
        )
        .await?
    {
        return Ok(());
    }

    let (mut program_id, start_offset, mut line_no) = match &existing {
        Some(row) if current_size < row.indexed_offset => {
            let last_error = format!(
                "file size {current_size} is smaller than indexed_offset {}; rebuild required",
                row.indexed_offset
            );
            mark_indexing_error(
                conn,
                &path_str,
                row.program_id,
                current_size,
                mtime,
                &last_error,
            )
            .await?;
            return Ok(());
        }
        Some(row) if current_size == row.indexed_offset => return Ok(()),
        Some(row) => (row.program_id, row.indexed_offset, row.indexed_lines),
        None => (None, 0i64, 0i64),
    };

    let buf = read_archive_tail(path, start_offset).await?;

    // `pos` tracks the current byte position from the start of the file.
    let mut pos = start_offset;
    for line_bytes in buf.split_inclusive(|&byte| byte == b'\n') {
        // Parse the final line only after the archive writer appends its terminating '\n'.
        if line_bytes.last() != Some(&b'\n') {
            break;
        }

        let line_start = pos;
        pos += line_bytes.len() as i64;
        line_no += 1;

        let text = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]);
        let text = text.trim_end_matches('\r');

        if text.is_empty() {
            continue;
        }

        let value: Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    line_no,
                    %error,
                    "Skip invalid JSONL line during search ingest"
                );
                continue;
            }
        };

        // A file can create or replace a program only after an EIT present record is read.
        if let Some(eit) = eit_present_from_value(&value) {
            match upsert_program(
                conn,
                &stream,
                &month,
                &filename,
                &path_str,
                &recording_started_at,
                &eit,
            )
            .await
            {
                Ok(id) => program_id = Some(id),
                Err(error) => return Err(error),
            }
            continue;
        }

        if let Some(caption) = caption_from_value(&value) {
            let Some(pid) = program_id else {
                tracing::debug!(
                    path = %path.display(),
                    line_no,
                    "Skip caption line before a program is known"
                );
                continue;
            };
            insert_caption_line(conn, pid, line_no, line_start, &caption).await?;
        }
    }

    upsert_indexed_file(
        conn,
        &path_str,
        program_id,
        current_size,
        mtime,
        pos,
        line_no,
    )
    .await?;
    Ok(())
}

async fn ingest_path_stream<S>(
    conn: &mut SqliteConnection,
    archive_root: &Path,
    paths: S,
) -> Result<()>
where
    S: Stream<Item = PathBuf>,
{
    let mut first_error: Option<anyhow::Error> = None;
    futures_util::pin_mut!(paths);
    while let Some(path) = paths.next().await {
        // Commit each path independently so that a failure does not roll back paths
        // indexed earlier in the pass.
        let mut tx = match conn.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "Failed to open ingest transaction");
                first_error.get_or_insert_with(|| {
                    anyhow::Error::from(error)
                        .context(format!("failed to index {}", path.display()))
                });
                continue;
            }
        };
        // Keep program replacement and caption cleanup in the same transaction.
        match ingest_file(&mut tx, archive_root, &path).await {
            Ok(()) => {
                if let Err(error) = tx.commit().await {
                    tracing::warn!(path = %path.display(), %error, "Failed to commit ingest transaction");
                    first_error.get_or_insert_with(|| {
                        anyhow::Error::from(error)
                            .context(format!("failed to index {}", path.display()))
                    });
                }
            }
            Err(error) => {
                tx.rollback().await.ok();
                tracing::warn!(path = %path.display(), %error, "Failed to ingest archive file");
                first_error.get_or_insert_with(|| {
                    error.context(format!("failed to index {}", path.display()))
                });
            }
        }
    }
    match first_error {
        None => Ok(()),
        Some(error) => Err(error),
    }
}

/// Attempts to index every given path and returns an error if any path fails.
pub async fn ingest_paths(
    conn: &mut SqliteConnection,
    archive_root: &Path,
    paths: impl IntoIterator<Item = PathBuf>,
) -> Result<()> {
    ingest_path_stream(conn, archive_root, stream::iter(paths)).await
}

/// Scans `archive_root` and indexes new data in its JSONL archive files.
pub async fn ingest_once(conn: &mut SqliteConnection, archive_root: &Path) -> Result<()> {
    let paths =
        walk_archive_paths(archive_root, ArchiveWalkTarget::Files).filter_map(|result| async {
            match result {
                Ok(path) if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") => {
                    Some(path)
                }
                Ok(_) => None,
                Err(error) => {
                    tracing::warn!(%error, "archive search scan failed");
                    None
                }
            }
        });
    ingest_path_stream(conn, archive_root, paths).await
}

/// Deletes the `programs`/`indexed_files` rows for `path`. `ON DELETE
/// CASCADE` on `caption_lines` and the FTS delete triggers
/// handle the rest.
async fn delete_indexed_path(conn: &mut SqliteConnection, path: &str) -> Result<()> {
    sqlx::query!("DELETE FROM programs WHERE path = ?1", path)
        .execute(&mut *conn)
        .await?;
    sqlx::query!("DELETE FROM indexed_files WHERE path = ?1", path)
        .execute(conn)
        .await?;
    Ok(())
}

/// Removes search-index rows for archive files retention GC has already deleted.
pub async fn cleanup_index_for_deleted_files(conn: &mut SqliteConnection) -> Result<usize> {
    // Union both tables rather than just `indexed_files`: they're written in
    // the same transaction and should stay in sync, but this guards against
    // either table holding a path the other doesn't.
    let paths: Vec<String> = sqlx::query_scalar!(
        "
        SELECT path AS \"path!\" FROM programs
        UNION
        SELECT path AS \"path!\" FROM indexed_files
        "
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut removed = 0;
    let mut tx = conn.begin().await?;
    for path in paths {
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            delete_indexed_path(&mut tx, &path).await?;
            removed += 1;
        }
    }
    tx.commit().await?;
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::super::db::open_and_migrate;
    use super::super::test_support::{
        TEST_DIR_PREFIX, caption_line, eit_line, eit_line_with_genre, write_file,
    };
    use super::super::{SearchFilter, parse_search_expression, search_captions};
    use super::*;
    use crate::test_support::TestDir;

    struct IngestFixture {
        _data_dir: TestDir,
        archive_root: PathBuf,
        db_path: PathBuf,
        conn: SqliteConnection,
    }

    impl IngestFixture {
        async fn new() -> Self {
            let data_dir = TestDir::new(TEST_DIR_PREFIX);
            let archive_root = data_dir.join("archive");
            let db_path = data_dir.join("search.sqlite3");
            let conn = open_and_migrate(&db_path).await.unwrap();
            Self {
                _data_dir: data_dir,
                archive_root,
                db_path,
                conn,
            }
        }

        fn write_program(&self, filename: &str, event_id: u64, title: &str) -> PathBuf {
            write_file(
                &self.archive_root,
                "nhk",
                "2026-07",
                filename,
                &format!("{}\n", eit_line(event_id, title, "")),
            )
        }

        async fn ingest_all(&mut self) {
            ingest_once(&mut self.conn, &self.archive_root)
                .await
                .unwrap();
        }

        async fn program_count(&mut self) -> i64 {
            sqlx::query_scalar("SELECT COUNT(*) FROM programs")
                .fetch_one(&mut self.conn)
                .await
                .unwrap()
        }

        async fn indexed_file_count(&mut self) -> i64 {
            sqlx::query_scalar("SELECT COUNT(*) FROM indexed_files")
                .fetch_one(&mut self.conn)
                .await
                .unwrap()
        }

        async fn only_program_path(&mut self) -> String {
            sqlx::query_scalar("SELECT path FROM programs")
                .fetch_one(&mut self.conn)
                .await
                .unwrap()
        }
    }

    async fn caption_count(conn: &mut SqliteConnection, program_id: i64) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM caption_lines WHERE program_id = ?1")
            .bind(program_id)
            .fetch_one(conn)
            .await
            .unwrap()
    }

    async fn indexed_position(conn: &mut SqliteConnection, path: &Path) -> (i64, i64) {
        sqlx::query_as("SELECT indexed_offset, indexed_lines FROM indexed_files WHERE path = ?1")
            .bind(path.to_string_lossy())
            .fetch_one(conn)
            .await
            .unwrap()
    }

    fn write_first_collision_program(fixture: &IngestFixture) -> PathBuf {
        write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.first.jsonl",
            &format!(
                "{}\n{}\n",
                eit_line(1, "first program", ""),
                caption_line("最初の番組の字幕", "2026-07-10T19:00:01.000+09:00")
            ),
        )
    }

    fn write_complete_collision(fixture: &IngestFixture) -> (PathBuf, PathBuf) {
        let first_path = write_first_collision_program(fixture);
        let second_path = write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.second.jsonl",
            &format!(
                "{}\n{}\n",
                eit_line(2, "second program", ""),
                caption_line("次の番組の字幕", "2026-07-10T19:00:01.000+09:00")
            ),
        );
        assert!(
            second_path > first_path,
            "collision tests rely on this path ordering"
        );
        (first_path, second_path)
    }

    fn write_collision_without_second_eit(fixture: &IngestFixture) -> (PathBuf, PathBuf) {
        let first_path = write_first_collision_program(fixture);
        let second_path = write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.second.jsonl",
            &format!(
                "{}\n",
                caption_line("迷子の字幕", "2026-07-10T19:00:01.000+09:00")
            ),
        );
        (first_path, second_path)
    }

    #[tokio::test]
    async fn ingest_resumes_from_indexed_offset() {
        let mut fixture = IngestFixture::new().await;
        let filename = "2026-07-10_19-00-00.news.jsonl";
        let mut content = String::new();
        content.push_str(&eit_line(10, "ニュース", "詳しい説明"));
        content.push('\n');
        content.push_str(&caption_line("台風が接近", "2026-07-10T19:00:01.000+09:00"));
        content.push('\n');
        let path = write_file(&fixture.archive_root, "nhk", "2026-07", filename, &content);

        fixture.ingest_all().await;
        let program_id: i64 = sqlx::query_scalar("SELECT id FROM programs WHERE path = ?1")
            .bind(path.to_string_lossy())
            .fetch_one(&mut fixture.conn)
            .await
            .unwrap();

        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(
                file,
                "{}",
                caption_line("地震速報です", "2026-07-10T19:00:02.000+09:00")
            )
            .unwrap();
        }
        fixture.ingest_all().await;

        // The first caption must not be indexed again when ingest resumes.
        assert_eq!(caption_count(&mut fixture.conn, program_id).await, 2);
        let (indexed_offset, indexed_lines) = indexed_position(&mut fixture.conn, &path).await;
        assert_eq!(indexed_offset as usize, fs::read(&path).unwrap().len());
        assert_eq!(indexed_lines, 3);
    }

    #[tokio::test]
    async fn newer_eit_replaces_program_genres() {
        let mut fixture = IngestFixture::new().await;
        let path = write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.news.jsonl",
            &format!("{}\n", eit_line_with_genre(10, "ニュース", "", 5, 2)),
        );
        fixture.ingest_all().await;

        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
            writeln!(file, "{}", eit_line_with_genre(10, "ニュース", "", 8, 4)).unwrap();
        }
        fixture.ingest_all().await;

        let genres: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT content_nibble_level1, content_nibble_level2 FROM program_genres",
        )
        .fetch_all(&mut fixture.conn)
        .await
        .unwrap();
        assert_eq!(genres, vec![(8, 4)]);
    }

    #[tokio::test]
    async fn ingest_stops_before_incomplete_trailing_line() {
        let mut fixture = IngestFixture::new().await;
        let complete = format!("{}\n", eit_line(10, "ニュース", ""));
        let incomplete = "{\"type\":\"caption\",\"text\":\"incomplete";
        let path = write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.news.jsonl",
            &format!("{complete}{incomplete}"),
        );

        fixture.ingest_all().await;

        let (indexed_offset, indexed_lines) = indexed_position(&mut fixture.conn, &path).await;
        assert_eq!(indexed_offset as usize, complete.len());
        assert_eq!(indexed_lines, 1);
    }

    #[tokio::test]
    async fn ingest_indexes_single_character_caption_in_fts() {
        let mut fixture = IngestFixture::new().await;
        let path = write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.news.jsonl",
            &format!(
                "{}\n{}\n",
                eit_line(10, "ニュース", ""),
                caption_line("あ", "2026-07-10T19:00:01.000+09:00")
            ),
        );

        fixture.ingest_all().await;

        let (text, color, pid, clear_screen, normalized_text, version): (
            String,
            Option<String>,
            Option<i64>,
            Option<bool>,
            String,
            Option<i64>,
        ) = sqlx::query_as(
            "
            SELECT cl.text, cl.color, cl.pid, cl.clear_screen, cl.normalized_text, p.version
            FROM caption_lines cl
            JOIN programs p ON p.id = cl.program_id
            WHERE p.path = ?1
            ",
        )
        .bind(path.to_string_lossy())
        .fetch_one(&mut fixture.conn)
        .await
        .unwrap();
        assert_eq!(text, "あ");
        assert_eq!(color.as_deref(), Some("0xffffffff"));
        assert_eq!(pid, Some(304));
        assert_eq!(clear_screen, Some(true));
        assert_eq!(normalized_text, "あ");
        assert_eq!(version, Some(1));

        let hit_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM caption_fts WHERE caption_fts MATCH '\"あ\"'")
                .fetch_one(&mut fixture.conn)
                .await
                .unwrap();
        assert_eq!(hit_count, 1);
    }

    #[tokio::test]
    async fn ingest_marks_shrunken_file_as_error() {
        let mut fixture = IngestFixture::new().await;
        let path = fixture.write_program("2026-07-10_19-00-00.news.jsonl", 10, "ニュース");

        fixture.ingest_all().await;
        fs::write(&path, "").unwrap();
        fixture.ingest_all().await;

        let (status, last_error): (String, Option<String>) =
            sqlx::query_as("SELECT status, last_error FROM indexed_files WHERE path = ?1")
                .bind(path.to_string_lossy())
                .fetch_one(&mut fixture.conn)
                .await
                .unwrap();
        assert_eq!(status, "error");
        assert!(last_error.unwrap().contains("rebuild required"));
    }

    #[tokio::test]
    async fn ingest_paths_indexes_directly_notified_archive_path() {
        // A directly notified path must be ingested even when no directory scan found it.
        let mut fixture = IngestFixture::new().await;
        let path = fixture.write_program("2026-07-10_19-00-00.news.jsonl", 10, "ニュース");

        ingest_paths(&mut fixture.conn, &fixture.archive_root, [path])
            .await
            .unwrap();

        assert_eq!(fixture.program_count().await, 1);
    }

    #[tokio::test]
    async fn ingest_paths_continues_after_a_bad_path() {
        let mut fixture = IngestFixture::new().await;
        let good_path = fixture.write_program("2026-07-10_19-00-00.news.jsonl", 1, "ニュース");
        let bad_path = fixture
            .archive_root
            .join("nhk")
            .join("2026-07")
            .join("2026-07-10_18-00-00.missing.jsonl");

        let error = ingest_paths(
            &mut fixture.conn,
            &fixture.archive_root,
            [bad_path.clone(), good_path],
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains(&bad_path.display().to_string()));
        assert_eq!(fixture.program_count().await, 1);
    }

    #[tokio::test]
    async fn ingest_paths_reports_transaction_start_failure() {
        let mut fixture = IngestFixture::new().await;
        let path = fixture.write_program("2026-07-10_19-00-00.news.jsonl", 1, "ニュース");
        sqlx::query("BEGIN")
            .execute(&mut fixture.conn)
            .await
            .unwrap();

        let result = ingest_paths(&mut fixture.conn, &fixture.archive_root, [path]).await;

        assert!(result.is_err());
        assert_eq!(fixture.program_count().await, 0);
        sqlx::query("ROLLBACK")
            .execute(&mut fixture.conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ingest_once_skips_files_with_unparseable_names() {
        let mut fixture = IngestFixture::new().await;
        write_file(
            &fixture.archive_root,
            "nhk",
            "2026-07",
            "deadbeef1234.jsonl",
            &eit_line(1, "x", ""),
        );

        fixture.ingest_all().await;

        assert_eq!(fixture.program_count().await, 0);
        assert_eq!(fixture.indexed_file_count().await, 0);
    }

    #[tokio::test]
    async fn collision_selects_lexicographically_greater_path() {
        let mut fixture = IngestFixture::new().await;
        let (first_path, second_path) = write_complete_collision(&fixture);
        fixture.ingest_all().await;

        // Only `second_path` must remain in `programs` because it is lexicographically greater.
        let programs: Vec<(String, String)> = sqlx::query_as("SELECT path, title FROM programs")
            .fetch_all(&mut fixture.conn)
            .await
            .unwrap();
        assert_eq!(
            programs,
            vec![(
                second_path.to_string_lossy().into_owned(),
                "second program".to_owned()
            )],
            "only the winning path keeps a program row"
        );

        let (status, last_error): (String, Option<String>) =
            sqlx::query_as("SELECT status, last_error FROM indexed_files WHERE path = ?1")
                .bind(first_path.to_string_lossy())
                .fetch_one(&mut fixture.conn)
                .await
                .unwrap();
        assert_eq!(status, "duplicate");
        assert!(last_error.unwrap().contains("second"));
    }

    #[tokio::test]
    async fn collision_indexes_only_winner_captions() {
        let mut fixture = IngestFixture::new().await;
        write_complete_collision(&fixture);
        fixture.ingest_all().await;

        let filter = SearchFilter::default();
        let results = search_captions(
            &mut fixture.conn,
            &parse_search_expression("字幕").unwrap(),
            &filter,
            20,
            5,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "second program");
        assert_eq!(results[0].hits.len(), 1);
        assert!(results[0].hits[0].text.contains("次の番組"));
    }

    #[tokio::test]
    async fn reingest_preserves_collision_winner() {
        let mut fixture = IngestFixture::new().await;
        let (_, second_path) = write_complete_collision(&fixture);
        fixture.ingest_all().await;
        let original_path = fixture.only_program_path().await;

        fixture.ingest_all().await;

        assert_eq!(fixture.program_count().await, 1);
        assert_eq!(original_path, second_path.to_string_lossy());
        assert_eq!(fixture.only_program_path().await, original_path);
    }

    #[tokio::test]
    async fn rebuild_preserves_collision_winner() {
        let mut fixture = IngestFixture::new().await;
        let (_, second_path) = write_complete_collision(&fixture);
        fixture.ingest_all().await;
        let original_path = fixture.only_program_path().await;

        drop(fixture.conn);
        fs::remove_file(&fixture.db_path).unwrap();
        let mut rebuilt = open_and_migrate(&fixture.db_path).await.unwrap();
        ingest_once(&mut rebuilt, &fixture.archive_root)
            .await
            .unwrap();
        let rebuilt_path: String = sqlx::query_scalar("SELECT path FROM programs")
            .fetch_one(&mut rebuilt)
            .await
            .unwrap();
        assert_eq!(original_path, second_path.to_string_lossy());
        assert_eq!(rebuilt_path, original_path);
    }

    #[tokio::test]
    async fn colliding_path_without_eit_does_not_change_program() {
        let mut fixture = IngestFixture::new().await;
        let (first_path, second_path) = write_collision_without_second_eit(&fixture);
        assert!(
            second_path > first_path,
            "test relies on this path ordering"
        );

        // `second_path` has the same stream and recording start as `first_path`, but no
        // EIT present record. Ingest must preserve `first_path`'s program and captions.
        fixture.ingest_all().await;

        let (path, title): (String, String) = sqlx::query_as("SELECT path, title FROM programs")
            .fetch_one(&mut fixture.conn)
            .await
            .unwrap();
        assert_eq!(path, first_path.to_string_lossy());
        assert_eq!(title, "first program");

        let filter = SearchFilter::default();
        let results = search_captions(
            &mut fixture.conn,
            &parse_search_expression("字幕").unwrap(),
            &filter,
            20,
            5,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "first program");
        assert_eq!(results[0].hits.len(), 1);
        assert!(results[0].hits[0].text.contains("最初の番組"));
        assert!(!results[0].hits[0].text.contains("迷子"));

        // `first_path` must never be demoted: it must stay `ok`, not `duplicate`.
        let first_status: String =
            sqlx::query_scalar("SELECT status FROM indexed_files WHERE path = ?1")
                .bind(first_path.to_string_lossy())
                .fetch_one(&mut fixture.conn)
                .await
                .unwrap();
        assert_eq!(first_status, "ok");
    }

    #[tokio::test]
    async fn colliding_path_without_eit_is_indexed_without_program() {
        let mut fixture = IngestFixture::new().await;
        let (_, second_path) = write_collision_without_second_eit(&fixture);
        fixture.ingest_all().await;

        let (second_status, second_program_id): (String, Option<i64>) =
            sqlx::query_as("SELECT status, program_id FROM indexed_files WHERE path = ?1")
                .bind(second_path.to_string_lossy())
                .fetch_one(&mut fixture.conn)
                .await
                .unwrap();
        assert_eq!(second_status, "ok");
        assert_eq!(second_program_id, None);
    }

    #[tokio::test]
    async fn cleanup_removes_index_rows_for_deleted_file() {
        let mut fixture = IngestFixture::new().await;
        let path = fixture.write_program("2026-07-10_19-00-00.news.jsonl", 1, "ニュース");

        fixture.ingest_all().await;
        assert_eq!(fixture.program_count().await, 1);
        assert_eq!(fixture.indexed_file_count().await, 1);

        fs::remove_file(&path).unwrap();
        let removed = cleanup_index_for_deleted_files(&mut fixture.conn)
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert_eq!(fixture.program_count().await, 0);
        assert_eq!(fixture.indexed_file_count().await, 0);
    }
}

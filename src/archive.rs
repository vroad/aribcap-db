use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Write as _},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone as _, Utc};
use futures_util::{Stream, StreamExt as _};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

const ARCHIVE_DIR: &str = "archive";
const STARTED_AT_FORMAT: &str = "%Y-%m-%d_%H-%M-%S";
const STARTED_AT_LEN: usize = 19;
const JST_OFFSET_SECONDS: i32 = 9 * 60 * 60;

fn jst() -> FixedOffset {
    FixedOffset::east_opt(JST_OFFSET_SECONDS).expect("valid JST fixed offset")
}

fn jst_now() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&jst())
}

/// Keeps the file currently being recorded for each configured stream.
#[derive(Debug)]
pub struct ArchiveStore {
    data_dir: PathBuf,
    active_archive_files: BTreeMap<String, ActiveArchiveFile>,
    dirty_paths: HashMap<PathBuf, u64>,
}

#[derive(Debug)]
struct ActiveArchiveFile {
    program: ProgramKey,
    path: PathBuf,
    file: File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgramKey {
    service_id: Option<u64>,
    transport_stream_id: Option<u64>,
    original_network_id: Option<u64>,
    event_id: Option<u64>,
    start_time: Option<DateTime<FixedOffset>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveEvent {
    ProgramStarted(PathBuf),
    SkippedNoProgram,
    SkippedInvalidJson,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GarbageCollectionDryRun {
    pub eligible_files: usize,
    pub cutoff: DateTime<FixedOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ProgramEntry {
    /// Archive stream name.
    pub stream: String,
    /// Archive month in `YYYY-MM` form.
    pub month: String,
    /// Complete JSONL archive filename.
    pub filename: String,
    /// HTTP path for retrieving the raw archived program.
    pub path: String,
    /// File size captured by the background search indexer.
    pub size_bytes: u64,
}

impl ArchiveStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            active_archive_files: BTreeMap::new(),
            dirty_paths: HashMap::new(),
        }
    }

    pub(crate) fn snapshot_dirty_paths(&self) -> HashMap<PathBuf, u64> {
        self.dirty_paths.clone()
    }

    pub(crate) fn clear_dirty_path_if_unchanged(&mut self, path: &Path, generation: u64) -> bool {
        if self.dirty_paths.get(path) != Some(&generation) {
            return false;
        }
        self.dirty_paths.remove(path);
        true
    }

    fn mark_dirty(&mut self, path: PathBuf) {
        let generation = self.dirty_paths.entry(path).or_default();
        *generation = generation.wrapping_add(1);
    }

    fn append_active(&mut self, stream_name: &str, line: &str) -> Result<bool> {
        let Some(active) = self.active_archive_files.get_mut(stream_name) else {
            return Ok(false);
        };
        active.write_line(line)?;
        let path = active.path.clone();
        self.mark_dirty(path);
        Ok(true)
    }

    fn append_if_same_program(
        &mut self,
        stream_name: &str,
        program: &ProgramKey,
        line: &str,
    ) -> Result<bool> {
        let Some(active) = self.active_archive_files.get_mut(stream_name) else {
            return Ok(false);
        };
        if active.program != *program {
            return Ok(false);
        }
        active.write_line(line)?;
        let path = active.path.clone();
        self.mark_dirty(path);
        Ok(true)
    }

    fn replace_active(&mut self, stream_name: String, active: ActiveArchiveFile) {
        self.mark_dirty(active.path.clone());
        self.active_archive_files.insert(stream_name, active);
    }

    fn active_paths(&self) -> HashSet<PathBuf> {
        self.active_archive_files
            .values()
            .map(|archive_file| archive_file.path.clone())
            .collect()
    }
}

impl ActiveArchiveFile {
    fn write_line(&mut self, line: &str) -> Result<()> {
        writeln!(self.file, "{line}")
            .with_context(|| format!("failed to write {}", self.path.display()))
    }
}

/// Handles an upstream JSONL line. Recording starts at an EIT present record;
/// lines received before that point are intentionally not archived.
pub fn handle_line(
    store: &Arc<Mutex<ArchiveStore>>,
    stream_name: &str,
    line: &str,
) -> Result<Option<ArchiveEvent>> {
    handle_line_at(store, stream_name, line, jst_now())
}

/// Closes and removes the active archive file for a stream, if one exists.
/// Subsequent lines are skipped until `handle_line` receives a present EIT
/// and opens a new archive file.
pub(crate) fn deactivate_stream(store: &Arc<Mutex<ArchiveStore>>, stream_name: &str) {
    lock_store(store).active_archive_files.remove(stream_name);
}

fn handle_line_at(
    store: &Arc<Mutex<ArchiveStore>>,
    stream_name: &str,
    line: &str,
    now: DateTime<FixedOffset>,
) -> Result<Option<ArchiveEvent>> {
    let value = match serde_json::from_str::<Value>(line) {
        Ok(value) => value,
        Err(error) => {
            tracing::debug!(stream = stream_name, %error, "Skip invalid JSONL record");
            return Ok(Some(ArchiveEvent::SkippedInvalidJson));
        }
    };

    let Some(program) = program_from_eit(&value) else {
        return if lock_store(store).append_active(stream_name, line)? {
            Ok(None)
        } else {
            Ok(Some(ArchiveEvent::SkippedNoProgram))
        };
    };

    let data_dir = {
        let mut store = lock_store(store);
        if store.append_if_same_program(stream_name, &program.key, line)? {
            return Ok(None);
        }
        store.data_dir.clone()
    };

    let mut active = open_archive_file(&data_dir, stream_name, &program, now)?;
    active.write_line(line)?;
    let path = active.path.clone();
    lock_store(store).replace_active(stream_name.to_owned(), active);
    Ok(Some(ArchiveEvent::ProgramStarted(path)))
}

/// Locks the archive store, recovering poisoned state so recording can continue.
pub(crate) fn lock_store(store: &Arc<Mutex<ArchiveStore>>) -> MutexGuard<'_, ArchiveStore> {
    match store.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!("Archive store mutex was poisoned; continuing with recovered state");
            store.clear_poison();
            poisoned.into_inner()
        }
    }
}

#[derive(Debug)]
struct Program {
    key: ProgramKey,
    title: String,
}

fn program_from_eit(value: &Value) -> Option<Program> {
    if value.get("type")?.as_str()? != "eit" || value.get("section")?.as_str()? != "present" {
        return None;
    }

    Some(Program {
        key: ProgramKey {
            service_id: value.get("serviceId").and_then(Value::as_u64),
            transport_stream_id: value.get("transportStreamId").and_then(Value::as_u64),
            original_network_id: value.get("originalNetworkId").and_then(Value::as_u64),
            event_id: value.get("eventId").and_then(Value::as_u64),
            start_time: value
                .get("startTime")
                .and_then(Value::as_str)
                .and_then(|time| DateTime::parse_from_rfc3339(time).ok()),
        },
        title: event_name_from_eit(value).unwrap_or_else(|| "no-title".to_owned()),
    })
}

pub(crate) fn event_name_from_eit(value: &Value) -> Option<String> {
    let events = value.get("shortEvents")?.as_array()?;
    events
        .iter()
        .find(|event| event.get("languageCode").and_then(Value::as_str) == Some("jpn"))
        .and_then(event_title)
        .or_else(|| events.iter().find_map(event_title))
}

fn event_title(event: &Value) -> Option<String> {
    event
        .get("eventName")?
        .as_str()
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(ToOwned::to_owned)
}

fn open_archive_file(
    data_dir: &Path,
    stream_name: &str,
    program: &Program,
    now: DateTime<FixedOffset>,
) -> Result<ActiveArchiveFile> {
    let stream = safe_component(stream_name, "stream");
    let month = now.format("%Y-%m").to_string();
    let started_at = now.format(STARTED_AT_FORMAT);
    let title = safe_component(&truncate_utf8_bytes(&program.title, 200), "no-title");
    let filename_stem = format!("{started_at}.{title}");
    let directory = archive_root(data_dir).join(stream).join(month);
    fs::create_dir_all(&directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;

    // A restarted process can record the same program in the same second.
    // Keep both captures rather than overwriting a previous archive file.
    for suffix in 0_u32.. {
        let collision_suffix = if suffix == 0 {
            String::new()
        } else {
            format!(".{suffix}")
        };
        let filename = format!("{filename_stem}{collision_suffix}.jsonl");
        let path = directory.join(filename);
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => {
                return Ok(ActiveArchiveFile {
                    program: program.key.clone(),
                    path,
                    file,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("failed to open {}", path.display()));
            }
        }
    }
    unreachable!("unbounded collision suffix loop")
}

fn safe_component(input: &str, fallback: &str) -> String {
    let sanitized = sanitize_filename::sanitize(input);
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        fallback.to_owned()
    } else {
        sanitized
    }
}

fn truncate_utf8_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_owned();
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    input[..end].to_owned()
}

pub fn archive_root(data_dir: &Path) -> PathBuf {
    data_dir.join(ARCHIVE_DIR)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveWalkTarget {
    Files,
    MonthDirectories,
}

fn archive_walk_error(error: io::Error, operation: &str, path: &Path) -> anyhow::Error {
    anyhow::Error::new(error).context(format!("failed to {operation} {}", path.display()))
}

pub(crate) fn walk_archive_paths(
    root: &Path,
    target: ArchiveWalkTarget,
) -> impl Stream<Item = Result<PathBuf>> + '_ {
    async_stream::stream! {
        let mut streams = match tokio::fs::read_dir(root).await {
            Ok(streams) => streams,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return,
            Err(error) => {
                yield Err(archive_walk_error(error, "open archive directory", root));
                return;
            }
        };

        loop {
            let stream = match streams.next_entry().await {
                Ok(Some(stream)) => stream,
                Ok(None) => break,
                Err(error) => {
                    yield Err(archive_walk_error(error, "enumerate archive directory", root));
                    break;
                }
            };
            let stream_path = stream.path();
            let stream_kind = match stream.file_type().await {
                Ok(kind) => kind,
                Err(error) => {
                    yield Err(archive_walk_error(error, "read archive entry type for", &stream_path));
                    continue;
                }
            };
            if !stream_kind.is_dir() {
                continue;
            }
            let mut months = match tokio::fs::read_dir(&stream_path).await {
                Ok(months) => months,
                Err(error) => {
                    yield Err(archive_walk_error(error, "open archive directory", &stream_path));
                    continue;
                }
            };

            loop {
                let month = match months.next_entry().await {
                    Ok(Some(month)) => month,
                    Ok(None) => break,
                    Err(error) => {
                        yield Err(archive_walk_error(error, "enumerate archive directory", &stream_path));
                        break;
                    }
                };
                let month_path = month.path();
                let month_kind = match month.file_type().await {
                    Ok(kind) => kind,
                    Err(error) => {
                        yield Err(archive_walk_error(error, "read archive entry type for", &month_path));
                        continue;
                    }
                };
                if !month_kind.is_dir() {
                    continue;
                }
                if target == ArchiveWalkTarget::MonthDirectories {
                    yield Ok(month_path);
                    continue;
                }
                let mut entries = match tokio::fs::read_dir(&month_path).await {
                    Ok(entries) => entries,
                    Err(error) => {
                        yield Err(archive_walk_error(error, "open archive directory", &month_path));
                        continue;
                    }
                };

                loop {
                    let entry = match entries.next_entry().await {
                        Ok(Some(entry)) => entry,
                        Ok(None) => break,
                        Err(error) => {
                            yield Err(archive_walk_error(error, "enumerate archive directory", &month_path));
                            break;
                        }
                    };
                    let path = entry.path();
                    let kind = match entry.file_type().await {
                        Ok(kind) => kind,
                        Err(error) => {
                            yield Err(archive_walk_error(error, "read archive entry type for", &path));
                            continue;
                        }
                    };
                    if kind.is_file() {
                        yield Ok(path);
                    }
                }
            }
        }
    }
}

pub fn list_streams(data_dir: &Path) -> io::Result<Vec<String>> {
    read_dir_names(&archive_root(data_dir), |entry| {
        entry.file_type().is_ok_and(|file_type| file_type.is_dir())
    })
}

pub fn list_months(data_dir: &Path, stream: &str) -> io::Result<Vec<String>> {
    validate_stream_component(stream)?;
    read_dir_names(&archive_root(data_dir).join(stream), |entry| {
        entry.file_type().is_ok_and(|file_type| file_type.is_dir())
            && is_month_component(&entry.file_name().to_string_lossy())
    })
}

pub fn resolve_archive_file_path(
    data_dir: &Path,
    stream: &str,
    month: &str,
    filename: &str,
) -> io::Result<Option<PathBuf>> {
    validate_stream_component(stream)?;
    validate_month_component(month)?;
    validate_filename_component(filename)?;

    if !filename.ends_with(".jsonl") {
        return Ok(None);
    }

    let path = archive_root(data_dir)
        .join(stream)
        .join(month)
        .join(filename);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(path)),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_dir_names(
    path: &Path,
    mut include: impl FnMut(&fs::DirEntry) -> bool,
) -> io::Result<Vec<String>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        if include(&entry) {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

pub(crate) fn validate_stream_component(stream: &str) -> io::Result<()> {
    validate_path_component(stream, "stream")
}

fn validate_filename_component(filename: &str) -> io::Result<()> {
    validate_path_component(filename, "filename")
}

fn validate_path_component(component: &str, name: &str) -> io::Result<()> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
        || component != sanitize_filename::sanitize(component)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {name} path component"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_month_component(month: &str) -> io::Result<()> {
    if !is_month_component(month) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid month path component",
        ));
    }
    Ok(())
}

fn is_month_component(month: &str) -> bool {
    matches_digit_shape(month, 7, &[(4, b'-')])
        && NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d").is_ok()
}

/// Checks that `input` has the expected byte length, with ASCII digits except
/// for the specified separators. Unlike chrono, this rejects signed or
/// non-zero-padded dates such as `+026-07-01` and `2026-7-1`.
pub(crate) fn matches_digit_shape(input: &str, len: usize, separators: &[(usize, u8)]) -> bool {
    input.len() == len
        && input.bytes().enumerate().all(|(index, byte)| {
            separators
                .iter()
                .find(|&&(separator_index, _)| separator_index == index)
                .map_or(byte.is_ascii_digit(), |&(_, separator)| byte == separator)
        })
}

/// Checks that `retention` can be represented as an archive cutoff date.
pub fn validate_retention(retention: Duration) -> Result<()> {
    retention_cutoff(jst_now(), retention).map(|_| ())
}

/// Runs garbage collection without deleting files and reports what it found.
pub async fn dry_run_garbage_collection(
    store: &Arc<Mutex<ArchiveStore>>,
    retention: Duration,
) -> Result<GarbageCollectionDryRun> {
    let cutoff = retention_cutoff(jst_now(), retention)?;
    let eligible_files = visit_expired_files(store, cutoff, |_| async { Ok(()) }).await?;
    Ok(GarbageCollectionDryRun {
        eligible_files,
        cutoff,
    })
}

/// Deletes completed archive files whose recording-start timestamp is older
/// than `retention`. Active files are never removed. Empty month directories
/// are removed, while stream directories are retained.
pub async fn collect_garbage(
    store: &Arc<Mutex<ArchiveStore>>,
    retention: Duration,
) -> Result<usize> {
    let cutoff = retention_cutoff(jst_now(), retention)?;
    let data_dir = lock_store(store).data_dir.clone();
    let files_removed = visit_expired_files(store, cutoff, |path| async move {
        tokio::fs::remove_file(&path)
            .await
            .with_context(|| format!("failed to remove {}", path.display()))?;
        Ok(())
    })
    .await?;
    remove_empty_month_directories(&data_dir).await?;
    Ok(files_removed)
}

fn retention_cutoff(
    now: DateTime<FixedOffset>,
    retention: Duration,
) -> Result<DateTime<FixedOffset>> {
    let retention = chrono::Duration::from_std(retention).context("retention is too large")?;
    now.checked_sub_signed(retention)
        .context("retention exceeds the supported date range")
}

async fn visit_expired_files<F, Fut>(
    store: &Arc<Mutex<ArchiveStore>>,
    cutoff: DateTime<FixedOffset>,
    mut on_expired: F,
) -> Result<usize>
where
    F: FnMut(PathBuf) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let (data_dir, active_paths) = {
        let store = lock_store(store);
        (store.data_dir.clone(), store.active_paths())
    };
    let mut eligible = 0;
    let root = archive_root(&data_dir);
    let paths = walk_archive_paths(&root, ArchiveWalkTarget::Files);
    futures_util::pin_mut!(paths);
    while let Some(path) = paths.next().await {
        let path = path?;
        if !is_expired(&path, cutoff) {
            continue;
        }
        if active_paths.contains(&path) {
            continue;
        }
        on_expired(path).await?;
        eligible += 1;
    }
    Ok(eligible)
}

async fn remove_empty_month_directories(data_dir: &Path) -> Result<()> {
    let root = archive_root(data_dir);
    let paths = walk_archive_paths(&root, ArchiveWalkTarget::MonthDirectories);
    futures_util::pin_mut!(paths);
    while let Some(path) = paths.next().await {
        let path = path?;
        let Some(month) = path.file_name() else {
            continue;
        };
        if !is_month_component(&month.to_string_lossy()) {
            continue;
        }
        match tokio::fs::remove_dir(&path).await {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", path.display()));
            }
        }
    }
    Ok(())
}

fn is_expired(path: &Path, cutoff: DateTime<FixedOffset>) -> bool {
    let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(prefix) = parse_recording_started_at(filename) else {
        return false;
    };
    let started_at = NaiveDateTime::parse_from_str(prefix, STARTED_AT_FORMAT)
        .expect("recording timestamp was already validated");
    jst()
        .from_local_datetime(&started_at)
        .single()
        .is_some_and(|time| time < cutoff)
}

pub(crate) fn parse_recording_started_at(filename: &str) -> Option<&str> {
    let prefix = filename.get(..STARTED_AT_LEN)?;
    if filename.as_bytes().get(STARTED_AT_LEN) != Some(&b'.') {
        return None;
    }
    NaiveDateTime::parse_from_str(prefix, STARTED_AT_FORMAT).ok()?;
    Some(prefix)
}

pub(crate) fn validate_recording_started_at(recording_started_at: &str) -> io::Result<()> {
    if recording_started_at.len() != STARTED_AT_LEN
        || NaiveDateTime::parse_from_str(recording_started_at, STARTED_AT_FORMAT).is_err()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid recording start time",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt as _;

    use super::*;
    use crate::test_support::TestDir;

    const TEST_DIR_PREFIX: &str = "aribcap-archive-test-";

    async fn collect_archive_walk(root: &Path, target: ArchiveWalkTarget) -> Result<Vec<PathBuf>> {
        walk_archive_paths(root, target).try_collect().await
    }

    #[tokio::test]
    async fn archive_walk_files_yields_only_regular_files_at_file_depth() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let root = archive_root(&data_dir);
        let month = root.join("nhk").join("2026-07");
        fs::create_dir_all(month.join("nested")).unwrap();
        let first = month.join("first.jsonl");
        let second = month.join("second.txt");
        fs::write(&first, "{}\n").unwrap();
        fs::write(&second, "ignored by consumers, not the walker\n").unwrap();
        fs::write(root.join("root-file.jsonl"), "{}\n").unwrap();
        fs::write(root.join("nhk").join("stream-file.jsonl"), "{}\n").unwrap();
        fs::write(month.join("nested").join("too-deep.jsonl"), "{}\n").unwrap();

        let mut paths = collect_archive_walk(&root, ArchiveWalkTarget::Files)
            .await
            .unwrap();
        paths.sort();

        assert_eq!(paths, vec![first, second]);
    }

    #[tokio::test]
    async fn archive_walk_month_directories_stops_at_month_depth() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let root = archive_root(&data_dir);
        let first = root.join("nhk").join("2026-06");
        let second = root.join("nhk").join("2026-07");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(first.join("program.jsonl"), "{}\n").unwrap();

        let mut paths = collect_archive_walk(&root, ArchiveWalkTarget::MonthDirectories)
            .await
            .unwrap();
        paths.sort();

        assert_eq!(paths, vec![first, second]);
    }

    #[tokio::test]
    async fn archive_walk_missing_root_is_empty() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let root = data_dir.join("missing");

        let paths = collect_archive_walk(&root, ArchiveWalkTarget::Files)
            .await
            .unwrap();

        assert!(paths.is_empty());
    }

    #[test]
    fn clearing_dirty_path_preserves_a_newer_generation() {
        let path = PathBuf::from("archive.jsonl");
        let mut store = ArchiveStore::new("data");
        store.mark_dirty(path.clone());
        let first_generation = store.snapshot_dirty_paths()[&path];

        store.mark_dirty(path.clone());

        assert!(!store.clear_dirty_path_if_unchanged(&path, first_generation));
        let second_generation = store.snapshot_dirty_paths()[&path];
        assert!(store.clear_dirty_path_if_unchanged(&path, second_generation));
        assert!(store.snapshot_dirty_paths().is_empty());
    }

    #[test]
    fn continues_writing_after_store_mutex_is_poisoned() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let store = Arc::new(Mutex::new(ArchiveStore::new(data_dir.to_path_buf())));
        let now = DateTime::parse_from_rfc3339("2026-07-13T12:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1}"#;
        let caption = r#"{"type":"caption","text":"after poison"}"#;
        let started = handle_line_at(&store, "nhk", eit, now).unwrap().unwrap();
        let ArchiveEvent::ProgramStarted(path) = started else {
            panic!("expected a new archive file")
        };

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.lock().unwrap();
            panic!("poison the archive store mutex");
        }));
        assert!(panic.is_err());
        assert!(store.is_poisoned());

        handle_line_at(&store, "nhk", caption, now).unwrap();

        assert!(!store.is_poisoned());
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            format!("{eit}\n{caption}\n")
        );
    }

    #[test]
    fn rejects_retention_outside_supported_date_range() {
        let retention = humantime::parse_duration("300000y").unwrap();

        let error = validate_retention(retention).unwrap_err();

        assert_eq!(
            error.to_string(),
            "retention exceeds the supported date range"
        );
    }

    #[tokio::test]
    async fn garbage_collection_dry_run_counts_without_deleting() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let directory = archive_root(&data_dir).join("nhk").join("2000-01");
        fs::create_dir_all(&directory).unwrap();
        let expired = directory.join("2000-01-01_00-00-00.news.jsonl");
        fs::write(&expired, "{}\n").unwrap();
        let store = Arc::new(Mutex::new(ArchiveStore::new(data_dir.to_path_buf())));

        let dry_run = dry_run_garbage_collection(&store, Duration::from_secs(24 * 60 * 60))
            .await
            .unwrap();

        assert_eq!(dry_run.eligible_files, 1);
        assert!(expired.exists());
    }

    #[tokio::test]
    async fn garbage_collection_deletes_expired_file_after_deactivation() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let store = Arc::new(Mutex::new(ArchiveStore::new(data_dir.to_path_buf())));
        let recorded_at = DateTime::parse_from_rfc3339("2000-01-01T00:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1}"#;
        let started = handle_line_at(&store, "nhk", eit, recorded_at)
            .unwrap()
            .unwrap();
        let ArchiveEvent::ProgramStarted(path) = started else {
            panic!("expected a new archive file")
        };
        let retention = Duration::from_secs(24 * 60 * 60);

        assert_eq!(collect_garbage(&store, retention).await.unwrap(), 0);
        assert!(path.exists());

        deactivate_stream(&store, "nhk");

        assert_eq!(collect_garbage(&store, retention).await.unwrap(), 1);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn garbage_collection_removes_empty_month_but_keeps_stream_directory() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let stream_directory = archive_root(&data_dir).join("nhk");
        let month_directory = stream_directory.join("2000-01");
        fs::create_dir_all(&month_directory).unwrap();
        fs::write(
            month_directory.join("2000-01-01_00-00-00.news.jsonl"),
            "{}\n",
        )
        .unwrap();
        let store = Arc::new(Mutex::new(ArchiveStore::new(data_dir.to_path_buf())));

        assert_eq!(
            collect_garbage(&store, Duration::from_secs(24 * 60 * 60))
                .await
                .unwrap(),
            1
        );
        assert!(!month_directory.exists());
        assert!(stream_directory.is_dir());
    }

    #[test]
    fn deactivated_stream_waits_for_next_present_eit() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let store = Arc::new(Mutex::new(ArchiveStore::new(data_dir.to_path_buf())));
        let now = DateTime::parse_from_rfc3339("2026-07-13T12:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1}"#;
        let started = handle_line_at(&store, "nhk", eit, now).unwrap().unwrap();

        deactivate_stream(&store, "nhk");
        let result = handle_line_at(
            &store,
            "nhk",
            r#"{"type":"caption","text":"after reconnect"}"#,
            now,
        )
        .unwrap();

        assert_eq!(result, Some(ArchiveEvent::SkippedNoProgram));
        let ArchiveEvent::ProgramStarted(path) = started else {
            panic!("expected a new archive file")
        };
        assert_eq!(fs::read_to_string(path).unwrap(), format!("{eit}\n"));
    }

    #[test]
    fn records_lines_after_present_eit_in_one_archive_file() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let store = Arc::new(Mutex::new(ArchiveStore::new(data_dir.to_path_buf())));
        let now = DateTime::parse_from_rfc3339("2026-07-13T12:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1,"shortEvents":[{"languageCode":"jpn","eventName":"ニュース"}]}"#;

        assert_eq!(
            handle_line_at(&store, "nhk", r#"{"type":"caption","text":"before"}"#, now).unwrap(),
            Some(ArchiveEvent::SkippedNoProgram)
        );
        let started = handle_line_at(&store, "nhk", eit, now).unwrap();
        handle_line_at(&store, "nhk", r#"{"type":"caption","text":"after"}"#, now).unwrap();

        let ArchiveEvent::ProgramStarted(path) = started.unwrap() else {
            panic!("expected a new archive file")
        };
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            format!("{eit}\n{{\"type\":\"caption\",\"text\":\"after\"}}\n")
        );
    }

    #[test]
    fn lists_archive_directories_and_resolves_archive_file_paths() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let directory = archive_root(&data_dir).join("nhk").join("2026-07");
        fs::create_dir_all(&directory).unwrap();
        let filename = "2026-07-14_12-00-00.news#weather.jsonl";
        fs::write(directory.join(filename), "{}\n").unwrap();
        fs::write(directory.join("ignored.txt"), "ignored").unwrap();

        assert_eq!(list_streams(&data_dir).unwrap(), ["nhk"]);
        assert_eq!(list_months(&data_dir, "nhk").unwrap(), ["2026-07"]);
        assert_eq!(
            resolve_archive_file_path(&data_dir, "nhk", "2026-07", filename).unwrap(),
            Some(directory.join(filename))
        );
        assert_eq!(
            resolve_archive_file_path(&data_dir, "nhk", "2026-07", "missing.jsonl").unwrap(),
            None
        );
        assert_eq!(
            resolve_archive_file_path(&data_dir, "nhk", "2026-07", "ignored.txt").unwrap(),
            None
        );

        let directory_entry = directory.join("directory.jsonl");
        fs::create_dir(&directory_entry).unwrap();
        assert_eq!(
            resolve_archive_file_path(&data_dir, "nhk", "2026-07", "directory.jsonl").unwrap(),
            None
        );

        #[cfg(unix)]
        {
            let symlink_entry = directory.join("symlink.jsonl");
            std::os::unix::fs::symlink(filename, &symlink_entry).unwrap();
            assert_eq!(
                resolve_archive_file_path(&data_dir, "nhk", "2026-07", "symlink.jsonl").unwrap(),
                None
            );
        }
    }

    #[test]
    fn archive_listing_rejects_invalid_path_components() {
        let data_dir = Path::new("data");

        assert_eq!(
            list_months(data_dir, "..").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            resolve_archive_file_path(data_dir, "nhk", "2026-07", "../archive.jsonl")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn month_components_must_be_real_calendar_months() {
        for month in ["2026-01", "2026-12"] {
            validate_month_component(month).unwrap();
        }
        for month in ["2026-00", "2026-13", "2026-1", "2026-aa"] {
            assert_eq!(
                validate_month_component(month).unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
                "accepted {month:?}"
            );
        }
    }
}

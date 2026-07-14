use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone as _, Utc};
use serde_json::Value;

const RECORDS_DIR: &str = "records";
const STARTED_AT_FORMAT: &str = "%Y-%m-%d_%H-%M-%S";
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
    active: BTreeMap<String, ActiveRecord>,
}

#[derive(Debug)]
struct ActiveRecord {
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

impl ArchiveStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            active: BTreeMap::new(),
        }
    }

    fn append_active(&mut self, stream_name: &str, line: &str) -> Result<bool> {
        let Some(active) = self.active.get_mut(stream_name) else {
            return Ok(false);
        };
        active.write_line(line)?;
        Ok(true)
    }

    fn append_if_same_program(
        &mut self,
        stream_name: &str,
        program: &ProgramKey,
        line: &str,
    ) -> Result<bool> {
        let Some(active) = self.active.get_mut(stream_name) else {
            return Ok(false);
        };
        if active.program != *program {
            return Ok(false);
        }
        active.write_line(line)?;
        Ok(true)
    }

    fn replace_active(&mut self, stream_name: String, active: ActiveRecord) {
        self.active.insert(stream_name, active);
    }
}

impl ActiveRecord {
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

/// Closes and removes the active archive for a stream, if one exists.
/// Subsequent lines are skipped until `handle_line` receives a present EIT
/// and opens a new archive.
pub fn deactivate_stream(store: &Arc<Mutex<ArchiveStore>>, stream_name: &str) -> Result<()> {
    lock_store(store)?.active.remove(stream_name);
    Ok(())
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
        return if lock_store(store)?.append_active(stream_name, line)? {
            Ok(None)
        } else {
            Ok(Some(ArchiveEvent::SkippedNoProgram))
        };
    };

    let data_dir = {
        let mut store = lock_store(store)?;
        if store.append_if_same_program(stream_name, &program.key, line)? {
            return Ok(None);
        }
        store.data_dir.clone()
    };

    let mut active = open_program_file(&data_dir, stream_name, &program, now)?;
    active.write_line(line)?;
    let path = active.path.clone();
    lock_store(store)?.replace_active(stream_name.to_owned(), active);
    Ok(Some(ArchiveEvent::ProgramStarted(path)))
}

fn lock_store(store: &Arc<Mutex<ArchiveStore>>) -> Result<MutexGuard<'_, ArchiveStore>> {
    store
        .lock()
        .map_err(|_| anyhow!("archive store mutex poisoned"))
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
        title: event_name(value).unwrap_or_else(|| "no-title".to_owned()),
    })
}

fn event_name(value: &Value) -> Option<String> {
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

fn open_program_file(
    data_dir: &Path,
    stream_name: &str,
    program: &Program,
    now: DateTime<FixedOffset>,
) -> Result<ActiveRecord> {
    let stream = safe_component(stream_name, "stream");
    let month = now.format("%Y-%m").to_string();
    let started_at = now.format(STARTED_AT_FORMAT);
    let title = safe_component(&truncate_utf8_bytes(&program.title, 200), "no-title");
    let filename_stem = format!("{started_at}.{title}");
    let directory = records_root(data_dir).join(stream).join(month);
    fs::create_dir_all(&directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;

    // A restarted process can record the same programme in the same second.
    // Keep both captures rather than overwriting a previous archive.
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
                return Ok(ActiveRecord {
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

pub fn records_root(data_dir: &Path) -> PathBuf {
    data_dir.join(RECORDS_DIR)
}

/// Checks that `retention` can be represented as an archive cutoff date.
pub fn validate_retention(retention: Duration) -> Result<()> {
    retention_cutoff(jst_now(), retention).map(|_| ())
}

/// Runs garbage collection without deleting files and reports what it found.
pub fn dry_run_garbage_collection(
    store: &Arc<Mutex<ArchiveStore>>,
    retention: Duration,
) -> Result<GarbageCollectionDryRun> {
    let cutoff = retention_cutoff(jst_now(), retention)?;
    let eligible_files = visit_expired_files(store, cutoff, |_| Ok(()))?;
    Ok(GarbageCollectionDryRun {
        eligible_files,
        cutoff,
    })
}

/// Deletes completed archive files whose recording-start timestamp is older
/// than `retention`. The active file for each stream is never removed.
pub fn collect_garbage(store: &Arc<Mutex<ArchiveStore>>, retention: Duration) -> Result<usize> {
    let cutoff = retention_cutoff(jst_now(), retention)?;
    visit_expired_files(store, cutoff, |path| {
        fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))
    })
}

fn retention_cutoff(
    now: DateTime<FixedOffset>,
    retention: Duration,
) -> Result<DateTime<FixedOffset>> {
    let retention = chrono::Duration::from_std(retention).context("retention is too large")?;
    now.checked_sub_signed(retention)
        .context("retention exceeds the supported date range")
}

fn visit_expired_files(
    store: &Arc<Mutex<ArchiveStore>>,
    cutoff: DateTime<FixedOffset>,
    mut on_expired: impl FnMut(&Path) -> Result<()>,
) -> Result<usize> {
    let (data_dir, active_paths) = {
        let store = lock_store(store)?;
        let active_paths = store
            .active
            .values()
            .map(|record| record.path.clone())
            .collect::<HashSet<_>>();
        (store.data_dir.clone(), active_paths)
    };
    let mut eligible = 0;
    let root = records_root(&data_dir);
    let Ok(streams) = fs::read_dir(&root) else {
        return Ok(eligible);
    };

    for stream in streams {
        let stream = stream?;
        if !stream.file_type()?.is_dir() {
            continue;
        }
        for month in fs::read_dir(stream.path())? {
            let month = month?;
            if !month.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(month.path())? {
                let entry = entry?;
                let path = entry.path();
                if !entry.file_type()?.is_file() || !is_expired(&path, cutoff) {
                    continue;
                }
                if active_paths.contains(&path) {
                    continue;
                }
                on_expired(&path)?;
                eligible += 1;
            }
        }
    }
    Ok(eligible)
}

fn is_expired(path: &Path, cutoff: DateTime<FixedOffset>) -> bool {
    let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(prefix) = filename.get(..19) else {
        return false;
    };
    let Ok(started_at) = NaiveDateTime::parse_from_str(prefix, STARTED_AT_FORMAT) else {
        return false;
    };
    jst()
        .from_local_datetime(&started_at)
        .single()
        .is_some_and(|time| time < cutoff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_retention_outside_supported_date_range() {
        let retention = humantime::parse_duration("300000y").unwrap();

        let error = validate_retention(retention).unwrap_err();

        assert_eq!(
            error.to_string(),
            "retention exceeds the supported date range"
        );
    }

    #[test]
    fn garbage_collection_dry_run_counts_without_deleting() {
        let data_dir = std::env::temp_dir().join(format!(
            "aribcap-archive-gc-dry-run-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&data_dir);
        let directory = records_root(&data_dir).join("nhk").join("2000-01");
        fs::create_dir_all(&directory).unwrap();
        let expired = directory.join("2000-01-01_00-00-00.news.jsonl");
        fs::write(&expired, "{}\n").unwrap();
        let store = Arc::new(Mutex::new(ArchiveStore::new(&data_dir)));

        let dry_run =
            dry_run_garbage_collection(&store, Duration::from_secs(24 * 60 * 60)).unwrap();

        assert_eq!(dry_run.eligible_files, 1);
        assert!(expired.exists());
        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn garbage_collection_deletes_expired_file_only_after_deactivation() {
        let data_dir = std::env::temp_dir().join(format!(
            "aribcap-archive-gc-active-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&data_dir);
        let store = Arc::new(Mutex::new(ArchiveStore::new(&data_dir)));
        let recorded_at = DateTime::parse_from_rfc3339("2000-01-01T00:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1}"#;
        let started = handle_line_at(&store, "nhk", eit, recorded_at)
            .unwrap()
            .unwrap();
        let ArchiveEvent::ProgramStarted(path) = started else {
            panic!("expected a new archive")
        };
        let retention = Duration::from_secs(24 * 60 * 60);

        assert_eq!(collect_garbage(&store, retention).unwrap(), 0);
        assert!(path.exists());

        deactivate_stream(&store, "nhk").unwrap();

        assert_eq!(collect_garbage(&store, retention).unwrap(), 1);
        assert!(!path.exists());
        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn deactivated_stream_waits_for_next_present_eit() {
        let data_dir = std::env::temp_dir().join(format!(
            "aribcap-archive-deactivate-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&data_dir);
        let store = Arc::new(Mutex::new(ArchiveStore::new(&data_dir)));
        let now = DateTime::parse_from_rfc3339("2026-07-13T12:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1}"#;
        let started = handle_line_at(&store, "nhk", eit, now).unwrap().unwrap();

        deactivate_stream(&store, "nhk").unwrap();
        let result = handle_line_at(
            &store,
            "nhk",
            r#"{"type":"caption","text":"after reconnect"}"#,
            now,
        )
        .unwrap();

        assert_eq!(result, Some(ArchiveEvent::SkippedNoProgram));
        let ArchiveEvent::ProgramStarted(path) = started else {
            panic!("expected a new archive")
        };
        assert_eq!(fs::read_to_string(path).unwrap(), format!("{eit}\n"));
        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn records_lines_after_present_eit_in_one_program_file() {
        let data_dir =
            std::env::temp_dir().join(format!("aribcap-archive-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&data_dir);
        let store = Arc::new(Mutex::new(ArchiveStore::new(&data_dir)));
        let now = DateTime::parse_from_rfc3339("2026-07-13T12:00:00+09:00").unwrap();
        let eit = r#"{"type":"eit","section":"present","eventId":1,"shortEvents":[{"languageCode":"jpn","eventName":"ニュース"}]}"#;

        assert_eq!(
            handle_line_at(&store, "nhk", r#"{"type":"caption","text":"before"}"#, now).unwrap(),
            Some(ArchiveEvent::SkippedNoProgram)
        );
        let started = handle_line_at(&store, "nhk", eit, now).unwrap();
        handle_line_at(&store, "nhk", r#"{"type":"caption","text":"after"}"#, now).unwrap();

        let ArchiveEvent::ProgramStarted(path) = started.unwrap() else {
            panic!("expected a new archive")
        };
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            format!("{eit}\n{{\"type\":\"caption\",\"text\":\"after\"}}\n")
        );
        fs::remove_dir_all(data_dir).unwrap();
    }
}

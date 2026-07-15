use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::{archive, search_db};

const DEFAULT_SEARCH_LIMIT: i64 = 20;
const MAX_SEARCH_LIMIT: i64 = 200;
const DEFAULT_INNER_HITS: i64 = 5;
const MAX_INNER_HITS: i64 = 50;
const DEFAULT_CAPTION_LIMIT: i64 = 100;
const MAX_CAPTION_LIMIT: i64 = 500;

#[derive(Debug)]
pub enum QueryServiceError {
    BadRequest(String),
    NotFound(String),
    Unavailable(String),
    Internal(anyhow::Error),
}

impl QueryServiceError {
    fn internal(error: impl Into<anyhow::Error>) -> Self {
        Self::Internal(error.into())
    }
}

impl std::fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) | Self::NotFound(message) | Self::Unavailable(message) => {
                formatter.write_str(message)
            }
            Self::Internal(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for QueryServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Internal(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for QueryServiceError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::InvalidInput => Self::BadRequest(error.to_string()),
            io::ErrorKind::NotFound => Self::NotFound(error.to_string()),
            _ => Self::internal(error),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct SearchRequest {
    /// Search program metadata and caption text with one expression.
    pub q: Option<String>,
    /// Search program titles and descriptions only.
    pub program_q: Option<String>,
    /// Search caption text only. May be combined with `program_q`.
    pub line_q: Option<String>,
    /// Genre filter in `0..15` or `0..15:0..15` form.
    pub genre: Option<String>,
    /// Restrict results to one archive stream. When omitted, search all streams.
    pub stream: Option<String>,
    /// Inclusive lower recording-time bound.
    pub from: Option<String>,
    /// Inclusive upper recording-time bound.
    pub to: Option<String>,
    /// Maximum programs to return. Defaults to 20 and is capped at 200.
    pub limit: Option<i64>,
    /// Maximum caption hits per program. Defaults to 5 and is capped at 50.
    pub inner_hits: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SearchResponse {
    pub items: Vec<SearchResultItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SearchResultItem {
    pub program_id: i64,
    pub stream: String,
    pub recording_started_at: String,
    pub start_time: Option<String>,
    pub title: String,
    pub description: String,
    pub path: String,
    pub hits: Vec<SearchHitItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SearchHitItem {
    pub line_id: i64,
    pub line_no: i64,
    pub time: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ListStreamsResponse {
    pub streams: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProgramCaptionsResponse {
    pub program: ProgramSummary,
    pub captions: Vec<ProgramCaption>,
    pub next_start_line: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProgramSummary {
    pub program_id: i64,
    pub stream: String,
    pub recording_started_at: String,
    pub start_time: Option<String>,
    pub duration_sec: Option<i64>,
    pub title: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProgramCaption {
    pub line_id: i64,
    pub line_no: i64,
    pub time: Option<String>,
    pub text: String,
    pub duration_ms: Option<i64>,
    pub language_code: Option<String>,
}

impl From<search_db::SearchHit> for SearchHitItem {
    fn from(hit: search_db::SearchHit) -> Self {
        let search_db::SearchHit {
            line_id,
            line_no,
            time,
            text,
        } = hit;
        Self {
            line_id,
            line_no,
            time,
            text,
        }
    }
}

impl From<search_db::ProgramDetails> for ProgramSummary {
    fn from(program: search_db::ProgramDetails) -> Self {
        let search_db::ProgramDetails {
            program_id,
            stream,
            recording_started_at,
            start_time,
            duration_sec,
            title,
            description,
        } = program;
        Self {
            program_id,
            stream,
            recording_started_at,
            start_time,
            duration_sec,
            title,
            description,
        }
    }
}

impl From<search_db::CaptionLine> for ProgramCaption {
    fn from(caption: search_db::CaptionLine) -> Self {
        let search_db::CaptionLine {
            line_id,
            line_no,
            time,
            text,
            duration_ms,
            language_code,
        } = caption;
        Self {
            line_id,
            line_no,
            time,
            text,
            duration_ms,
            language_code,
        }
    }
}

enum SearchMode {
    General(search_db::SearchExpression),
    Program(search_db::SearchExpression),
    Line(search_db::SearchExpression),
    Combined(search_db::SearchExpression, search_db::SearchExpression),
}

impl SearchMode {
    fn resolve(query: &SearchRequest) -> Result<Self, QueryServiceError> {
        let q = non_empty(query.q.as_deref());
        let program_q = non_empty(query.program_q.as_deref());
        let line_q = non_empty(query.line_q.as_deref());
        let parse = |value: &str| {
            search_db::parse_search_expression(value)
                .map_err(|message| QueryServiceError::BadRequest(message.to_owned()))
        };

        match (q, program_q, line_q) {
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(QueryServiceError::BadRequest(
                "combine `q` with `program_q`/`line_q` is not supported".to_owned(),
            )),
            (Some(q), None, None) => Ok(Self::General(parse(q)?)),
            (None, Some(program_q), None) => Ok(Self::Program(parse(program_q)?)),
            (None, None, Some(line_q)) => Ok(Self::Line(parse(line_q)?)),
            (None, Some(program_q), Some(line_q)) => {
                Ok(Self::Combined(parse(program_q)?, parse(line_q)?))
            }
            (None, None, None) => Err(QueryServiceError::BadRequest(
                "provide one of `q`, `program_q`, or `line_q`".to_owned(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct ArchiveQueryService {
    data_dir: Arc<PathBuf>,
    search_pool: SqlitePool,
    search_db_ready: Arc<AtomicBool>,
}

impl ArchiveQueryService {
    pub fn new(
        data_dir: PathBuf,
        search_db_path: PathBuf,
        search_db_ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            data_dir: Arc::new(data_dir),
            search_pool: search_db::open_reader_pool(&search_db_path),
            search_db_ready,
        }
    }

    fn require_ready(&self) -> Result<(), QueryServiceError> {
        if self.search_db_ready.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(QueryServiceError::Unavailable(
                "search database is not ready".to_owned(),
            ))
        }
    }

    pub async fn list_streams(&self) -> Result<Vec<String>, QueryServiceError> {
        self.require_ready()?;
        let data_dir = self.data_dir.clone();
        blocking_io(move || archive::list_streams(&data_dir)).await
    }

    pub async fn list_months(&self, stream: String) -> Result<Vec<String>, QueryServiceError> {
        self.require_ready()?;
        archive::validate_stream_component(&stream).map_err(QueryServiceError::from)?;
        let data_dir = self.data_dir.clone();
        blocking_io(move || archive::list_months(&data_dir, &stream)).await
    }

    pub async fn list_programs(
        &self,
        stream: String,
        month: String,
    ) -> Result<Vec<archive::ProgramEntry>, QueryServiceError> {
        self.require_ready()?;
        archive::validate_stream_component(&stream).map_err(QueryServiceError::from)?;
        archive::validate_month_component(&month).map_err(QueryServiceError::from)?;
        let mut connection = self
            .search_pool
            .acquire()
            .await
            .map_err(QueryServiceError::internal)?;
        let programs = search_db::list_indexed_programs(&mut connection, &stream, &month)
            .await
            .map_err(QueryServiceError::internal)?
            .into_iter()
            .map(|program| archive::ProgramEntry {
                path: program_api_path(&program.stream, &program.recording_started_at),
                stream: program.stream,
                month: program.month,
                filename: program.filename,
                size_bytes: u64::try_from(program.size_bytes).unwrap_or(0),
            })
            .collect();
        Ok(programs)
    }

    pub async fn resolve_program_path(
        &self,
        stream: String,
        recording_started_at: String,
    ) -> Result<PathBuf, QueryServiceError> {
        self.require_ready()?;
        archive::validate_stream_component(&stream).map_err(QueryServiceError::from)?;
        archive::validate_recording_started_at(&recording_started_at)
            .map_err(QueryServiceError::from)?;
        let mut connection = self
            .search_pool
            .acquire()
            .await
            .map_err(QueryServiceError::internal)?;
        let Some(program) =
            search_db::find_indexed_program(&mut connection, &stream, &recording_started_at)
                .await
                .map_err(QueryServiceError::internal)?
        else {
            return Err(QueryServiceError::NotFound("program not found".to_owned()));
        };
        let data_dir = self.data_dir.clone();
        let path = blocking_io(move || {
            archive::resolve_archive_file_path(
                &data_dir,
                &program.stream,
                &program.month,
                &program.filename,
            )
        })
        .await?;
        path.ok_or_else(|| QueryServiceError::NotFound("program not found".to_owned()))
    }

    pub async fn search(&self, query: SearchRequest) -> Result<SearchResponse, QueryServiceError> {
        self.require_ready()?;
        let limit = query
            .limit
            .unwrap_or(DEFAULT_SEARCH_LIMIT)
            .clamp(1, MAX_SEARCH_LIMIT);
        let inner_hits = query
            .inner_hits
            .unwrap_or(DEFAULT_INNER_HITS)
            .clamp(1, MAX_INNER_HITS);
        let stream = non_empty(query.stream.as_deref()).map(str::to_owned);
        if let Some(stream) = &stream {
            archive::validate_stream_component(stream).map_err(QueryServiceError::from)?;
        }
        let from = non_empty(query.from.as_deref()).map(search_db::expand_from_bound);
        let to = non_empty(query.to.as_deref()).map(search_db::expand_to_bound);
        let genre = parse_genre_filter(query.genre.as_deref())?;
        let mode = SearchMode::resolve(&query)?;

        let mut connection = self
            .search_pool
            .acquire()
            .await
            .map_err(QueryServiceError::internal)?;
        let filter = search_db::SearchFilter {
            stream: stream.as_deref(),
            from: from.as_deref(),
            to: to.as_deref(),
            genre,
        };
        let programs = match mode {
            SearchMode::General(expression) => {
                search_db::search_general(&mut connection, &expression, &filter, limit, inner_hits)
                    .await
            }
            SearchMode::Program(expression) => {
                search_db::search_program_metadata(&mut connection, &expression, &filter, limit)
                    .await
            }
            SearchMode::Line(expression) => {
                search_db::search_captions(&mut connection, &expression, &filter, limit, inner_hits)
                    .await
            }
            SearchMode::Combined(program_expression, line_expression) => {
                search_db::search_combined(
                    &mut connection,
                    &program_expression,
                    &line_expression,
                    &filter,
                    limit,
                    inner_hits,
                )
                .await
            }
        }
        .map_err(QueryServiceError::internal)?;

        let items = programs
            .into_iter()
            .map(|program| SearchResultItem {
                program_id: program.program_id,
                path: program_api_path(&program.stream, &program.recording_started_at),
                stream: program.stream,
                recording_started_at: program.recording_started_at,
                start_time: program.start_time,
                title: program.title,
                description: program.description,
                hits: program.hits.into_iter().map(Into::into).collect(),
            })
            .collect();
        Ok(SearchResponse { items })
    }

    pub async fn get_program_captions(
        &self,
        stream: String,
        recording_started_at: String,
        start_line: Option<i64>,
        limit: Option<i64>,
    ) -> Result<ProgramCaptionsResponse, QueryServiceError> {
        self.require_ready()?;
        archive::validate_stream_component(&stream).map_err(QueryServiceError::from)?;
        archive::validate_recording_started_at(&recording_started_at)
            .map_err(QueryServiceError::from)?;
        let start_line = start_line.unwrap_or(1);
        if start_line < 1 {
            return Err(QueryServiceError::BadRequest(
                "start_line must be at least 1".to_owned(),
            ));
        }
        let limit = limit
            .unwrap_or(DEFAULT_CAPTION_LIMIT)
            .clamp(1, MAX_CAPTION_LIMIT);
        let mut connection = self
            .search_pool
            .acquire()
            .await
            .map_err(QueryServiceError::internal)?;
        let Some(page) = search_db::get_caption_page(
            &mut connection,
            &stream,
            &recording_started_at,
            start_line,
            limit,
        )
        .await
        .map_err(QueryServiceError::internal)?
        else {
            return Err(QueryServiceError::NotFound("program not found".to_owned()));
        };

        let next_start_line = if page.has_more {
            page.captions.last().map(|caption| caption.line_no + 1)
        } else {
            None
        };
        Ok(ProgramCaptionsResponse {
            program: page.program.into(),
            captions: page.captions.into_iter().map(Into::into).collect(),
            next_start_line,
        })
    }
}

pub fn program_api_path(stream: &str, recording_started_at: &str) -> String {
    format!(
        "/api/programs/{}/{}",
        urlencoding::encode(stream),
        urlencoding::encode(recording_started_at)
    )
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn parse_genre_filter(
    value: Option<&str>,
) -> Result<Option<search_db::GenreFilter>, QueryServiceError> {
    let Some(value) = non_empty(value) else {
        return Ok(None);
    };
    let (level1, level2) = match value.split_once(':') {
        Some((level1, level2)) => (level1, Some(level2)),
        None => (value, None),
    };
    let parse_nibble = |value: &str| {
        value
            .parse::<i64>()
            .ok()
            .filter(|value| (0..=15).contains(value))
            .ok_or_else(|| {
                QueryServiceError::BadRequest("genre must be `0..15` or `0..15:0..15`".to_owned())
            })
    };

    Ok(Some(search_db::GenreFilter {
        level1: parse_nibble(level1)?,
        level2: level2.map(parse_nibble).transpose()?,
    }))
}

async fn blocking_io<T>(
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, QueryServiceError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(QueryServiceError::internal)?
        .map_err(QueryServiceError::from)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use super::*;

    static NEXT_TEMP_DIR: AtomicUsize = AtomicUsize::new(0);
    const RECORDING_STARTED_AT: &str = "2026-07-15_12-00-00";

    #[test]
    fn io_errors_map_to_shared_error_categories() {
        let bad_request =
            QueryServiceError::from(io::Error::new(io::ErrorKind::InvalidInput, "invalid input"));
        assert!(matches!(bad_request, QueryServiceError::BadRequest(_)));

        let not_found =
            QueryServiceError::from(io::Error::new(io::ErrorKind::NotFound, "not found"));
        assert!(matches!(not_found, QueryServiceError::NotFound(_)));

        let internal = QueryServiceError::from(io::Error::other("internal"));
        assert!(matches!(internal, QueryServiceError::Internal(_)));
    }

    #[tokio::test]
    async fn search_supports_all_four_modes_and_shared_validation() {
        let (data_dir, service) = seeded_service(2).await;

        for request in [
            SearchRequest {
                q: Some("caption".into()),
                ..Default::default()
            },
            SearchRequest {
                program_q: Some("program".into()),
                ..Default::default()
            },
            SearchRequest {
                line_q: Some("caption".into()),
                ..Default::default()
            },
            SearchRequest {
                program_q: Some("program".into()),
                line_q: Some("caption".into()),
                ..Default::default()
            },
        ] {
            let result = service.search(request).await.unwrap();
            assert_eq!(result.items.len(), 1);
            assert_eq!(result.items[0].title, "program title");
        }

        let error = service
            .search(SearchRequest {
                q: Some("caption".into()),
                line_q: Some("caption".into()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(error, QueryServiceError::BadRequest(_)));

        let error = service
            .search(SearchRequest {
                q: Some("caption".into()),
                genre: Some("16".into()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(error, QueryServiceError::BadRequest(_)));

        service.search_pool.close().await;
        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn caption_pages_handle_defaults_caps_gaps_and_the_final_page() {
        let (data_dir, service) = seeded_service(501).await;

        let default_page = service
            .get_program_captions("nhk".into(), RECORDING_STARTED_AT.into(), None, None)
            .await
            .unwrap();
        assert_eq!(default_page.captions.len(), 100);
        assert_eq!(default_page.captions[0].line_no, 2);
        assert_eq!(default_page.next_start_line, Some(103));

        let after_gap = service
            .get_program_captions("nhk".into(), RECORDING_STARTED_AT.into(), Some(3), Some(1))
            .await
            .unwrap();
        assert_eq!(after_gap.captions[0].line_no, 4);
        assert_eq!(after_gap.next_start_line, Some(5));

        let capped_page = service
            .get_program_captions(
                "nhk".into(),
                RECORDING_STARTED_AT.into(),
                Some(1),
                Some(999),
            )
            .await
            .unwrap();
        assert_eq!(capped_page.captions.len(), 500);
        assert_eq!(capped_page.next_start_line, Some(503));

        let final_page = service
            .get_program_captions(
                "nhk".into(),
                RECORDING_STARTED_AT.into(),
                Some(503),
                Some(10),
            )
            .await
            .unwrap();
        assert_eq!(final_page.captions.len(), 1);
        assert_eq!(final_page.captions[0].line_no, 503);
        assert_eq!(final_page.next_start_line, None);

        let invalid_start = service
            .get_program_captions("nhk".into(), RECORDING_STARTED_AT.into(), Some(0), None)
            .await
            .unwrap_err();
        assert!(matches!(invalid_start, QueryServiceError::BadRequest(_)));

        let missing = service
            .get_program_captions("nhk".into(), "2026-07-15_13-00-00".into(), None, None)
            .await
            .unwrap_err();
        assert!(matches!(missing, QueryServiceError::NotFound(_)));

        service.search_pool.close().await;
        fs::remove_dir_all(data_dir).unwrap();
    }

    async fn seeded_service(caption_count: usize) -> (PathBuf, ArchiveQueryService) {
        let id = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let data_dir = std::env::temp_dir().join(format!(
            "aribcap-db-query-service-test-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&data_dir);
        let month_dir = archive::archive_root(&data_dir).join("nhk").join("2026-07");
        fs::create_dir_all(&month_dir).unwrap();

        let mut body = String::from(
            "{\"type\":\"eit\",\"section\":\"present\",\"startTime\":\"2026-07-15T12:00:00.000+09:00\",\"durationSec\":1800,\"shortEvents\":[{\"languageCode\":\"jpn\",\"eventName\":\"program title\",\"text\":\"program description\"}]}\n",
        );
        for index in 0..caption_count {
            if index == 1 {
                body.push_str("{}\n");
            }
            body.push_str(&format!(
                "{{\"type\":\"caption\",\"time\":\"2026-07-15T12:00:01.000+09:00\",\"text\":\"caption {index}\",\"durationMs\":500,\"languageCode\":\"jpn\"}}\n"
            ));
        }
        fs::write(month_dir.join("2026-07-15_12-00-00.program.jsonl"), body).unwrap();

        let db_path = search_db::search_db_path(&data_dir);
        let mut connection = search_db::open_and_migrate(&db_path).await.unwrap();
        search_db::ingest_once(&mut connection, &archive::archive_root(&data_dir))
            .await
            .unwrap();
        drop(connection);

        let service =
            ArchiveQueryService::new(data_dir.clone(), db_path, Arc::new(AtomicBool::new(true)));
        (data_dir, service)
    }
}

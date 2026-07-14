use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::stream::unfold;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::{broadcast, watch};
use tokio_util::io::ReaderStream;
use tower_http::trace::TraceLayer;

use crate::{archive, live::LiveBroadcaster, search_db};

#[derive(Clone)]
struct AppState {
    data_dir: Arc<PathBuf>,
    search_pool: SqlitePool,
    live: Arc<LiveBroadcaster>,
    shutdown: watch::Receiver<bool>,
}

pub fn router(
    data_dir: PathBuf,
    search_db_path: PathBuf,
    live: Arc<LiveBroadcaster>,
    search_db_ready: Arc<AtomicBool>,
    shutdown: watch::Receiver<bool>,
) -> Router {
    let archive_routes = Router::new()
        .route("/api/streams", get(api_streams))
        .route("/api/months", get(api_months))
        .route("/api/records", get(api_records))
        .route("/api/records/search", get(api_search))
        .route(
            "/api/records/{stream}/{recording_started_at}",
            get(raw_record),
        )
        .route_layer(middleware::from_fn(move |request, next| {
            require_search_db(search_db_ready.clone(), request, next)
        }));

    Router::new()
        .merge(archive_routes)
        .route("/api/live/{stream}", get(live_stream))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            data_dir: Arc::new(data_dir),
            search_pool: search_db::open_reader_pool(&search_db_path),
            live,
            shutdown,
        })
}

async fn require_search_db(
    search_db_ready: Arc<AtomicBool>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !search_db_ready.load(Ordering::Acquire) {
        return HttpError::service_unavailable("search database is not ready").into_response();
    }
    next.run(request).await
}

async fn api_streams(State(state): State<AppState>) -> Result<Json<Vec<String>>, HttpError> {
    let data_dir = state.data_dir.clone();
    blocking_io(move || archive::list_streams(&data_dir))
        .await
        .map(Json)
}

async fn api_months(
    State(state): State<AppState>,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<Json<Vec<String>>, HttpError> {
    let Query(query) = query?;
    let data_dir = state.data_dir.clone();
    blocking_io(move || archive::list_months(&data_dir, &query.stream))
        .await
        .map(Json)
}

async fn api_records(
    State(state): State<AppState>,
    query: Result<Query<RecordsQuery>, QueryRejection>,
) -> Result<Json<Vec<archive::RecordEntry>>, HttpError> {
    let Query(query) = query?;
    archive::validate_stream_component(&query.stream).map_err(HttpError::from_io)?;
    archive::validate_month_component(&query.month).map_err(HttpError::from_io)?;
    let mut connection = state
        .search_pool
        .acquire()
        .await
        .map_err(HttpError::internal)?;
    let records = search_db::list_indexed_records(&mut connection, &query.stream, &query.month)
        .await
        .map_err(HttpError::internal)?
        .into_iter()
        .map(|record| archive::RecordEntry {
            path: record_api_path(&record.stream, &record.recording_started_at),
            stream: record.stream,
            month: record.month,
            filename: record.filename,
            size_bytes: u64::try_from(record.size_bytes).unwrap_or(0),
        })
        .collect();
    Ok(Json(records))
}

const DEFAULT_SEARCH_LIMIT: i64 = 20;
const MAX_SEARCH_LIMIT: i64 = 200;
const DEFAULT_INNER_HITS: i64 = 5;
const MAX_INNER_HITS: i64 = 50;

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: Option<String>,
    program_q: Option<String>,
    line_q: Option<String>,
    genre: Option<String>,
    stream: Option<String>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<i64>,
    inner_hits: Option<i64>,
}

enum SearchMode {
    General(search_db::SearchExpression),
    Program(search_db::SearchExpression),
    Line(search_db::SearchExpression),
    Combined(search_db::SearchExpression, search_db::SearchExpression),
}

impl SearchMode {
    fn resolve(query: &SearchQuery) -> Result<Self, String> {
        let q = non_empty(query.q.as_deref());
        let program_q = non_empty(query.program_q.as_deref());
        let line_q = non_empty(query.line_q.as_deref());
        let parse = |value: &str| search_db::parse_search_expression(value).map_err(str::to_owned);

        match (q, program_q, line_q) {
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                Err("combine `q` with `program_q`/`line_q` is not supported".to_owned())
            }
            (Some(q), None, None) => Ok(Self::General(parse(q)?)),
            (None, Some(program_q), None) => Ok(Self::Program(parse(program_q)?)),
            (None, None, Some(line_q)) => Ok(Self::Line(parse(line_q)?)),
            (None, Some(program_q), Some(line_q)) => {
                Ok(Self::Combined(parse(program_q)?, parse(line_q)?))
            }
            (None, None, None) => Err("provide one of `q`, `program_q`, or `line_q`".to_owned()),
        }
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn parse_genre_filter(value: Option<&str>) -> Result<Option<search_db::GenreFilter>, String> {
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
            .ok_or_else(|| "genre must be `0..15` or `0..15:0..15`".to_owned())
    };

    Ok(Some(search_db::GenreFilter {
        level1: parse_nibble(level1)?,
        level2: level2.map(parse_nibble).transpose()?,
    }))
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    items: Vec<SearchResultItem>,
}

#[derive(Debug, Serialize)]
struct SearchResultItem {
    #[serde(rename = "programId")]
    program_id: i64,
    stream: String,
    #[serde(rename = "recordingStartedAt")]
    recording_started_at: String,
    #[serde(rename = "startTime")]
    start_time: Option<String>,
    title: String,
    description: String,
    path: String,
    hits: Vec<SearchHitItem>,
}

#[derive(Debug, Serialize)]
struct SearchHitItem {
    #[serde(rename = "lineId")]
    line_id: i64,
    #[serde(rename = "lineNo")]
    line_no: i64,
    time: Option<String>,
    text: String,
}

async fn api_search(
    State(state): State<AppState>,
    query: Result<Query<SearchQuery>, QueryRejection>,
) -> Result<Json<SearchResponse>, HttpError> {
    let Query(query) = query?;
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
        archive::validate_stream_component(stream).map_err(HttpError::from_io)?;
    }
    let from = non_empty(query.from.as_deref()).map(search_db::expand_from_bound);
    let to = non_empty(query.to.as_deref()).map(search_db::expand_to_bound);
    let genre = parse_genre_filter(query.genre.as_deref()).map_err(HttpError::bad_request)?;
    let mode = SearchMode::resolve(&query).map_err(HttpError::bad_request)?;

    let mut connection = state
        .search_pool
        .acquire()
        .await
        .map_err(HttpError::internal)?;
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
            search_db::search_programs(&mut connection, &expression, &filter, limit).await
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
    .map_err(HttpError::internal)?;

    let items = programs
        .into_iter()
        .map(|program| SearchResultItem {
            program_id: program.program_id,
            path: record_api_path(&program.stream, &program.recording_started_at),
            stream: program.stream,
            recording_started_at: program.recording_started_at,
            start_time: program.start_time,
            title: program.title,
            description: program.description,
            hits: program
                .hits
                .into_iter()
                .map(|hit| SearchHitItem {
                    line_id: hit.line_id,
                    line_no: hit.line_no,
                    time: hit.time,
                    text: hit.text,
                })
                .collect(),
        })
        .collect();
    Ok(Json(SearchResponse { items }))
}

async fn raw_record(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path((stream, recording_started_at)) = path?;
    archive::validate_stream_component(&stream).map_err(HttpError::from_io)?;
    archive::validate_recording_started_at(&recording_started_at).map_err(HttpError::from_io)?;
    let mut connection = state
        .search_pool
        .acquire()
        .await
        .map_err(HttpError::internal)?;
    let Some(record) =
        search_db::find_indexed_record(&mut connection, &stream, &recording_started_at)
            .await
            .map_err(HttpError::internal)?
    else {
        return Err(HttpError::not_found("record not found"));
    };
    let data_dir = state.data_dir.clone();
    let Some(path) = blocking_io(move || {
        archive::resolve_record_path(&data_dir, &record.stream, &record.month, &record.filename)
    })
    .await?
    else {
        return Err(HttpError::not_found("record not found"));
    };

    let file = tokio::fs::File::open(path)
        .await
        .map_err(HttpError::from_io)?;
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response())
}

fn record_api_path(stream: &str, recording_started_at: &str) -> String {
    format!(
        "/api/records/{}/{}",
        urlencoding::encode(stream),
        urlencoding::encode(recording_started_at)
    )
}

async fn live_stream(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path(stream) = path?;
    let Some(receiver) = state.live.subscribe(&stream) else {
        return Err(HttpError::not_found("stream not found"));
    };
    let shutdown = state.shutdown.clone();
    let lines = unfold(
        (receiver, shutdown, stream),
        |(mut receiver, mut shutdown, stream)| async move {
            loop {
                if *shutdown.borrow() {
                    return None;
                }
                tokio::select! {
                    result = receiver.recv() => match result {
                        Ok(line) => {
                            let mut chunk = line.as_bytes().to_vec();
                            chunk.push(b'\n');
                            return Some((
                                Ok::<_, io::Error>(chunk),
                                (receiver, shutdown, stream),
                            ));
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(
                                stream,
                                skipped,
                                "Live subscriber lagged; dropping buffered lines"
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    },
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            return None;
                        }
                    }
                }
            }
        },
    );

    Ok((
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(lines),
    )
        .into_response())
}

async fn blocking_io<T>(
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, HttpError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(HttpError::internal)?
        .map_err(HttpError::from_io)
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    stream: String,
}

#[derive(Debug, Deserialize)]
struct RecordsQuery {
    stream: String,
    month: String,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn from_io(error: io::Error) -> Self {
        let status = match error.kind() {
            io::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }
}

impl From<QueryRejection> for HttpError {
    fn from(error: QueryRejection) -> Self {
        Self {
            status: error.status(),
            message: error.body_text(),
        }
    }
}

impl From<PathRejection> for HttpError {
    fn from(error: PathRejection) -> Self {
        Self {
            status: error.status(),
            message: error.body_text(),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{body::to_bytes, http::Request};
    use futures_util::StreamExt as _;
    use tower::ServiceExt as _;

    use super::*;

    static NEXT_TEMP_DIR: AtomicUsize = AtomicUsize::new(0);
    const RECORD_FILENAME: &str = "2020-01-01_00-00-00.title#part.jsonl";
    const RECORD_BODY: &str = "{\"type\":\"eit\",\"section\":\"present\",\"shortEvents\":[{\"languageCode\":\"jpn\",\"eventName\":\"title\"}]}\n{\"type\":\"caption\",\"text\":\"caption\"}\n";
    const RAW_RECORD_PATH: &str = "/api/records/nhk/2020-01-01_00-00-00";

    #[tokio::test]
    async fn archive_routes_list_and_stream_record() {
        let (data_dir, app) = app_with_record().await;

        let response = get(&app, "/api/streams").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, r#"["nhk"]"#);

        let response = get(&app, "/api/months?stream=nhk").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, r#"["2020-01"]"#);

        let response = get(&app, "/api/records?stream=nhk&month=2020-01").await;
        assert_eq!(response.status(), StatusCode::OK);
        let records: Vec<serde_json::Value> =
            serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["filename"], RECORD_FILENAME);
        assert_eq!(records[0]["path"], RAW_RECORD_PATH);

        let response = get(&app, RAW_RECORD_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/x-ndjson"
        );
        assert_eq!(body_text(response).await, RECORD_BODY);

        let response = get(&app, "/api/records/search?q=caption").await;
        assert_eq!(response.status(), StatusCode::OK);
        let search: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(search["items"][0]["path"], RAW_RECORD_PATH);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn archive_routes_return_only_the_collision_winner() {
        let data_dir = temp_dir();
        let record_dir = archive::records_root(&data_dir).join("nhk").join("2020-01");
        fs::create_dir_all(&record_dir).unwrap();
        fs::write(record_dir.join(RECORD_FILENAME), RECORD_BODY).unwrap();
        fs::write(
            record_dir.join("2020-01-01_00-00-00.title#part.1.jsonl"),
            "{\"type\":\"eit\",\"section\":\"present\",\"shortEvents\":[{\"eventName\":\"loser\"}]}\n",
        )
        .unwrap();
        let db_path = search_db::search_db_path(&data_dir);
        let mut connection = search_db::open_and_migrate(&db_path).await.unwrap();
        search_db::ingest_once(&mut connection, &archive::records_root(&data_dir))
            .await
            .unwrap();
        drop(connection);
        let app = test_router(data_dir.clone(), Arc::new(LiveBroadcaster::new([])));

        let response = get(&app, "/api/records?stream=nhk&month=2020-01").await;
        let records: Vec<serde_json::Value> =
            serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["filename"], RECORD_FILENAME);
        assert_eq!(records[0]["path"], RAW_RECORD_PATH);

        let response = get(&app, RAW_RECORD_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, RECORD_BODY);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn archive_routes_reject_invalid_or_missing_records() {
        let (data_dir, app) = empty_app().await;

        assert_json_error(&app, "/api/months", StatusCode::BAD_REQUEST).await;
        assert_json_error(&app, "/api/records?stream=nhk", StatusCode::BAD_REQUEST).await;
        let response = get(&app, "/api/records?stream=..&month=2020-01").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = get(&app, "/api/records/nhk/2020-01-01_00-00-00").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = get(
            &app,
            "/api/records/nhk/2020-01/2020-01-01_00-00-00.title.jsonl",
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        assert_json_error(
            &app,
            "/api/records/nhk/not-a-timestamp",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_json_error(&app, "/api/records/search", StatusCode::BAD_REQUEST).await;

        assert_json_error(&app, "/api/live/%FF", StatusCode::BAD_REQUEST).await;

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn live_route_streams_lines_and_rejects_unknown_streams() {
        let data_dir = temp_dir();
        let broadcaster = Arc::new(LiveBroadcaster::new(["nhk".to_owned()]));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let app = router(
            data_dir.clone(),
            search_db::search_db_path(&data_dir),
            broadcaster.clone(),
            Arc::new(AtomicBool::new(false)),
            shutdown_rx,
        );
        let response = get(&app, "/api/live/nhk").await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();

        broadcaster.publish("nhk", r#"{"type":"caption","text":"hi"}"#);

        let chunk = body.next().await.unwrap().unwrap();
        assert_eq!(chunk.as_ref(), b"{\"type\":\"caption\",\"text\":\"hi\"}\n");

        shutdown_tx.send(true).unwrap();
        assert!(body.next().await.is_none());

        let response = get(&app, "/api/live/other").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn non_live_routes_return_service_unavailable_until_search_db_is_ready() {
        let data_dir = temp_dir();
        let ready = Arc::new(AtomicBool::new(false));
        let (_, shutdown) = watch::channel(false);
        let app = router(
            data_dir.clone(),
            search_db::search_db_path(&data_dir),
            Arc::new(LiveBroadcaster::new(["nhk".to_owned()])),
            ready.clone(),
            shutdown,
        );

        for path in [
            "/api/streams",
            "/api/months?stream=nhk",
            "/api/records?stream=nhk&month=2020-01",
            "/api/records/search?q=caption",
            RAW_RECORD_PATH,
        ] {
            assert_json_error(&app, path, StatusCode::SERVICE_UNAVAILABLE).await;
        }
        assert_eq!(get(&app, "/api/live/nhk").await.status(), StatusCode::OK);

        search_db::open_and_migrate(&search_db::search_db_path(&data_dir))
            .await
            .unwrap();
        ready.store(true, Ordering::Release);
        assert_eq!(get(&app, "/api/streams").await.status(), StatusCode::OK);

        fs::remove_dir_all(data_dir).unwrap();
    }

    async fn get(app: &Router, uri: &str) -> Response {
        app.clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body_text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn assert_json_error(app: &Router, uri: &str, status: StatusCode) {
        let response = get(app, uri).await;
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let body: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert!(body["error"].is_string());
    }

    async fn empty_app() -> (PathBuf, Router) {
        let data_dir = temp_dir();
        let db_path = search_db::search_db_path(&data_dir);
        search_db::open_and_migrate(&db_path).await.unwrap();
        let app = test_router(data_dir.clone(), Arc::new(LiveBroadcaster::new([])));
        (data_dir, app)
    }

    async fn app_with_record() -> (PathBuf, Router) {
        let data_dir = temp_dir();
        let record_dir = archive::records_root(&data_dir).join("nhk").join("2020-01");
        fs::create_dir_all(&record_dir).unwrap();
        fs::write(record_dir.join(RECORD_FILENAME), RECORD_BODY).unwrap();
        let db_path = search_db::search_db_path(&data_dir);
        let mut connection = search_db::open_and_migrate(&db_path).await.unwrap();
        search_db::ingest_once(&mut connection, &archive::records_root(&data_dir))
            .await
            .unwrap();
        drop(connection);
        let app = test_router(data_dir.clone(), Arc::new(LiveBroadcaster::new([])));
        (data_dir, app)
    }

    fn test_router(data_dir: PathBuf, live: Arc<LiveBroadcaster>) -> Router {
        let (_, shutdown) = watch::channel(false);
        router(
            data_dir.clone(),
            search_db::search_db_path(&data_dir),
            live,
            Arc::new(AtomicBool::new(true)),
            shutdown,
        )
    }

    fn temp_dir() -> PathBuf {
        let id = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "aribcap-db-server-test-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }
}

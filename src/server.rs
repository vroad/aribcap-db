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
use tokio::sync::{broadcast, watch};
use tokio_util::{io::ReaderStream, sync::CancellationToken};
use tower_http::trace::TraceLayer;

use crate::{
    archive,
    live::LiveBroadcaster,
    mcp,
    query_service::{ArchiveQueryService, QueryServiceError, SearchRequest},
};

#[derive(Clone)]
struct AppState {
    query_service: ArchiveQueryService,
    live: Arc<LiveBroadcaster>,
    shutdown: watch::Receiver<bool>,
}

pub fn router(
    data_dir: PathBuf,
    search_db_path: PathBuf,
    live: Arc<LiveBroadcaster>,
    search_db_ready: Arc<AtomicBool>,
    shutdown: watch::Receiver<bool>,
    mcp_enabled: bool,
    mcp_cancellation: CancellationToken,
) -> Router {
    let query_service = ArchiveQueryService::new(data_dir, search_db_path, search_db_ready.clone());
    let archive_routes = Router::new()
        .route("/api/streams", get(api_streams))
        .route("/api/months", get(api_months))
        .route("/api/programs", get(api_programs))
        .route("/api/programs/search", get(api_search))
        .route(
            "/api/programs/{stream}/{recording_started_at}",
            get(raw_program),
        )
        .route_layer(middleware::from_fn(move |request, next| {
            require_search_db(search_db_ready.clone(), request, next)
        }));

    let app = Router::new()
        .merge(archive_routes)
        .route("/api/live/{stream}", get(live_stream))
        .with_state(AppState {
            query_service: query_service.clone(),
            live,
            shutdown,
        });
    let app = if mcp_enabled {
        app.merge(mcp::router(query_service, mcp_cancellation))
    } else {
        app
    };

    app.layer(TraceLayer::new_for_http())
}

async fn require_search_db(
    search_db_ready: Arc<AtomicBool>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !search_db_ready.load(Ordering::Acquire) {
        return HttpError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "search database is not ready".to_owned(),
        }
        .into_response();
    }
    next.run(request).await
}

async fn api_streams(State(state): State<AppState>) -> Result<Json<Vec<String>>, HttpError> {
    state
        .query_service
        .list_streams()
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn api_months(
    State(state): State<AppState>,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<Json<Vec<String>>, HttpError> {
    let Query(query) = query?;
    state
        .query_service
        .list_months(query.stream)
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn api_programs(
    State(state): State<AppState>,
    query: Result<Query<ProgramsQuery>, QueryRejection>,
) -> Result<Json<Vec<archive::ProgramEntry>>, HttpError> {
    let Query(query) = query?;
    state
        .query_service
        .list_programs(query.stream, query.month)
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn api_search(
    State(state): State<AppState>,
    query: Result<Query<SearchRequest>, QueryRejection>,
) -> Result<Json<crate::query_service::SearchResponse>, HttpError> {
    let Query(query) = query?;
    state
        .query_service
        .search(query)
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn raw_program(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path((stream, recording_started_at)) = path?;
    let path = state
        .query_service
        .resolve_program_path(stream, recording_started_at)
        .await?;

    let file = tokio::fs::File::open(path)
        .await
        .map_err(QueryServiceError::from)?;
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response())
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

#[derive(Debug, Deserialize)]
struct StreamQuery {
    stream: String,
}

#[derive(Debug, Deserialize)]
struct ProgramsQuery {
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
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl From<QueryServiceError> for HttpError {
    fn from(error: QueryServiceError) -> Self {
        let status = match &error {
            QueryServiceError::BadRequest(_) => StatusCode::BAD_REQUEST,
            QueryServiceError::NotFound(_) => StatusCode::NOT_FOUND,
            QueryServiceError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            QueryServiceError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
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
    use crate::search_db;

    static NEXT_TEMP_DIR: AtomicUsize = AtomicUsize::new(0);
    const ARCHIVE_FILE_NAME: &str = "2020-01-01_00-00-00.title#part.jsonl";
    const ARCHIVE_FILE_BODY: &str = "{\"type\":\"eit\",\"section\":\"present\",\"startTime\":\"2020-01-01T00:00:00.000+09:00\",\"durationSec\":1800,\"shortEvents\":[{\"languageCode\":\"jpn\",\"eventName\":\"title\"}]}\n{\"type\":\"caption\",\"time\":\"2020-01-01T00:00:01.000+09:00\",\"text\":\"caption\",\"languageCode\":\"jpn\",\"durationMs\":500}\n{\"type\":\"caption\",\"time\":\"2020-01-01T00:00:02.000+09:00\",\"text\":\"second caption\",\"languageCode\":\"jpn\",\"durationMs\":600}\n";
    const OTHER_ARCHIVE_FILE_NAME: &str = "2020-01-02_00-00-00.other.jsonl";
    const OTHER_ARCHIVE_FILE_BODY: &str = "{\"type\":\"eit\",\"section\":\"present\",\"startTime\":\"2020-01-02T00:00:00.000+09:00\",\"durationSec\":1800,\"shortEvents\":[{\"languageCode\":\"jpn\",\"eventName\":\"other title\"}]}\n{\"type\":\"caption\",\"time\":\"2020-01-02T00:00:01.000+09:00\",\"text\":\"caption\",\"languageCode\":\"jpn\",\"durationMs\":500}\n";
    const RAW_PROGRAM_PATH: &str = "/api/programs/nhk/2020-01-01_00-00-00";

    #[tokio::test]
    async fn archive_routes_list_and_stream_program() {
        let (data_dir, app) = app_with_program().await;

        let response = get(&app, "/api/streams").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, r#"["nhk"]"#);

        let response = get(&app, "/api/months?stream=nhk").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, r#"["2020-01"]"#);

        let response = get(&app, "/api/programs?stream=nhk&month=2020-01").await;
        assert_eq!(response.status(), StatusCode::OK);
        let programs: Vec<serde_json::Value> =
            serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(programs.len(), 1);
        assert_eq!(programs[0]["filename"], ARCHIVE_FILE_NAME);
        assert_eq!(programs[0]["path"], RAW_PROGRAM_PATH);

        let response = get(&app, RAW_PROGRAM_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/x-ndjson"
        );
        assert_eq!(body_text(response).await, ARCHIVE_FILE_BODY);

        let response = get(&app, "/api/programs/search?q=caption").await;
        assert_eq!(response.status(), StatusCode::OK);
        let search: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(search["items"][0]["path"], RAW_PROGRAM_PATH);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn search_without_stream_covers_all_streams_for_http_and_mcp() {
        let (data_dir, app) = app_with_programs_mcp().await;

        let response = get(&app, "/api/programs/search?q=caption").await;
        assert_eq!(response.status(), StatusCode::OK);
        let search: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        let streams = search["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["stream"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(streams, ["bs", "nhk"]);

        let response = get(&app, "/api/programs/search?q=caption&stream=nhk").await;
        let search: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(search["items"].as_array().unwrap().len(), 1);
        assert_eq!(search["items"][0]["stream"], "nhk");

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
            None,
        )
        .await;
        let session_id = response.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        body_text(response).await;
        assert_eq!(
            mcp_post(
                &app,
                r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                Some(&session_id),
            )
            .await
            .status(),
            StatusCode::ACCEPTED
        );

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_programs","arguments":{"q":"caption"}}}"#,
            Some(&session_id),
        )
        .await;
        let result = sse_json(&body_text(response).await);
        let streams = result["result"]["structuredContent"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["stream"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(streams, ["bs", "nhk"]);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn archive_routes_return_only_the_collision_winner() {
        let data_dir = temp_dir();
        let month_dir = archive::archive_root(&data_dir).join("nhk").join("2020-01");
        fs::create_dir_all(&month_dir).unwrap();
        fs::write(month_dir.join(ARCHIVE_FILE_NAME), ARCHIVE_FILE_BODY).unwrap();
        fs::write(
            month_dir.join("2020-01-01_00-00-00.title#part.1.jsonl"),
            "{\"type\":\"eit\",\"section\":\"present\",\"shortEvents\":[{\"eventName\":\"loser\"}]}\n",
        )
        .unwrap();
        let db_path = search_db::search_db_path(&data_dir);
        let mut connection = search_db::open_and_migrate(&db_path).await.unwrap();
        search_db::ingest_once(&mut connection, &archive::archive_root(&data_dir))
            .await
            .unwrap();
        drop(connection);
        let app = test_router(data_dir.clone(), Arc::new(LiveBroadcaster::new([])));

        let response = get(&app, "/api/programs?stream=nhk&month=2020-01").await;
        let programs: Vec<serde_json::Value> =
            serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(programs.len(), 1);
        assert_eq!(programs[0]["filename"], ARCHIVE_FILE_NAME);
        assert_eq!(programs[0]["path"], RAW_PROGRAM_PATH);

        let response = get(&app, RAW_PROGRAM_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, ARCHIVE_FILE_BODY);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn archive_routes_reject_invalid_or_missing_programs() {
        let (data_dir, app) = empty_app().await;

        assert_json_error(&app, "/api/months", StatusCode::BAD_REQUEST).await;
        assert_json_error(&app, "/api/programs?stream=nhk", StatusCode::BAD_REQUEST).await;
        let response = get(&app, "/api/programs?stream=..&month=2020-01").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = get(&app, "/api/programs/nhk/2020-01-01_00-00-00").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = get(
            &app,
            "/api/programs/nhk/2020-01/2020-01-01_00-00-00.title.jsonl",
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        assert_json_error(
            &app,
            "/api/programs/nhk/not-a-timestamp",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_json_error(&app, "/api/programs/search", StatusCode::BAD_REQUEST).await;

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
            false,
            CancellationToken::new(),
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
            true,
            CancellationToken::new(),
        );

        for path in [
            "/api/streams",
            "/api/months?stream=nhk",
            "/api/programs?stream=nhk&month=2020-01",
            "/api/programs/search?q=caption",
            RAW_PROGRAM_PATH,
        ] {
            assert_json_error(&app, path, StatusCode::SERVICE_UNAVAILABLE).await;
        }
        assert_eq!(get(&app, "/api/live/nhk").await.status(), StatusCode::OK);

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let session_id = response.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let initialized = sse_json(&body_text(response).await);
        assert_eq!(initialized["result"]["serverInfo"]["name"], "aribcap-db");

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            Some(&session_id),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            Some(&session_id),
        )
        .await;
        assert_eq!(
            sse_json(&body_text(response).await)["result"]["tools"]
                .as_array()
                .unwrap()
                .len(),
            3
        );

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_streams","arguments":{}}}"#,
            Some(&session_id),
        )
        .await;
        let result = sse_json(&body_text(response).await);
        assert_eq!(result["result"]["isError"], true);
        assert!(
            result["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("search database is not ready")
        );

        search_db::open_and_migrate(&search_db::search_db_path(&data_dir))
            .await
            .unwrap();
        ready.store(true, Ordering::Release);
        assert_eq!(get(&app, "/api/streams").await.status(), StatusCode::OK);

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn mcp_route_is_opt_in_and_exposes_read_only_tools() {
        let (data_dir, disabled_app) = app_with_program().await;
        let response = mcp_post(
            &disabled_app,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        fs::remove_dir_all(data_dir).unwrap();

        let (data_dir, app) = app_with_program_mcp().await;
        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let session_id = response.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let initialized = sse_json(&body_text(response).await);
        assert!(initialized["result"]["capabilities"]["tools"].is_object());

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            Some(&session_id),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            Some(&session_id),
        )
        .await;
        let tools = sse_json(&body_text(response).await);
        let tools = tools["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 3);
        for name in ["list_streams", "search_programs", "get_program_captions"] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert_eq!(tool["annotations"]["readOnlyHint"], true);
            assert_eq!(tool["annotations"]["idempotentHint"], true);
        }

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_streams","arguments":{}}}"#,
            Some(&session_id),
        )
        .await;
        let result = sse_json(&body_text(response).await);
        assert_eq!(result["result"]["structuredContent"]["streams"][0], "nhk");

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search_programs","arguments":{"q":"caption"}}}"#,
            Some(&session_id),
        )
        .await;
        let result = sse_json(&body_text(response).await);
        assert_eq!(
            result["result"]["structuredContent"]["items"][0]["title"],
            "title"
        );

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_program_captions","arguments":{"stream":"nhk","recording_started_at":"2020-01-01_00-00-00","limit":1}}}"#,
            Some(&session_id),
        )
        .await;
        let result = sse_json(&body_text(response).await);
        let output = &result["result"]["structuredContent"];
        assert_eq!(output["program"]["title"], "title");
        assert_eq!(output["captions"][0]["lineNo"], 2);
        assert_eq!(output["captions"][0]["durationMs"], 500);
        assert_eq!(output["nextStartLine"], 3);

        fs::remove_dir_all(data_dir).unwrap();
    }

    async fn get(app: &Router, uri: &str) -> Response {
        app.clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn mcp_post(app: &Router, body: &'static str, session_id: Option<&str>) -> Response {
        let mut request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header(header::HOST, "localhost")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream");
        if let Some(session_id) = session_id {
            request = request.header("mcp-session-id", session_id);
        }
        app.clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap()
    }

    fn sse_json(body: &str) -> serde_json::Value {
        body.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .filter(|data| !data.is_empty())
            .filter_map(|data| serde_json::from_str(data).ok())
            .next_back()
            .expect("SSE response should contain a JSON data event")
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

    async fn app_with_program() -> (PathBuf, Router) {
        app_with_program_options(false).await
    }

    async fn app_with_program_mcp() -> (PathBuf, Router) {
        app_with_program_options(true).await
    }

    async fn app_with_programs_mcp() -> (PathBuf, Router) {
        app_with_program_options_and_second_stream(true, true).await
    }

    async fn app_with_program_options(mcp_enabled: bool) -> (PathBuf, Router) {
        app_with_program_options_and_second_stream(mcp_enabled, false).await
    }

    async fn app_with_program_options_and_second_stream(
        mcp_enabled: bool,
        include_second_stream: bool,
    ) -> (PathBuf, Router) {
        let data_dir = temp_dir();
        let month_dir = archive::archive_root(&data_dir).join("nhk").join("2020-01");
        fs::create_dir_all(&month_dir).unwrap();
        fs::write(month_dir.join(ARCHIVE_FILE_NAME), ARCHIVE_FILE_BODY).unwrap();
        if include_second_stream {
            let other_month_dir = archive::archive_root(&data_dir).join("bs").join("2020-01");
            fs::create_dir_all(&other_month_dir).unwrap();
            fs::write(
                other_month_dir.join(OTHER_ARCHIVE_FILE_NAME),
                OTHER_ARCHIVE_FILE_BODY,
            )
            .unwrap();
        }
        let db_path = search_db::search_db_path(&data_dir);
        let mut connection = search_db::open_and_migrate(&db_path).await.unwrap();
        search_db::ingest_once(&mut connection, &archive::archive_root(&data_dir))
            .await
            .unwrap();
        drop(connection);
        let (_, shutdown) = watch::channel(false);
        let app = router(
            data_dir.clone(),
            search_db::search_db_path(&data_dir),
            Arc::new(LiveBroadcaster::new([])),
            Arc::new(AtomicBool::new(true)),
            shutdown,
            mcp_enabled,
            CancellationToken::new(),
        );
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
            false,
            CancellationToken::new(),
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

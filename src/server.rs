use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use aide::{
    axum::{
        ApiRouter,
        routing::{get, get_with},
    },
    generate,
    openapi::{Info, MediaType, OpenApi, Response as ApiResponse, SchemaObject},
    operation::OperationOutput,
    scalar::Scalar,
    transform::TransformOperation,
};
use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use futures_util::stream::unfold;
use schemars::{JsonSchema, json_schema};
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
    let app = rest_api_routes(search_db_ready).with_state(AppState {
        query_service: query_service.clone(),
        live,
        shutdown,
    });
    let app = if mcp_enabled {
        app.merge(mcp::router(query_service, mcp_cancellation))
    } else {
        app
    };

    let app = app
        .route(
            "/docs",
            Scalar::new("/openapi.json")
                .with_title("aribcap-db HTTP API")
                .axum_route(),
        )
        .route("/openapi.json", get(serve_openapi));
    let (app, api) = finish_openapi(app);

    app.layer(Extension(Arc::new(api)))
        .layer(TraceLayer::new_for_http())
}

fn rest_api_routes(search_db_ready: Arc<AtomicBool>) -> ApiRouter<AppState> {
    configure_openapi_generation();
    let archive_routes = ApiRouter::new()
        .api_route(
            "/api/streams",
            get_with(api_streams, |op| {
                document_operation(
                    op,
                    "listStreams",
                    "List streams",
                    "List archive stream names that can be searched.",
                )
                .response_with::<200, Json<Vec<String>>, _>(|response| {
                    response.description("Archive stream names.")
                })
                .with(document_unavailable_error)
                .with(document_internal_error)
            }),
        )
        .api_route(
            "/api/months",
            get_with(api_months, |op| {
                document_operation(
                    op,
                    "listMonths",
                    "List months",
                    "List archive months available for one stream.",
                )
                .response_with::<200, Json<Vec<String>>, _>(|response| {
                    response.description("Archive months in `YYYY-MM` form.")
                })
                .with(document_bad_request_error)
                .with(document_unavailable_error)
                .with(document_internal_error)
            }),
        )
        .api_route(
            "/api/programs",
            get_with(api_programs, |op| {
                document_operation(
                    op,
                    "listPrograms",
                    "List programs",
                    "List indexed archived programs for one stream and month.",
                )
                .response_with::<200, Json<Vec<archive::ProgramEntry>>, _>(|response| {
                    response.description("Indexed archived programs.")
                })
                .with(document_bad_request_error)
                .with(document_unavailable_error)
                .with(document_internal_error)
            }),
        )
        .api_route(
            "/api/programs/search",
            get_with(api_search, |op| {
                document_operation(
                    op,
                    "searchPrograms",
                    "Search programs",
                    "Search archived program metadata and caption text.",
                )
                .response_with::<200, Json<crate::query_service::SearchResponse>, _>(|response| {
                    response.description("Matching programs and caption hits.")
                })
                .with(document_bad_request_error)
                .with(document_unavailable_error)
                .with(document_internal_error)
            }),
        )
        .api_route(
            "/api/programs/{stream}/{recording_started_at}",
            get_with(raw_program, |op| {
                document_operation(
                    op,
                    "getRawProgram",
                    "Get raw program",
                    "Stream the archived program's raw JSONL records.",
                )
                .response_with::<200, NdjsonResponse, _>(|response| {
                    response.description("Raw archived program records.")
                })
                .with(document_bad_request_error)
                .with(document_not_found_error)
                .with(document_unavailable_error)
                .with(document_internal_error)
            }),
        )
        .route_layer(middleware::from_fn(move |request, next| {
            require_search_db(search_db_ready.clone(), request, next)
        }));

    ApiRouter::new().merge(archive_routes).api_route(
        "/api/live/{stream}",
        get_with(live_stream, |op| {
            document_operation(
                op,
                "getLiveStream",
                "Get live stream",
                "Stream raw JSONL records received from the existing upstream connection.",
            )
            .response_with::<200, NdjsonResponse, _>(|response| {
                response.description("Live raw JSONL records.")
            })
            .with(document_bad_request_error)
            .with(document_not_found_error)
        }),
    )
}

fn finish_openapi<S>(app: ApiRouter<S>) -> (Router<S>, OpenApi)
where
    S: Clone + Send + Sync + 'static,
{
    let mut api = OpenApi {
        info: Info {
            title: "aribcap-db HTTP API".to_owned(),
            description: Some(
                "Read-only archive discovery, search, and JSONL streaming API.".to_owned(),
            ),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            ..Info::default()
        },
        ..OpenApi::default()
    };
    let app = app.finish_api(&mut api);
    generate::infer_responses(true);

    (app, api)
}

#[cfg(test)]
pub(crate) fn openapi_document() -> OpenApi {
    let (_, api) = finish_openapi(rest_api_routes(Arc::new(AtomicBool::new(false))));
    api
}

fn configure_openapi_generation() {
    generate::infer_responses(false);
    #[cfg(test)]
    generate::on_error(|error| panic!("failed to generate OpenAPI: {error}"));
    #[cfg(not(test))]
    generate::on_error(|error| tracing::warn!(%error, "Failed to generate part of OpenAPI"));
}

async fn serve_openapi(Extension(api): Extension<Arc<OpenApi>>) -> Json<Arc<OpenApi>> {
    Json(api)
}

fn document_operation<'a>(
    op: TransformOperation<'a>,
    id: &str,
    summary: &str,
    description: &str,
) -> TransformOperation<'a> {
    op.id(id).summary(summary).description(description)
}

fn document_bad_request_error(op: TransformOperation<'_>) -> TransformOperation<'_> {
    op.response_with::<400, Json<ErrorBody>, _>(|response| {
        response.description("Invalid path or query parameters.")
    })
}

fn document_not_found_error(op: TransformOperation<'_>) -> TransformOperation<'_> {
    op.response_with::<404, Json<ErrorBody>, _>(|response| {
        response.description("The requested resource was not found.")
    })
}

fn document_unavailable_error(op: TransformOperation<'_>) -> TransformOperation<'_> {
    op.response_with::<503, Json<ErrorBody>, _>(|response| {
        response.description("The search database is not ready.")
    })
}

fn document_internal_error(op: TransformOperation<'_>) -> TransformOperation<'_> {
    op.response_with::<500, Json<ErrorBody>, _>(|response| {
        response.description("Internal server error.")
    })
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

async fn api_streams(
    State(state): State<AppState>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Json<Vec<String>>, HttpError> {
    let Query(NoQuery {}) = query?;
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
    path: Result<Path<RawProgramPath>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, HttpError> {
    let Query(NoQuery {}) = query?;
    let Path(RawProgramPath {
        stream,
        recording_started_at,
    }) = path?;
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
    path: Result<Path<LiveStreamPath>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, HttpError> {
    let Query(NoQuery {}) = query?;
    let Path(LiveStreamPath { stream }) = path?;
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NoQuery {}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamQuery {
    /// Archive stream name.
    stream: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ProgramsQuery {
    /// Archive stream name.
    stream: String,
    /// Archive month in `YYYY-MM` form.
    month: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RawProgramPath {
    /// Archive stream name.
    stream: String,
    /// Recording start timestamp in `YYYY-MM-DD_HH-MM-SS` form.
    recording_started_at: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct LiveStreamPath {
    /// Configured live stream name.
    stream: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ErrorBody {
    /// Client-safe error description.
    error: String,
}

struct NdjsonResponse;

impl OperationOutput for NdjsonResponse {
    type Inner = String;

    fn operation_response(
        _ctx: &mut aide::generate::GenContext,
        _operation: &mut aide::openapi::Operation,
    ) -> Option<ApiResponse> {
        let mut response = ApiResponse {
            // Callers always override the description below via `.response_with(|r| r.description(...))`.
            description: String::new(),
            ..ApiResponse::default()
        };
        response.content.insert(
            "application/x-ndjson".to_owned(),
            MediaType {
                schema: Some(SchemaObject {
                    json_schema: json_schema!({ "type": "string" }),
                    example: None,
                    external_docs: None,
                }),
                ..MediaType::default()
            },
        );
        Some(response)
    }
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
            message: error.into_client_message("http"),
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

impl OperationOutput for HttpError {
    type Inner = ErrorBody;

    fn operation_response(
        ctx: &mut aide::generate::GenContext,
        operation: &mut aide::openapi::Operation,
    ) -> Option<ApiResponse> {
        <Json<ErrorBody> as OperationOutput>::operation_response(ctx, operation)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, atomic::AtomicBool},
    };

    use axum::{body::to_bytes, http::Request};
    use futures_util::StreamExt as _;
    use tower::ServiceExt as _;

    use super::*;
    use crate::search_db;
    use crate::test_support::TestDir;

    const TEST_DIR_PREFIX: &str = "aribcap-db-server-test-";
    const ARCHIVE_FILE_NAME: &str = "2020-01-01_00-00-00.title#part.jsonl";
    const ARCHIVE_FILE_BODY: &str = "{\"type\":\"eit\",\"section\":\"present\",\"startTime\":\"2020-01-01T00:00:00.000+09:00\",\"durationSec\":1800,\"shortEvents\":[{\"languageCode\":\"jpn\",\"eventName\":\"title\"}]}\n{\"type\":\"caption\",\"time\":\"2020-01-01T00:00:01.000+09:00\",\"text\":\"caption\",\"languageCode\":\"jpn\",\"durationMs\":500}\n{\"type\":\"caption\",\"time\":\"2020-01-01T00:00:02.000+09:00\",\"text\":\"second caption\",\"languageCode\":\"jpn\",\"durationMs\":600}\n";
    const OTHER_ARCHIVE_FILE_NAME: &str = "2020-01-02_00-00-00.other.jsonl";
    const OTHER_ARCHIVE_FILE_BODY: &str = "{\"type\":\"eit\",\"section\":\"present\",\"startTime\":\"2020-01-02T00:00:00.000+09:00\",\"durationSec\":1800,\"shortEvents\":[{\"languageCode\":\"jpn\",\"eventName\":\"other title\"}]}\n{\"type\":\"caption\",\"time\":\"2020-01-02T00:00:01.000+09:00\",\"text\":\"caption\",\"languageCode\":\"jpn\",\"durationMs\":500}\n";
    const RAW_PROGRAM_PATH: &str = "/api/programs/nhk/2020-01-01_00-00-00";

    #[tokio::test]
    async fn internal_query_error_is_redacted_in_http_response() {
        let response = HttpError::from(QueryServiceError::Internal(anyhow::anyhow!(
            "sensitive database detail"
        )))
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": "internal query error",
            })
        );
    }

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

        drop(data_dir);
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

        let response = get(&app, "/api/programs/search?q=caption&stream=").await;
        let search: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(search["items"].as_array().unwrap().len(), 2);

        for stream in ["%20", "%20nhk%20"] {
            let response = get(
                &app,
                &format!("/api/programs/search?q=caption&stream={stream}"),
            )
            .await;
            let search: serde_json::Value =
                serde_json::from_str(&body_text(response).await).unwrap();
            assert!(search["items"].as_array().unwrap().is_empty());
        }

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

        for (id, stream, expected_len) in [
            (3, None, 2),
            (4, Some(""), 2),
            (5, Some(" "), 0),
            (6, Some(" nhk "), 0),
        ] {
            let stream = stream.map_or(serde_json::Value::Null, |stream| {
                serde_json::Value::String(stream.to_owned())
            });
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {
                    "name": "search_programs",
                    "arguments": {"q": "caption", "stream": stream},
                },
            })
            .to_string();
            let response = mcp_post(&app, &body, Some(&session_id)).await;
            let result = sse_json(&body_text(response).await);
            assert_eq!(
                result["result"]["structuredContent"]["items"]
                    .as_array()
                    .unwrap()
                    .len(),
                expected_len
            );
        }

        drop(data_dir);
    }

    #[tokio::test]
    async fn archive_routes_return_only_the_collision_winner() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
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
        let app = test_router(data_dir.to_path_buf(), Arc::new(LiveBroadcaster::new([])));

        let response = get(&app, "/api/programs?stream=nhk&month=2020-01").await;
        let programs: Vec<serde_json::Value> =
            serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(programs.len(), 1);
        assert_eq!(programs[0]["filename"], ARCHIVE_FILE_NAME);
        assert_eq!(programs[0]["path"], RAW_PROGRAM_PATH);

        let response = get(&app, RAW_PROGRAM_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, ARCHIVE_FILE_BODY);

        drop(data_dir);
    }

    #[tokio::test]
    async fn archive_routes_reject_invalid_or_missing_programs() {
        let (data_dir, app) = empty_app().await;

        assert_json_error(&app, "/api/months", StatusCode::BAD_REQUEST).await;
        assert_json_error(&app, "/api/programs?stream=nhk", StatusCode::BAD_REQUEST).await;
        let response = get(&app, "/api/programs?stream=..&month=2020-01").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        for month in ["2020-00", "2020-13"] {
            assert_json_error(
                &app,
                &format!("/api/programs?stream=nhk&month={month}"),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }

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
        for uri in [
            "/api/programs/search?q=caption&from=2026-13-45",
            "/api/programs/search?q=caption&to=2026-07-15_24-00-00",
            "/api/programs/search?q=caption&from=2026-07-16&to=2026-07-15",
        ] {
            assert_json_error(&app, uri, StatusCode::BAD_REQUEST).await;
        }
        let oversized_query = format!("/api/programs/search?q={}", "a".repeat(101));
        assert_json_error(&app, &oversized_query, StatusCode::BAD_REQUEST).await;

        assert_json_error(&app, "/api/live/%FF", StatusCode::BAD_REQUEST).await;

        drop(data_dir);
    }

    #[tokio::test]
    async fn all_http_routes_reject_unknown_query_parameters() {
        let (data_dir, app) = app_with_program().await;

        for uri in [
            "/api/streams?unexpected=1",
            "/api/months?stream=nhk&unexpected=1",
            "/api/programs?stream=nhk&month=2020-01&unexpected=1",
            "/api/programs/search?q=caption&unexpected=1",
            "/api/programs/nhk/2020-01-01_00-00-00?unexpected=1",
            "/api/live/nhk?unexpected=1",
        ] {
            assert_json_error(&app, uri, StatusCode::BAD_REQUEST).await;
        }

        drop(data_dir);
    }

    #[tokio::test]
    async fn live_route_streams_lines_and_rejects_unknown_streams() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let broadcaster = Arc::new(LiveBroadcaster::new(["nhk".to_owned()]));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let app = router(
            data_dir.to_path_buf(),
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

        drop(data_dir);
    }

    #[tokio::test]
    async fn non_live_routes_return_service_unavailable_until_search_db_is_ready() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let ready = Arc::new(AtomicBool::new(false));
        let (_, shutdown) = watch::channel(false);
        let app = router(
            data_dir.to_path_buf(),
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

        drop(data_dir);
    }

    #[tokio::test]
    async fn openapi_and_docs_are_available_before_search_db_is_ready() {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let app = test_router(
            data_dir.to_path_buf(),
            Arc::new(LiveBroadcaster::new(["nhk".to_owned()])),
        );

        let response = get(&app, "/openapi.json").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let api: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(api["openapi"], "3.1.0");
        assert_eq!(api["info"]["title"], "aribcap-db HTTP API");
        assert_eq!(api["info"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(api, serde_json::to_value(openapi_document()).unwrap());

        let paths = api["paths"].as_object().unwrap();
        assert_eq!(paths.len(), 6);
        for path in [
            "/api/streams",
            "/api/months",
            "/api/programs",
            "/api/programs/search",
            "/api/programs/{stream}/{recording_started_at}",
            "/api/live/{stream}",
        ] {
            assert!(paths[path]["get"].is_object(), "missing GET {path}");
        }
        for path in ["/openapi.json", "/docs", "/mcp"] {
            assert!(!paths.contains_key(path));
        }

        let raw = &paths["/api/programs/{stream}/{recording_started_at}"]["get"];
        let parameter_names = raw["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|parameter| parameter["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(parameter_names, ["stream", "recording_started_at"]);
        assert!(raw["responses"]["200"]["content"]["application/x-ndjson"].is_object());
        assert!(raw["responses"]["400"].is_object());
        assert!(raw["responses"]["404"].is_object());
        assert!(raw["responses"]["503"].is_object());
        assert!(raw["responses"]["500"].is_object());

        let response = get(&app, "/docs").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert!(body_text(response).await.contains("/openapi.json"));

        drop(data_dir);
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
        drop(data_dir);

        let (data_dir, app) = app_with_program_mcp().await;
        let response = get(&app, "/openapi.json").await;
        let api: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(api["paths"].as_object().unwrap().len(), 6);
        assert!(api["paths"].get("/mcp").is_none());

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

        for tool in tools {
            assert_eq!(tool["annotations"]["readOnlyHint"], true, "tool {tool}");
            assert_eq!(tool["annotations"]["idempotentHint"], true, "tool {tool}");
            assert_eq!(
                tool["inputSchema"]["additionalProperties"], false,
                "tool {tool}"
            );
        }
        for name in ["list_streams", "search_programs", "get_program_captions"] {
            assert!(
                tools.iter().any(|tool| tool["name"] == name),
                "missing tool {name}"
            );
        }

        let response = mcp_post(
            &app,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_streams"}}"#,
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

        for (id, name, arguments) in [
            (10, "list_streams", r#"{"unexpected":1}"#),
            (11, "search_programs", r#"{"q":"caption","unexpected":1}"#),
            (
                12,
                "get_program_captions",
                r#"{"stream":"nhk","recording_started_at":"2020-01-01_00-00-00","unexpected":1}"#,
            ),
        ] {
            let arguments: serde_json::Value = serde_json::from_str(arguments).unwrap();
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            })
            .to_string();
            let response = mcp_post(&app, &body, Some(&session_id)).await;
            let error = sse_json(&body_text(response).await);
            assert_eq!(error["result"]["isError"], true, "tool {name}");
            assert!(
                error["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("unknown field"),
                "tool {name}: {error}"
            );
        }

        for (id, arguments) in [
            (
                20,
                serde_json::json!({"q": "caption", "from": "2026-13-45"}),
            ),
            (
                21,
                serde_json::json!({
                    "q": "caption",
                    "from": "2026-07-16",
                    "to": "2026-07-15",
                }),
            ),
            (22, serde_json::json!({"q": "a".repeat(101)})),
        ] {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": "search_programs", "arguments": arguments},
            })
            .to_string();
            let response = mcp_post(&app, &body, Some(&session_id)).await;
            let error = sse_json(&body_text(response).await);
            assert_eq!(error["result"]["isError"], true, "request {id}");
        }

        drop(data_dir);
    }

    async fn get(app: &Router, uri: &str) -> Response {
        app.clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn mcp_post(app: &Router, body: &str, session_id: Option<&str>) -> Response {
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
            .oneshot(request.body(Body::from(body.to_owned())).unwrap())
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

    async fn empty_app() -> (TestDir, Router) {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let db_path = search_db::search_db_path(&data_dir);
        search_db::open_and_migrate(&db_path).await.unwrap();
        let app = test_router(data_dir.to_path_buf(), Arc::new(LiveBroadcaster::new([])));
        (data_dir, app)
    }

    async fn app_with_program() -> (TestDir, Router) {
        app_with_program_options(false).await
    }

    async fn app_with_program_mcp() -> (TestDir, Router) {
        app_with_program_options(true).await
    }

    async fn app_with_programs_mcp() -> (TestDir, Router) {
        app_with_program_options_and_second_stream(true, true).await
    }

    async fn app_with_program_options(mcp_enabled: bool) -> (TestDir, Router) {
        app_with_program_options_and_second_stream(mcp_enabled, false).await
    }

    async fn app_with_program_options_and_second_stream(
        mcp_enabled: bool,
        include_second_stream: bool,
    ) -> (TestDir, Router) {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
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
            data_dir.to_path_buf(),
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
}

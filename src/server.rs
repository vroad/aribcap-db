use std::{io, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::stream::unfold;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};
use tokio_util::io::ReaderStream;
use tower_http::trace::TraceLayer;

use crate::{archive, live::LiveBroadcaster};

#[derive(Clone)]
struct AppState {
    data_dir: Arc<PathBuf>,
    live: Arc<LiveBroadcaster>,
    shutdown: watch::Receiver<bool>,
}

pub fn router(
    data_dir: PathBuf,
    live: Arc<LiveBroadcaster>,
    shutdown: watch::Receiver<bool>,
) -> Router {
    Router::new()
        .route("/api/streams", get(api_streams))
        .route("/api/months", get(api_months))
        .route("/api/records", get(api_records))
        .route("/api/records/{stream}/{month}/{filename}", get(raw_record))
        .route("/api/live/{stream}", get(live_stream))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            data_dir: Arc::new(data_dir),
            live,
            shutdown,
        })
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
    let data_dir = state.data_dir.clone();
    blocking_io(move || archive::list_records(&data_dir, &query.stream, &query.month))
        .await
        .map(Json)
}

async fn raw_record(
    State(state): State<AppState>,
    path: Result<Path<(String, String, String)>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path((stream, month, filename)) = path?;
    let data_dir = state.data_dir.clone();
    let Some(path) =
        blocking_io(move || archive::resolve_record_path(&data_dir, &stream, &month, &filename))
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

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
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

    static NEXT_TEMP_DIR: AtomicUsize = AtomicUsize::new(0);
    const RECORD_FILENAME: &str = "2020-01-01_00-00-00.title#part.jsonl";
    const RECORD_BODY: &str = "{\"type\":\"caption\"}\n";
    const RAW_RECORD_PATH: &str = "/api/records/nhk/2020-01/2020-01-01_00-00-00.title%23part.jsonl";

    #[tokio::test]
    async fn archive_routes_list_and_stream_record() {
        let (data_dir, app) = app_with_record();

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

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn archive_routes_reject_invalid_or_missing_records() {
        let (data_dir, app) = empty_app();

        assert_json_error(&app, "/api/months", StatusCode::BAD_REQUEST).await;
        assert_json_error(&app, "/api/records?stream=nhk", StatusCode::BAD_REQUEST).await;
        let response = get(&app, "/api/records?stream=..&month=2020-01").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = get(
            &app,
            "/api/records/nhk/2020-01/2020-01-01_00-00-00.missing.jsonl",
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = get(&app, "/api/records/search?q=caption").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        assert_json_error(&app, "/api/live/%FF", StatusCode::BAD_REQUEST).await;

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[tokio::test]
    async fn live_route_streams_lines_and_rejects_unknown_streams() {
        let data_dir = temp_dir();
        let broadcaster = Arc::new(LiveBroadcaster::new(["nhk".to_owned()]));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let app = router(data_dir.clone(), broadcaster.clone(), shutdown_rx);
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

    fn empty_app() -> (PathBuf, Router) {
        let data_dir = temp_dir();
        let app = test_router(data_dir.clone(), Arc::new(LiveBroadcaster::new([])));
        (data_dir, app)
    }

    fn app_with_record() -> (PathBuf, Router) {
        let data_dir = temp_dir();
        let record_dir = archive::records_root(&data_dir).join("nhk").join("2020-01");
        fs::create_dir_all(&record_dir).unwrap();
        fs::write(record_dir.join(RECORD_FILENAME), RECORD_BODY).unwrap();
        let app = test_router(data_dir.clone(), Arc::new(LiveBroadcaster::new([])));
        (data_dir, app)
    }

    fn test_router(data_dir: PathBuf, live: Arc<LiveBroadcaster>) -> Router {
        let (_, shutdown) = watch::channel(false);
        router(data_dir, live, shutdown)
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

use std::{sync::Arc, time::Duration};

use axum::Router;
use rmcp::{
    Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::query_service::{
    ArchiveQueryService, ListStreamsResponse, ProgramCaptionsResponse, QueryServiceError,
    SearchRequest, SearchResponse,
};

const MCP_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EmptyToolArguments {}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetProgramCaptionsRequest {
    /// Archive stream name, such as `nhk`.
    pub stream: String,
    /// Recording start timestamp from a search result, in `YYYY-MM-DD_HH-MM-SS` form.
    pub recording_started_at: String,
    /// First JSONL line number to include. One-based and inclusive; defaults to 1.
    pub start_line: Option<i64>,
    /// Maximum number of captions to return. Defaults to 100 and is clamped to `1..500`.
    pub limit: Option<i64>,
}

#[derive(Clone)]
pub struct AribcapMcp {
    query_service: ArchiveQueryService,
    tool_router: ToolRouter<Self>,
}

impl AribcapMcp {
    fn new(query_service: ArchiveQueryService) -> Self {
        Self {
            query_service,
            tool_router: Self::tool_router(),
        }
    }

    #[cfg(test)]
    pub(crate) fn tools() -> Vec<rmcp::model::Tool> {
        Self::tool_router().list_all()
    }
}

#[tool_router]
impl AribcapMcp {
    #[tool(
        description = "List archive stream names that can be searched",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_streams(
        &self,
        Parameters(EmptyToolArguments {}): Parameters<EmptyToolArguments>,
    ) -> Result<Json<ListStreamsResponse>, String> {
        self.query_service
            .list_streams()
            .await
            .map(|streams| Json(ListStreamsResponse { streams }))
            .map_err(tool_error)
    }

    #[tool(
        description = "Search archived program titles, descriptions, and caption text. `q`, `program_q`, and `line_q` are all optional; when all three are omitted, programs are listed using only the `stream`/`from`/`to`/`genre` filters, with no caption hits. When stream is omitted, all archive streams are searched. Results are ordered by newest program first; caption hits are ordered by their occurrence in the program, not relevance.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn search_programs(
        &self,
        Parameters(query): Parameters<SearchRequest>,
    ) -> Result<Json<SearchResponse>, String> {
        self.query_service
            .search(query)
            .await
            .map(Json)
            .map_err(tool_error)
    }

    #[tool(
        description = "Get a bounded page of structured caption lines for one archived program",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_program_captions(
        &self,
        Parameters(request): Parameters<GetProgramCaptionsRequest>,
    ) -> Result<Json<ProgramCaptionsResponse>, String> {
        self.query_service
            .get_program_captions(
                request.stream,
                request.recording_started_at,
                request.start_line,
                request.limit,
            )
            .await
            .map(Json)
            .map_err(tool_error)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for AribcapMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("aribcap-db", env!("CARGO_PKG_VERSION"))
                    .with_title("aribcap-db archive search")
                    .with_description(
                        "Read-only search and caption access for the aribcap-db program archive",
                    ),
            )
            .with_instructions(
                "Search archived Japanese TV program metadata and caption text. Search results are newest-first, and caption hits are in program order rather than relevance order."
            )
    }
}

pub fn router(query_service: ArchiveQueryService, cancellation_token: CancellationToken) -> Router {
    let mut session_manager = LocalSessionManager::default();
    session_manager.session_config.keep_alive = Some(MCP_SESSION_IDLE_TIMEOUT);
    let session_manager = Arc::new(session_manager);
    router_with_session_manager(query_service, cancellation_token, session_manager)
}

fn router_with_session_manager(
    query_service: ArchiveQueryService,
    cancellation_token: CancellationToken,
    session_manager: Arc<LocalSessionManager>,
) -> Router {
    let service = StreamableHttpService::new(
        move || Ok::<_, std::io::Error>(AribcapMcp::new(query_service.clone())),
        session_manager,
        StreamableHttpServerConfig::default()
            .with_cancellation_token(cancellation_token.child_token()),
    );
    Router::new().nest_service("/mcp", service)
}

fn tool_error(error: QueryServiceError) -> String {
    error.into_client_message("mcp")
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Arc, atomic::AtomicBool},
    };

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[test]
    fn internal_tool_errors_are_redacted() {
        let error = QueryServiceError::Internal(anyhow::anyhow!("sensitive database detail"));

        assert_eq!(tool_error(error), "internal query error");
    }

    #[tokio::test]
    async fn data_tools_report_when_search_database_is_not_ready() {
        let data_dir = PathBuf::from("test-data");
        let service = ArchiveQueryService::new(
            data_dir.clone(),
            data_dir.join("search.sqlite3"),
            Arc::new(AtomicBool::new(false)),
        );

        let error = match AribcapMcp::new(service)
            .list_streams(Parameters(EmptyToolArguments {}))
            .await
        {
            Ok(_) => panic!("list_streams should fail while the database is not ready"),
            Err(error) => error,
        };

        assert_eq!(error, "search database is not ready");
    }

    #[tokio::test]
    async fn idle_sessions_expire_and_clients_can_initialize_again() {
        let data_dir = PathBuf::from("test-data");
        let query_service = ArchiveQueryService::new(
            data_dir.clone(),
            data_dir.join("search.sqlite3"),
            Arc::new(AtomicBool::new(false)),
        );
        let mut session_manager = LocalSessionManager::default();
        session_manager.session_config.keep_alive = Some(Duration::from_millis(20));
        let session_manager = Arc::new(session_manager);
        let cancellation_token = CancellationToken::new();
        let app = router_with_session_manager(
            query_service,
            cancellation_token.clone(),
            session_manager.clone(),
        );

        let response = post(
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
        to_bytes(response.into_body(), usize::MAX).await.unwrap();

        let response = post(
            &app,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            Some(&session_id),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if session_manager.sessions.read().await.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("idle MCP session should be removed");

        let response = post(
            &app,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            Some(&session_id),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = post(
            &app,
            r#"{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_ne!(response.headers()["mcp-session-id"], session_id);

        cancellation_token.cancel();
    }

    async fn post(
        app: &Router,
        body: &'static str,
        session_id: Option<&str>,
    ) -> axum::response::Response {
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
}

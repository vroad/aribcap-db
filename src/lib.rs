pub mod archive;
pub mod cli;
pub mod config;
#[cfg(test)]
mod docs_gen;
#[cfg(test)]
mod http_api_docs;
pub mod live;
pub mod logging;
pub mod mcp;
#[cfg(test)]
mod mcp_docs;
pub mod query_service;
pub mod render;
pub mod search_db;
pub mod serve;
pub mod server;
pub mod stream;
pub mod tail;

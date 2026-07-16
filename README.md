# aribcap-db

aribcap-db records TV programs and captions from the mirakc streaming API and
serves them over HTTP with full-text search.

## Features

- Caption and program metadata recording
  - Each program is stored as a plain JSONL file.
  - Old JSONL files are deleted automatically after a configurable retention period.
- Provides full-text search over program metadata and captions
- Live caption feed
  - `GET /api/live/<stream>` returns captions of the broadcast in real time as an HTTP stream.
  - aribcap-db already ingests the extracted JSONL for recording and indexing, so live clients
    can reuse it instead of opening another mirakc session to parse the same TS again.
- Listens on any number of TCP and Unix socket addresses
- Includes an optional read-only MCP server

## How it works

mirakc streams each configured channel as broadcast MPEG-2 TS. The `aribcap-dump` post-filter
converts the TS into JSONL records of EIT program information and ARIB captions. aribcap-db
subscribes to these filtered streams, archives the records per program, and indexes them for
full-text search.

## Quick start

`aribcap-db serve` subscribes to every stream in the config, stores raw JSONL, and serves archived
programs over HTTP.

```sh
cargo run --bin aribcap-db -- serve --config ./config.toml
```

Set `data_dir`, `addrs`, `retention`, and the optional MCP switch in the `[serve]` section:

```toml
[serve]
data_dir = "./aribcap-db-data"
addrs = [{ tcp = "127.0.0.1:40773" }]
retention = "30d"
```

The HTTP server listens on `127.0.0.1:40773` when `[serve].addrs` is omitted. Use
[config.example.toml](config.example.toml) as a starting point.

## Unix socket listeners

Alongside TCP, `addrs` accepts `unix_socket` entries. Every listener serves the same HTTP API:

```toml
[serve]
addrs = [
  { tcp = "127.0.0.1:40773" },
  { unix_socket = "/run/aribcap-db/aribcap-db.sock" },
]
```

For each `unix_socket` address, aribcap-db opens or creates a lock file at `<path>.lock`
(`/run/aribcap-db/aribcap-db.sock.lock` for the example above) and holds it while running.
You do not need to manually remove either file before a restart, because aribcap-db replaces the
stale socket on its own.

## HTTP API

```text
GET /api/streams
GET /api/months?stream=nhk
GET /api/programs?stream=nhk&month=2026-07
GET /api/programs/nhk/2026-07-14_12-00-00
GET /api/programs/search?q=caption
GET /api/live/nhk
```

Until the search database migrations finish, every endpoint except `/api/live` returns
`503 Service Unavailable`.

See the [HTTP API reference](docs/http-api.md) for request parameters, response behavior,
program collisions, and live-stream delivery semantics.

## MCP server

Enable the read-only Streamable HTTP MCP server in the configuration:

```toml
[serve]
data_dir = "./aribcap-db-data"
addrs = [{ tcp = "127.0.0.1:40773" }]
mcp = true
```

The endpoint is `http://<server-address>:40773/mcp`. It provides tools for listing archive
streams, searching program metadata and captions, and retrieving structured caption lines.

See the [MCP server reference](docs/mcp.md) for client setup, tool arguments and results,
search and pagination behavior, availability, and network considerations.

## Operations

### Archive lifecycle

Recording starts with an EIT `present` record. Caption records received before the first EIT
`present` record are skipped. Each program is written under `archive/<stream>/<YYYY-MM>/`, and
completed archive files older than the configured retention period are removed.

### Service lifecycle

HTTP failures do not stop archive ingest or garbage collection. After a bind or serve error, a
listener waits 15 seconds and tries to bind and serve again. On shutdown, every listener stops
accepting new HTTP connections and waits up to 10 seconds for in-flight responses before closing
them. Each address in `[serve].addrs` runs and retries independently, so a failure on one does
not affect the others.

### Search index

The search index is stored at `<data-dir>/search.sqlite3`. The background indexer scans existing
archive files at startup and then processes archive changes every 10 seconds.

To rebuild the index from all archive files in the program archive, stop `aribcap-db serve` for
that data directory and run:

```sh
aribcap-db search-rebuild --config ./config.toml
```

Rebuild deletes and recreates the SQLite database; it does not modify archive files.

## Development

Generated SQLx query metadata is committed under `.sqlx/`, allowing builds and tests without
setting `DATABASE_URL`. After changing a SQLx-checked query or a migration, regenerate the
metadata with:

```sh
export DATABASE_URL=sqlite://target/sqlx-check.sqlite3
cargo sqlx database create
cargo sqlx migrate run
cargo sqlx prepare
```

Use `cargo sqlx prepare --check` with the same `DATABASE_URL` to verify that the committed
metadata is current.

Applied migrations must not be edited because SQLx records their checksums. Add a new
timestamped migration for later schema changes.

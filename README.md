# aribcap-db

Utilities for working with JSONL streams produced by `aribcap-dump`.

## Quick start

`aribcap-db serve` subscribes to every stream in the config, stores raw JSONL,
and serves the archives over HTTP.

```sh
cargo run --bin aribcap-db -- serve --config ./config.toml
```

Set `data_dir`, `listen`, and `retention` in the `[serve]` section, or override
them on the command line:

```sh
aribcap-db serve \
  --config ./config.toml \
  --data-dir ./aribcap-db-data \
  --listen 127.0.0.1:40773 \
  --retention 30d
```

The HTTP server listens on `127.0.0.1:40773` when `listen` is omitted.
Use [config.example.toml](config.example.toml) as a starting point.

## HTTP API

```text
GET /api/streams
GET /api/months?stream=nhk
GET /api/records?stream=nhk&month=2026-07
GET /api/records/nhk/2026-07-14_12-00-00
GET /api/records/search?q=caption
GET /api/live/nhk
```

Until the search database migrations finish, every endpoint except `/api/live`
returns `503 Service Unavailable`.

See the [HTTP API reference](docs/http-api.md) for request parameters, response
behavior, record collisions, and live-stream delivery semantics.

## Operations

### Archive lifecycle

Recording starts with an EIT `present` record. Caption records received before
the first EIT `present` record are skipped. Each program is written under
`records/<stream>/<YYYY-MM>/`, and completed files older than the configured
retention period are removed.

### Service lifecycle

HTTP failures do not stop archive ingest or garbage collection. After a bind or
serve error, the HTTP server waits 15 seconds and tries to bind and serve again.
On shutdown, the server stops accepting new HTTP connections and waits up to 10
seconds for in-flight responses before closing them.

### Search index

The search index is stored at `<data-dir>/search.sqlite3`. The background
indexer scans existing archives at startup and then processes archive changes
every 10 seconds.

To rebuild the index from all stored JSONL archives, stop `aribcap-db serve`
for that data directory and run:

```sh
aribcap-db search-rebuild --data-dir ./aribcap-db-data
```

Rebuild deletes and recreates the SQLite database; it does not modify archive
files.

## Development

Generated SQLx query metadata is committed under `.sqlx/`, allowing builds and
tests without setting `DATABASE_URL`. After changing a SQLx-checked query or a
migration, regenerate the metadata with:

```sh
export DATABASE_URL=sqlite://target/sqlx-check.sqlite3
cargo sqlx database create
cargo sqlx migrate run
cargo sqlx prepare
```

Use `cargo sqlx prepare --check` with the same `DATABASE_URL` to verify that the
committed metadata is current.

Applied migrations must not be edited because SQLx records their checksums.
Add a new timestamped migration for later schema changes.

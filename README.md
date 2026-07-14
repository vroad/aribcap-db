# aribcap-db

Utilities for working with JSONL streams produced by `aribcap-dump`.

## Archive JSONL

`aribcap-db serve` subscribes to every stream in the config, stores raw JSONL,
and serves the archives over HTTP. Recording starts with an EIT `present`
record. Caption records received before the first EIT `present` record are
skipped. Each program is written under `records/<stream>/<YYYY-MM>/`, and
completed files older than the configured retention period are removed.

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
HTTP failures do not stop archive ingest or garbage collection. After a bind or
serve error, the HTTP server waits 15 seconds and tries to bind and serve again.
On shutdown, the server stops accepting new HTTP connections and waits up to 10
seconds for in-flight responses before closing them.

HTTP endpoints:

```text
GET /api/streams
GET /api/months?stream=nhk
GET /api/records?stream=nhk&month=2026-07
GET /api/records/nhk/2026-07/<filename>.jsonl
GET /api/live/nhk
```

The live endpoint streams raw JSONL from the existing upstream connection.
Each subscriber has a 256-line buffer; a subscriber that falls behind skips
old buffered lines without blocking ingest or other subscribers.

Use [config.example.toml](config.example.toml) as a starting point.

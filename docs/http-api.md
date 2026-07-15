# HTTP API

`aribcap-db serve` exposes archive discovery, search, raw-program, and live-stream
endpoints. JSON endpoints return errors in the following form:

```json
{"error":"error description"}
```

## Availability

Until the search database migrations finish, every endpoint except `/api/live`
returns `503 Service Unavailable`. The full archive scan continues in the
background after the migrations finish, so archive and search results can be
partial during the initial scan.

## Archive discovery

### List streams

```text
GET /api/streams
```

Returns the archive stream names.

### List months

```text
GET /api/months?stream=nhk
```

Returns the archive months for `stream` in `YYYY-MM` format.

### List programs

```text
GET /api/programs?stream=nhk&month=2026-07
```

Returns programs for the selected stream and month. Each entry includes its API
path, stream, month, complete filename, and indexed file size.

`size_bytes` is the file size captured by the background indexer, not a live
filesystem measurement. While a recording is growing, the value may lag behind
the current file size by approximately one indexing interval (10 seconds). Do
not use an unchanged `size_bytes` value as a recording-completion signal.

## Raw program

```text
GET /api/programs/nhk/2026-07-14_12-00-00
```

Archived programs use their recording-start timestamp as the HTTP identifier.
The server resolves the complete JSONL filename through the SQLite search
index. The response has the `application/x-ndjson` content type.

If multiple files have the same stream and timestamp, only the index winner
appears in the program list and can be retrieved through the raw program endpoint.

The programs API may include a file that is still being recorded. A raw program
response ends at the file's current end and does not wait for lines appended
later. Use the live endpoint to receive subsequent lines continuously.

## Search

```text
GET /api/programs/search?q=caption
```

Use one of the following query forms:

- `q` searches program metadata and caption text.
- `program_q` searches only program metadata.
- `line_q` searches only caption text.
- `program_q` and `line_q` can be combined; `q` cannot be combined with them.

Search expressions support `AND` and `OR`. The following parameters further
control the results:

- `stream` restricts results to one stream. If omitted, all streams are searched.
- `from` and `to` restrict results by recording time.
- `genre` accepts `0..15` or `0..15:0..15`.
- `limit` controls the number of programs and is clamped to `1..200`.
- `inner_hits` controls caption hits per program and is clamped to `1..50`.

The default `limit` is 20, and the default `inner_hits` is 5.

## Live stream

```text
GET /api/live/nhk
```

The live endpoint streams raw JSONL from the existing upstream connection with
the `application/x-ndjson` content type. It does not open a separate upstream
connection for each subscriber.

Each subscriber has a 256-line buffer. A subscriber that falls behind skips old
buffered lines without blocking ingest or other subscribers.

## Errors

- Invalid path or query values return `400 Bad Request`.
- Unknown live streams and missing raw programs return `404 Not Found`.
- Endpoints other than `/api/live` return `503 Service Unavailable` until the
  search database migrations finish.
- Unexpected filesystem or database failures return `500 Internal Server
  Error`.

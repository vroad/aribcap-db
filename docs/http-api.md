# HTTP API

`aribcap-db serve` exposes archive discovery, search, raw-program, and live-stream
endpoints.

## Error response format

JSON endpoints return errors in the following form:

```json
{"error":"error description"}
```

When the server is running, `/docs` provides an interactive Scalar reference and
`/openapi.json` provides the generated OpenAPI 3.1 document. The endpoint tables
below are generated from that same document; the surrounding behavior and
operational notes are maintained by hand.

## Availability

Until the search database migrations finish, every endpoint except `/api/live`
returns `503 Service Unavailable`. The full archive scan continues in the
background after the migrations finish, so archive and search results can be
partial during the initial scan.

## Archive discovery

### List streams

<!-- generated: http GET /api/streams -->

```text
GET /api/streams
```

List archive stream names that can be searched.

Parameters: none.

Responses:

| Status | Content-Type | Schema | Description |
| --- | --- | --- | --- |
| 200 | `application/json` | array of string | Archive stream names. |
| 500 | `application/json` | `ErrorBody` | Internal server error. |
| 503 | `application/json` | `ErrorBody` | The search database is not ready. |

<!-- generated: http GET /api/streams end -->

Returns the archive stream names.

### List months

<!-- generated: http GET /api/months -->

```text
GET /api/months
```

List archive months available for one stream.

Parameters:

| Name | Location | Type | Required | Description |
| --- | --- | --- | --- | --- |
| `stream` | query | string | yes | Archive stream name. |

Responses:

| Status | Content-Type | Schema | Description |
| --- | --- | --- | --- |
| 200 | `application/json` | array of string | Archive months in `YYYY-MM` form. |
| 400 | `application/json` | `ErrorBody` | Invalid path or query parameters. |
| 500 | `application/json` | `ErrorBody` | Internal server error. |
| 503 | `application/json` | `ErrorBody` | The search database is not ready. |

<!-- generated: http GET /api/months end -->

Returns the archive months for `stream` in `YYYY-MM` format.

### List programs

<!-- generated: http GET /api/programs -->

```text
GET /api/programs
```

List indexed archived programs for one stream and month.

Parameters:

| Name | Location | Type | Required | Description |
| --- | --- | --- | --- | --- |
| `stream` | query | string | yes | Archive stream name. |
| `month` | query | string | yes | Archive month in `YYYY-MM` form. |

Responses:

| Status | Content-Type | Schema | Description |
| --- | --- | --- | --- |
| 200 | `application/json` | array of `ProgramEntry` | Indexed archived programs. |
| 400 | `application/json` | `ErrorBody` | Invalid path or query parameters. |
| 500 | `application/json` | `ErrorBody` | Internal server error. |
| 503 | `application/json` | `ErrorBody` | The search database is not ready. |

<!-- generated: http GET /api/programs end -->

Returns programs for the selected stream and month. `month` uses `YYYY-MM`
format with a month from `01` through `12`.

Each entry contains the following fields:

- `path`: API path for the program
- `stream`: archive stream name
- `month`: archive month
- `filename`: complete archive filename
- `size_bytes`: indexed file size

`size_bytes` is the file size captured by the background indexer, not a live
filesystem measurement. While a recording is growing, the value may lag behind
the current file size by approximately one indexing interval (10 seconds). Do
not use an unchanged `size_bytes` value as a recording-completion signal.

## Raw program

<!-- generated: http GET /api/programs/{stream}/{recording_started_at} -->

```text
GET /api/programs/{stream}/{recording_started_at}
```

Stream the archived program's raw JSONL records.

Parameters:

| Name | Location | Type | Required | Description |
| --- | --- | --- | --- | --- |
| `stream` | path | string | yes | Archive stream name. |
| `recording_started_at` | path | string | yes | Recording start timestamp in `YYYY-MM-DD_HH-MM-SS` form. |

Responses:

| Status | Content-Type | Schema | Description |
| --- | --- | --- | --- |
| 200 | `application/x-ndjson` | string | Raw archived program records. |
| 400 | `application/json` | `ErrorBody` | Invalid path or query parameters. |
| 404 | `application/json` | `ErrorBody` | The requested resource was not found. |
| 500 | `application/json` | `ErrorBody` | Internal server error. |
| 503 | `application/json` | `ErrorBody` | The search database is not ready. |

<!-- generated: http GET /api/programs/{stream}/{recording_started_at} end -->

Archived programs use their recording-start timestamp as the HTTP identifier.
The server resolves the complete JSONL filename through the SQLite search
index. The response has the `application/x-ndjson` content type.

If multiple files have the same stream and timestamp, only the index winner
appears in the program list and can be retrieved through the raw program endpoint.

The programs API may include a file that is still being recorded. A raw program
response ends at the file's current end and does not wait for lines appended
later. Use the live endpoint to receive subsequent lines continuously.

## Search

<!-- generated: http GET /api/programs/search -->

```text
GET /api/programs/search
```

Search archived program metadata and caption text. `q`, `program_q`, and `line_q` are all optional; when all three are omitted, programs are listed using only the `stream`/`from`/`to`/`genre` filters, with no caption hits.

Parameters:

| Name | Location | Type | Required | Description |
| --- | --- | --- | --- | --- |
| `q` | query | string | no | Search program metadata and caption text with one expression. Limited to 100 Unicode characters. |
| `program_q` | query | string | no | Search program titles and descriptions only. May be combined with `line_q`. Limited to 100 Unicode characters. |
| `line_q` | query | string | no | Search caption text only. May be combined with `program_q`. Limited to 100 Unicode characters.  When `q`, `program_q`, and `line_q` are all omitted, programs are listed using only the `stream`/`from`/`to`/`genre` filters (or all programs, if none of those are given either), newest first, with no caption hits. |
| `genre` | query | string | no | Genre filter in `0..15` or `0..15:0..15` form. |
| `stream` | query | string | no | Restrict results to one archive stream. When omitted, null, or empty, search all streams. |
| `from` | query | string | no | Inclusive lower recording-time bound in `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS` form. A date-only value expands to `YYYY-MM-DD_00-00-00`. Must not be later than `to` when both are provided. |
| `to` | query | string | no | Inclusive upper recording-time bound in `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS` form. A date-only value expands to `YYYY-MM-DD_23-59-59`. |
| `limit` | query | integer | no | Maximum programs to return. Defaults to 20 and is clamped to `1..200`. |
| `inner_hits` | query | integer | no | Maximum caption hits per program. Defaults to 5 and is clamped to `1..50`. |

Responses:

| Status | Content-Type | Schema | Description |
| --- | --- | --- | --- |
| 200 | `application/json` | `SearchResponse` | Matching programs and caption hits. |
| 400 | `application/json` | `ErrorBody` | Invalid path or query parameters. |
| 500 | `application/json` | `ErrorBody` | Internal server error. |
| 503 | `application/json` | `ErrorBody` | The search database is not ready. |

<!-- generated: http GET /api/programs/search end -->

Use one of the following query forms:

- `q` searches program metadata and caption text.
- `program_q` searches only program metadata.
- `line_q` searches only caption text.
- `program_q` and `line_q` can be combined; `q` cannot be combined with them.

Search expressions support `AND` and `OR`. The following parameters further
control the results:

- `q`, `program_q`, and `line_q` are each limited to 100 Unicode characters.
- `stream` restricts results to one stream. If omitted or empty, all streams are
  searched.
- `from` and `to` restrict results by recording time. They accept
  `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS`.
- `from` must not be later than `to`.
- `genre` accepts `0..15` or `0..15:0..15`.
- `limit` controls the number of programs and is clamped to `1..200`.
- `inner_hits` controls caption hits per program and is clamped to `1..50`.

The default `limit` is 20, and the default `inner_hits` is 5.

## Live stream

<!-- generated: http GET /api/live/{stream} -->

```text
GET /api/live/{stream}
```

Stream raw JSONL records received from the existing upstream connection.

Parameters:

| Name | Location | Type | Required | Description |
| --- | --- | --- | --- | --- |
| `stream` | path | string | yes | Configured live stream name. |

Responses:

| Status | Content-Type | Schema | Description |
| --- | --- | --- | --- |
| 200 | `application/x-ndjson` | string | Live raw JSONL records. |
| 400 | `application/json` | `ErrorBody` | Invalid path or query parameters. |
| 404 | `application/json` | `ErrorBody` | The requested resource was not found. |

<!-- generated: http GET /api/live/{stream} end -->

The live endpoint streams raw JSONL from the existing upstream connection with
the `application/x-ndjson` content type. It does not open a separate upstream
connection for each subscriber.

Each subscriber has a 256-line buffer. A subscriber that falls behind skips old
buffered lines without blocking ingest or other subscribers.

## Errors

- Invalid path or query values return `400 Bad Request`.
- Undocumented query parameters return `400 Bad Request` on every endpoint.
- Unknown live streams and missing raw programs return `404 Not Found`.
- Endpoints other than `/api/live` return `503 Service Unavailable` until the
  search database migrations finish.
- Unexpected filesystem or database failures return `500 Internal Server
  Error`.

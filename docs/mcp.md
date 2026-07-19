# MCP server

`aribcap-db serve` can expose a read-only Model Context Protocol (MCP) server
over Streamable HTTP. It provides tools for discovering archive streams,
searching program metadata and captions, and retrieving structured caption
lines.

## Enable the server

The MCP server is disabled by default. Enable it in the `[serve]` section of
the configuration:

```toml
[serve]
data_dir = "./aribcap-db-data"
addrs = [{ tcp = "127.0.0.1:40773" }]
mcp = true
```

Start `aribcap-db serve` as usual. The MCP server uses the same router as the
HTTP API, reachable from every address in `[serve].addrs`, and exposes its
endpoint at `/mcp`:

```text
http://127.0.0.1:40773/mcp
```

Replace the host and port with one of the configured `addrs` entries when
connecting from another machine. If `mcp` is omitted or set to `false`, the
MCP server is not exposed and requests to `/mcp` return `404 Not Found`.

Clients must support Streamable HTTP to connect to the MCP server at `/mcp`.

## Inspect the server with MCP Inspector

To inspect the server interactively, start MCP Inspector:

```sh
HOST=127.0.0.1 ALLOWED_ORIGINS=http://127.0.0.1:6274 \
  npx @modelcontextprotocol/inspector
```

In Inspector, select `Streamable HTTP` and enter the endpoint URL (`http://127.0.0.1:40773/mcp`). The
client can then initialize a session, list the available tools, and call them.

## Sessions

The server expires an inactive MCP session after one hour. A client using an
expired session must initialize a new session.

## Tools

All tools are read-only and idempotent. The server does not provide MCP
resources or prompts.

<!-- generated: tool list_streams -->
### `list_streams`

List archive stream names that can be searched

The tool takes no arguments.
<!-- generated: tool list_streams end -->

Example result:

```json
{
  "streams": ["nhk", "nhke"]
}
```

<!-- generated: tool search_programs -->
### `search_programs`

Search archived program titles, descriptions, and caption text. When stream is omitted, all archive streams are searched. Results are ordered by newest program first; caption hits are ordered by their occurrence in the program, not relevance.

Arguments:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `q` | string | no | Search program metadata and caption text with one expression. |
| `program_q` | string | no | Search program titles and descriptions only. May be combined with `line_q`. |
| `line_q` | string | no | Search caption text only. May be combined with `program_q`. |
| `genre` | string | no | Genre filter in `0..15` or `0..15:0..15` form. |
| `stream` | string | no | Restrict results to one archive stream. When omitted, search all streams. |
| `from` | string | no | Inclusive lower recording-time bound in `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS` form. A date-only value expands to `YYYY-MM-DD_00-00-00`. |
| `to` | string | no | Inclusive upper recording-time bound in `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS` form. A date-only value expands to `YYYY-MM-DD_23-59-59`. |
| `limit` | integer | no | Maximum programs to return. Defaults to 20 and is clamped to `1..200`. |
| `inner_hits` | integer | no | Maximum caption hits per program. Defaults to 5 and is clamped to `1..50`. |

<!-- generated: tool search_programs end -->

Use one of these search forms:

- `q` by itself
- `program_q` by itself
- `line_q` by itself
- `program_q` and `line_q` together

At least one search expression is required. `q` cannot be combined with
`program_q` or `line_q`.

Search expressions support `AND` and `OR`, with `AND` taking precedence over
`OR`. Parentheses and unary operators such as `NOT` are not supported; use
explicit `AND` and `OR` operators to combine terms.

Search terms are normalized with Unicode NFKC, converted to lowercase, and
stripped of whitespace and non-alphanumeric characters. For example,
`weather forecast` is treated as the single term `weatherforecast`; use
`weather AND forecast` to require both terms separately.

Example arguments:

```json
{
  "program_q": "news",
  "line_q": "weather OR forecast",
  "stream": "nhk",
  "limit": 10,
  "inner_hits": 5
}
```

Example result:

```json
{
  "items": [
    {
      "programId": 42,
      "stream": "nhk",
      "recordingStartedAt": "2026-07-15_12-00-00",
      "startTime": "2026-07-15T12:00:00+09:00",
      "title": "Program title",
      "description": "Program description",
      "path": "/api/programs/nhk/2026-07-15_12-00-00",
      "hits": [
        {
          "lineId": 1001,
          "lineNo": 25,
          "time": "2026-07-15T12:01:30+09:00",
          "text": "Caption text"
        }
      ]
    }
  ]
}
```

Result behavior:

| Item | Behavior |
| --- | --- |
| Programs | Ordered from newest to oldest. |
| Caption hits | Ordered by their occurrence within each program, not by relevance. |
| Program-only search `hits` | An empty array. |
| `startTime` | `null` when the program start time is unavailable. |
| Caption hit `time` | `null` when the timestamp is unavailable. |

<!-- generated: tool get_program_captions -->
### `get_program_captions`

Get a bounded page of structured caption lines for one archived program

Arguments:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `stream` | string | yes | Archive stream name, such as `nhk`. |
| `recording_started_at` | string | yes | Recording start timestamp from a search result, in `YYYY-MM-DD_HH-MM-SS` form. |
| `start_line` | integer | no | First JSONL line number to include. One-based and inclusive; defaults to 1. |
| `limit` | integer | no | Maximum number of captions to return. Defaults to 100 and is clamped to `1..500`. |

<!-- generated: tool get_program_captions end -->

Use the `stream` and `recordingStartedAt` values from a `search_programs` result
as the `stream` and `recording_started_at` arguments.

Example arguments:

```json
{
  "stream": "nhk",
  "recording_started_at": "2026-07-15_12-00-00",
  "start_line": 1,
  "limit": 100
}
```

Example result:

```json
{
  "program": {
    "programId": 42,
    "stream": "nhk",
    "recordingStartedAt": "2026-07-15_12-00-00",
    "startTime": "2026-07-15T12:00:00+09:00",
    "durationSec": 1800,
    "title": "Program title",
    "description": "Program description"
  },
  "captions": [
    {
      "lineId": 1001,
      "lineNo": 25,
      "time": "2026-07-15T12:01:30+09:00",
      "text": "Caption text",
      "durationMs": 2000,
      "languageCode": "jpn"
    }
  ],
  "nextStartLine": 26
}
```

Result behavior:

| Item | Behavior |
| --- | --- |
| `program` | Metadata for the requested archived program. |
| `captions` | Up to `limit` caption lines whose `lineNo` is at least `start_line`, ordered by `lineNo` ascending. |
| Caption `lineNo` | The original JSONL line number; gaps are possible. |
| `nextStartLine` | The `start_line` for the next page, or `null` on the final page. |
| `program.startTime`, `program.durationSec` | `null` when unavailable. |
| Caption `time`, `durationMs`, `languageCode` | `null` when unavailable. |

## Availability and errors

Clients can initialize an MCP session and discover tools while the search
database is being prepared. During that period, calls to data tools fail with:

```text
search database is not ready
```

Invalid arguments, an unavailable search database, and a missing program are
reported as tool errors. Unexpected filesystem or database failures are logged
by the server and reported to the client as `internal query error`.

The initial archive scan continues in the background after the database is
ready, so tool results can be partial during that scan.

## Network considerations

The built-in HTTP server, including both the REST API and MCP endpoint, does
not provide authentication, authorization, TLS, or Origin validation. When
binding beyond loopback, use it on a trusted network or place it behind an
appropriately secured reverse proxy.

# aribcap-db

Utilities for working with JSONL streams produced by `aribcap-dump`.

## Archive JSONL

`aribcap-db serve` subscribes to every stream in the config and stores raw
JSONL after it receives an EIT `present` record. It writes a separate file for
each programme under `records/<stream>/<YYYY-MM>/` and removes completed files
older than the configured retention period.

```sh
cargo run --bin aribcap-db -- serve --config ./config.toml
```

Set `data_dir` and `retention` in the `[serve]` section, or override either
one on the command line:

```sh
aribcap-db serve --config ./config.toml --data-dir ./aribcap-db-data --retention 30d
```

Use [config.example.toml](config.example.toml) as a starting point. This branch
stores archive files only; it does not create or query a search database.

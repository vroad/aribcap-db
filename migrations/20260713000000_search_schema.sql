CREATE TABLE IF NOT EXISTS programs (
  id INTEGER PRIMARY KEY,

  stream TEXT NOT NULL,
  month TEXT NOT NULL,
  filename TEXT NOT NULL,
  path TEXT NOT NULL UNIQUE,

  -- Time when aribcap-db started recording to the JSONL archive file, parsed
  -- from its filename.
  recording_started_at TEXT NOT NULL,

  -- Program fields copied from the EIT present record, in JSONL field order
  -- (`type` and `section` are intentionally not stored; `shortEvents` and
  -- `extendedText` are represented by `title` and `description`).
  start_time TEXT,
  duration_sec INTEGER,

  title TEXT NOT NULL DEFAULT '',
  description TEXT NOT NULL DEFAULT '',
  version INTEGER,

  service_id INTEGER,
  transport_stream_id INTEGER,
  original_network_id INTEGER,
  event_id INTEGER,

  -- Normalized title and description used for search.
  normalized_title TEXT NOT NULL DEFAULT '',
  normalized_description TEXT NOT NULL DEFAULT '',

  UNIQUE(stream, recording_started_at)
);

CREATE INDEX IF NOT EXISTS programs_event_idx
ON programs(
  service_id,
  transport_stream_id,
  original_network_id,
  event_id,
  start_time
);

CREATE TABLE IF NOT EXISTS program_genres (
  program_id INTEGER NOT NULL REFERENCES programs(id) ON DELETE CASCADE,

  content_nibble_level1 INTEGER NOT NULL,
  content_nibble_level2 INTEGER NOT NULL,
  user_nibble1 INTEGER NOT NULL,
  user_nibble2 INTEGER NOT NULL,

  PRIMARY KEY (
    program_id,
    content_nibble_level1,
    content_nibble_level2,
    user_nibble1,
    user_nibble2
  )
);

CREATE INDEX IF NOT EXISTS program_genres_content_idx
ON program_genres(content_nibble_level1, content_nibble_level2, program_id);

CREATE TABLE IF NOT EXISTS caption_lines (
  id INTEGER PRIMARY KEY,

  program_id INTEGER NOT NULL REFERENCES programs(id) ON DELETE CASCADE,

  line_no INTEGER NOT NULL,
  byte_offset INTEGER NOT NULL,

  -- Caption fields copied from the Caption record, in JSONL field order
  -- (`type` and `ruby` are intentionally not stored).
  time TEXT,
  text TEXT NOT NULL,
  color TEXT,
  pid INTEGER,

  caption_type TEXT,
  language_code TEXT,
  duration_ms INTEGER,
  clear_screen INTEGER,

  -- Normalized text used for search.
  normalized_text TEXT NOT NULL,

  UNIQUE(program_id, line_no)
);

CREATE INDEX IF NOT EXISTS caption_lines_program_line_idx
ON caption_lines(program_id, line_no);

CREATE INDEX IF NOT EXISTS caption_lines_program_byte_offset_idx
ON caption_lines(program_id, byte_offset);

CREATE INDEX IF NOT EXISTS caption_lines_time_idx
ON caption_lines(time);

CREATE INDEX IF NOT EXISTS caption_lines_text_idx
ON caption_lines(text);

CREATE INDEX IF NOT EXISTS caption_lines_color_idx
ON caption_lines(color);

CREATE INDEX IF NOT EXISTS caption_lines_pid_idx
ON caption_lines(pid);

CREATE INDEX IF NOT EXISTS caption_lines_caption_type_idx
ON caption_lines(caption_type);

CREATE INDEX IF NOT EXISTS caption_lines_language_code_idx
ON caption_lines(language_code);

CREATE INDEX IF NOT EXISTS caption_lines_duration_ms_idx
ON caption_lines(duration_ms);

CREATE INDEX IF NOT EXISTS caption_lines_clear_screen_idx
ON caption_lines(clear_screen);

CREATE INDEX IF NOT EXISTS caption_lines_normalized_text_idx
ON caption_lines(normalized_text);

CREATE TABLE IF NOT EXISTS indexed_files (
  path TEXT PRIMARY KEY,

  program_id INTEGER REFERENCES programs(id) ON DELETE SET NULL,

  size_bytes INTEGER NOT NULL DEFAULT 0,
  mtime INTEGER NOT NULL DEFAULT 0,

  indexed_offset INTEGER NOT NULL DEFAULT 0,
  indexed_lines INTEGER NOT NULL DEFAULT 0,

  -- Indexing status of the JSONL archive file:
  --
  -- ok:        ready for incremental indexing.
  -- error:     incremental indexing cannot continue; see last_error for details.
  -- duplicate: another file has the same stream and recording_started_at;
  --            this file is not indexed; see last_error for details.
  status TEXT NOT NULL CHECK(status IN ('ok', 'error', 'duplicate')),
  last_error TEXT
);

-- `normalized_*` contains one alphanumeric character per whitespace-separated
-- token. Full FTS5 detail retains token positions, allowing phrase MATCH to
-- implement exact normalized substring search for terms of any length.
CREATE VIRTUAL TABLE caption_fts USING fts5(
  normalized_text,
  content='caption_lines',
  content_rowid='id',
  tokenize='unicode61 remove_diacritics 0',
  detail='full'
);

CREATE VIRTUAL TABLE program_fts USING fts5(
  normalized_title,
  normalized_description,
  content='programs',
  content_rowid='id',
  tokenize='unicode61 remove_diacritics 0',
  detail='full'
);

CREATE TRIGGER caption_lines_fts_ai AFTER INSERT ON caption_lines BEGIN
  INSERT INTO caption_fts(rowid, normalized_text)
  VALUES (new.id, new.normalized_text);
END;

CREATE TRIGGER caption_lines_fts_ad AFTER DELETE ON caption_lines BEGIN
  INSERT INTO caption_fts(caption_fts, rowid, normalized_text)
  VALUES ('delete', old.id, old.normalized_text);
END;

CREATE TRIGGER caption_lines_fts_au AFTER UPDATE ON caption_lines BEGIN
  INSERT INTO caption_fts(caption_fts, rowid, normalized_text)
  VALUES ('delete', old.id, old.normalized_text);
  INSERT INTO caption_fts(rowid, normalized_text)
  VALUES (new.id, new.normalized_text);
END;

CREATE TRIGGER programs_fts_ai AFTER INSERT ON programs BEGIN
  INSERT INTO program_fts(rowid, normalized_title, normalized_description)
  VALUES (new.id, new.normalized_title, new.normalized_description);
END;

CREATE TRIGGER programs_fts_ad AFTER DELETE ON programs BEGIN
  INSERT INTO program_fts(
    program_fts, rowid, normalized_title, normalized_description
  ) VALUES (
    'delete', old.id, old.normalized_title, old.normalized_description
  );
END;

CREATE TRIGGER programs_fts_au AFTER UPDATE ON programs BEGIN
  INSERT INTO program_fts(
    program_fts, rowid, normalized_title, normalized_description
  ) VALUES (
    'delete', old.id, old.normalized_title, old.normalized_description
  );
  INSERT INTO program_fts(rowid, normalized_title, normalized_description)
  VALUES (new.id, new.normalized_title, new.normalized_description);
END;

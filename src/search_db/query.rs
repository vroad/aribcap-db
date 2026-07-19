use std::collections::HashSet;

use anyhow::Result;
use sqlx::{
    AssertSqlSafe, FromRow, Sqlite, SqliteConnection, query::QueryAs, sqlite::SqliteArguments,
};

use super::text::SearchExpression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenreFilter {
    pub level1: i64,
    pub level2: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SearchFilter<'a> {
    pub stream: Option<&'a str>,
    pub from: Option<&'a str>,
    pub to: Option<&'a str>,
    pub genre: Option<GenreFilter>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub line_id: i64,
    pub line_no: i64,
    pub time: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchProgram {
    pub program_id: i64,
    pub stream: String,
    pub month: String,
    pub filename: String,
    pub recording_started_at: String,
    pub start_time: Option<String>,
    pub title: String,
    pub description: String,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct IndexedProgram {
    pub stream: String,
    pub month: String,
    pub filename: String,
    pub recording_started_at: String,
    pub size_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ProgramDetails {
    pub program_id: i64,
    pub stream: String,
    pub recording_started_at: String,
    pub start_time: Option<String>,
    pub duration_sec: Option<i64>,
    pub title: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct CaptionLine {
    pub line_id: i64,
    pub line_no: i64,
    pub time: Option<String>,
    pub text: String,
    pub duration_ms: Option<i64>,
    pub language_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptionPage {
    pub program: ProgramDetails,
    pub captions: Vec<CaptionLine>,
    pub has_more: bool,
}

#[derive(FromRow)]
struct GroupedRow {
    program_id: i64,
    stream: String,
    month: String,
    filename: String,
    recording_started_at: String,
    start_time: Option<String>,
    title: String,
    description: String,
    line_id: i64,
    line_no: i64,
    time: Option<String>,
    text: String,
}

#[derive(FromRow)]
struct ProgramRow {
    program_id: i64,
    stream: String,
    month: String,
    filename: String,
    recording_started_at: String,
    start_time: Option<String>,
    title: String,
    description: String,
}

fn group_rows(rows: Vec<GroupedRow>) -> Vec<SearchProgram> {
    let mut results: Vec<SearchProgram> = Vec::new();
    for row in rows {
        let hit = SearchHit {
            line_id: row.line_id,
            line_no: row.line_no,
            time: row.time,
            text: row.text,
        };

        if let Some(last) = results.last_mut()
            && last.program_id == row.program_id
        {
            last.hits.push(hit);
            continue;
        }

        results.push(SearchProgram {
            program_id: row.program_id,
            stream: row.stream,
            month: row.month,
            filename: row.filename,
            recording_started_at: row.recording_started_at,
            start_time: row.start_time,
            title: row.title,
            description: row.description,
            hits: vec![hit],
        });
    }
    results
}

fn fts_phrase(term: &str) -> String {
    let tokens = term
        .chars()
        .map(|ch| ch.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    format!("\"{tokens}\"")
}

fn expression_fts(expression: &SearchExpression) -> String {
    expression
        .clauses
        .iter()
        .map(|clause| {
            let query = clause
                .iter()
                .map(|term| fts_phrase(term))
                .collect::<Vec<_>>()
                .join(" AND ");
            if clause.len() > 1 {
                format!("({query})")
            } else {
                query
            }
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

const CAPTION_MATCH_CTES: &str = r#"
matched_lines AS (
    SELECT rowid AS line_id
    FROM caption_fts
    WHERE caption_fts MATCH ?
)
"#;

const PROGRAM_MATCH_CTES: &str = r#"
matched_programs AS (
    SELECT rowid AS program_id
    FROM program_fts
    WHERE program_fts MATCH ?
)
"#;

const PROGRAM_FILTER_SQL: &str = r#"
WHERE (? IS NULL OR p.stream = ?)
    AND (? IS NULL OR p.recording_started_at >= ?)
    AND (? IS NULL OR p.recording_started_at <= ?)
    AND (
        ? IS NULL
        OR EXISTS (
            SELECT 1 FROM program_genres pg
            WHERE pg.program_id = p.id
                AND pg.content_nibble_level1 = ?
                AND (? IS NULL OR pg.content_nibble_level2 = ?)
        )
    )
"#;

fn ranked_caption_sql(match_ctes: &str, program_match_join: &str) -> String {
    format!(
        r#"
        WITH {match_ctes}, ranked AS (
            SELECT
                p.id AS program_id, p.stream, p.month, p.filename,
                p.recording_started_at, p.start_time, p.title, p.description,
                cl.id AS line_id, cl.line_no, cl.time, cl.text,
                DENSE_RANK() OVER (
                    ORDER BY p.recording_started_at DESC, p.id DESC
                ) AS program_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY p.id ORDER BY cl.line_no ASC
                ) AS hit_rank
            FROM matched_lines ml
            JOIN caption_lines cl ON cl.id = ml.line_id
            JOIN programs p ON p.id = cl.program_id
            {program_match_join}
            {PROGRAM_FILTER_SQL}
        )
        SELECT
            program_id, stream, month, filename, recording_started_at,
            start_time, title, description, line_id, line_no, time, text
        FROM ranked
        WHERE program_rank <= ? AND hit_rank <= ?
        ORDER BY program_rank ASC, hit_rank ASC
        "#
    )
}

fn bind_program_filter<'q, O>(
    query: QueryAs<'q, Sqlite, O, SqliteArguments>,
    filter: &SearchFilter<'q>,
) -> QueryAs<'q, Sqlite, O, SqliteArguments> {
    let genre_level1 = filter.genre.map(|genre| genre.level1);
    let genre_level2 = filter.genre.and_then(|genre| genre.level2);
    query
        .bind(filter.stream)
        .bind(filter.stream)
        .bind(filter.from)
        .bind(filter.from)
        .bind(filter.to)
        .bind(filter.to)
        .bind(genre_level1)
        .bind(genre_level1)
        .bind(genre_level2)
        .bind(genre_level2)
}

/// Searches caption text and groups matching lines by program.
pub async fn search_captions(
    conn: &mut SqliteConnection,
    expression: &SearchExpression,
    filter: &SearchFilter<'_>,
    limit: i64,
    inner_hits: i64,
) -> Result<Vec<SearchProgram>> {
    let fts_query = expression_fts(expression);

    let sql = ranked_caption_sql(CAPTION_MATCH_CTES, "");
    let query = sqlx::query_as::<_, GroupedRow>(AssertSqlSafe(sql)).bind(fts_query);
    let rows = bind_program_filter(query, filter)
        .bind(limit)
        .bind(inner_hits)
        .fetch_all(conn)
        .await?;

    Ok(group_rows(rows))
}

/// Searches program titles and descriptions.
pub async fn search_program_metadata(
    conn: &mut SqliteConnection,
    expression: &SearchExpression,
    filter: &SearchFilter<'_>,
    limit: i64,
) -> Result<Vec<SearchProgram>> {
    let fts_query = expression_fts(expression);

    let sql = format!(
        r#"
        WITH {PROGRAM_MATCH_CTES}
        SELECT
            p.id AS program_id, p.stream, p.month, p.filename,
            p.recording_started_at, p.start_time, p.title, p.description
        FROM matched_programs m
        JOIN programs p ON p.id = m.program_id
        {PROGRAM_FILTER_SQL}
        ORDER BY p.recording_started_at DESC, p.id DESC
        LIMIT ?
        "#
    );
    let query = sqlx::query_as::<_, ProgramRow>(AssertSqlSafe(sql)).bind(fts_query);
    let rows = bind_program_filter(query, filter)
        .bind(limit)
        .fetch_all(conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| SearchProgram {
            program_id: row.program_id,
            stream: row.stream,
            month: row.month,
            filename: row.filename,
            recording_started_at: row.recording_started_at,
            start_time: row.start_time,
            title: row.title,
            description: row.description,
            hits: Vec::new(),
        })
        .collect())
}

/// Searches `line_q` in captions within programs whose title or description matches `program_q`.
pub async fn search_combined(
    conn: &mut SqliteConnection,
    program_expression: &SearchExpression,
    line_expression: &SearchExpression,
    filter: &SearchFilter<'_>,
    limit: i64,
    inner_hits: i64,
) -> Result<Vec<SearchProgram>> {
    let program_fts_query = expression_fts(program_expression);
    let line_fts_query = expression_fts(line_expression);

    let match_ctes = format!("{PROGRAM_MATCH_CTES}, {CAPTION_MATCH_CTES}");
    let sql = ranked_caption_sql(
        &match_ctes,
        "JOIN matched_programs mp ON mp.program_id = p.id",
    );
    let query = sqlx::query_as::<_, GroupedRow>(AssertSqlSafe(sql))
        .bind(program_fts_query)
        .bind(line_fts_query);
    let rows = bind_program_filter(query, filter)
        .bind(limit)
        .bind(inner_hits)
        .fetch_all(conn)
        .await?;

    Ok(group_rows(rows))
}

/// Searches for programs whose title, description, or caption text satisfies
/// `expression`.
///
/// Runs separate metadata and caption searches using their respective FTS5
/// indexes, then merges the results by program ID. Caption results are retained
/// when a program matches both searches.
pub async fn search_general(
    conn: &mut SqliteConnection,
    expression: &SearchExpression,
    filter: &SearchFilter<'_>,
    limit: i64,
    inner_hits: i64,
) -> Result<Vec<SearchProgram>> {
    let by_line = search_captions(conn, expression, filter, limit, inner_hits).await?;
    let by_program = search_program_metadata(conn, expression, filter, limit).await?;

    let mut merged: Vec<SearchProgram> = Vec::new();
    let mut seen = HashSet::new();

    // Caption hits carry more information than a metadata-only match, so
    // prefer them when a program appears in both result sets.
    for program in by_line {
        seen.insert(program.program_id);
        merged.push(program);
    }
    for program in by_program {
        if seen.insert(program.program_id) {
            merged.push(program);
        }
    }

    merged.sort_by(|a, b| {
        b.recording_started_at
            .cmp(&a.recording_started_at)
            .then_with(|| b.program_id.cmp(&a.program_id))
    });
    merged.truncate(limit.max(0) as usize);
    Ok(merged)
}

pub async fn list_indexed_programs(
    conn: &mut SqliteConnection,
    stream: &str,
    month: &str,
) -> Result<Vec<IndexedProgram>> {
    sqlx::query_as::<_, IndexedProgram>(
        "
        SELECT
            p.stream,
            p.month,
            p.filename,
            p.recording_started_at,
            COALESCE(i.size_bytes, 0) AS size_bytes
        FROM programs p
        LEFT JOIN indexed_files i ON i.path = p.path
        WHERE p.stream = ?1 AND p.month = ?2
        ORDER BY p.recording_started_at, p.filename
        ",
    )
    .bind(stream)
    .bind(month)
    .fetch_all(conn)
    .await
    .map_err(Into::into)
}

pub async fn find_indexed_program(
    conn: &mut SqliteConnection,
    stream: &str,
    recording_started_at: &str,
) -> Result<Option<IndexedProgram>> {
    sqlx::query_as::<_, IndexedProgram>(
        "
        SELECT
            p.stream,
            p.month,
            p.filename,
            p.recording_started_at,
            COALESCE(i.size_bytes, 0) AS size_bytes
        FROM programs p
        LEFT JOIN indexed_files i ON i.path = p.path
        WHERE p.stream = ?1 AND p.recording_started_at = ?2
        ",
    )
    .bind(stream)
    .bind(recording_started_at)
    .fetch_optional(conn)
    .await
    .map_err(Into::into)
}

/// Returns one program and a bounded page of its caption lines.
pub async fn get_caption_page(
    conn: &mut SqliteConnection,
    stream: &str,
    recording_started_at: &str,
    start_line: i64,
    limit: i64,
) -> Result<Option<CaptionPage>> {
    anyhow::ensure!(limit > 0, "caption page limit must be greater than zero");

    let program = sqlx::query_as::<_, ProgramDetails>(
        r#"
        SELECT
            id AS program_id,
            stream,
            recording_started_at,
            start_time,
            duration_sec,
            title,
            description
        FROM programs
        WHERE stream = ?1 AND recording_started_at = ?2
        "#,
    )
    .bind(stream)
    .bind(recording_started_at)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(program) = program else {
        return Ok(None);
    };

    let mut captions = sqlx::query_as::<_, CaptionLine>(
        r#"
        SELECT
            id AS line_id,
            line_no,
            time,
            text,
            duration_ms,
            language_code
        FROM caption_lines
        WHERE program_id = ?1 AND line_no >= ?2
        ORDER BY line_no ASC
        LIMIT ?3
        "#,
    )
    .bind(program.program_id)
    .bind(start_line)
    .bind(limit.saturating_add(1))
    .fetch_all(&mut *conn)
    .await?;
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_more = captions.len() > limit_usize;
    if has_more {
        captions.pop();
    }

    Ok(Some(CaptionPage {
        program,
        captions,
        has_more,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::db::open_and_migrate;
    use super::super::ingest::ingest_once;
    use super::super::test_support::{
        TEST_DIR_PREFIX, caption_line, eit_line_with_genre, write_file,
    };
    use super::super::text::parse_search_expression;
    use super::*;
    use crate::test_support::TestDir;

    fn expression(input: &str) -> SearchExpression {
        parse_search_expression(input).unwrap()
    }

    async fn seed_search_db() -> (crate::test_support::TestDir, SqliteConnection) {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let archive_root = data_dir.join("archive");
        let content = format!(
            "{}\n{}\n{}\n",
            eit_line_with_genre(10, "ニュース", "台風関連のニュース", 5, 2),
            caption_line(
                "台風が関東に接近しています",
                "2026-07-10T19:00:01.000+09:00"
            ),
            caption_line("地震速報です", "2026-07-10T19:00:02.000+09:00")
        );
        write_file(
            &archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.news.jsonl",
            &content,
        );

        let other_content = format!(
            "{}\n{}\n",
            eit_line_with_genre(11, "天気予報", "", 5, 3),
            caption_line("明日は晴れです", "2026-07-10T20:00:01.000+09:00")
        );
        write_file(
            &archive_root,
            "bs",
            "2026-07",
            "2026-07-10_20-00-00.weather.jsonl",
            &other_content,
        );

        let db_path = data_dir.join("search.sqlite3");
        let mut conn = open_and_migrate(&db_path).await.unwrap();
        ingest_once(&mut conn, &archive_root).await.unwrap();
        (data_dir, conn)
    }

    async fn seed_many_caption_hits(
        count: usize,
    ) -> (crate::test_support::TestDir, SqliteConnection) {
        let data_dir = TestDir::new(TEST_DIR_PREFIX);
        let archive_root = data_dir.join("archive");
        let mut content = format!("{}\n", eit_line_with_genre(10, "ニュース", "", 5, 2));
        for index in 0..count {
            content.push_str(&caption_line(
                &format!("速報{index}"),
                "2026-07-10T19:00:01.000+09:00",
            ));
            content.push('\n');
        }
        write_file(
            &archive_root,
            "nhk",
            "2026-07",
            "2026-07-10_19-00-00.news.jsonl",
            &content,
        );

        let mut conn = open_and_migrate(&data_dir.join("search.sqlite3"))
            .await
            .unwrap();
        ingest_once(&mut conn, &archive_root).await.unwrap();
        (data_dir, conn)
    }

    #[tokio::test]
    async fn search_captions_finds_two_character_hit() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("台風"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "ニュース");
        assert_eq!(results[0].hits.len(), 1);
        assert!(results[0].hits[0].text.contains("台風"));
    }

    #[tokio::test]
    async fn search_captions_requires_exact_normalized_substring() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("関東に"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].hits[0].text.contains("関東に"));

        let results = search_captions(&mut conn, &expression("関東接近"), &filter, 20, 5)
            .await
            .unwrap();
        assert!(
            results.is_empty(),
            "FTS5 phrase matching must preserve adjacency"
        );
    }

    #[tokio::test]
    async fn search_captions_ignores_non_alphanumeric_characters() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("関・東"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].hits[0].text.contains("関東"));
    }

    #[tokio::test]
    async fn search_captions_supports_mixed_length_and_expression() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("台 AND 関東に"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        let results = search_captions(&mut conn, &expression("台 AND 明日は"), &filter, 20, 5)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn search_captions_supports_or_expression() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("関東に OR 明日は"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].stream, "bs");
        assert_eq!(results[1].stream, "nhk");
    }

    #[tokio::test]
    async fn search_program_metadata_returns_metadata_match_without_hits() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_program_metadata(&mut conn, &expression("ニュース"), &filter, 20)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "ニュース");
        assert!(results[0].hits.is_empty());
    }

    #[tokio::test]
    async fn search_general_merges_metadata_and_caption_match() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_general(&mut conn, &expression("台風"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "ニュース");
        assert_eq!(results[0].hits.len(), 1);
        assert!(results[0].hits[0].text.contains("台風"));
    }

    #[tokio::test]
    async fn search_general_returns_empty_hits_for_metadata_only_match() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_general(&mut conn, &expression("天気"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "天気予報");
        assert!(results[0].hits.is_empty());
    }

    #[tokio::test]
    async fn search_combined_requires_both_program_and_line_match() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_combined(
            &mut conn,
            &expression("ニュース"),
            &expression("台風"),
            &filter,
            20,
            5,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "ニュース");

        let results = search_combined(
            &mut conn,
            &expression("天気予報"),
            &expression("台風"),
            &filter,
            20,
            5,
        )
        .await
        .unwrap();
        assert!(
            results.is_empty(),
            "program and line queries target different programs"
        );
    }

    #[tokio::test]
    async fn search_captions_limits_hits_per_program() {
        let (_data_dir, mut conn) = seed_many_caption_hits(10).await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("速報"), &filter, 20, 3)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].hits.len(), 3);
    }

    #[tokio::test]
    async fn search_captions_limits_program_count() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter::default();

        let results = search_captions(&mut conn, &expression("です"), &filter, 1, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "天気予報");
    }

    #[tokio::test]
    async fn search_captions_applies_stream_filter() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter {
            stream: Some("bs"),
            ..Default::default()
        };

        let results = search_captions(&mut conn, &expression("です"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].stream, "bs");
        assert_eq!(results[0].title, "天気予報");
    }

    #[tokio::test]
    async fn search_captions_applies_time_filter() {
        let (_data_dir, mut conn) = seed_search_db().await;
        let filter = SearchFilter {
            from: Some("2026-07-10_20-00-00"),
            ..Default::default()
        };

        let results = search_captions(&mut conn, &expression("です"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "天気予報");
    }

    #[tokio::test]
    async fn search_captions_applies_genre_filter_at_both_levels() {
        let (_data_dir, mut conn) = seed_search_db().await;

        let level1_filter = SearchFilter {
            genre: Some(GenreFilter {
                level1: 5,
                level2: None,
            }),
            ..Default::default()
        };
        let results = search_captions(&mut conn, &expression("です"), &level1_filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        let level2_filter = SearchFilter {
            genre: Some(GenreFilter {
                level1: 5,
                level2: Some(2),
            }),
            ..Default::default()
        };
        let results = search_captions(&mut conn, &expression("です"), &level2_filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "ニュース");
    }

    #[tokio::test]
    async fn search_captions_finds_one_character_hit_with_stream_filter() {
        let (_data_dir, mut conn) = seed_search_db().await;

        let filter = SearchFilter {
            stream: Some("nhk"),
            ..Default::default()
        };
        let results = search_captions(&mut conn, &expression("台"), &filter, 20, 5)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].hits[0].text.contains("台"));
    }

    #[tokio::test]
    async fn caption_page_is_ordered_and_reports_a_next_page() {
        let (_data_dir, mut conn) = seed_search_db().await;

        let first = get_caption_page(&mut conn, "nhk", "2026-07-10_19-00-00", 1, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.program.title, "ニュース");
        assert_eq!(first.captions.len(), 1);
        assert_eq!(first.captions[0].line_no, 2);
        assert!(first.has_more);

        let second = get_caption_page(&mut conn, "nhk", "2026-07-10_19-00-00", 3, 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.captions.len(), 1);
        assert_eq!(second.captions[0].line_no, 3);
        assert!(!second.has_more);
    }

    #[tokio::test]
    async fn caption_page_returns_none_for_an_unknown_program() {
        let (_data_dir, mut conn) = seed_search_db().await;

        let page = get_caption_page(&mut conn, "nhk", "2026-07-10_00-00-00", 1, 100)
            .await
            .unwrap();

        assert!(page.is_none());
    }

    #[tokio::test]
    async fn caption_page_rejects_non_positive_limits() {
        let (_data_dir, mut conn) = seed_search_db().await;

        for limit in [0, -1] {
            let error = get_caption_page(&mut conn, "nhk", "2026-07-10_19-00-00", 1, limit)
                .await
                .unwrap_err();

            assert_eq!(
                error.to_string(),
                "caption page limit must be greater than zero"
            );
        }
    }
}

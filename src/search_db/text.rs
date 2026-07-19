use chrono::{NaiveDate, NaiveDateTime};
use unicode_normalization::UnicodeNormalization as _;

const DATE_FORMAT: &str = "%Y-%m-%d";
const DATE_TIME_FORMAT: &str = "%Y-%m-%d_%H-%M-%S";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchExpression {
    /// OR-separated clauses, each containing AND-separated normalized terms.
    pub clauses: Vec<Vec<String>>,
}

pub fn normalize_search_text(input: &str) -> String {
    input
        .nfkc()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_alphanumeric())
        .collect()
}

/// Converts normalized search text into one `unicode61` token per character.
/// FTS5 phrase queries can then enforce character order and adjacency.
pub(super) fn search_index_text(input: &str) -> String {
    normalize_search_text(input)
        .chars()
        .map(|ch| ch.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Parses a simple boolean search expression. `AND` binds more tightly than
/// `OR`; parentheses and unary operators are intentionally not supported.
/// Whitespace not adjacent to an operator is removed by normalization.
pub fn parse_search_expression(input: &str) -> Result<SearchExpression, &'static str> {
    let mut clauses = Vec::new();
    let mut clause = Vec::new();
    let mut term_parts: Vec<&str> = Vec::new();
    let mut expecting_term = true;

    let flush_term = |parts: &mut Vec<&str>, clause: &mut Vec<String>| {
        if parts.is_empty() {
            return false;
        }
        let normalized = normalize_search_text(&parts.concat());
        parts.clear();
        if normalized.is_empty() {
            return false;
        }
        clause.push(normalized);
        true
    };

    for token in input.split_whitespace() {
        let operator = if token.eq_ignore_ascii_case("AND") {
            Some("AND")
        } else if token.eq_ignore_ascii_case("OR") {
            Some("OR")
        } else {
            None
        };

        match operator {
            None => {
                term_parts.push(token);
                expecting_term = false;
            }
            Some(_) if expecting_term => return Err("expected a search term"),
            Some("AND") => {
                if !flush_term(&mut term_parts, &mut clause) {
                    return Err("query must contain an alphanumeric search term");
                }
                expecting_term = true;
            }
            Some("OR") => {
                if !flush_term(&mut term_parts, &mut clause) {
                    return Err("query must contain an alphanumeric search term");
                }
                clauses.push(std::mem::take(&mut clause));
                expecting_term = true;
            }
            Some(_) => unreachable!(),
        }
    }

    if expecting_term {
        return if clauses.is_empty() && clause.is_empty() {
            Err("query must not be empty after normalization")
        } else {
            Err("expected a search term after operator")
        };
    }
    if !flush_term(&mut term_parts, &mut clause) {
        return Err("query must contain an alphanumeric search term");
    }
    clauses.push(clause);
    Ok(SearchExpression { clauses })
}

/// Expands a date-only lower bound to the start of that day.
/// Valid values that already include a time pass through unchanged.
pub fn expand_from_bound(input: &str) -> Result<String, &'static str> {
    expand_bound(input, "00-00-00").ok_or("`from` must be `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS`")
}

/// Expands a date-only upper bound to the end of that day.
/// Valid values that already include a time pass through unchanged.
pub fn expand_to_bound(input: &str) -> Result<String, &'static str> {
    expand_bound(input, "23-59-59").ok_or("`to` must be `YYYY-MM-DD` or `YYYY-MM-DD_HH-MM-SS`")
}

fn expand_bound(input: &str, date_only_time: &str) -> Option<String> {
    match input.len() {
        10 => {
            if !crate::archive::matches_digit_shape(input, 10, &[(4, b'-'), (7, b'-')]) {
                return None;
            }
            NaiveDate::parse_from_str(input, DATE_FORMAT).ok()?;
            Some(format!("{input}_{date_only_time}"))
        }
        19 => {
            if !crate::archive::matches_digit_shape(
                input,
                19,
                &[(4, b'-'), (7, b'-'), (10, b'_'), (13, b'-'), (16, b'-')],
            ) {
                return None;
            }
            NaiveDateTime::parse_from_str(input, DATE_TIME_FORMAT).ok()?;
            Some(input.to_owned())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_nfkc_lowercase_and_strips_non_alphanumeric_characters() {
        assert_eq!(normalize_search_text("ＡＢＣ"), "abc");
        assert_eq!(normalize_search_text("ABC"), "abc");
        assert_eq!(normalize_search_text("台風 が\t接近"), "台風が接近");
        assert_eq!(normalize_search_text("東京・大阪！"), "東京大阪");
    }

    #[test]
    fn builds_one_unicode61_token_per_character() {
        assert_eq!(search_index_text("Ａ・B 京"), "a b 京");
    }

    #[test]
    fn parses_boolean_expression_with_and_precedence() {
        assert_eq!(
            parse_search_expression("台 AND ニュース OR 地震 速報").unwrap(),
            SearchExpression {
                clauses: vec![
                    vec!["台".to_owned(), "ニュース".to_owned()],
                    vec!["地震速報".to_owned()]
                ]
            }
        );
        assert!(parse_search_expression("台 AND").is_err());
        assert!(parse_search_expression("OR 台").is_err());
        assert!(parse_search_expression("・・・").is_err());
    }

    #[test]
    fn expands_date_only_bounds() {
        assert_eq!(
            expand_from_bound("2026-07-01").unwrap(),
            "2026-07-01_00-00-00"
        );
        assert_eq!(
            expand_to_bound("2026-07-10").unwrap(),
            "2026-07-10_23-59-59"
        );
        assert_eq!(
            expand_from_bound("2026-07-01_12-00-00").unwrap(),
            "2026-07-01_12-00-00"
        );
        assert_eq!(
            expand_from_bound("2024-02-29").unwrap(),
            "2024-02-29_00-00-00"
        );
    }

    #[test]
    fn rejects_invalid_recording_time_bounds() {
        for input in [
            "2026-00-01",
            "2026-13-01",
            "2026-02-29",
            "2026-07-01_24-00-00",
            "2026-07-01T12:00:00",
            "+026-07-01",
            "",
            " ",
        ] {
            assert!(expand_from_bound(input).is_err(), "accepted {input:?}");
            assert!(expand_to_bound(input).is_err(), "accepted {input:?}");
        }
    }
}

use unicode_normalization::UnicodeNormalization as _;

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
/// Values that already include a time pass through unchanged.
pub fn expand_from_bound(input: &str) -> String {
    if input.len() == 10 {
        format!("{input}_00-00-00")
    } else {
        input.to_owned()
    }
}

/// Expands a date-only upper bound to the end of that day.
/// Values that already include a time pass through unchanged.
pub fn expand_to_bound(input: &str) -> String {
    if input.len() == 10 {
        format!("{input}_23-59-59")
    } else {
        input.to_owned()
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
        assert_eq!(expand_from_bound("2026-07-01"), "2026-07-01_00-00-00");
        assert_eq!(expand_to_bound("2026-07-10"), "2026-07-10_23-59-59");
        assert_eq!(
            expand_from_bound("2026-07-01_12-00-00"),
            "2026-07-01_12-00-00"
        );
    }
}

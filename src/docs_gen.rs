use std::{collections::HashSet, env, fs, path::Path};

use similar::TextDiff;

pub fn check_freshness(
    path: &Path,
    update_env: &str,
    update_command: &str,
    render_document: impl FnOnce(&str) -> Result<String, String>,
) {
    let current = fs::read_to_string(path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    });
    let expected = render_document(&current).unwrap_or_else(|error| {
        panic!("failed to render {}: {error}", path.display());
    });

    if current == expected {
        return;
    }

    if env::var_os(update_env).is_some_and(|value| value == "1") {
        fs::write(path, &expected).unwrap_or_else(|error| {
            panic!("failed to update {}: {error}", path.display());
        });
    } else {
        panic!(
            "{} is stale; run {update_command}\n\n{}",
            path.display(),
            document_diff(path, &current, &expected)
        );
    }
}

pub fn rewrite_markers<'a>(
    document: &str,
    marker_kind: &str,
    known_ids: impl IntoIterator<Item = &'a str>,
    mut render: impl FnMut(&str) -> Result<String, String>,
) -> Result<String, String> {
    let marker_open = format!("<!-- generated: {marker_kind} ");
    const MARKER_CLOSE: &str = " -->";
    let known_ids = known_ids.into_iter().collect::<HashSet<_>>();
    let mut remaining_ids = known_ids.clone();
    let mut seen_ids = HashSet::new();
    let mut output = String::with_capacity(document.len());
    let mut cursor = 0;

    while let Some(relative_start) = document[cursor..].find(&marker_open) {
        let open_marker_start = cursor + relative_start;
        output.push_str(&document[cursor..open_marker_start]);

        let id_start = open_marker_start + marker_open.len();
        let Some(relative_id_end) = document[id_start..].find(MARKER_CLOSE) else {
            let line = document[..open_marker_start].matches('\n').count() + 1;
            return Err(format!(
                "opening marker on line {line} has no matching closing marker (`{MARKER_CLOSE}`)"
            ));
        };
        let id_end = id_start + relative_id_end;
        let id = document[id_start..id_end].trim();

        if !known_ids.contains(id) {
            return Err(format!("marker references unknown {marker_kind} `{id}`"));
        }
        if !seen_ids.insert(id) {
            return Err(format!("duplicate marker for {marker_kind} `{id}`"));
        }
        remaining_ids.remove(id);

        let close_marker = format!("{marker_open}{id} end{MARKER_CLOSE}");
        let body_start = id_end + MARKER_CLOSE.len();
        let Some(end_marker_offset) = document[body_start..].find(&close_marker) else {
            return Err(format!("end marker missing for {marker_kind} `{id}`"));
        };
        let close_marker_start = body_start + end_marker_offset;

        output.push_str(&format!("{marker_open}{id}{MARKER_CLOSE}\n"));
        output.push_str(&render(id)?);
        output.push_str(&close_marker);
        cursor = close_marker_start + close_marker.len();
    }
    output.push_str(&document[cursor..]);

    if !remaining_ids.is_empty() {
        let mut missing = remaining_ids.into_iter().collect::<Vec<_>>();
        missing.sort_unstable();
        return Err(format!("markers missing for {marker_kind}s: {missing:?}"));
    }

    Ok(output)
}

pub fn escape_for_table_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\n', '\r'], " ")
}

fn document_diff(path: &Path, current: &str, expected: &str) -> String {
    let label = path.display().to_string();
    TextDiff::from_lines(current, expected)
        .unified_diff()
        .header(
            &format!("{label} (checked in)"),
            &format!("{label} (generated)"),
        )
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_markers_preserves_hand_written_text() {
        let document = "before\n<!-- generated: item one -->\nstale\n<!-- generated: item one end -->\nafter\n";
        let expected = "before\n<!-- generated: item one -->\nfresh\n<!-- generated: item one end -->\nafter\n";

        assert_eq!(
            rewrite_markers(document, "item", ["one"], |_| Ok("fresh\n".to_owned())).unwrap(),
            expected
        );
    }

    #[test]
    fn rewrite_markers_rejects_unknown_duplicate_and_missing_ids() {
        let unknown = "<!-- generated: item two -->\n<!-- generated: item two end -->";
        assert_eq!(
            rewrite_markers(unknown, "item", ["one"], |_| Ok(String::new())).unwrap_err(),
            "marker references unknown item `two`"
        );

        let duplicate = "<!-- generated: item one -->\n<!-- generated: item one end -->\n<!-- generated: item one -->\n<!-- generated: item one end -->";
        assert_eq!(
            rewrite_markers(duplicate, "item", ["one"], |_| Ok(String::new())).unwrap_err(),
            "duplicate marker for item `one`"
        );

        assert_eq!(
            rewrite_markers("no markers", "item", ["one"], |_| Ok(String::new())).unwrap_err(),
            "markers missing for items: [\"one\"]"
        );
    }
}

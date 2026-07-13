use std::io::{self, Write};

use anstyle::{Ansi256Color, AnsiColor, Color};
use colored_json::{ColorMode, ColoredFormatter, CompactFormatter};
use serde_json::Value;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const FNV1A_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV1A_PRIME: u32 = 0x0100_0193;
const ELLIPSIS: &str = "...";
const SEPARATOR: &str = " │ ";
const MIN_NORMAL_BODY_CHARS: usize = 30;

const LABEL_PALETTE: [Color; 25] = [
    Color::Ansi(AnsiColor::BrightBlue),
    Color::Ansi256(Ansi256Color(208)),
    Color::Ansi(AnsiColor::BrightMagenta),
    Color::Ansi(AnsiColor::BrightYellow),
    Color::Ansi(AnsiColor::BrightCyan),
    Color::Ansi(AnsiColor::Red),
    Color::Ansi(AnsiColor::BrightGreen),
    Color::Ansi256(Ansi256Color(75)),
    Color::Ansi(AnsiColor::Blue),
    Color::Ansi256(Ansi256Color(129)),
    Color::Ansi(AnsiColor::Yellow),
    Color::Ansi(AnsiColor::Magenta),
    Color::Ansi256(Ansi256Color(214)),
    Color::Ansi256(Ansi256Color(30)),
    Color::Ansi(AnsiColor::Cyan),
    Color::Ansi256(Ansi256Color(172)),
    Color::Ansi256(Ansi256Color(37)),
    Color::Ansi256(Ansi256Color(203)),
    Color::Ansi256(Ansi256Color(111)),
    Color::Ansi(AnsiColor::Green),
    Color::Ansi256(Ansi256Color(65)),
    Color::Ansi256(Ansi256Color(99)),
    Color::Ansi256(Ansi256Color(141)),
    Color::Ansi256(Ansi256Color(167)),
    Color::Ansi(AnsiColor::BrightRed),
];

pub fn terminal_width() -> Option<usize> {
    terminal_size::terminal_size()
        .map(|(terminal_size::Width(width), _)| usize::from(width))
        .filter(|width| *width > 0)
}

pub fn format_jsonl_line(label: &str, line: &str, color: bool) -> String {
    let body = if color {
        colorize_jsonl_line(line)
    } else {
        line.to_owned()
    };

    format!("{}{body}", format_label_prefix(label, color))
}

pub fn write_jsonl_line(
    writer: &mut impl Write,
    label: &str,
    line: &str,
    color: bool,
) -> io::Result<()> {
    writeln!(writer, "{}", format_jsonl_line(label, line, color))
}

pub fn format_normal_line(
    label: &str,
    line: &str,
    color: bool,
    max_width: Option<usize>,
) -> Option<String> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    let body = normal_body(&value)?;

    if let Some(max_width) = max_width {
        return Some(format_fitted_normal_line(label, &body, color, max_width));
    }

    Some(format!("{}{}", format_label_prefix(label, color), body))
}

pub fn write_normal_line(
    writer: &mut impl Write,
    label: &str,
    line: &str,
    color: bool,
    max_width: Option<usize>,
) -> io::Result<()> {
    let Some(rendered) = format_normal_line(label, line, color, max_width) else {
        return Ok(());
    };

    writeln!(writer, "{rendered}")
}

pub fn format_label_prefix(label: &str, color: bool) -> String {
    if color {
        let label_style = label_style(label);
        format!(
            "{}{}{}{}",
            label_style.render(),
            label,
            label_style.render_reset(),
            SEPARATOR
        )
    } else {
        format!("{label}{SEPARATOR}")
    }
}

pub fn fnv1a32(input: &str) -> u32 {
    let mut hash = FNV1A_OFFSET_BASIS;

    for byte in input.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV1A_PRIME);
    }

    hash
}

pub fn label_color_index(label: &str) -> usize {
    fnv1a32(label) as usize % LABEL_PALETTE.len()
}

pub fn colorize_jsonl_line(line: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        return line.to_owned();
    };

    ColoredFormatter::new(CompactFormatter {})
        .to_colored_json(&value, ColorMode::On)
        .unwrap_or_else(|_| line.to_owned())
}

fn label_style(label: &str) -> anstyle::Style {
    LABEL_PALETTE[label_color_index(label)].on_default().bold()
}

fn normal_body(value: &Value) -> Option<String> {
    match value.get("type")?.as_str()? {
        "caption" => caption_body(value),
        "eit" => eit_body(value),
        _ => None,
    }
}

fn caption_body(value: &Value) -> Option<String> {
    non_empty_inline_text(value.get("text")?.as_str()?)
}

fn eit_body(value: &Value) -> Option<String> {
    if value.get("section")?.as_str()? != "present" {
        return None;
    }

    let event_name = eit_event_name(value).unwrap_or_default();
    let extended_text = value
        .get("extendedText")
        .and_then(Value::as_str)
        .and_then(non_empty_inline_text)
        .unwrap_or_default();

    match (event_name.is_empty(), extended_text.is_empty()) {
        (true, true) => None,
        (true, false) => Some(format!("[番組] {extended_text}")),
        (false, true) => Some(format!("[番組] {event_name}")),
        (false, false) => Some(format!("[番組] {event_name} - {extended_text}")),
    }
}

fn eit_event_name(value: &Value) -> Option<String> {
    let short_events = value.get("shortEvents")?.as_array()?;
    let jpn_event_name = short_events
        .iter()
        .find(|event| event.get("languageCode").and_then(Value::as_str) == Some("jpn"))
        .and_then(|event| event.get("eventName"))
        .and_then(Value::as_str)
        .and_then(non_empty_inline_text);

    jpn_event_name.or_else(|| {
        short_events
            .iter()
            .filter_map(|event| event.get("eventName").and_then(Value::as_str))
            .find_map(non_empty_inline_text)
    })
}

fn non_empty_inline_text(text: &str) -> Option<String> {
    let normalized = normalize_inline_text(text);

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn normalize_inline_text(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    let mut last_was_space = false;

    for ch in text.chars() {
        // Map control characters (including ESC) to spaces to block
        // terminal escape-sequence injection.
        let ch = if ch.is_control() { ' ' } else { ch };

        if ch == ' ' {
            if !last_was_space {
                normalized.push(ch);
            }
            last_was_space = true;
        } else {
            normalized.push(ch);
            last_was_space = false;
        }
    }

    normalized.trim().to_owned()
}

fn format_fitted_normal_line(label: &str, body: &str, color: bool, max_width: usize) -> String {
    let label_width = UnicodeWidthStr::width(label);
    let separator_width = UnicodeWidthStr::width(SEPARATOR);
    let terminal_body_budget = max_width.saturating_sub(label_width + separator_width);
    let body_budget = terminal_body_budget.max(minimum_body_budget(body));
    format!(
        "{}{}",
        format_label_prefix(label, color),
        truncate_to_width(body, body_budget)
    )
}

fn truncate_to_width(input: &str, max_width: usize) -> String {
    if UnicodeWidthStr::width(input) <= max_width {
        return input.to_owned();
    }

    if max_width == 0 {
        return String::new();
    }

    let suffix_width = usize::min(UnicodeWidthStr::width(ELLIPSIS), max_width);
    let content_width = max_width - suffix_width;
    let mut truncated = String::with_capacity(input.len());
    let mut width = 0;

    for ch in input.chars() {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width + ch_width > content_width {
            break;
        }
        truncated.push(ch);
        width += ch_width;
    }

    truncated.push_str(&ELLIPSIS[..suffix_width]);
    truncated
}

fn minimum_body_budget(body: &str) -> usize {
    if body.chars().count() <= MIN_NORMAL_BODY_CHARS {
        return UnicodeWidthStr::width(body);
    }

    let content_width = body
        .chars()
        .take(MIN_NORMAL_BODY_CHARS)
        .map(|ch| UnicodeWidthChar::width(ch).unwrap_or(0))
        .sum::<usize>();
    content_width + UnicodeWidthStr::width(ELLIPSIS)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    #[test]
    fn renders_label_and_original_line() {
        assert_eq!(
            format_jsonl_line("NHK", r#"{"text":"hello"}"#, false),
            r#"NHK │ {"text":"hello"}"#
        );
    }

    #[test]
    fn disabled_color_rendering_has_no_ansi_escape() {
        let rendered = format_jsonl_line("NHK", r#"{"text":"hello"}"#, false);

        assert!(!rendered.contains("\u{1b}["));
    }

    #[test]
    fn invalid_json_is_rendered_without_parsing() {
        assert_eq!(
            format_jsonl_line("NHK", "{invalid-json", false),
            "NHK │ {invalid-json"
        );
    }

    #[test]
    fn colorizes_json_tokens_by_default() {
        let rendered = format_jsonl_line(
            "NHK",
            r#"{"text":"hello","count":12,"ok":true,"empty":null}"#,
            true,
        );

        assert!(rendered.contains("\u{1b}["));
        assert_eq!(
            strip_ansi(&rendered),
            r#"NHK │ {"text":"hello","count":12,"ok":true,"empty":null}"#
        );
    }

    #[test]
    fn colorized_json_stays_single_line_by_compacting() {
        let rendered = format_jsonl_line("NHK", r#"{ "text" : "hello" }"#, true);

        assert_eq!(strip_ansi(&rendered), r#"NHK │ {"text":"hello"}"#);
    }

    #[test]
    fn colorized_json_handles_escaped_strings() {
        let rendered = format_jsonl_line(
            "NHK",
            r#"{"text":"hello \"world\"","path":"C:\\tmp"}"#,
            true,
        );

        assert_eq!(
            strip_ansi(&rendered),
            r#"NHK │ {"text":"hello \"world\"","path":"C:\\tmp"}"#
        );
    }

    #[test]
    fn colorized_invalid_json_keeps_original_text() {
        let rendered = format_jsonl_line("NHK", r#"{"text":"unterminated"#, true);

        assert_eq!(strip_ansi(&rendered), r#"NHK │ {"text":"unterminated"#);
    }

    #[test]
    fn fnv1a_hash_is_stable() {
        assert_eq!(fnv1a32("NHK"), 0x6ee1_1bb0);
    }

    #[test]
    fn same_label_gets_same_color_index() {
        assert_eq!(label_color_index("NHK"), label_color_index("NHK"));
    }

    #[test]
    fn configured_station_labels_get_distinct_color_indexes() {
        let labels = [
            "ＡＢＣテレビ",
            "ＫＢＳ京都",
            "関西テレビ",
            "ＭＢＳ毎日放送",
            "ＮＨＫ総合",
            "ＮＨＫＥテレ",
            "サンテレビ",
            "テレビ大阪",
            "読売テレビ",
        ];
        let indexes = labels
            .into_iter()
            .map(label_color_index)
            .collect::<BTreeSet<_>>();

        assert_eq!(indexes.len(), labels.len());
    }

    #[test]
    fn normal_and_jsonl_share_label_prefix() {
        let line = r#"{"type":"caption","text":"hello","color":"0xffffffff","ruby":["x"]}"#;

        let normal = format_normal_line("NHK", line, false, None).unwrap();
        let jsonl = format_jsonl_line("NHK", line, false);

        assert!(normal.starts_with(&format_label_prefix("NHK", false)));
        assert!(jsonl.starts_with(&format_label_prefix("NHK", false)));
    }

    #[test]
    fn normal_caption_renders_text_only() {
        let rendered = format_normal_line(
            "NHK",
            r#"{"type":"caption","color":"0xffff00ff","text":"caption","ruby":["ruby"]}"#,
            false,
            None,
        );

        assert_eq!(rendered.as_deref(), Some("NHK │ caption"));
    }

    #[test]
    fn normal_caption_replaces_newlines_with_spaces() {
        let rendered = format_normal_line(
            "NHK",
            r#"{"type":"caption","text":"hello\nworld\r\nagain"}"#,
            false,
            None,
        );

        assert_eq!(rendered.as_deref(), Some("NHK │ hello world again"));
    }

    #[test]
    fn normal_caption_neutralizes_control_characters() {
        let rendered = format_normal_line(
            "NHK",
            r#"{"type":"caption","text":"hello\u001b[2Jworld\u0007\u009bend"}"#,
            false,
            None,
        );

        assert_eq!(rendered.as_deref(), Some("NHK │ hello [2Jworld end"));
    }

    #[test]
    fn normal_colors_only_the_label() {
        let rendered =
            format_normal_line("NHK", r#"{"type":"caption","text":"caption"}"#, true, None)
                .unwrap();

        assert!(rendered.contains("\u{1b}["));
        assert_eq!(strip_ansi(&rendered), "NHK │ caption");
        assert_eq!(rendered.split_once(SEPARATOR).unwrap().1, "caption");
    }

    #[test]
    fn normal_skips_empty_caption() {
        assert_eq!(
            format_normal_line("NHK", r#"{"type":"caption","text":""}"#, false, None),
            None
        );
    }

    #[test]
    fn normal_skips_diagnostic_following_eit_and_invalid_json() {
        assert_eq!(
            format_normal_line(
                "NHK",
                r#"{"type":"diagnostic","kind":"captionDecodeError","pid":291}"#,
                false,
                None
            ),
            None
        );
        assert_eq!(
            format_normal_line(
                "NHK",
                r#"{"type":"eit","section":"following","shortEvents":[{"languageCode":"jpn","eventName":"next"}],"extendedText":"later"}"#,
                false,
                None
            ),
            None
        );
        assert_eq!(
            format_normal_line(
                "NHK",
                r#"{"type":"caption","text":"unterminated"#,
                false,
                None
            ),
            None
        );
    }

    #[test]
    fn normal_eit_present_renders_event_name_and_extended_text() {
        let rendered = format_normal_line(
            "NHK",
            r#"{"type":"eit","section":"present","shortEvents":[{"languageCode":"eng","eventName":"English"},{"languageCode":"jpn","eventName":"番組名","text":"unused"}],"extendedText":"詳しい説明"}"#,
            false,
            None,
        );

        assert_eq!(
            rendered.as_deref(),
            Some("NHK │ [番組] 番組名 - 詳しい説明")
        );
    }

    #[test]
    fn normal_eit_present_falls_back_to_first_event_name() {
        let rendered = format_normal_line(
            "NHK",
            r#"{"type":"eit","section":"present","shortEvents":[{"languageCode":"eng","eventName":"English"}],"extendedText":"details"}"#,
            false,
            None,
        );

        assert_eq!(rendered.as_deref(), Some("NHK │ [番組] English - details"));
    }

    #[test]
    fn normal_truncates_body_to_unicode_display_width() {
        let rendered = format_normal_line(
            "NHK",
            r#"{"type":"caption","text":"abcdefghijklmnopqrstuvwxyz0123456789"}"#,
            false,
            Some(40),
        )
        .unwrap();

        assert!(UnicodeWidthStr::width(rendered.as_str()) <= 40);
        assert_eq!(rendered, "NHK │ abcdefghijklmnopqrstuvwxyz01234...");
    }

    #[test]
    fn normal_never_truncates_label_prefix_and_keeps_minimum_body_chars() {
        let rendered = format_normal_line(
            "ＮＨＫ総合",
            r#"{"type":"caption","text":"abcdefghijklmnopqrstuvwxyz0123456789"}"#,
            false,
            Some(10),
        )
        .unwrap();

        assert_eq!(rendered, "ＮＨＫ総合 │ abcdefghijklmnopqrstuvwxyz0123...");
    }

    fn strip_ansi(input: &str) -> String {
        let mut stripped = String::with_capacity(input.len());
        let bytes = input.as_bytes();
        let mut index = 0;

        while index < bytes.len() {
            if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'[') {
                index += 2;
                while index < bytes.len() && !(0x40..=0x7e).contains(&bytes[index]) {
                    index += 1;
                }
                index += usize::from(index < bytes.len());
            } else {
                let ch = input[index..].chars().next().unwrap();
                stripped.push(ch);
                index += ch.len_utf8();
            }
        }

        stripped
    }
}

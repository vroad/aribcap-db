use std::path::{Path, PathBuf};

use serde_json::Value;
use walkdir::WalkDir;

pub(super) struct EitPresent {
    pub(super) start_time: Option<String>,
    pub(super) duration_sec: Option<i64>,
    pub(super) title: String,
    pub(super) description: String,
    pub(super) version: Option<i64>,
    pub(super) service_id: Option<i64>,
    pub(super) transport_stream_id: Option<i64>,
    pub(super) original_network_id: Option<i64>,
    pub(super) event_id: Option<i64>,
    pub(super) genres: Vec<Genre>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Genre {
    pub(super) content_nibble_level1: i64,
    pub(super) content_nibble_level2: i64,
    pub(super) user_nibble1: i64,
    pub(super) user_nibble2: i64,
}

pub(super) fn eit_present_from_value(value: &Value) -> Option<EitPresent> {
    if value.get("type")?.as_str()? != "eit" || value.get("section")?.as_str()? != "present" {
        return None;
    }

    Some(EitPresent {
        start_time: value
            .get("startTime")
            .and_then(Value::as_str)
            .map(str::to_owned),
        duration_sec: value.get("durationSec").and_then(Value::as_i64),
        title: crate::archive::event_name_from_eit(value).unwrap_or_default(),
        description: description_from_eit(value),
        version: value.get("version").and_then(Value::as_i64),
        service_id: value.get("serviceId").and_then(Value::as_i64),
        transport_stream_id: value.get("transportStreamId").and_then(Value::as_i64),
        original_network_id: value.get("originalNetworkId").and_then(Value::as_i64),
        event_id: value.get("eventId").and_then(Value::as_i64),
        genres: genres_from_eit(value),
    })
}

fn genres_from_eit(value: &Value) -> Vec<Genre> {
    value
        .get("genres")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|genre| {
            Some(Genre {
                content_nibble_level1: genre.get("contentNibbleLevel1")?.as_i64()?,
                content_nibble_level2: genre.get("contentNibbleLevel2")?.as_i64()?,
                user_nibble1: genre
                    .get("userNibble1")
                    .and_then(Value::as_i64)
                    .unwrap_or(15),
                user_nibble2: genre
                    .get("userNibble2")
                    .and_then(Value::as_i64)
                    .unwrap_or(15),
            })
        })
        .collect()
}

fn description_from_eit(value: &Value) -> String {
    fn text_field(event: &Value) -> Option<&str> {
        event
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
    }

    let short_text = value
        .get("shortEvents")
        .and_then(Value::as_array)
        .and_then(|short_events| {
            short_events
                .iter()
                .find(|event| event.get("languageCode").and_then(Value::as_str) == Some("jpn"))
                .and_then(text_field)
                .or_else(|| short_events.iter().find_map(text_field))
        });
    let extended_text = value
        .get("extendedText")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty());

    // Short-event text and extendedText are independent EIT fields, not a
    // summary/detail pair with one superseding the other; keep both when
    // they differ so search covers whichever the query happens to match.
    match (short_text, extended_text) {
        (Some(short), Some(extended)) if short != extended => format!("{short}\n{extended}"),
        (Some(short), _) => short.to_owned(),
        (None, Some(extended)) => extended.to_owned(),
        (None, None) => String::new(),
    }
}

pub(super) struct CaptionRecord {
    pub(super) time: Option<String>,
    pub(super) text: String,
    pub(super) color: Option<String>,
    pub(super) pid: Option<i64>,
    pub(super) caption_type: Option<String>,
    pub(super) language_code: Option<String>,
    pub(super) duration_ms: Option<i64>,
    pub(super) clear_screen: Option<bool>,
}

pub(super) fn caption_from_value(value: &Value) -> Option<CaptionRecord> {
    if value.get("type")?.as_str()? != "caption" {
        return None;
    }

    Some(CaptionRecord {
        time: value.get("time").and_then(Value::as_str).map(str::to_owned),
        text: value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        color: value
            .get("color")
            .and_then(Value::as_str)
            .map(str::to_owned),
        pid: value.get("pid").and_then(Value::as_i64),
        caption_type: value
            .get("captionType")
            .and_then(Value::as_str)
            .map(str::to_owned),
        language_code: value
            .get("languageCode")
            .and_then(Value::as_str)
            .map(str::to_owned),
        duration_ms: value.get("durationMs").and_then(Value::as_i64),
        clear_screen: value.get("clearScreen").and_then(Value::as_bool),
    })
}

/// Extracts the stream, month, and filename from a valid archive path.
/// Returns `None` unless the path has exactly three components under `records_root`.
pub(super) fn stream_month_filename(
    records_root: &Path,
    path: &Path,
) -> Option<(String, String, String)> {
    let rel = path.strip_prefix(records_root).ok()?;
    let mut components = rel.components();
    let stream = components.next()?.as_os_str().to_str()?.to_owned();
    let month = components.next()?.as_os_str().to_str()?.to_owned();
    let filename = components.next()?.as_os_str().to_str()?.to_owned();

    if components.next().is_some() {
        return None;
    }

    Some((stream, month, filename))
}

pub(super) fn scan_jsonl_files(records_root: &Path) -> Vec<PathBuf> {
    if !records_root.exists() {
        return Vec::new();
    }

    WalkDir::new(records_root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
        })
        .map(|entry| entry.path().to_path_buf())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eit_extraction_prefers_jpn_and_falls_back() {
        let value: Value = serde_json::from_str(
            r#"{"type":"eit","startTime":"2020-01-01T00:00:00.000+09:00","durationSec":1800,"shortEvents":[{"languageCode":"eng","eventName":"English","text":"english text"},{"languageCode":"jpn","eventName":"日本語","text":"にほんご"}],"extendedText":"詳細","version":7,"section":"present","genres":[{"contentNibbleLevel1":5,"contentNibbleLevel2":2,"userNibble1":15,"userNibble2":15}]}"#,
        )
        .unwrap();
        let eit = eit_present_from_value(&value).unwrap();
        assert_eq!(eit.title, "日本語");
        assert_eq!(eit.description, "にほんご\n詳細");
        assert_eq!(eit.duration_sec, Some(1800));
        assert_eq!(eit.version, Some(7));
        assert_eq!(
            eit.genres,
            vec![Genre {
                content_nibble_level1: 5,
                content_nibble_level2: 2,
                user_nibble1: 15,
                user_nibble2: 15,
            }]
        );

        let value: Value = serde_json::from_str(
            r#"{"type":"eit","section":"present","shortEvents":[{"languageCode":"eng","eventName":"English"}]}"#,
        )
        .unwrap();
        let eit = eit_present_from_value(&value).unwrap();
        assert_eq!(eit.title, "English");
    }

    #[test]
    fn description_concatenates_short_and_extended_text_when_they_differ() {
        let value: Value = serde_json::from_str(
            r#"{"type":"eit","section":"present","shortEvents":[{"languageCode":"jpn","eventName":"t","text":"短い説明"}],"extendedText":"詳しい説明"}"#,
        )
        .unwrap();
        assert_eq!(description_from_eit(&value), "短い説明\n詳しい説明");

        let value: Value = serde_json::from_str(
            r#"{"type":"eit","section":"present","shortEvents":[{"languageCode":"jpn","eventName":"t","text":"同じ説明"}],"extendedText":"同じ説明"}"#,
        )
        .unwrap();
        assert_eq!(
            description_from_eit(&value),
            "同じ説明",
            "identical short/extended text must not be duplicated"
        );

        let value: Value = serde_json::from_str(
            r#"{"type":"eit","section":"present","shortEvents":[{"languageCode":"jpn","eventName":"t","text":""}]}"#,
        )
        .unwrap();
        assert_eq!(description_from_eit(&value), "");
    }

    #[test]
    fn caption_extraction_reads_expected_fields() {
        let value: Value = serde_json::from_str(
            r#"{"type":"caption","time":"2020-01-01T00:00:01.000+09:00","text":"こんにちは","ruby":["こんにちは"],"color":"0xffffffff","pid":304,"captionType":"caption","languageCode":"jpn","durationMs":500,"clearScreen":true}"#,
        )
        .unwrap();
        let caption = caption_from_value(&value).unwrap();
        assert_eq!(caption.text, "こんにちは");
        assert_eq!(caption.duration_ms, Some(500));
        assert_eq!(caption.caption_type.as_deref(), Some("caption"));
        assert_eq!(caption.color.as_deref(), Some("0xffffffff"));
        assert_eq!(caption.pid, Some(304));
        assert_eq!(caption.clear_screen, Some(true));
    }
}

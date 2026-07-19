use std::{
    fs,
    path::{Path, PathBuf},
};

pub(super) const TEST_DIR_PREFIX: &str = "aribcap-db-search-db-test-";

pub(super) fn write_file(
    archive_root: &Path,
    stream: &str,
    month: &str,
    filename: &str,
    content: &str,
) -> PathBuf {
    let dir = archive_root.join(stream).join(month);
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(filename);
    fs::write(&path, content).unwrap();
    path
}

pub(super) fn eit_line(event_id: u64, title: &str, extended_text: &str) -> String {
    format!(
        r#"{{"type":"eit","startTime":"2026-07-10T19:00:00.000+09:00","durationSec":1800,"shortEvents":[{{"languageCode":"jpn","eventName":"{title}","text":""}}],"extendedText":"{extended_text}","version":1,"serviceId":1,"transportStreamId":2,"originalNetworkId":3,"eventId":{event_id},"section":"present"}}"#
    )
}

pub(super) fn eit_line_with_genre(
    event_id: u64,
    title: &str,
    extended_text: &str,
    level1: u8,
    level2: u8,
) -> String {
    format!(
        r#"{{"type":"eit","startTime":"2026-07-10T19:00:00.000+09:00","durationSec":1800,"shortEvents":[{{"languageCode":"jpn","eventName":"{title}","text":""}}],"extendedText":"{extended_text}","version":1,"serviceId":1,"transportStreamId":2,"originalNetworkId":3,"eventId":{event_id},"section":"present","genres":[{{"contentNibbleLevel1":{level1},"contentNibbleLevel2":{level2},"userNibble1":15,"userNibble2":15}}]}}"#
    )
}

pub(super) fn caption_line(text: &str, line_time: &str) -> String {
    format!(
        r#"{{"type":"caption","time":"{line_time}","text":"{text}","ruby":[],"color":"0xffffffff","pid":304,"captionType":"caption","languageCode":"jpn","durationMs":500,"clearScreen":true}}"#
    )
}

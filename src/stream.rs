use std::future::Future;

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;

/// Upper bound on bytes buffered while waiting for a newline.
const MAX_LINE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Default)]
pub struct LineBuffer {
    pending: Vec<u8>,
}

impl LineBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<String>> {
        self.pending.extend_from_slice(chunk);

        let mut lines = Vec::new();
        let mut line_start = 0;

        for (index, byte) in self.pending.iter().enumerate() {
            if *byte == b'\n' {
                let mut line = self.pending[line_start..index].to_vec();
                if line.ends_with(b"\r") {
                    line.pop();
                }
                lines.push(String::from_utf8_lossy(&line).into_owned());
                line_start = index + 1;
            }
        }

        if line_start > 0 {
            self.pending.drain(..line_start);
        }

        if self.pending.len() > MAX_LINE_BYTES {
            bail!("line exceeds {MAX_LINE_BYTES} bytes without a newline");
        }

        Ok(lines)
    }
}

pub async fn tail_until_ctrl_c<F, Fut>(url: &str, on_line: F) -> Result<()>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let client = reqwest::Client::new();

    tokio::select! {
        result = tail_once(&client, url, on_line) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for Ctrl-C")?;
            Ok(())
        }
    }
}

pub async fn tail_once<F, Fut>(client: &reqwest::Client, url: &str, mut on_line: F) -> Result<()>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to GET {url}"))?;
    let status = response.status();

    if !status.is_success() {
        bail!("GET {url} returned HTTP {status}");
    }

    let mut body = response.bytes_stream();
    let mut buffer = LineBuffer::new();

    while let Some(chunk) = body.next().await {
        let chunk = chunk.context("failed to read response chunk")?;
        for line in buffer.push_chunk(&chunk)? {
            on_line(line).await?;
        }
    }

    bail!("stream ended")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restores_lines_split_across_chunks() {
        let mut buffer = LineBuffer::new();

        assert!(buffer.push_chunk(br#"{"text":"hel"#).unwrap().is_empty());
        assert_eq!(
            buffer
                .push_chunk(
                    br#"lo"}
{"text":"wor"#
                )
                .unwrap(),
            vec![r#"{"text":"hello"}"#]
        );
        assert_eq!(
            buffer
                .push_chunk(
                    br#"ld"}
"#
                )
                .unwrap(),
            vec![r#"{"text":"world"}"#]
        );
    }

    #[test]
    fn keeps_partial_line_until_next_chunk() {
        let mut buffer = LineBuffer::new();

        assert!(buffer.push_chunk(br#"{"a":1}"#).unwrap().is_empty());
        assert_eq!(buffer.push_chunk(b"\n").unwrap(), vec![r#"{"a":1}"#]);
    }

    #[test]
    fn strips_carriage_return_before_newline() {
        let mut buffer = LineBuffer::new();

        assert_eq!(
            buffer.push_chunk(b"{\"a\":1}\r\n").unwrap(),
            vec![r#"{"a":1}"#]
        );
    }

    #[test]
    fn rejects_line_longer_than_max_line_bytes() {
        let mut buffer = LineBuffer::new();

        assert!(
            buffer
                .push_chunk(&vec![b'a'; MAX_LINE_BYTES])
                .unwrap()
                .is_empty()
        );
        assert!(buffer.push_chunk(b"a").is_err());
    }

    #[test]
    fn accepts_long_input_when_newlines_keep_lines_short() {
        let mut buffer = LineBuffer::new();

        let chunk = b"{\"a\":1}\n".repeat(MAX_LINE_BYTES / 4);
        let lines = buffer.push_chunk(&chunk).unwrap();
        assert_eq!(lines.len(), MAX_LINE_BYTES / 4);
    }
}

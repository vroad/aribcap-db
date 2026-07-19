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
    #[cfg(unix)]
    const TEST_DIR_PREFIX: &str = "aribcap-db-stream-test-";

    #[cfg(unix)]
    use std::sync::{Arc, Mutex};

    #[cfg(unix)]
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    #[cfg(unix)]
    #[tokio::test]
    async fn tails_http_stream_over_configured_unix_socket() {
        use crate::{config::Config, test_support::TestDir};

        let temp_dir = TestDir::new(TEST_DIR_PREFIX);
        let socket_path = temp_dir.join("stream.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let bytes_read = connection.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..bytes_read]);
            assert!(request.starts_with("GET /captions HTTP/1.1\r\n"));

            let body = b"{\"text\":\"hello\"}\n";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            connection.write_all(headers.as_bytes()).await.unwrap();
            connection.write_all(body).await.unwrap();
        });

        let config: Config = toml::from_str(&format!(
            r#"
[upstream]
url_template = "http://localhost/captions"
unix_socket = "{}"

[streams.test]
"#,
            socket_path.display()
        ))
        .unwrap();
        let client = config.build_http_client().unwrap();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let received = lines.clone();

        let error = tail_once(&client, "http://localhost/captions", move |line| {
            received.lock().unwrap().push(line);
            std::future::ready(Ok(()))
        })
        .await
        .unwrap_err();

        server.await.unwrap();
        assert_eq!(error.to_string(), "stream ended");
        assert_eq!(*lines.lock().unwrap(), vec![r#"{"text":"hello"}"#]);
    }
}

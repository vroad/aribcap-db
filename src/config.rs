use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub url_template: String,

    #[serde(default)]
    pub unix_socket: Option<PathBuf>,

    pub streams: BTreeMap<String, StreamConfig>,

    #[serde(default)]
    pub serve: Option<ServeConfig>,
}

#[derive(Debug, Deserialize)]
pub struct StreamConfig {
    pub label: Option<String>,

    #[serde(default)]
    pub vars: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServeConfig {
    pub data_dir: Option<PathBuf>,
    pub listen: Option<String>,
    pub retention: Option<String>,
    #[serde(default)]
    pub mcp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStream {
    pub name: String,
    pub label: String,
    pub url: String,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        toml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))
    }

    pub fn resolve_stream(&self, target: &str) -> Result<ResolvedStream> {
        let stream = self
            .streams
            .get(target)
            .with_context(|| format!("stream target '{target}' is not defined"))?;

        self.resolve_stream_config(target, stream)
    }

    fn resolve_stream_config(&self, target: &str, stream: &StreamConfig) -> Result<ResolvedStream> {
        if target.is_empty() {
            bail!("stream name must not be empty");
        }

        let sanitized_target = sanitize_filename::sanitize(target);
        if sanitized_target != target {
            bail!(
                "stream name '{target}' is not usable as an archive path component (sanitized form: '{sanitized_target}')"
            );
        }

        let label = stream.label.as_deref().unwrap_or(target).to_owned();
        let url = render_url_template(&self.url_template, &stream.vars)
            .with_context(|| format!("failed to resolve stream '{target}'"))?;

        Ok(ResolvedStream {
            name: target.to_owned(),
            label,
            url,
        })
    }

    pub fn resolve_streams(&self, targets: &[String]) -> Result<Vec<ResolvedStream>> {
        targets
            .iter()
            .map(|target| self.resolve_stream(target))
            .collect()
    }

    pub fn resolve_all_streams(&self) -> Result<Vec<ResolvedStream>> {
        if self.streams.is_empty() {
            bail!("config defines no streams");
        }

        self.streams
            .iter()
            .map(|(target, stream)| self.resolve_stream_config(target, stream))
            .collect()
    }

    pub fn build_http_client(&self) -> Result<reqwest::Client> {
        let builder = reqwest::Client::builder();

        #[cfg(unix)]
        let builder = match &self.unix_socket {
            Some(path) => builder.unix_socket(path.as_path()),
            None => builder,
        };

        #[cfg(not(unix))]
        if let Some(path) = &self.unix_socket {
            bail!(
                "unix_socket is not supported on this platform: {}",
                path.display()
            );
        }

        builder.build().context("failed to build HTTP client")
    }
}

pub fn render_url_template(template: &str, vars: &BTreeMap<String, String>) -> Result<String> {
    let engine = upon::Engine::new();
    let template = engine.compile(template)?;
    Ok(template.render(&engine, vars).to_string()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_url_template_placeholders() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://localhost:40772/api/timeshift/{{ channel }}/tuner-stream?post-filters[]=aribcap-dump"

[streams.nhk]
label = "NHK"
vars.channel = "nhk"
"#,
        )
        .unwrap();

        let stream = config.resolve_stream("nhk").unwrap();

        assert_eq!(stream.name, "nhk");
        assert_eq!(stream.label, "NHK");
        assert_eq!(
            stream.url,
            "http://localhost:40772/api/timeshift/nhk/tuner-stream?post-filters[]=aribcap-dump"
        );
    }

    #[test]
    fn parses_unix_socket() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://localhost/api/{{ channel }}"
unix_socket = "/run/mirakc/mirakc.sock"

[streams.nhk]
vars.channel = "nhk"
"#,
        )
        .unwrap();

        assert_eq!(
            config.unix_socket,
            Some(PathBuf::from("/run/mirakc/mirakc.sock"))
        );
    }

    #[test]
    fn unix_socket_defaults_to_none() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams.nhk]
vars.channel = "nhk"
"#,
        )
        .unwrap();

        assert_eq!(config.unix_socket, None);
        config.build_http_client().unwrap();
    }

    #[test]
    fn mcp_defaults_to_false_and_can_be_enabled() {
        let disabled: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams.nhk]
vars.channel = "nhk"

[serve]
listen = "127.0.0.1:40773"
"#,
        )
        .unwrap();
        assert!(!disabled.serve.unwrap().mcp);

        let enabled: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams.nhk]
vars.channel = "nhk"

[serve]
mcp = true
"#,
        )
        .unwrap();
        assert!(enabled.serve.unwrap().mcp);
    }

    #[test]
    fn rejects_missing_url_template_placeholders() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}/{{ missing }}"

[streams.nhk]
vars.channel = "nhk"
"#,
        )
        .unwrap();

        let error = config.resolve_stream("nhk").unwrap_err();

        assert!(error.to_string().contains("failed to resolve stream 'nhk'"));
        let cause = format!("{:#}", error.root_cause());
        assert!(cause.contains("missing"), "{error:#}");
        assert!(cause.contains("not found in this scope"), "{error:#}");
    }

    #[test]
    fn falls_back_to_target_name_for_label() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams.nhk]
vars.channel = "nhk"
"#,
        )
        .unwrap();

        let stream = config.resolve_stream("nhk").unwrap();

        assert_eq!(stream.label, "nhk");
    }

    #[test]
    fn resolves_all_streams_in_stable_order() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams.nhk_e]
label = "NHK-E"
vars.channel = "nhk-e"

[streams.nhk]
label = "NHK"
vars.channel = "nhk"
"#,
        )
        .unwrap();

        let streams = config.resolve_all_streams().unwrap();

        assert_eq!(
            streams
                .into_iter()
                .map(|stream| stream.name)
                .collect::<Vec<_>>(),
            vec!["nhk", "nhk_e"]
        );
    }

    #[test]
    fn rejects_stream_names_changed_by_sanitization() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams."bs:1"]
vars.channel = "bs1"
"#,
        )
        .unwrap();

        let error = config.resolve_all_streams().unwrap_err();

        assert_eq!(
            error.to_string(),
            "stream name 'bs:1' is not usable as an archive path component (sanitized form: 'bs1')"
        );
    }

    #[test]
    fn rejects_empty_stream_name() {
        let config: Config = toml::from_str(
            r#"
url_template = "http://example.test/{{ channel }}"

[streams.""]
vars.channel = "empty"
"#,
        )
        .unwrap();

        let error = config.resolve_all_streams().unwrap_err();

        assert_eq!(error.to_string(), "stream name must not be empty");
    }
}

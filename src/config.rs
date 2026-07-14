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
    pub retention: Option<String>,
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
}

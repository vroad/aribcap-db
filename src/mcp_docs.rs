use std::{collections::HashSet, env, fs, path::Path};

use rmcp::model::Tool;
use serde_json::{Map, Value};
use similar::TextDiff;

use crate::mcp::AribcapMcp;

const DOCS_PATH: &str = "docs/mcp.md";
const GENERATED_TOOL_MARKER_OPEN: &str = "<!-- generated: tool ";
const GENERATED_TOOL_MARKER_CLOSE: &str = " -->";

#[test]
fn mcp_docs_are_fresh() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(DOCS_PATH);
    let current = fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    });
    let tools = AribcapMcp::tools();
    let expected = rewrite_markers(&current, &tools).unwrap_or_else(|error| {
        panic!("failed to render {}: {error}", path.display());
    });

    if current != expected {
        if env::var_os("UPDATE_MCP_DOCS").is_some_and(|value| value == "1") {
            fs::write(&path, &expected).unwrap_or_else(|error| {
                panic!("failed to update {}: {error}", path.display());
            });
        } else {
            panic!(
                "{} is stale; run UPDATE_MCP_DOCS=1 cargo test mcp_docs\n\n{}",
                path.display(),
                document_diff(&current, &expected)
            );
        }
    }
}

fn rewrite_markers(document: &str, tools: &[Tool]) -> Result<String, String> {
    let mut remaining_tool_names = tools
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect::<HashSet<_>>();
    let mut output = String::with_capacity(document.len());
    let mut cursor = 0;

    while let Some(relative_start) = document[cursor..].find(GENERATED_TOOL_MARKER_OPEN) {
        // `output.push_str` here copies the hand-written text between the
        // previous close marker (or the document start, on the first match)
        // and this open marker, unchanged.
        let open_marker_start = cursor + relative_start;
        output.push_str(&document[cursor..open_marker_start]);

        let tool_name_start = open_marker_start + GENERATED_TOOL_MARKER_OPEN.len();
        let Some(relative_name_end) = document[tool_name_start..].find(GENERATED_TOOL_MARKER_CLOSE)
        else {
            let line = document[..open_marker_start].matches('\n').count() + 1;
            return Err(format!(
                "opening marker on line {line} has no matching closing marker (`{GENERATED_TOOL_MARKER_CLOSE}`)"
            ));
        };
        let tool_name_end = tool_name_start + relative_name_end;

        let tool_name = document[tool_name_start..tool_name_end].trim();
        let Some(tool) = tools.iter().find(|tool| tool.name == tool_name) else {
            return Err(format!("marker references unknown tool `{tool_name}`"));
        };
        if !remaining_tool_names.remove(tool_name) {
            return Err(format!("duplicate marker for tool `{tool_name}`"));
        }

        let close_marker =
            format!("{GENERATED_TOOL_MARKER_OPEN}{tool_name} end{GENERATED_TOOL_MARKER_CLOSE}");
        let body_start = tool_name_end + GENERATED_TOOL_MARKER_CLOSE.len();
        let Some(end_marker_offset) = document[body_start..].find(&close_marker) else {
            return Err(format!("end marker missing for tool `{tool_name}`"));
        };
        let close_marker_start = body_start + end_marker_offset;

        output.push_str(&render_tool(tool));
        output.push_str(&close_marker);

        cursor = close_marker_start + close_marker.len();
    }
    output.push_str(&document[cursor..]);

    if !remaining_tool_names.is_empty() {
        let mut missing = remaining_tool_names.into_iter().collect::<Vec<_>>();
        missing.sort_unstable();
        return Err(format!("markers missing for tools: {missing:?}"));
    }

    Ok(output)
}

fn render_tool(tool: &Tool) -> String {
    let mut rendered = format!(
        "<!-- generated: tool {} -->\n### `{}`\n\n",
        tool.name, tool.name
    );
    if let Some(description) = &tool.description {
        rendered.push_str(description);
        rendered.push_str("\n\n");
    }

    let schema = tool.input_schema.as_ref();
    let properties = match schema.get("properties") {
        Some(Value::Object(properties)) => Some(properties),
        Some(_) => panic!("tool `{}`: schema `properties` is not an object", tool.name),
        None => None,
    };

    let Some(properties) = properties.filter(|properties| !properties.is_empty()) else {
        rendered.push_str("The tool takes no arguments.\n");
        return rendered;
    };
    rendered.push_str(
        "Arguments:\n\n| Name | Type | Required | Description |\n| --- | --- | --- | --- |\n",
    );

    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_default();

    for (name, property) in properties {
        let property = property
            .as_object()
            .unwrap_or_else(|| panic!("tool `{}`: property `{name}` is not an object", tool.name));
        let display_type = property_type(property)
            .unwrap_or_else(|error| panic!("tool `{}`: property `{name}`: {error}", tool.name));
        let description = property
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_else(|| {
                panic!("tool `{}`: property `{name}` has no description", tool.name)
            });

        rendered.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            escape_for_table_cell(name),
            display_type,
            if required.contains(&name.as_str()) {
                "yes"
            } else {
                "no"
            },
            escape_for_table_cell(description)
        ));
    }
    rendered.push('\n');
    rendered
}

fn property_type(property: &Map<String, Value>) -> Result<String, String> {
    let mut types = Vec::new();
    match property.get("type") {
        Some(Value::String(kind)) => types.push(kind.as_str()),
        Some(Value::Array(kinds)) => {
            for kind in kinds {
                types.push(
                    kind.as_str()
                        .ok_or_else(|| "type array contains a non-string".to_owned())?,
                );
            }
        }
        Some(_) => return Err("type must be a string or an array of strings".to_owned()),
        None => {
            let any_of = property
                .get("anyOf")
                .and_then(Value::as_array)
                .ok_or_else(|| "schema has no supported type".to_owned())?;
            for variant in any_of {
                let variant = variant
                    .as_object()
                    .ok_or_else(|| "anyOf contains a non-object".to_owned())?;
                let kind = variant
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "anyOf variant has no string type".to_owned())?;
                types.push(kind);
            }
        }
    }

    let non_null_types = types
        .iter()
        .copied()
        .filter(|kind| *kind != "null")
        .collect::<Vec<_>>();
    if non_null_types.len() != 1 || types.len() > 2 {
        return Err("expected one scalar type, optionally combined with null".to_owned());
    }
    match non_null_types[0] {
        "string" | "integer" | "number" | "boolean" => Ok(non_null_types[0].to_owned()),
        other => Err(format!("unsupported property type `{other}`")),
    }
}

fn escape_for_table_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\n', '\r'], " ")
}

fn document_diff(current: &str, expected: &str) -> String {
    TextDiff::from_lines(current, expected)
        .unified_diff()
        .header("docs/mcp.md (checked in)", "docs/mcp.md (generated)")
        .to_string()
}

#[test]
fn render_tool_treats_missing_properties_as_no_arguments() {
    let tool = Tool::new(
        "no_arguments",
        "A tool without arguments",
        Map::from_iter([("type".to_owned(), Value::String("object".to_owned()))]),
    );

    assert_eq!(
        render_tool(&tool),
        "<!-- generated: tool no_arguments -->\n### `no_arguments`\n\n\
        A tool without arguments\n\nThe tool takes no arguments.\n"
    );
}

#[test]
#[should_panic(expected = "tool `invalid`: schema `properties` is not an object")]
fn render_tool_rejects_non_object_properties() {
    let tool = Tool::new(
        "invalid",
        "An invalid tool",
        Map::from_iter([
            ("type".to_owned(), Value::String("object".to_owned())),
            ("properties".to_owned(), Value::Array(Vec::new())),
        ]),
    );

    render_tool(&tool);
}

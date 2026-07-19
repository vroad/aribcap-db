use std::path::Path;

use crate::{
    docs_gen::{check_freshness, escape_for_table_cell, rewrite_markers},
    mcp::AribcapMcp,
};
use rmcp::model::Tool;
use serde_json::{Map, Value};

const DOCS_PATH: &str = "docs/mcp.md";
#[test]
fn mcp_docs_are_fresh() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(DOCS_PATH);
    let tools = AribcapMcp::tools();
    check_freshness(
        &path,
        "UPDATE_MCP_DOCS",
        "UPDATE_MCP_DOCS=1 cargo test mcp_docs",
        |document| {
            rewrite_markers(
                document,
                "tool",
                tools.iter().map(|tool| tool.name.as_ref()),
                |tool_name| {
                    let tool = tools
                        .iter()
                        .find(|tool| tool.name == tool_name)
                        .ok_or_else(|| format!("unknown tool `{tool_name}`"))?;
                    Ok(render_tool(tool))
                },
            )
        },
    );
}

fn render_tool(tool: &Tool) -> String {
    let mut rendered = format!("### `{}`\n\n", tool.name);
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

#[test]
fn render_tool_treats_missing_properties_as_no_arguments() {
    let tool = Tool::new(
        "no_arguments",
        "A tool without arguments",
        Map::from_iter([("type".to_owned(), Value::String("object".to_owned()))]),
    );

    assert_eq!(
        render_tool(&tool),
        "### `no_arguments`\n\nA tool without arguments\n\nThe tool takes no arguments.\n"
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

use std::path::Path;

use serde_json::Value;

use crate::{
    docs_gen::{check_freshness, escape_for_table_cell, rewrite_markers},
    server,
};

const DOCS_PATH: &str = "docs/http-api.md";

#[test]
fn http_api_docs_are_fresh() {
    let api = serde_json::to_value(server::openapi_document()).unwrap();
    let operations =
        get_operations(&api).unwrap_or_else(|error| panic!("invalid OpenAPI: {error}"));
    let ids = operations
        .iter()
        .map(|(path, _)| format!("GET {path}"))
        .collect::<Vec<_>>();
    let docs_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(DOCS_PATH);

    check_freshness(
        &docs_path,
        "UPDATE_HTTP_API_DOCS",
        "UPDATE_HTTP_API_DOCS=1 cargo test http_api_docs",
        |document| {
            rewrite_markers(document, "http", ids.iter().map(String::as_str), |id| {
                let path = id
                    .strip_prefix("GET ")
                    .ok_or_else(|| format!("invalid HTTP marker ID `{id}`"))?;
                let operation = operations
                    .iter()
                    .find_map(|(candidate, operation)| (*candidate == path).then_some(*operation))
                    .ok_or_else(|| format!("unknown OpenAPI operation `{id}`"))?;
                render_operation(path, operation)
            })
        },
    );
}

fn get_operations(api: &Value) -> Result<Vec<(&str, &Value)>, String> {
    let paths = api
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| "`paths` is not an object".to_owned())?;
    let mut operations = Vec::with_capacity(paths.len());

    for (path, item) in paths {
        let item = item
            .as_object()
            .ok_or_else(|| format!("path `{path}` is not an object"))?;
        let operation = item
            .get("get")
            .ok_or_else(|| format!("path `{path}` has no GET operation"))?;
        if item.keys().any(|method| method != "get") {
            return Err(format!("path `{path}` has an unsupported non-GET field"));
        }
        operations.push((path.as_str(), operation));
    }

    operations.sort_unstable_by_key(|(path, _)| *path);
    Ok(operations)
}

fn render_operation(path: &str, operation: &Value) -> Result<String, String> {
    let operation = operation
        .as_object()
        .ok_or_else(|| format!("GET `{path}` is not an object"))?;
    let description = operation
        .get("description")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("GET `{path}` has no description"))?;
    let mut output = format!("\n```text\nGET {path}\n```\n\n{description}\n\n");

    let parameters = operation
        .get("parameters")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if parameters.is_empty() {
        output.push_str("Parameters: none.\n\n");
    } else {
        output.push_str(
            "Parameters:\n\n| Name | Location | Type | Required | Description |\n| --- | --- | --- | --- | --- |\n",
        );
        for parameter in parameters {
            let name = string_field(parameter, "name", "parameter")?;
            let location = string_field(parameter, "in", name)?;
            let schema = parameter
                .get("schema")
                .ok_or_else(|| format!("parameter `{name}` has no schema"))?;
            let description = string_field(parameter, "description", name)?;
            output.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                escape_for_table_cell(name),
                escape_for_table_cell(location),
                display_schema(schema),
                if parameter
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    "yes"
                } else {
                    "no"
                },
                escape_for_table_cell(description),
            ));
        }
        output.push('\n');
    }

    let responses = operation
        .get("responses")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("GET `{path}` has no responses"))?;
    let mut responses = responses.iter().collect::<Vec<_>>();
    responses.sort_unstable_by_key(|(status, _)| status.parse::<u16>().unwrap_or(u16::MAX));
    output.push_str(
        "Responses:\n\n| Status | Content-Type | Schema | Description |\n| --- | --- | --- | --- |\n",
    );
    for (status, response) in responses {
        let description = string_field(response, "description", status)?;
        let content = response
            .get("content")
            .and_then(Value::as_object)
            .ok_or_else(|| format!("response `{status}` has no content"))?;
        if content.is_empty() {
            output.push_str(&format!(
                "| {status} | - | - | {} |\n",
                escape_for_table_cell(description)
            ));
        } else {
            for (content_type, media) in content {
                let schema = media
                    .get("schema")
                    .ok_or_else(|| format!("response `{status}` content has no schema"))?;
                output.push_str(&format!(
                    "| {status} | `{}` | {} | {} |\n",
                    escape_for_table_cell(content_type),
                    display_schema(schema),
                    escape_for_table_cell(description),
                ));
            }
        }
    }
    output.push('\n');

    Ok(output)
}

fn string_field<'a>(value: &'a Value, field: &str, context: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("`{context}` has no string `{field}`"))
}

fn display_schema(schema: &Value) -> String {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return format!("`{}`", reference.rsplit('/').next().unwrap_or(reference));
    }

    if schema.get("type").and_then(Value::as_str) == Some("array") {
        return format!(
            "array of {}",
            schema
                .get("items")
                .map(display_schema)
                .unwrap_or_else(|| "values".to_owned())
        );
    }

    if let Some(types) = schema.get("type").and_then(Value::as_array) {
        let types = types
            .iter()
            .filter_map(Value::as_str)
            .filter(|kind| *kind != "null")
            .collect::<Vec<_>>();
        if types.len() == 1 {
            return types[0].to_owned();
        }
        return types.join(" or ");
    }

    if let Some(kind) = schema.get("type").and_then(Value::as_str) {
        return kind.to_owned();
    }

    for variants in ["anyOf", "oneOf"] {
        if let Some(variants) = schema.get(variants).and_then(Value::as_array) {
            let variants = variants
                .iter()
                .filter(|variant| variant.get("type").and_then(Value::as_str) != Some("null"))
                .map(display_schema)
                .collect::<Vec<_>>();
            if !variants.is_empty() {
                return variants.join(" or ");
            }
        }
    }

    "value".to_owned()
}

#[test]
fn display_schema_handles_refs_arrays_and_nullable_scalars() {
    assert_eq!(
        display_schema(&serde_json::json!({
            "type": "array",
            "items": { "$ref": "#/components/schemas/ProgramEntry" }
        })),
        "array of `ProgramEntry`"
    );
    assert_eq!(
        display_schema(&serde_json::json!({ "type": ["string", "null"] })),
        "string"
    );
}

use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::process::ExitCode;

use aionui_api_types::{
    TeamToolCall, TeamToolCliEnvelope, TeamToolErrorCode, TeamToolErrorPayload, TeamToolName,
    TeamToolRuntimeBatchCallRequest, TeamToolRuntimeCallRequest, tool_name_for_cli_path,
};
use serde_json::{Value, json};

use crate::cli::{TeamArgs, TeamCommand, TeamRoomCommand, TeamTaskCommand};
use crate::commands::team_capabilities;

const ENV_BASE_URL: &str = "AIONUI_BASE_URL";
const ENV_USER_ID: &str = "AIONUI_USER_ID";
const ENV_CONVERSATION_ID: &str = "AIONUI_CONVERSATION_ID";
const ENV_RUNTIME_TOKEN: &str = "AIONUI_RUNTIME_TOKEN";

pub(crate) async fn run_team(args: TeamArgs) -> ExitCode {
    match run_team_inner(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

async fn run_team_inner(args: TeamArgs) -> Result<(), ExitCode> {
    match args.command {
        TeamCommand::Capabilities => print_json(&TeamToolCliEnvelope::success(
            team_capabilities::data(),
            Some("team capabilities".to_owned()),
        )),
        TeamCommand::Help => print_json(&TeamToolCliEnvelope::success(
            json!({ "format": "markdown", "text": team_capabilities::help_markdown() }),
            Some("team help".to_owned()),
        )),
        TeamCommand::Context => {
            let env = runtime_env("team context")?;
            let url = format!("{}/api/runtime/team-tools/context", env.base_url.trim_end_matches('/'));
            let response = reqwest::Client::new()
                .get(url)
                .headers(env.headers())
                .send()
                .await
                .map_err(|error| runtime_error("team context", "TEAM_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
            print_response(response).await
        }
        TeamCommand::Members => call_tool(vec!["members"]).await,
        TeamCommand::ReadMessages => call_tool(vec!["read-messages"]).await,
        TeamCommand::SendMessage => call_tool(vec!["send-message"]).await,
        TeamCommand::InterruptAgent => call_tool(vec!["interrupt-agent"]).await,
        TeamCommand::Task(task) => match task.command {
            TeamTaskCommand::Create => call_tool(vec!["task", "create"]).await,
            TeamTaskCommand::CreateBatch => call_task_create_batch("team task create-batch").await,
            TeamTaskCommand::Update => call_tool(vec!["task", "update"]).await,
            TeamTaskCommand::List => call_tool(vec!["task", "list"]).await,
            TeamTaskCommand::Unknown(path) => dispatch_unknown_task_command(path).await,
        },
        TeamCommand::ListAssistants => call_tool(vec!["list-assistants"]).await,
        TeamCommand::DescribeAssistant => call_tool(vec!["describe-assistant"]).await,
        TeamCommand::SpawnAgent => call_tool(vec!["spawn-agent"]).await,
        TeamCommand::RenameAgent => call_tool(vec!["rename-agent"]).await,
        TeamCommand::ClearAgentContext => call_tool(vec!["clear-agent-context"]).await,
        TeamCommand::ShutdownAgent => call_tool(vec!["shutdown-agent"]).await,
        TeamCommand::NotifyUser => call_tool(vec!["notify-user"]).await,
        TeamCommand::Batch => call_batch("team batch").await,
        TeamCommand::Room(room) => match room.command {
            TeamRoomCommand::Read => call_tool(vec!["room", "read"]).await,
            TeamRoomCommand::Post => call_tool(vec!["room", "post"]).await,
            TeamRoomCommand::TaskCreate => call_tool(vec!["room", "task-create"]).await,
            TeamRoomCommand::TaskUpdate => call_tool(vec!["room", "task-update"]).await,
            TeamRoomCommand::Unknown(path) => Err(unknown_command("team room", path, "unknown team room command")),
        },
        TeamCommand::Unknown(path) => dispatch_unknown_team_command(path).await,
    }
}

async fn dispatch_unknown_task_command(path: Vec<OsString>) -> Result<(), ExitCode> {
    let tokens = path
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    match tokens.as_slice() {
        [cmd] if matches!(cmd.as_str(), "batch" | "create_batch") => {
            call_task_create_batch("team task create-batch").await
        }
        [cmd] if matches!(cmd.as_str(), "complete" | "done") => call_tool(vec!["task", "update"]).await,
        _ => Err(unknown_command("team task", path, "unknown team task command")),
    }
}

async fn dispatch_unknown_team_command(path: Vec<OsString>) -> Result<(), ExitCode> {
    let tokens = path
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    match tokens.as_slice() {
        [cmd] if matches!(cmd.as_str(), "task-create" | "task_create" | "create-task") => {
            call_tool(vec!["task", "create"]).await
        }
        [cmd]
            if matches!(
                cmd.as_str(),
                "task-create-batch" | "task_create_batch" | "tasks-create" | "create-tasks"
            ) =>
        {
            call_task_create_batch("team task create-batch").await
        }
        [cmd] if matches!(cmd.as_str(), "task-update" | "task_update" | "update-task") => {
            call_tool(vec!["task", "update"]).await
        }
        [cmd] if matches!(cmd.as_str(), "task-list" | "task_list" | "tasks") => {
            call_tool(vec!["task", "list"]).await
        }
        [cmd] if matches!(cmd.as_str(), "send" | "reply" | "message" | "send_message") => {
            call_tool(vec!["send-message"]).await
        }
        [cmd] if matches!(cmd.as_str(), "read" | "inbox" | "messages" | "read_messages") => {
            call_tool(vec!["read-messages"]).await
        }
        [verb, raw_tool] if matches!(verb.as_str(), "call" | "tool-call" | "tool_call") => {
            let normalized = raw_tool.trim().replace('-', "_");
            let canonical = if normalized.starts_with("team_") {
                normalized
            } else {
                format!("team_{normalized}")
            };
            if let Some(tool) = TeamToolName::parse(&canonical) {
                call_named_tool(&format!("team {verb} {raw_tool}"), tool).await
            } else {
                Err(unknown_command("team", path, "unknown team tool"))
            }
        }
        _ => Err(unknown_command("team", path, "unknown team command")),
    }
}

async fn call_named_tool(command: &str, tool: TeamToolName) -> Result<(), ExitCode> {
    let env = runtime_env(command)?;
    let arguments = read_stdin_json_object(command, tool)?;
    let url = format!("{}/api/runtime/team-tools/call", env.base_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .post(url)
        .headers(env.headers())
        .json(&TeamToolRuntimeCallRequest { tool, arguments })
        .send()
        .await
        .map_err(|error| runtime_error(command, "TEAM_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
    print_response(response).await
}

async fn call_task_create_batch(command: &str) -> Result<(), ExitCode> {
    let raw = read_stdin_raw_json(command)?;
    let task_items = match raw {
        Value::Array(items) => items,
        Value::Object(mut obj) => match obj.remove("tasks") {
            Some(Value::Array(items)) if obj.is_empty() => items,
            _ => {
                return Err(print_failure(
                    command,
                    "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                    TeamToolErrorPayload::new(
                        TeamToolErrorCode::SchemaValidationFailed,
                        "stdin JSON for `team task create-batch` must be an array `[...]` or `{\"tasks\":[...]}`",
                    ),
                ));
            }
        },
        _ => {
            return Err(print_failure(
                command,
                "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                TeamToolErrorPayload::new(
                    TeamToolErrorCode::SchemaValidationFailed,
                    "stdin JSON for `team task create-batch` must be an array `[...]` or `{\"tasks\":[...]}`",
                ),
            ));
        }
    };
    if task_items.is_empty() || task_items.len() > 32 {
        return Err(print_failure(
            command,
            "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
            TeamToolErrorPayload::new(
                TeamToolErrorCode::SchemaValidationFailed,
                "tasks array must contain between 1 and 32 items",
            ),
        ));
    }
    let mut calls = Vec::with_capacity(task_items.len());
    for item in task_items {
        let validated = validate_against_descriptor(command, TeamToolName::TeamTaskCreate, item)?;
        calls.push(TeamToolCall {
            tool: TeamToolName::TeamTaskCreate,
            arguments: validated,
        });
    }
    post_batch_calls(command, calls).await
}

async fn call_batch(command: &str) -> Result<(), ExitCode> {
    let raw = read_stdin_raw_json(command)?;
    let call_values = match raw {
        Value::Array(items) => items,
        Value::Object(mut obj) => match obj.remove("calls") {
            Some(Value::Array(items)) if obj.is_empty() => items,
            _ => {
                return Err(print_failure(
                    command,
                    "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                    TeamToolErrorPayload::new(
                        TeamToolErrorCode::SchemaValidationFailed,
                        "stdin JSON for `team batch` must be an array `[...]` or `{\"calls\":[...]}`",
                    ),
                ));
            }
        },
        _ => {
            return Err(print_failure(
                command,
                "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                TeamToolErrorPayload::new(
                    TeamToolErrorCode::SchemaValidationFailed,
                    "stdin JSON for `team batch` must be an array `[...]` or `{\"calls\":[...]}`",
                ),
            ));
        }
    };
    if call_values.is_empty() || call_values.len() > 32 {
        return Err(print_failure(
            command,
            "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
            TeamToolErrorPayload::new(
                TeamToolErrorCode::SchemaValidationFailed,
                "batch calls array must contain between 1 and 32 items",
            ),
        ));
    }
    let mut calls = Vec::with_capacity(call_values.len());
    for raw_call in call_values {
        let parsed: TeamToolCall = serde_json::from_value(raw_call).map_err(|error| {
            print_failure(
                command,
                "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                TeamToolErrorPayload::new(TeamToolErrorCode::SchemaValidationFailed, error.to_string()),
            )
        })?;
        let validated_args = validate_against_descriptor(command, parsed.tool, parsed.arguments)?;
        calls.push(TeamToolCall {
            tool: parsed.tool,
            arguments: validated_args,
        });
    }
    post_batch_calls(command, calls).await
}

async fn post_batch_calls(command: &str, calls: Vec<TeamToolCall>) -> Result<(), ExitCode> {
    let env = runtime_env(command)?;
    let url = format!(
        "{}/api/runtime/team-tools/call-batch",
        env.base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(url)
        .headers(env.headers())
        .json(&TeamToolRuntimeBatchCallRequest { calls })
        .send()
        .await
        .map_err(|error| runtime_error(command, "TEAM_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
    print_response(response).await
}

fn read_stdin_raw_json(command: &str) -> Result<Value, ExitCode> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).map_err(|error| {
        print_failure(
            command,
            "TEAM_CLI_STDIN_READ_FAILED",
            TeamToolErrorPayload::new(TeamToolErrorCode::SchemaValidationFailed, error.to_string()),
        )
    })?;
    if input.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(&input).map_err(|error| {
        print_failure(
            command,
            "TEAM_CLI_STDIN_JSON_INVALID",
            TeamToolErrorPayload::new(TeamToolErrorCode::SchemaValidationFailed, error.to_string()),
        )
    })
}

async fn call_tool(path: Vec<&'static str>) -> Result<(), ExitCode> {
    let command = format!("team {}", path.join(" "));
    let env = runtime_env(&command)?;
    let tool =
        tool_name_for_cli_path(&path.iter().map(|part| (*part).to_owned()).collect::<Vec<_>>()).ok_or_else(|| {
            print_failure(
                &command,
                "TEAM_CLI_UNKNOWN_COMMAND",
                TeamToolErrorPayload::new(TeamToolErrorCode::UnknownTool, "unknown team command"),
            )
        })?;
    let arguments = read_stdin_json_object(&command, tool)?;
    let url = format!("{}/api/runtime/team-tools/call", env.base_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .post(url)
        .headers(env.headers())
        .json(&TeamToolRuntimeCallRequest { tool, arguments })
        .send()
        .await
        .map_err(|error| runtime_error(&command, "TEAM_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
    print_response(response).await
}

struct RuntimeEnv {
    base_url: String,
    user_id: String,
    conversation_id: String,
    runtime_token: String,
}

impl RuntimeEnv {
    fn headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-aionui-user-id", self.user_id.parse().unwrap());
        headers.insert("x-aionui-conversation-id", self.conversation_id.parse().unwrap());
        headers.insert("x-aionui-runtime-token", self.runtime_token.parse().unwrap());
        headers
    }
}

fn runtime_env(command: &str) -> Result<RuntimeEnv, ExitCode> {
    let base_url = required_env(command, ENV_BASE_URL)?;
    let user_id = required_env(command, ENV_USER_ID)?;
    let conversation_id = required_env(command, ENV_CONVERSATION_ID)?;
    let runtime_token = required_env(command, ENV_RUNTIME_TOKEN)?;
    Ok(RuntimeEnv {
        base_url,
        user_id,
        conversation_id,
        runtime_token,
    })
}

fn required_env(command: &str, name: &'static str) -> Result<String, ExitCode> {
    std::env::var(name).map_err(|_| {
        print_failure(
            command,
            "TEAM_CLI_ENV_MISSING",
            TeamToolErrorPayload::new(
                TeamToolErrorCode::RuntimeContextMissing,
                format!("missing required environment variable: {name}"),
            ),
        )
    })
}

fn read_stdin_json_object(command: &str, tool: TeamToolName) -> Result<Value, ExitCode> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).map_err(|error| {
        print_failure(
            command,
            "TEAM_CLI_STDIN_READ_FAILED",
            TeamToolErrorPayload::new(TeamToolErrorCode::SchemaValidationFailed, error.to_string()),
        )
    })?;
    let value = if input.trim().is_empty() {
        json!({})
    } else {
        serde_json::from_str(&input).map_err(|error| {
            print_failure(
                command,
                "TEAM_CLI_STDIN_JSON_INVALID",
                TeamToolErrorPayload::new(TeamToolErrorCode::SchemaValidationFailed, error.to_string()),
            )
        })?
    };
    validate_against_descriptor(command, tool, value)
}

fn validate_against_descriptor(command: &str, tool: TeamToolName, value: Value) -> Result<Value, ExitCode> {
    let Some(object) = value.as_object() else {
        return Err(print_failure(
            command,
            "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
            TeamToolErrorPayload::new(
                TeamToolErrorCode::SchemaValidationFailed,
                "stdin JSON must be an object",
            ),
        ));
    };
    let descriptor = aionui_api_types::team_tool_descriptor(tool.as_str()).expect("descriptor for canonical tool");
    let properties = descriptor.input_schema["properties"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    for key in object.keys() {
        if !properties.contains_key(key) {
            return Err(print_failure(
                command,
                "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                TeamToolErrorPayload::new(
                    TeamToolErrorCode::SchemaValidationFailed,
                    format!("unknown stdin field: {key}"),
                )
                .with_details(json!({ "expected_schema": descriptor.input_schema })),
            ));
        }
    }
    if let Some(required) = descriptor.input_schema["required"].as_array() {
        for key in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(key) {
                return Err(print_failure(
                    command,
                    "TEAM_CLI_SCHEMA_VALIDATION_FAILED",
                    TeamToolErrorPayload::new(
                        TeamToolErrorCode::SchemaValidationFailed,
                        format!("missing required stdin field: {key}"),
                    )
                    .with_details(json!({ "expected_schema": descriptor.input_schema })),
                ));
            }
        }
    }
    Ok(value)
}

async fn print_response(response: reqwest::Response) -> Result<(), ExitCode> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| runtime_error("team", "TEAM_CLI_HTTP_RESPONSE_FAILED", error.to_string()))?;
    if !status.is_success() {
        eprintln!(
            "TEAM_CLI_HTTP_STATUS_ERROR command=team status={status}: runtime bridge returned non-success status"
        );
        println!("{text}");
        return Err(ExitCode::from(3));
    }
    println!("{text}");
    Ok(())
}

fn runtime_error(command: &str, code: &'static str, message: String) -> ExitCode {
    print_failure(
        command,
        code,
        TeamToolErrorPayload::new(TeamToolErrorCode::TransportUnavailable, message),
    )
}

fn unknown_command(prefix: &str, path: Vec<OsString>, message: &'static str) -> ExitCode {
    let suffix = path
        .into_iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    let command = if suffix.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix} {suffix}")
    };
    print_failure(
        &command,
        "TEAM_CLI_UNKNOWN_COMMAND",
        TeamToolErrorPayload::new(TeamToolErrorCode::UnknownTool, message),
    )
}

fn print_failure(command: &str, stderr_code: &'static str, error: TeamToolErrorPayload) -> ExitCode {
    eprintln!("{stderr_code} command={command}: {}", error.message);
    let _ = print_json(&TeamToolCliEnvelope::<Value>::failure(error, Some(command.to_owned())));
    ExitCode::from(2)
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), ExitCode> {
    let rendered = serde_json::to_string_pretty(value).map_err(|_| ExitCode::from(1))?;
    let mut stdout = io::stdout();
    stdout
        .write_all(rendered.as_bytes())
        .and_then(|_| stdout.write_all(b"\n"))
        .map_err(|_| ExitCode::from(1))
}

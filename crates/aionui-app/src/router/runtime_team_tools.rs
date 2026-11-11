use std::sync::Arc;

use aionui_ai_agent::{RuntimeTokenScope, RuntimeTokenService, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use aionui_api_types::{
    TeamToolCall, TeamToolCliEnvelope, TeamToolContextResponse, TeamToolErrorCode, TeamToolErrorPayload, TeamToolName,
    TeamToolRuntimeBatchCallRequest, TeamToolRuntimeCallRequest, team_tool_descriptor,
};
use aionui_team::TeamSessionService;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

const HEADER_USER_ID: &str = "x-aionui-user-id";
const HEADER_CONVERSATION_ID: &str = "x-aionui-conversation-id";
const HEADER_RUNTIME_TOKEN: &str = "x-aionui-runtime-token";

#[derive(Clone)]
pub struct RuntimeTeamToolsState {
    pub team_service: Arc<TeamSessionService>,
    pub runtime_token_service: Arc<RuntimeTokenService>,
}

pub fn runtime_team_tools_routes(state: RuntimeTeamToolsState) -> Router {
    Router::new()
        .route("/api/runtime/team-tools/context", get(context))
        .route("/api/runtime/team-tools/call", post(call))
        .route("/api/runtime/team-tools/call-batch", post(call_batch))
        .with_state(state)
}

async fn context(
    State(state): State<RuntimeTeamToolsState>,
    headers: HeaderMap,
) -> (StatusCode, Json<TeamToolCliEnvelope<TeamToolContextResponse>>) {
    let Ok(runtime) = runtime_headers(&headers, RuntimeTokenScope::TeamContext, &state.runtime_token_service) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(TeamToolErrorCode::RuntimeAuthFailed, "runtime auth failed"),
                Some("team context".to_owned()),
            )),
        );
    };
    match state
        .team_service
        .resolve_team_tool_context(&runtime.user_id, &runtime.conversation_id)
        .await
    {
        Ok(resolved) => (
            StatusCode::OK,
            Json(TeamToolCliEnvelope::success(
                resolved.response,
                Some("team context".to_owned()),
            )),
        ),
        Err(error) => (
            status_for_error(error.code),
            Json(TeamToolCliEnvelope::failure(error, Some("team context".to_owned()))),
        ),
    }
}

async fn call(
    State(state): State<RuntimeTeamToolsState>,
    headers: HeaderMap,
    Json(request): Json<TeamToolRuntimeCallRequest>,
) -> (StatusCode, Json<TeamToolCliEnvelope<Value>>) {
    let Ok(runtime) = runtime_headers(&headers, RuntimeTokenScope::TeamCall, &state.runtime_token_service) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(TeamToolErrorCode::RuntimeAuthFailed, "runtime auth failed"),
                Some("team call".to_owned()),
            )),
        );
    };
    let resolved = match state
        .team_service
        .resolve_team_tool_context(&runtime.user_id, &runtime.conversation_id)
        .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return (
                status_for_error(error.code),
                Json(TeamToolCliEnvelope::failure(error, Some("team call".to_owned()))),
            );
        }
    };
    let Some(context) = resolved.context else {
        return (
            StatusCode::FORBIDDEN,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(TeamToolErrorCode::NotInTeam, "conversation is not in a team"),
                Some("team call".to_owned()),
            )),
        );
    };
    match state
        .team_service
        .execute_team_tool(
            &context,
            aionui_api_types::TeamToolCall {
                tool: request.tool,
                arguments: request.arguments,
            },
        )
        .await
    {
        Ok(data) => (
            StatusCode::OK,
            Json(TeamToolCliEnvelope::success(data, Some("team call".to_owned()))),
        ),
        Err(error) => (
            status_for_error(error.code),
            Json(TeamToolCliEnvelope::failure(error, Some("team call".to_owned()))),
        ),
    }
}

async fn call_batch(
    State(state): State<RuntimeTeamToolsState>,
    headers: HeaderMap,
    Json(request): Json<TeamToolRuntimeBatchCallRequest>,
) -> (StatusCode, Json<TeamToolCliEnvelope<Value>>) {
    let Ok(runtime) = runtime_headers(&headers, RuntimeTokenScope::TeamCall, &state.runtime_token_service) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(TeamToolErrorCode::RuntimeAuthFailed, "runtime auth failed"),
                Some("team batch".to_owned()),
            )),
        );
    };
    if request.calls.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(
                    TeamToolErrorCode::SchemaValidationFailed,
                    "batch calls array must not be empty",
                ),
                Some("team batch".to_owned()),
            )),
        );
    }
    if request.calls.len() > 32 {
        return (
            StatusCode::BAD_REQUEST,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(
                    TeamToolErrorCode::SchemaValidationFailed,
                    "batch calls array exceeds maximum of 32 items",
                ),
                Some("team batch".to_owned()),
            )),
        );
    }
    let resolved = match state
        .team_service
        .resolve_team_tool_context(&runtime.user_id, &runtime.conversation_id)
        .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return (
                status_for_error(error.code),
                Json(TeamToolCliEnvelope::failure(error, Some("team batch".to_owned()))),
            );
        }
    };
    let Some(context) = resolved.context else {
        return (
            StatusCode::FORBIDDEN,
            Json(TeamToolCliEnvelope::failure(
                TeamToolErrorPayload::new(TeamToolErrorCode::NotInTeam, "conversation is not in a team"),
                Some("team batch".to_owned()),
            )),
        );
    };

    // Pre-validate every call BEFORE executing any call so invalid syntax/permissions
    // on item #2 never cause partial execution of item #1.
    for (idx, item) in request.calls.iter().enumerate() {
        if let Err(error) = prevalidate_batch_item(idx, item, &resolved.response.allowed_tools) {
            return (
                status_for_error(error.code),
                Json(TeamToolCliEnvelope::failure(error, Some("team batch".to_owned()))),
            );
        }
    }

    let mut results = Vec::with_capacity(request.calls.len());
    for (idx, item) in request.calls.into_iter().enumerate() {
        let tool_name = item.tool.as_str().to_owned();
        match state.team_service.execute_team_tool(&context, item).await {
            Ok(data) => results.push(json!({
                "index": idx,
                "tool": tool_name,
                "data": data,
            })),
            Err(error) => {
                let code = error.code;
                let enriched = error.with_details(json!({
                    "failed_index": idx,
                    "failed_tool": tool_name,
                    "completed_results": results,
                }));
                return (
                    status_for_error(code),
                    Json(TeamToolCliEnvelope::failure(enriched, Some("team batch".to_owned()))),
                );
            }
        }
    }

    (
        StatusCode::OK,
        Json(TeamToolCliEnvelope::success(
            json!({
                "count": results.len(),
                "results": results,
            }),
            Some("team batch".to_owned()),
        )),
    )
}

fn prevalidate_batch_item(
    idx: usize,
    item: &TeamToolCall,
    allowed_tools: &[String],
) -> Result<(), TeamToolErrorPayload> {
    let tool_name = item.tool.as_str();
    if !allowed_tools.iter().any(|allowed| allowed == tool_name) {
        return Err(TeamToolErrorPayload::new(
            TeamToolErrorCode::PermissionDenied,
            format!("batch item #{idx} ({tool_name}): tool is not permitted for this role/context"),
        ));
    }
    let Some(descriptor) = team_tool_descriptor(tool_name) else {
        return Err(TeamToolErrorPayload::new(
            TeamToolErrorCode::UnknownTool,
            format!("batch item #{idx}: unknown tool {tool_name}"),
        ));
    };
    let Some(object) = item.arguments.as_object() else {
        return Err(TeamToolErrorPayload::new(
            TeamToolErrorCode::SchemaValidationFailed,
            format!("batch item #{idx} ({tool_name}): arguments must be a JSON object"),
        ));
    };
    let properties = descriptor.input_schema["properties"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    for key in object.keys() {
        if !properties.contains_key(key) {
            return Err(TeamToolErrorPayload::new(
                TeamToolErrorCode::SchemaValidationFailed,
                format!("batch item #{idx} ({tool_name}): unknown field '{key}'"),
            )
            .with_details(json!({ "index": idx, "expected_schema": descriptor.input_schema })));
        }
    }
    if let Some(required) = descriptor.input_schema["required"].as_array() {
        for key in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(key) {
                return Err(TeamToolErrorPayload::new(
                    TeamToolErrorCode::SchemaValidationFailed,
                    format!("batch item #{idx} ({tool_name}): missing required field '{key}'"),
                )
                .with_details(json!({ "index": idx, "expected_schema": descriptor.input_schema })));
            }
        }
    }
    if matches!(item.tool, TeamToolName::TeamTaskCreate)
        && object
            .get("subject")
            .and_then(Value::as_str)
            .is_none_or(|s| s.trim().is_empty())
    {
        return Err(TeamToolErrorPayload::new(
            TeamToolErrorCode::SchemaValidationFailed,
            format!("batch item #{idx} ({tool_name}): subject must be a non-empty string"),
        ));
    }
    if matches!(item.tool, TeamToolName::TeamTaskUpdate)
        && object
            .get("task_id")
            .and_then(Value::as_str)
            .is_none_or(|s| s.trim().is_empty())
    {
        return Err(TeamToolErrorPayload::new(
            TeamToolErrorCode::SchemaValidationFailed,
            format!("batch item #{idx} ({tool_name}): task_id must be a non-empty string"),
        ));
    }
    Ok(())
}

struct RuntimeHeaders {
    user_id: String,
    conversation_id: String,
}

fn runtime_headers(
    headers: &HeaderMap,
    scope: RuntimeTokenScope,
    token_service: &RuntimeTokenService,
) -> Result<RuntimeHeaders, ()> {
    let user_id = required_header(headers, HEADER_USER_ID)?;
    let conversation_id = required_header(headers, HEADER_CONVERSATION_ID)?;
    let token = required_header(headers, HEADER_RUNTIME_TOKEN)?;
    token_service
        .validate(
            Some(&token),
            &user_id,
            &conversation_id,
            scope,
            TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        )
        .map_err(|_| ())?;
    Ok(RuntimeHeaders {
        user_id,
        conversation_id,
    })
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ()> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or(())
}

fn status_for_error(code: TeamToolErrorCode) -> StatusCode {
    match code {
        TeamToolErrorCode::RuntimeAuthFailed => StatusCode::UNAUTHORIZED,
        TeamToolErrorCode::PermissionDenied | TeamToolErrorCode::NotInTeam => StatusCode::FORBIDDEN,
        TeamToolErrorCode::ConversationNotFound
        | TeamToolErrorCode::TeamNotFound
        | TeamToolErrorCode::AgentNotFound => StatusCode::NOT_FOUND,
        TeamToolErrorCode::UnknownTool | TeamToolErrorCode::SchemaValidationFailed => StatusCode::BAD_REQUEST,
        TeamToolErrorCode::TransportUnavailable | TeamToolErrorCode::RuntimeContextMissing => StatusCode::CONFLICT,
    }
}

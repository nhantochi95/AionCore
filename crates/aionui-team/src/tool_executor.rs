use std::sync::Weak;
use std::time::Instant;

use aionui_api_types::{
    TeamToolCall, TeamToolDescriptor, TeamToolErrorCode, TeamToolErrorPayload, TeamToolName, TeamToolRole,
    TeamToolTransport,
};
use serde_json::Value;
use tracing::{info, warn};

use crate::mcp::server::ToolCallError;
use crate::scheduler::TeammateManager;
use crate::service::TeamSessionService;
use crate::types::TeammateRole;

#[derive(Debug, Clone)]
pub struct TeamToolContext {
    pub team_id: String,
    pub caller_slot_id: String,
    pub caller_role: TeammateRole,
    pub user_id: Option<String>,
    pub conversation_id: Option<String>,
    pub transport: TeamToolTransport,
}

pub struct TeamToolExecutor<'a> {
    scheduler: &'a TeammateManager,
    service: &'a Weak<TeamSessionService>,
}

impl<'a> TeamToolExecutor<'a> {
    pub fn new(scheduler: &'a TeammateManager, service: &'a Weak<TeamSessionService>) -> Self {
        Self { scheduler, service }
    }

    pub fn list_tools(&self, context: &TeamToolContext) -> Vec<TeamToolDescriptor> {
        let include_shared_room_tools = match self.service.upgrade() {
            Some(svc) => svc.include_shared_room_tools(&context.team_id),
            None => true,
        };
        aionui_api_types::team_tool_descriptors_for_context(
            team_tool_role(context.caller_role),
            include_shared_room_tools,
        )
    }

    pub async fn execute(&self, context: &TeamToolContext, call: TeamToolCall) -> Result<Value, TeamToolErrorPayload> {
        let started = Instant::now();
        let tool = call.tool.as_str();
        let result = crate::mcp::server::dispatch_tool(
            tool,
            &call.arguments,
            self.scheduler,
            self.service,
            &context.team_id,
            &context.caller_slot_id,
            context.caller_role,
        )
        .await
        .map(|text| serde_json::from_str(&text).unwrap_or(Value::String(text)))
        .map_err(map_tool_error);

        let duration_ms = started.elapsed().as_millis();
        match &result {
            Ok(_) => info!(
                transport = ?context.transport,
                team_id = %context.team_id,
                caller_slot_id = %context.caller_slot_id,
                conversation_id = context.conversation_id.as_deref().unwrap_or(""),
                tool,
                result = "success",
                duration_ms,
                "Team tool call completed"
            ),
            Err(error) => warn!(
                transport = ?context.transport,
                team_id = %context.team_id,
                caller_slot_id = %context.caller_slot_id,
                conversation_id = context.conversation_id.as_deref().unwrap_or(""),
                tool,
                error_code = ?error.code,
                classification = error.details.as_ref().and_then(|details| details.get("classification")).and_then(|value| value.as_str()).unwrap_or("business_error"),
                duration_ms,
                "Team tool call failed"
            ),
        }
        result
    }
}

pub fn team_tool_call_from_name(tool_name: &str, arguments: Value) -> Result<TeamToolCall, TeamToolErrorPayload> {
    let tool = TeamToolName::parse(tool_name).ok_or_else(|| {
        TeamToolErrorPayload::new(TeamToolErrorCode::UnknownTool, format!("Unknown tool: {tool_name}"))
    })?;
    Ok(TeamToolCall { tool, arguments })
}

fn team_tool_role(role: TeammateRole) -> TeamToolRole {
    match role {
        TeammateRole::Lead => TeamToolRole::Lead,
        TeammateRole::Teammate => TeamToolRole::Teammate,
    }
}

pub(crate) async fn execute_sessionless_tool(
    service: &Weak<TeamSessionService>,
    context: &TeamToolContext,
    call: &TeamToolCall,
) -> Option<Result<Value, TeamToolErrorPayload>> {
    let tool = call.tool.as_str();
    let raw = crate::mcp::server::try_dispatch_sessionless_tool(
        tool,
        &call.arguments,
        service,
        &context.team_id,
        context.caller_role,
    )
    .await?;
    Some(
        raw.map(|text| serde_json::from_str(&text).unwrap_or(Value::String(text)))
            .map_err(map_tool_error),
    )
}

fn map_tool_error(error: ToolCallError) -> TeamToolErrorPayload {
    let code = if let Some(code) = error.tool_error_code {
        code
    } else if error.message.starts_with("Unknown tool:") {
        TeamToolErrorCode::UnknownTool
    } else if error.message.starts_with("Only Lead") {
        TeamToolErrorCode::PermissionDenied
    } else if error.message.starts_with("Invalid params")
        || error.message.starts_with("Missing required field")
        || error.message.contains("does not accept arguments")
        || error.message.contains("is no longer accepted")
    {
        TeamToolErrorCode::SchemaValidationFailed
    } else if error.message.contains("Invalid agent target") {
        TeamToolErrorCode::AgentNotFound
    } else if error.message.contains("No active session") {
        TeamToolErrorCode::TeamNotFound
    } else if error.message.contains("Team service not available") {
        TeamToolErrorCode::TransportUnavailable
    } else {
        TeamToolErrorCode::RuntimeContextMissing
    };

    let mut payload = TeamToolErrorPayload::new(code, error.message);
    if let Some(details) = error.details {
        payload = payload.with_details(details);
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_tool_maps_to_shared_error_payload() {
        let err = team_tool_call_from_name("missing_tool", json!({})).unwrap_err();
        assert_eq!(err.code, TeamToolErrorCode::UnknownTool);
        assert!(err.message.contains("Unknown tool"));
    }

    #[test]
    fn typed_team_errors_preserve_classified_error_codes() {
        use crate::error::TeamError;

        let busy = map_tool_error(ToolCallError::from_team_error(TeamError::MemberBusy {
            team_id: "t1".into(), slot_id: "s1".into(), conversation_id: "c1".into(),
        }));
        assert_eq!(busy.code, TeamToolErrorCode::TransportUnavailable);

        let starting = map_tool_error(ToolCallError::from_team_error(TeamError::MemberRuntimeStarting {
            team_id: "t1".into(), slot_id: "s1".into(), conversation_id: "c1".into(),
        }));
        assert_eq!(starting.code, TeamToolErrorCode::TransportUnavailable);

        let missing_session = map_tool_error(ToolCallError::from_team_error(TeamError::SessionNotFound("t1".into())));
        assert_eq!(missing_session.code, TeamToolErrorCode::TeamNotFound);

        let missing_agent = map_tool_error(ToolCallError::from_team_error(TeamError::AgentNotFound("slot-9".into())));
        assert_eq!(missing_agent.code, TeamToolErrorCode::AgentNotFound);

        let forbidden = map_tool_error(ToolCallError::from_team_error(TeamError::LeaderOnly("notify".into())));
        assert_eq!(forbidden.code, TeamToolErrorCode::PermissionDenied);

        let invalid = map_tool_error(ToolCallError::from_team_error(TeamError::InvalidRequest("bad filter".into())));
        assert_eq!(invalid.code, TeamToolErrorCode::SchemaValidationFailed);
    }
}

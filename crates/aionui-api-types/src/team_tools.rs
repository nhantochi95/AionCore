use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const TEAM_TOOLS_SCHEMA_VERSION: u32 = 1;

pub const TEAM_SPAWN_AGENT_DESCRIPTION: &str = r#"Create a new teammate agent to join the team.

Use only when the user explicitly approved the proposed lineup or explicitly asked to create a teammate immediately.
Before calling this tool in normal planning: briefly state why teammates help, present a table (name, responsibility, recommended assistant), remind the user they can adjust or replace teammates later, and wait for approval in a later user message.
Always pass assistant_id from team_list_assistants; do not pass a model."#;

pub const TEAM_DESCRIBE_ASSISTANT_DESCRIPTION: &str = "Get detailed information about an assistant before spawning it as a teammate.\n\n\
Returns the assistant's full description, enabled skills, and example tasks so you can\n\
judge whether it fits the user's request. Use this when two or more assistants look\n\
relevant from the one-line catalog in your system prompt.\n\n\
Use team_list_assistants to find candidate assistant_id values.\n\
After confirming a match, call team_spawn_agent with the same assistant_id.";

pub const TEAM_LIST_ASSISTANTS_DESCRIPTION: &str = "List the assistants available for team spawning. Returns the real assistant catalog with \
real assistant_id values, names, backends, descriptions, and skills.\n\nUse this before \
team_spawn_agent when you need the exact assistant_id for a teammate. Do NOT guess from backend \
names like claude/codex/gemini — only use assistant_id values returned here.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolPermission {
    AnyTeamAgent,
    LeadOnly,
}

impl TeamToolPermission {
    pub fn is_lead_only(self) -> bool {
        self == Self::LeadOnly
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolRole {
    Lead,
    Teammate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolTransport {
    Mcp,
    CliAssumed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolName {
    TeamMembers,
    TeamReadMessages,
    TeamSendMessage,
    TeamInterruptAgent,
    TeamTaskCreate,
    TeamTaskUpdate,
    TeamTaskList,
    TeamListAssistants,
    TeamDescribeAssistant,
    TeamSpawnAgent,
    TeamRenameAgent,
    TeamClearAgentContext,
    TeamShutdownAgent,
    TeamNotifyUser,
    TeamRoomRead,
    TeamRoomPost,
    TeamRoomTaskCreate,
    TeamRoomTaskUpdate,
}

impl TeamToolName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TeamMembers => "team_members",
            Self::TeamReadMessages => "team_read_messages",
            Self::TeamSendMessage => "team_send_message",
            Self::TeamInterruptAgent => "team_interrupt_agent",
            Self::TeamTaskCreate => "team_task_create",
            Self::TeamTaskUpdate => "team_task_update",
            Self::TeamTaskList => "team_task_list",
            Self::TeamListAssistants => "team_list_assistants",
            Self::TeamDescribeAssistant => "team_describe_assistant",
            Self::TeamSpawnAgent => "team_spawn_agent",
            Self::TeamRenameAgent => "team_rename_agent",
            Self::TeamClearAgentContext => "team_clear_agent_context",
            Self::TeamShutdownAgent => "team_shutdown_agent",
            Self::TeamNotifyUser => "team_notify_user",
            Self::TeamRoomRead => "team_room_read",
            Self::TeamRoomPost => "team_room_post",
            Self::TeamRoomTaskCreate => "team_room_task_create",
            Self::TeamRoomTaskUpdate => "team_room_task_update",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "team_members" => Self::TeamMembers,
            "team_read_messages" => Self::TeamReadMessages,
            "team_send_message" => Self::TeamSendMessage,
            "team_interrupt_agent" => Self::TeamInterruptAgent,
            "team_task_create" => Self::TeamTaskCreate,
            "team_task_update" => Self::TeamTaskUpdate,
            "team_task_list" => Self::TeamTaskList,
            "team_list_assistants" => Self::TeamListAssistants,
            "team_describe_assistant" => Self::TeamDescribeAssistant,
            "team_spawn_agent" => Self::TeamSpawnAgent,
            "team_rename_agent" => Self::TeamRenameAgent,
            "team_clear_agent_context" => Self::TeamClearAgentContext,
            "team_shutdown_agent" => Self::TeamShutdownAgent,
            "team_notify_user" => Self::TeamNotifyUser,
            "team_room_read" => Self::TeamRoomRead,
            "team_room_post" => Self::TeamRoomPost,
            "team_room_task_create" => Self::TeamRoomTaskCreate,
            "team_room_task_update" => Self::TeamRoomTaskUpdate,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolDescriptor {
    pub name: String,
    pub permission: TeamToolPermission,
    pub description: String,
    pub input_schema: Value,
    pub cli_command: Vec<String>,
    pub when: String,
    pub input_summary: String,
}

impl TeamToolDescriptor {
    pub fn lead_only(&self) -> bool {
        self.permission.is_lead_only()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolCall {
    #[serde(alias = "tool_name", alias = "name")]
    pub tool: TeamToolName,
    #[serde(default, alias = "args")]
    pub arguments: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolErrorCode {
    UnknownTool,
    SchemaValidationFailed,
    PermissionDenied,
    TeamNotFound,
    ConversationNotFound,
    AgentNotFound,
    NotInTeam,
    TransportUnavailable,
    RuntimeContextMissing,
    RuntimeAuthFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamToolErrorPayload {
    pub code: TeamToolErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl TeamToolErrorPayload {
    pub fn new(code: TeamToolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolCliMeta {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolCliEnvelope<T> {
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TeamToolErrorPayload>,
    pub meta: TeamToolCliMeta,
}

impl<T> TeamToolCliEnvelope<T> {
    pub fn success(data: T, command: Option<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            meta: TeamToolCliMeta {
                schema_version: TEAM_TOOLS_SCHEMA_VERSION,
                command,
            },
        }
    }

    pub fn failure(error: TeamToolErrorPayload, command: Option<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
            meta: TeamToolCliMeta {
                schema_version: TEAM_TOOLS_SCHEMA_VERSION,
                command,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolContextResponse {
    pub in_team: bool,
    pub conversation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<TeamToolRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<TeamToolTransport>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolRuntimeCallRequest {
    pub tool: TeamToolName,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolRuntimeBatchCallRequest {
    pub calls: Vec<TeamToolCall>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolRuntimeCallResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TeamToolErrorPayload>,
}

pub fn team_tool_descriptors() -> Vec<TeamToolDescriptor> {
    tool_specs()
        .into_iter()
        .map(|spec| TeamToolDescriptor {
            name: spec.name.as_str().to_owned(),
            permission: spec.permission,
            description: spec.description.to_owned(),
            input_schema: spec.input_schema,
            cli_command: spec.cli_command.iter().map(|part| (*part).to_owned()).collect(),
            when: spec.when.to_owned(),
            input_summary: spec.input_summary.to_owned(),
        })
        .collect()
}

pub fn team_tool_descriptors_for_role(role: TeamToolRole) -> Vec<TeamToolDescriptor> {
    team_tool_descriptors_for_context(role, true)
}

pub fn team_tool_descriptors_for_context(
    role: TeamToolRole,
    include_shared_room_tools: bool,
) -> Vec<TeamToolDescriptor> {
    let is_lead = role == TeamToolRole::Lead;
    team_tool_descriptors()
        .into_iter()
        .filter(|descriptor| is_lead || !descriptor.permission.is_lead_only())
        .filter(|descriptor| include_shared_room_tools || !is_shared_room_tool_name(&descriptor.name))
        .collect()
}

pub fn is_shared_room_tool_name(name: &str) -> bool {
    matches!(
        name,
        "team_room_read" | "team_room_post" | "team_room_task_create" | "team_room_task_update"
    )
}

pub fn team_tool_descriptor(name: &str) -> Option<TeamToolDescriptor> {
    team_tool_descriptors()
        .into_iter()
        .find(|descriptor| descriptor.name == name)
}

pub fn cli_command_for_tool(name: &str) -> Option<&'static [&'static str]> {
    tool_specs()
        .into_iter()
        .find(|spec| spec.name.as_str() == name)
        .map(|spec| spec.cli_command)
}

pub fn tool_name_for_cli_path(path: &[String]) -> Option<TeamToolName> {
    tool_specs()
        .into_iter()
        .find(|spec| spec.cli_command == path.iter().map(String::as_str).collect::<Vec<_>>())
        .map(|spec| spec.name)
}

#[derive(Debug, Clone)]
struct TeamToolSpec {
    name: TeamToolName,
    permission: TeamToolPermission,
    description: &'static str,
    input_schema: Value,
    cli_command: &'static [&'static str],
    when: &'static str,
    input_summary: &'static str,
}

fn tool_specs() -> Vec<TeamToolSpec> {
    vec![
        TeamToolSpec {
            name: TeamToolName::TeamMembers,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "List all team members with their roles and current status.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
            cli_command: &["members"],
            when: "Check roster/status",
            input_summary: "{}",
        },
        TeamToolSpec {
            name: TeamToolName::TeamReadMessages,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Peek at your own unread team mailbox messages. Returns at most the oldest 50 unread messages in FIFO order, each with a message_id. When has_more is true, call again with since_message_id set to the returned next_since_message_id to read the following page. Messages returned in full are marked read only if the current turn completes successfully; failed or cancelled turns preserve them for retry. A message with content_truncated=true is a preview only: it stays unread and is redelivered in full on a later turn, so do not act on it yet.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "since_message_id": {
                        "type": "string",
                        "description": "Resume after this message_id; use the next_since_message_id from the previous page"
                    }
                }
            }),
            cli_command: &["read-messages"],
            when: "Check queued messages",
            input_summary: "optional since_message_id",
        },
        TeamToolSpec {
            name: TeamToolName::TeamSendMessage,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Send a message to a teammate or broadcast to all (to=\"*\"). When delegating work that depends on user attachments, forward their absolute paths in files.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "to": { "type": "string", "description": "Target agent slot_id or \"*\" for broadcast" },
                    "message": { "type": "string", "description": "Message content" },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Absolute attachment paths to forward to the target agent"
                    }
                },
                "required": ["to", "message"]
            }),
            cli_command: &["send-message"],
            when: "Send teammate message",
            input_summary: "to, message",
        },
        TeamToolSpec {
            name: TeamToolName::TeamInterruptAgent,
            permission: TeamToolPermission::LeadOnly,
            description: "Immediately stop a teammate's active turn and durably deliver a replacement instruction as its next highest-priority message.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "slot_id": { "type": "string", "description": "Exact teammate slot_id; wildcard is not supported" },
                    "message": { "type": "string", "description": "Replacement instruction delivered after interruption" },
                    "files": { "type": "array", "items": { "type": "string" }, "description": "Absolute attachment paths" },
                    "reason": { "type": "string", "description": "Optional interruption reason" }
                },
                "required": ["slot_id", "message"]
            }),
            cli_command: &["interrupt-agent"],
            when: "Interrupt teammate turn",
            input_summary: "slot_id, message, optional files/reason",
        },
        TeamToolSpec {
            name: TeamToolName::TeamTaskCreate,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Create a new task on the team task board.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "subject": { "type": "string", "description": "Task subject" },
                    "description": { "type": "string", "description": "Task description" },
                    "owner": { "type": "string", "description": "Owning agent slotId" },
                    "blocked_by": { "type": "array", "items": { "type": "string" }, "description": "Task IDs this task depends on" }
                },
                "required": ["subject"]
            }),
            cli_command: &["task", "create"],
            when: "Create task",
            input_summary: "subject, optional owner/deps",
        },
        TeamToolSpec {
            name: TeamToolName::TeamTaskUpdate,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Update an existing task on the team task board (and optionally deliver a 1-shot reply_message to the Team Lead in the same atomic call).",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "task_id": { "type": "string", "description": "Task ID (full UUID or unambiguous 8+ char prefix)" },
                    "status": { "type": "string", "enum": ["pending", "in_progress", "completed", "deleted"], "description": "New status" },
                    "description": { "type": "string", "description": "New description" },
                    "owner": { "type": "string", "description": "New owning agent slotId" },
                    "blocked_by": { "type": "array", "items": { "type": "string" }, "description": "New dependency list" },
                    "reply_message": { "type": "string", "description": "Optional 1-shot message delivered atomically to the Team Lead mailbox when updating/completing this task (replaces a separate team_send_message call)" },
                    "reply_files": { "type": "array", "items": { "type": "string" }, "description": "Optional workspace file paths attached to reply_message" }
                },
                "required": ["task_id"]
            }),
            cli_command: &["task", "update"],
            when: "Update task",
            input_summary: "task_id, optional status/owner/deps/reply_message/reply_files",
        },
        TeamToolSpec {
            name: TeamToolName::TeamTaskList,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "List tasks on the team task board. Pass {} for up to 50 tasks, or use owner/status/include_deleted/limit for filtered views.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "owner": {
                        "type": "string",
                        "description": "Return only tasks owned by this exact agent slot_id"
                    },
                    "status": {
                        "description": "Return only tasks with these statuses. Accepts one status string (including comma-separated values or \"all\") or an array of status strings (empty array [] means all). Supports canonical statuses (pending, in_progress, completed, deleted) and governance aliases (todo, open, active, in_review, blocked, done, cancelled, canceled).",
                        "anyOf": [
                            {
                                "type": "string"
                            },
                            {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            }
                        ]
                    },
                    "statuses": {
                        "description": "Alias for status. Accepts one status string or an array of status strings.",
                        "anyOf": [
                            {
                                "type": "string"
                            },
                            {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            }
                        ]
                    },
                    "include_deleted": {
                        "type": "boolean",
                        "description": "When status is omitted, include deleted tasks. Defaults to true so {} returns the full board."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Maximum number of returned tasks (default 50, max 200)."
                    }
                }
            }),
            cli_command: &["task", "list"],
            when: "List tasks",
            input_summary: "owner, status, include_deleted, limit",
        },
        TeamToolSpec {
            name: TeamToolName::TeamListAssistants,
            permission: TeamToolPermission::LeadOnly,
            description: TEAM_LIST_ASSISTANTS_DESCRIPTION,
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
            cli_command: &["list-assistants"],
            when: "List spawn assistants",
            input_summary: "{}",
        },
        TeamToolSpec {
            name: TeamToolName::TeamDescribeAssistant,
            permission: TeamToolPermission::LeadOnly,
            description: TEAM_DESCRIBE_ASSISTANT_DESCRIPTION,
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "assistant_id": { "type": "string", "description": "The assistant ID from the available assistants catalog (e.g., \"word-creator\")." },
                    "locale": { "type": "string", "description": "Locale like \"zh-CN\" or \"en-US\". Defaults to the user's current UI language when omitted." }
                },
                "required": ["assistant_id"]
            }),
            cli_command: &["describe-assistant"],
            when: "Inspect assistant",
            input_summary: "assistant_id, optional locale",
        },
        TeamToolSpec {
            name: TeamToolName::TeamSpawnAgent,
            permission: TeamToolPermission::LeadOnly,
            description: TEAM_SPAWN_AGENT_DESCRIPTION,
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "name": { "type": "string", "description": "Agent display name" },
                    "assistant_id": { "type": "string", "description": "Assistant ID to spawn. Call team_list_assistants when you need candidates; the runtime backend is derived from this assistant." }
                },
                "required": ["name", "assistant_id"]
            }),
            cli_command: &["spawn-agent"],
            when: "Spawn teammate",
            input_summary: "name, assistant_id",
        },
        TeamToolSpec {
            name: TeamToolName::TeamRenameAgent,
            permission: TeamToolPermission::LeadOnly,
            description: "Rename a team member. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "slot_id": { "type": "string", "description": "Agent slot_id to rename" },
                    "new_name": { "type": "string", "description": "New display name" }
                },
                "required": ["slot_id", "new_name"]
            }),
            cli_command: &["rename-agent"],
            when: "Rename teammate",
            input_summary: "slot_id, new_name",
        },
        TeamToolSpec {
            name: TeamToolName::TeamClearAgentContext,
            permission: TeamToolPermission::LeadOnly,
            description: "Start a fresh backend context for a teammate while preserving team membership, settings, visible chat history, tasks, files, and unread mailbox messages. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "slot_id": { "type": "string", "description": "Agent slot_id whose context should be cleared" }
                },
                "required": ["slot_id"]
            }),
            cli_command: &["clear-agent-context"],
            when: "Start fresh teammate context",
            input_summary: "slot_id",
        },
        TeamToolSpec {
            name: TeamToolName::TeamShutdownAgent,
            permission: TeamToolPermission::LeadOnly,
            description: "Initiate shutdown of a teammate. Lead only. Sends a shutdown_request to the target agent.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "slot_id": { "type": "string", "description": "Agent slot_id to shut down" },
                    "reason": { "type": "string", "description": "Reason for shutdown" }
                },
                "required": ["slot_id"]
            }),
            cli_command: &["shutdown-agent"],
            when: "Shut down teammate",
            input_summary: "slot_id, optional reason",
        },
        TeamToolSpec {
            name: TeamToolName::TeamNotifyUser,
            permission: TeamToolPermission::LeadOnly,
            description: "Proactively notify the human user via Telegram/external channels with a concise progress milestone, approval request, blocker alert, or completion report, optionally attaching workspace output files. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": ["milestone", "approval_needed", "completed", "blocker"],
                        "description": "Notification category: milestone, approval_needed, completed, or blocker"
                    },
                    "summary": {
                        "type": "string",
                        "description": "Concise human-readable status message or approval prompt"
                    },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional absolute paths inside the team workspace or shared artifacts to send to the user"
                    }
                },
                "required": ["kind", "summary"]
            }),
            cli_command: &["notify-user"],
            when: "Report milestone/approval/blocker/result to user",
            input_summary: "kind, summary, optional files",
        },
        TeamToolSpec {
            name: TeamToolName::TeamRoomRead,
            permission: TeamToolPermission::LeadOnly,
            description: "Read delta messages and open cross-team tasks from a Multi-Team Shared Room (Leader Hub). Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "room_id": {
                        "type": "string",
                        "description": "Optional Shared Room ID or name; defaults to the active pending handoff room or single joined room"
                    },
                    "since_msg_id": {
                        "type": "string",
                        "description": "Optional message ID cursor; return only messages newer than this ID"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Maximum recent messages to return (default 12, max 50)"
                    },
                    "include_completed_tasks": {
                        "type": "boolean",
                        "description": "Include completed/cancelled cross-team tasks (default false)"
                    }
                }
            }),
            cli_command: &["room", "read"],
            when: "Read shared room delta",
            input_summary: "optional room_id, since_msg_id, limit, include_completed_tasks",
        },
        TeamToolSpec {
            name: TeamToolName::TeamRoomPost,
            permission: TeamToolPermission::LeadOnly,
            description: "Post a message, handoff summary, or shared workspace artifacts to a Multi-Team Shared Room, optionally mentioning @TeamName to trigger another team's Leader. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "room_id": {
                        "type": "string",
                        "description": "Optional Shared Room ID or name; defaults to the active pending handoff room or single joined room"
                    },
                    "message": {
                        "type": "string",
                        "description": "Message to post in the Shared Room (may include @TeamName or @all)"
                    },
                    "mentions": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional explicit target team IDs or names to notify"
                    },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional absolute file paths inside the team workspace to stage into Shared Room artifacts and copy to recipient .shared-inbox/"
                    }
                },
                "required": ["message"]
            }),
            cli_command: &["room", "post"],
            when: "Post update/handoff to shared room",
            input_summary: "message, optional room_id/mentions/files",
        },
        TeamToolSpec {
            name: TeamToolName::TeamRoomTaskCreate,
            permission: TeamToolPermission::LeadOnly,
            description: "Create a cross-team task in a Multi-Team Shared Room assigned to another participating Team's Leader, with optional blocked_by dependencies and shared artifact files. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "room_id": {
                        "type": "string",
                        "description": "Optional Shared Room ID or name"
                    },
                    "to_team": {
                        "type": "string",
                        "description": "Target team ID or display name"
                    },
                    "title": {
                        "type": "string",
                        "description": "Short cross-team task title"
                    },
                    "description": {
                        "type": "string",
                        "description": "Detailed cross-team task specification and acceptance criteria"
                    },
                    "blocked_by": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Cross-team task IDs that must reach in_review/completed before waking the target team"
                    },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional workspace files to attach and sync to the target team's .shared-inbox/"
                    }
                },
                "required": ["to_team", "title"]
            }),
            cli_command: &["room", "task-create"],
            when: "Assign cross-team task in shared room",
            input_summary: "to_team, title, optional room_id/description/blocked_by/files",
        },
        TeamToolSpec {
            name: TeamToolName::TeamRoomTaskUpdate,
            permission: TeamToolPermission::LeadOnly,
            description: "Update cross-team task status, summary, or output artifacts in a Shared Room. Agents should set status=\"in_review\" when ready for human Done approval (which unblocks dependent teams) or \"blocked\" on blockers. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Cross-team task ID to update"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["pending", "in_progress", "in_review", "blocked", "completed", "cancelled"],
                        "description": "Updated cross-team task status (use in_review when work is ready; completed is human-only)"
                    },
                    "result_summary": {
                        "type": "string",
                        "description": "Concise deliverable summary when reporting progress or completion"
                    },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional output files to publish to Shared Room artifacts and downstream .shared-inbox/"
                    }
                },
                "required": ["task_id"]
            }),
            cli_command: &["room", "task-update"],
            when: "Update cross-team room task",
            input_summary: "task_id, optional status/result_summary/files",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn descriptor_count_and_names_are_unique() {
        let descriptors = team_tool_descriptors();
        assert_eq!(descriptors.len(), 18);
        let names = descriptors
            .iter()
            .map(|descriptor| descriptor.name.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), descriptors.len());
    }

    #[test]
    fn descriptors_have_required_prompt_and_schema_fields() {
        for descriptor in team_tool_descriptors() {
            assert!(!descriptor.name.is_empty());
            assert!(!descriptor.description.is_empty());
            assert!(!descriptor.when.is_empty());
            assert!(!descriptor.input_summary.is_empty());
            assert!(!descriptor.cli_command.is_empty());
            assert_eq!(descriptor.input_schema["type"], "object");
        }
    }

    #[test]
    fn cli_command_mapping_matches_spec() {
        let cases = [
            ("team_members", vec!["members"]),
            ("team_read_messages", vec!["read-messages"]),
            ("team_send_message", vec!["send-message"]),
            ("team_interrupt_agent", vec!["interrupt-agent"]),
            ("team_task_create", vec!["task", "create"]),
            ("team_task_update", vec!["task", "update"]),
            ("team_task_list", vec!["task", "list"]),
            ("team_list_assistants", vec!["list-assistants"]),
            ("team_describe_assistant", vec!["describe-assistant"]),
            ("team_spawn_agent", vec!["spawn-agent"]),
            ("team_rename_agent", vec!["rename-agent"]),
            ("team_clear_agent_context", vec!["clear-agent-context"]),
            ("team_shutdown_agent", vec!["shutdown-agent"]),
            ("team_notify_user", vec!["notify-user"]),
            ("team_room_read", vec!["room", "read"]),
            ("team_room_post", vec!["room", "post"]),
            ("team_room_task_create", vec!["room", "task-create"]),
            ("team_room_task_update", vec!["room", "task-update"]),
        ];
        for (tool, path) in cases {
            assert_eq!(cli_command_for_tool(tool), Some(path.as_slice()));
            let owned = path.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert_eq!(tool_name_for_cli_path(&owned).map(TeamToolName::as_str), Some(tool));
        }
    }

    #[test]
    fn teammate_role_hides_lead_only_tools() {
        let names = team_tool_descriptors_for_role(TeamToolRole::Teammate)
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();
        assert!(!names.contains(&"team_spawn_agent".to_owned()));
        assert!(!names.contains(&"team_rename_agent".to_owned()));
        assert!(!names.contains(&"team_clear_agent_context".to_owned()));
        assert!(!names.contains(&"team_shutdown_agent".to_owned()));
        assert!(!names.contains(&"team_interrupt_agent".to_owned()));
        assert!(!names.contains(&"team_notify_user".to_owned()));
        assert!(!names.contains(&"team_room_read".to_owned()));
        assert!(!names.contains(&"team_room_post".to_owned()));
        assert!(!names.contains(&"team_room_task_create".to_owned()));
        assert!(!names.contains(&"team_room_task_update".to_owned()));
        assert!(!names.contains(&"team_list_assistants".to_owned()));
        assert!(!names.contains(&"team_describe_assistant".to_owned()));
        assert!(names.contains(&"team_read_messages".to_owned()));
        assert!(names.contains(&"team_send_message".to_owned()));
    }

    #[test]
    fn context_scoped_descriptors_gate_shared_room_tools_when_unbound() {
        let standalone = team_tool_descriptors_for_context(TeamToolRole::Lead, false);
        let standalone_names: Vec<&str> = standalone.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(standalone.len(), 14);
        assert!(!standalone_names.contains(&"team_room_read"));
        assert!(!standalone_names.contains(&"team_room_post"));
        assert!(!standalone_names.contains(&"team_room_task_create"));
        assert!(!standalone_names.contains(&"team_room_task_update"));
        assert!(standalone_names.contains(&"team_spawn_agent"));

        let room_bound = team_tool_descriptors_for_context(TeamToolRole::Lead, true);
        assert_eq!(room_bound.len(), 18);

        let room_read = team_tool_descriptor("team_room_read").expect("room_read");
        let read_props = room_read.input_schema["properties"].as_object().unwrap();
        assert!(read_props.contains_key("since_msg_id"));
        assert!(read_props.contains_key("limit"));
        assert!(read_props.contains_key("include_completed_tasks"));

        let room_task_update = team_tool_descriptor("team_room_task_update").expect("room_task_update");
        let statuses = room_task_update.input_schema["properties"]["status"]["enum"]
            .as_array()
            .unwrap();
        assert!(statuses.contains(&json!("in_review")));
        assert!(statuses.contains(&json!("blocked")));
    }

    #[test]
    fn read_messages_schema_exposes_only_an_optional_cursor() {
        let descriptor = team_tool_descriptor("team_read_messages").expect("read messages descriptor");
        assert_eq!(descriptor.permission, TeamToolPermission::AnyTeamAgent);
        assert_eq!(descriptor.input_schema["additionalProperties"], json!(false));
        assert!(
            descriptor.input_schema.get("required").is_none(),
            "every argument must stay optional so a bare call still works"
        );
        let properties = descriptor.input_schema["properties"].as_object().unwrap();
        assert_eq!(
            properties.keys().collect::<Vec<_>>(),
            vec!["since_message_id"],
            "the cursor is the only accepted argument"
        );
        assert!(
            descriptor.description.contains("oldest 50"),
            "the description must state the page starts at the oldest unread message"
        );
        assert!(
            descriptor.description.contains("next_since_message_id"),
            "has_more needs a documented follow-up path"
        );
        assert!(
            descriptor.description.contains("redelivered in full"),
            "truncated messages must be documented as previews that stay unread"
        );
    }

    #[test]
    fn interrupt_schema_is_lead_only_and_requires_exact_target_and_message() {
        let descriptor = team_tool_descriptor("team_interrupt_agent").expect("interrupt descriptor");
        assert_eq!(descriptor.permission, TeamToolPermission::LeadOnly);
        let required = descriptor.input_schema["required"].as_array().unwrap();
        assert!(required.contains(&json!("slot_id")));
        assert!(required.contains(&json!("message")));
        assert!(
            !descriptor.input_schema["properties"]
                .as_object()
                .unwrap()
                .contains_key("target")
        );
    }

    #[test]
    fn spawn_schema_is_assistant_first_and_excludes_legacy_fields() {
        let descriptor = team_tool_descriptor("team_spawn_agent").expect("spawn descriptor");
        assert_eq!(descriptor.permission, TeamToolPermission::LeadOnly);
        let props = descriptor.input_schema["properties"].as_object().unwrap();
        assert!(props.contains_key("name"));
        assert!(props.contains_key("assistant_id"));
        assert!(!props.contains_key("model"));
        assert!(!props.contains_key("backend"));
        assert!(!props.contains_key("agent_type"));
        assert!(!props.contains_key("role"));
        let required = descriptor.input_schema["required"].as_array().unwrap();
        assert!(required.contains(&json!("name")));
        assert!(required.contains(&json!("assistant_id")));
    }

    #[test]
    fn notify_and_room_tools_are_lead_only_with_strict_schemas() {
        let notify = team_tool_descriptor("team_notify_user").expect("notify descriptor");
        assert_eq!(notify.permission, TeamToolPermission::LeadOnly);
        assert_eq!(notify.input_schema["required"], json!(["kind", "summary"]));

        let room_post = team_tool_descriptor("team_room_post").expect("room_post descriptor");
        assert_eq!(room_post.permission, TeamToolPermission::LeadOnly);
        assert_eq!(room_post.input_schema["required"], json!(["message"]));
    }
}

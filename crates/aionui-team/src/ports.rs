use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use aionui_api_types::{ConversationRuntimeSummary, TeamRunTargetRole};
use aionui_common::ResolvedBackendCapabilities;
use async_trait::async_trait;

use crate::error::TeamError;

/// External bridge port allowing `TeamSessionService` and MCP/CLI tool dispatch
/// to notify connected user channels (e.g. Telegram) and collaborate in
/// Multi-Team Shared Rooms without coupling `aionui-team` to `aionui-channel`.
#[async_trait]
pub trait TeamExternalBridgePort: Send + Sync {
    async fn notify_user(
        &self,
        user_id: &str,
        team_id: &str,
        kind: &str,
        summary: &str,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String>;

    async fn room_read(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, String>;

    async fn room_post(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        message: &str,
        mentions: Vec<String>,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String>;

    #[allow(clippy::too_many_arguments)]
    async fn room_task_create(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        to_team: &str,
        title: &str,
        description: Option<&str>,
        blocked_by: Vec<String>,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String>;

    async fn room_task_update(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        status: Option<&str>,
        result_summary: Option<&str>,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String>;

    async fn pending_room_handoff_banner(&self, user_id: &str, team_id: &str) -> Option<String>;

    fn on_team_delegation(&self, team_id: &str);

    async fn has_pending_slot_handover(
        &self,
        _user_id: &str,
        _team_id: &str,
        _slot_id: &str,
        _conversation_id: &str,
    ) -> bool {
        false
    }

    async fn slot_requires_fresh_role_prompt(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        conversation_id: &str,
    ) -> bool {
        self.has_pending_slot_handover(user_id, team_id, slot_id, conversation_id)
            .await
    }

    async fn rearm_slot_handover(
        &self,
        _user_id: &str,
        _team_id: &str,
        _slot_id: &str,
        _conversation_id: &str,
    ) {
    }

    async fn pending_slot_handover_banner(
        &self,
        _user_id: &str,
        _team_id: &str,
        _slot_id: &str,
        _conversation_id: &str,
    ) -> Option<String> {
        None
    }

    fn on_slot_turn_settled(
        &self,
        _user_id: &str,
        _team_id: &str,
        _slot_id: &str,
        _conversation_id: &str,
    ) {
    }

    fn has_shared_room_binding(&self, _user_id: &str, _team_id: &str) -> bool {
        true
    }

    fn on_slot_wake_delivered(
        &self,
        _user_id: &str,
        _team_id: &str,
        _slot_id: &str,
        _conversation_id: &str,
        _delivered_chars: usize,
        _coalesced_wakes_saved: u64,
    ) {
    }

    async fn room_read_delta(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        limit: usize,
        since_msg_id: Option<&str>,
        include_completed_tasks: bool,
    ) -> Result<serde_json::Value, String> {
        let mut value = self.room_read(user_id, team_id, room_id, limit).await?;
        if let Some(cursor) = since_msg_id.map(str::trim).filter(|c| !c.is_empty())
            && let Some(messages) = value.get_mut("messages").and_then(serde_json::Value::as_array_mut)
            && let Some(pos) = messages.iter().position(|item| {
                item.get("id")
                    .or_else(|| item.get("message_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(cursor)
            })
        {
            *messages = messages.split_off(pos + 1);
        }
        if !include_completed_tasks
            && let Some(tasks) = value.get_mut("tasks").and_then(serde_json::Value::as_array_mut)
        {
            tasks.retain(|task| {
                !matches!(
                    task.get("status").and_then(serde_json::Value::as_str),
                    Some("completed" | "done" | "cancelled")
                )
            });
        }
        Ok(value)
    }
}

/// Resolves Team tool capability without exposing backend-specific descriptors
/// or persisted handshake shapes to the Team domain.
#[async_trait]
pub trait TeamToolCapabilityPort: Send + Sync {
    async fn resolve(
        &self,
        user_id: &str,
        backend: &str,
        agent_id: Option<&str>,
    ) -> Result<ResolvedBackendCapabilities, TeamError>;
}

/// Safe default for test-only/service construction that does not inject the
/// composition resolver: unknown backends retain the CLI fallback.
pub struct UnknownTeamToolCapabilityPort;

#[async_trait]
impl TeamToolCapabilityPort for UnknownTeamToolCapabilityPort {
    async fn resolve(
        &self,
        _user_id: &str,
        _backend: &str,
        _agent_id: Option<&str>,
    ) -> Result<ResolvedBackendCapabilities, TeamError> {
        Ok(ResolvedBackendCapabilities {
            cli_fallback: true,
            ..Default::default()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamConversationBindingLookup {
    pub conversation_id: String,
    pub user_id: String,
    pub team_id: Option<String>,
    pub slot_id: Option<String>,
    pub role: Option<String>,
}

#[async_trait]
pub trait TeamConversationLookupPort: Send + Sync {
    async fn lookup_team_binding_by_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<TeamConversationBindingLookup>, TeamError>;
}

/// Which tier of the recognition degradation chain produced a slash catalog.
/// All three point at the same `capabilities().slash_commands` source; they
/// differ only in freshness/liveness (see the fix spec §8-1 degradation chain).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlashCatalogSource {
    /// Read from a live backend session's current capabilities.
    Live,
    /// Read from the persisted `agent_metadata.available_commands` snapshot.
    Cached,
    /// Read from a static backend catalog (e.g. codex builtin commands).
    Static,
}

impl SlashCatalogSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Cached => "cached",
            Self::Static => "static",
        }
    }
}

/// Result of asking whether a user message is a native backend slash command.
///
/// Carries the source tag (not a bare `bool`) so the team layer can emit the
/// structured recognition log required by the fix spec (§14/AC11) while staying
/// backend-agnostic — it only consumes this enum, never a concrete backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlashCommandRecognition {
    /// `content` is a single command this member's backend advertises. `command`
    /// is the name without the leading `/`.
    Recognized {
        command: String,
        source: SlashCatalogSource,
    },
    /// Leading `/` with a non-empty name, but the name is not in the advertised
    /// catalog → ordinary body text (§14 debug: `not_in_catalog`).
    NotInCatalog { name: String },
    /// Leading `/` with a non-empty name, but the catalog RESOLVED to an EMPTY
    /// list (e.g. a live backend returned no commands). Distinct from
    /// `NotInCatalog` (a populated catalog that simply lacks this name): an empty
    /// catalog is an anomaly that silently disables every team command, so it is
    /// surfaced separately and logged at `warn` (§14 warn: `catalog_empty`).
    /// Still falls back to the wrapped wake turn (zero regression).
    CatalogEmpty { name: String },
    /// Leading `/` with a non-empty name, but the catalog could not be resolved
    /// (degradation chain exhausted) → fall back to the wrapped wake turn
    /// (§14 warn: `catalog_unavailable`).
    CatalogUnavailable { name: String },
    /// First character is not `/` (leading whitespace included) or the name is
    /// empty → not a command at all.
    NotCommand,
}

/// Recognizes whether a raw user message is a native backend slash command for a
/// given conversation. Backend-agnostic contract owned by the team layer; the
/// composition layer supplies an implementation that consults the backend's
/// self-advertised command catalog (live → cached → static degradation chain).
#[async_trait]
pub trait NativeSlashCommandPort: Send + Sync {
    async fn recognize(&self, conversation_id: &str, content: &str) -> SlashCommandRecognition;
}

/// No-op recognizer: every message is `NotCommand`, so team behaviour is
/// byte-identical to the pre-fix wrapped-wake path. Used as the default when no
/// composition-layer recognizer is wired (e.g. unit tests that don't exercise
/// command recognition).
pub struct NoopNativeSlashCommandPort;

#[async_trait]
impl NativeSlashCommandPort for NoopNativeSlashCommandPort {
    async fn recognize(&self, _conversation_id: &str, _content: &str) -> SlashCommandRecognition {
        SlashCommandRecognition::NotCommand
    }
}

#[async_trait]
pub trait TeamAssistantCatalogPort: Send + Sync {
    async fn list_team_selectable_assistants(&self, user_id: &str)
    -> Result<Vec<TeamAssistantCatalogEntry>, TeamError>;

    async fn resolve_team_selectable_assistant(
        &self,
        user_id: &str,
        assistant_id: &str,
    ) -> Result<Option<TeamAssistantCatalogEntry>, TeamError> {
        Ok(self
            .list_team_selectable_assistants(user_id)
            .await?
            .into_iter()
            .find(|assistant| assistant.assistant_id == assistant_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamAssistantCatalogEntry {
    pub assistant_id: String,
    pub name: String,
    pub backend: String,
    pub description: String,
    pub skills: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTurnSource {
    Mailbox {
        unread_message_ids: Vec<String>,
        unread_count: usize,
    },
}

#[derive(Clone)]
pub struct AgentTurnRequest {
    pub team_run_id: Option<String>,
    pub team_id: String,
    pub slot_id: String,
    pub role: TeamRunTargetRole,
    pub conversation_id: String,
    pub user_id: String,
    pub content: String,
    pub files: Vec<String>,
    pub source: AgentTurnSource,
    pub on_started: Option<AgentTurnStartedCallback>,
}

impl fmt::Debug for AgentTurnRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTurnRequest")
            .field("team_run_id", &self.team_run_id)
            .field("team_id", &self.team_id)
            .field("slot_id", &self.slot_id)
            .field("role", &self.role)
            .field("conversation_id", &self.conversation_id)
            .field("user_id", &self.user_id)
            .field("files", &self.files)
            .field("source", &self.source)
            .field("has_on_started", &self.on_started.is_some())
            .finish_non_exhaustive()
    }
}

pub type AgentTurnStartedCallback =
    Arc<dyn Fn(AgentTurnStarted) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnStarted {
    pub team_run_id: Option<String>,
    pub slot_id: String,
    pub role: TeamRunTargetRole,
    pub conversation_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentTurnStatus {
    Completed,
    Failed,
    Skipped,
}

impl AgentTurnStatus {
    pub fn is_success(self) -> bool {
        matches!(self, Self::Completed)
    }
}

#[derive(Debug, Clone)]
pub struct AgentTurnOutcome {
    pub conversation_id: String,
    pub turn_id: String,
    pub status: AgentTurnStatus,
    pub runtime: Option<ConversationRuntimeSummary>,
}

#[derive(Debug, thiserror::Error)]
pub enum AgentTurnExecutionError {
    #[error("agent turn skipped: {reason}")]
    Skipped { reason: String },
    #[error("agent turn failed: {reason}")]
    Failed { reason: String },
}

#[async_trait]
pub trait AgentTurnExecutionPort: Send + Sync {
    async fn run_agent_turn(&self, request: AgentTurnRequest) -> Result<AgentTurnOutcome, AgentTurnExecutionError>;
}

#[async_trait]
pub trait AgentTurnCancellationPort: Send + Sync {
    async fn cancel_agent_turn(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<(), AgentTurnExecutionError>;
}

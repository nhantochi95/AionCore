use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::ChannelError;
use crate::types::{ActionContext, ActionResponse, UnifiedIncomingMessage};

/// View DTO returned to AionUi Desktop for configuring Telegram <-> Team bridge settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamTelegramBridgeConfigDto {
    pub team_id: String,
    pub telegram_plugin_enabled: bool,
    pub telegram_connected: bool,
    pub bot_username: Option<String>,
    pub has_authorized_user: bool,
    /// Whether proactive Telegram notifications are enabled for this Team.
    pub notify_enabled: bool,
    /// `"milestones"` (Smart Notification Filter) or `"full_stream"`.
    pub stream_mode: String,
    /// Whether this Team is currently selected as the active 1-1 DM target.
    pub is_active_dm_target: bool,
    /// Optional bound Telegram Group / Supergroup `chat_id` (e.g. `"-100123456789"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_chat_id: Option<String>,
    /// Optional bound Telegram Forum Topic `message_thread_id` (e.g. `"42"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_thread_id: Option<String>,
}

/// Request payload for `PUT /api/channel/team-bindings/{team_id}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpsertTeamTelegramBridgeRequest {
    #[serde(default)]
    pub notify_enabled: Option<bool>,
    #[serde(default)]
    pub stream_mode: Option<String>,
    #[serde(default)]
    pub set_active_dm_target: Option<bool>,
    #[serde(default)]
    pub group_chat_id: Option<String>,
    #[serde(default)]
    pub group_thread_id: Option<String>,
    #[serde(default)]
    pub clear_group_binding: Option<bool>,
}

/// Late-injected bridge port decoupling `aionui-channel` from `aionui-team` and Shared Rooms.
#[async_trait]
pub trait ChannelTeamBridgePort: Send + Sync {
    /// Handles slash commands (`/team`, `/room`, `/status`, `/stop`, `/solo`, etc.).
    /// Returns `Ok(Some(response))` if the command was handled by the bridge,
    /// or `Ok(None)` to fall through to default channel handling.
    async fn handle_bridge_command(
        &self,
        owner_user_id: &str,
        msg: &UnifiedIncomingMessage,
        command: &str,
        args: &str,
    ) -> Result<Option<ActionResponse>, ChannelError>;

    /// Handles compact `t:<10-char>` inline callback tokens.
    async fn handle_bridge_token(
        &self,
        owner_user_id: &str,
        token: &str,
        context: &ActionContext,
        raw_meta: Option<&serde_json::Value>,
    ) -> Result<ActionResponse, ChannelError>;

    /// Attempts to route a non-command incoming message to a bound Team Lead or Shared Room
    /// (via Reply-route -> Group/Topic binding -> Active DM target).
    /// Returns `Ok(true)` if routed to a Team or Shared Room, or `Ok(false)` to fall back to solo chat.
    async fn try_dispatch_to_target(
        &self,
        owner_user_id: &str,
        msg: &UnifiedIncomingMessage,
    ) -> Result<bool, ChannelError>;

    /// Lists Telegram bridge configurations for all Teams owned by `owner_user_id`.
    async fn list_team_bridge_configs(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<TeamTelegramBridgeConfigDto>, ChannelError>;

    /// Gets the Telegram bridge configuration for one Team.
    async fn get_team_bridge_config(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<TeamTelegramBridgeConfigDto, ChannelError>;

    /// Updates notification preferences, active DM target, or Group/Topic binding for a Team.
    async fn upsert_team_bridge_config(
        &self,
        owner_user_id: &str,
        team_id: &str,
        req: UpsertTeamTelegramBridgeRequest,
    ) -> Result<TeamTelegramBridgeConfigDto, ChannelError>;

    /// Clears any Group/Topic binding for a Team.
    async fn delete_team_group_binding(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<TeamTelegramBridgeConfigDto, ChannelError>;
}

//! Central coordinator bridging Telegram (`aionui-channel`), Team Sessions (`aionui-team`),
//! and Multi-Team Shared Rooms (Leader Hub).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use aionui_api_types::{
    ChatFileRef, TeamAgentRuntimeStatus, TeamAgentRuntimeStatusPayload, TeamResponse, TeamRunPayload, WebSocketMessage,
};
use aionui_channel::error::ChannelError;
use aionui_channel::formatter::{
    TELEGRAM_DOCUMENT_THRESHOLD_CHARS, TELEGRAM_SAFE_CHUNK_CHARS, chunk_telegram_html, markdown_to_telegram_html,
};
use aionui_channel::manager::ChannelManager;
use aionui_channel::team_bridge::{
    ChannelTeamBridgePort, TeamTelegramBridgeConfigDto, UpsertTeamTelegramBridgeRequest,
};
use aionui_channel::types::{
    ActionBehavior, ActionButton, ActionContext, ActionResponse, OutgoingMessageType, ParseMode,
    UnifiedIncomingMessage, UnifiedOutgoingMessage, format_chat_with_thread, split_chat_and_thread,
};
use aionui_common::{generate_prefixed_id, now_ms};
use aionui_db::custom_bridge_schema::{
    CreateActionInboxParams, RecordCostRunParams, WalletType, classify_agent_wallet,
};
use aionui_db::{
    CreateSharedRoomMessageParams, CreateSharedRoomTaskParams, CustomSharedRoomMessageRow, CustomSharedRoomRow,
    CustomSharedRoomTaskRow, CustomStreamMode, CustomTargetType, IChannelRepository, IConversationRepository,
    IProviderRepository, ISettingsRepository, MessagePageDirection, MessagePageParams, SqliteCustomBridgeRepository,
    UpdateSharedRoomTaskParams, UpsertChannelBindingParams,
};
use aionui_project::GitWorktreeEngine;
use aionui_realtime::{BroadcastEventBus, EventBroadcaster};
use aionui_team::events::{
    TEAM_AGENT_RUNTIME_STATUS_CHANGED_EVENT, TEAM_REMOVED_EVENT, TEAM_RUN_CANCELLED_EVENT,
    TEAM_RUN_COMPLETED_EVENT, TEAM_RUN_FAILED_EVENT, TEAM_RUN_STARTED_EVENT,
};
use aionui_team::{TeamExternalBridgePort, TeamSessionService};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::bridge::file_guard::{
    StagedRoomArtifact, is_image_file_name, sanitize_file_name, save_inbound_telegram_attachment,
    stage_room_artifacts_batch, validate_outbound_file_path,
};
use crate::bridge::judge::{
    ParticipatingTeam, RoomRoutingInput, RoutedTarget, is_task_blocked,
    is_team_blocked_by_dependencies, mention_slug, resolve_room_targets, validate_task_dependencies,
};
use crate::bridge::locales::{HubI18n, HubLocale};
use crate::routes::inbox::ActionInboxCardDto;

/// Minimum run duration (45s) for Desktop-initiated runs to trigger a proactive Telegram summary
/// when no teammates were delegated and `stream_mode == "milestones"`.
const SMART_FILTER_DESKTOP_DURATION_MS: i64 = 45_000;

#[derive(Debug, Clone)]
pub enum RunOrigin {
    Telegram {
        owner_user_id: String,
        plugin_id: String,
        reply_chat: String,
        reply_to_msg_id: Option<String>,
    },
    SharedRoom {
        owner_user_id: String,
        room_id: String,
    },
    Desktop {
        owner_user_id: String,
    },
}

impl RunOrigin {
    pub fn owner_user_id(&self) -> &str {
        match self {
            Self::Telegram { owner_user_id, .. }
            | Self::SharedRoom { owner_user_id, .. }
            | Self::Desktop { owner_user_id } => owner_user_id,
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveTeamRunTracker {
    origin: RunOrigin,
    started_at_ms: i64,
    delegated: bool,
    notify_completed_sent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedRoomMemberDto {
    pub team_id: String,
    pub team_name: String,
    pub workspace: String,
    pub leader_name: String,
    pub is_host: bool,
    pub pending_handoff_msg_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedRoomDto {
    pub id: String,
    pub name: String,
    pub description: String,
    pub rules: String,
    pub host_team_id: Option<String>,
    pub max_hops: i64,
    pub hop_count_since_human: i64,
    pub artifacts_dir: String,
    pub members: Vec<SharedRoomMemberDto>,
    pub telegram_chat_id: Option<String>,
    pub telegram_thread_id: Option<String>,
    pub is_active_dm_target: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedRoomMessageDto {
    pub id: String,
    pub room_id: String,
    pub sender_type: String,
    pub sender_team_id: Option<String>,
    pub sender_label: String,
    pub reply_to_id: Option<String>,
    pub content: String,
    pub attachments: Vec<StagedRoomArtifact>,
    pub target_team_ids: Vec<String>,
    pub hop_index: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedRoomTaskDto {
    pub id: String,
    pub room_id: String,
    pub from_team_id: Option<String>,
    pub from_team_name: Option<String>,
    pub to_team_id: String,
    pub to_team_name: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub blocked_by: Vec<String>,
    pub artifacts: Vec<StagedRoomArtifact>,
    pub result_summary: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

pub struct PostRoomMessageArgs<'a> {
    pub owner_user_id: &'a str,
    pub room_id: &'a str,
    pub sender_type: &'a str,
    pub sender_team_id: Option<&'a str>,
    pub content: &'a str,
    pub explicit_mentions: Vec<String>,
    pub source_files: Vec<String>,
    pub reply_to_id: Option<String>,
    pub origin_telegram_chat: Option<String>,
    pub wake_targets: Option<Vec<String>>,
}

pub struct TelegramTeamHub {
    bridge_repo: Arc<SqliteCustomBridgeRepository>,
    channel_repo: Arc<dyn IChannelRepository>,
    channel_manager: Arc<ChannelManager>,
    team_service: Arc<TeamSessionService>,
    conversation_repo: Arc<dyn IConversationRepository>,
    provider_repo: Arc<dyn IProviderRepository>,
    settings_repo: Arc<dyn ISettingsRepository>,
    event_bus: Arc<BroadcastEventBus>,
    encryption_key: [u8; 32],
    data_dir: PathBuf,
    active_runs: DashMap<String, ActiveTeamRunTracker>,
    orchestrator: std::sync::OnceLock<crate::bridge::context_compactor::TeamHubOrchestrator>,
}

impl TelegramTeamHub {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bridge_repo: Arc<SqliteCustomBridgeRepository>,
        channel_repo: Arc<dyn IChannelRepository>,
        channel_manager: Arc<ChannelManager>,
        team_service: Arc<TeamSessionService>,
        conversation_repo: Arc<dyn IConversationRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        settings_repo: Arc<dyn ISettingsRepository>,
        event_bus: Arc<BroadcastEventBus>,
        encryption_key: [u8; 32],
        data_dir: PathBuf,
    ) -> Arc<Self> {
        Arc::new(Self {
            bridge_repo,
            channel_repo,
            channel_manager,
            team_service,
            conversation_repo,
            provider_repo,
            settings_repo,
            event_bus,
            encryption_key,
            data_dir,
            active_runs: DashMap::new(),
            orchestrator: std::sync::OnceLock::new(),
        })
    }

    pub fn set_team_hub_orchestrator(&self, orchestrator: crate::bridge::context_compactor::TeamHubOrchestrator) {
        let _ = self.orchestrator.set(orchestrator);
    }

    pub fn team_hub_orchestrator(&self) -> Option<&crate::bridge::context_compactor::TeamHubOrchestrator> {
        self.orchestrator.get()
    }

    pub fn bridge_repo(&self) -> &Arc<SqliteCustomBridgeRepository> {
        &self.bridge_repo
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    pub async fn resolve_locale(&self, user_id: &str) -> HubLocale {
        let lang = self
            .settings_repo
            .get_settings(user_id)
            .await
            .ok()
            .flatten()
            .map(|s| s.language)
            .unwrap_or_else(|| "en-US".to_owned());
        HubLocale::from_lang(&lang)
    }

    /// Spawns the background EventBus watcher that handles:
    /// 1. Desktop-origin Team run tracking (`team.runStarted`)
    /// 2. Immediate crash / rate-limit / quota alerts (`team.agentRuntimeStatusChanged` / `team.runFailed`)
    /// 3. Closed-loop Shared Room synthesis mirroring + Smart Notification Filter (`team.runCompleted`)
    pub fn spawn_event_watcher(self: &Arc<Self>) {
        let mut rx = self.event_bus.subscribe();
        let hub = Arc::clone(self);

        tokio::spawn(async move {
            loop {
                let event = match rx.recv().await {
                    Ok(ev) => ev,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "TelegramTeamHub event watcher lagged; continuing");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };

                let hub_clone = Arc::clone(&hub);
                match event.name.as_str() {
                    TEAM_RUN_STARTED_EVENT => {
                        if let Some(user_id) = event.data.get("user_id").and_then(serde_json::Value::as_str)
                            && let Ok(payload) = serde_json::from_value::<TeamRunPayload>(event.data.clone())
                        {
                            let uid = user_id.to_owned();
                            hub_clone
                                .active_runs
                                .entry(payload.team_id.clone())
                                .or_insert_with(|| ActiveTeamRunTracker {
                                    origin: RunOrigin::Desktop {
                                        owner_user_id: uid.clone(),
                                    },
                                    started_at_ms: now_ms(),
                                    delegated: false,
                                    notify_completed_sent: false,
                                });
                            tokio::spawn(async move {
                                hub_clone.enforce_wallet_fuse_on_run_started(&uid, payload).await;
                            });
                        }
                    }
                    TEAM_AGENT_RUNTIME_STATUS_CHANGED_EVENT => {
                        if let Some(user_id) = event.data.get("user_id").and_then(serde_json::Value::as_str)
                            && let Ok(payload) =
                                serde_json::from_value::<TeamAgentRuntimeStatusPayload>(event.data.clone())
                            && payload.status == TeamAgentRuntimeStatus::Failed
                        {
                            let uid = user_id.to_owned();
                            tokio::spawn(async move {
                                hub_clone.handle_member_runtime_failed(&uid, payload).await;
                            });
                        }
                    }
                    TEAM_RUN_FAILED_EVENT => {
                        if let Some(user_id) = event.data.get("user_id").and_then(serde_json::Value::as_str)
                            && let Ok(payload) = serde_json::from_value::<TeamRunPayload>(event.data.clone())
                        {
                            let uid = user_id.to_owned();
                            tokio::spawn(async move {
                                hub_clone.handle_team_run_failed(&uid, payload).await;
                            });
                        }
                    }
                    TEAM_RUN_CANCELLED_EVENT => {
                        if let Ok(payload) = serde_json::from_value::<TeamRunPayload>(event.data.clone()) {
                            hub_clone.active_runs.remove(&payload.team_id);
                        }
                    }
                    TEAM_REMOVED_EVENT => {
                        if let Some(user_id) = event.data.get("user_id").and_then(serde_json::Value::as_str)
                            && let Some(team_id) = event.data.get("team_id").and_then(serde_json::Value::as_str)
                        {
                            let uid = user_id.to_owned();
                            let tid = team_id.to_owned();
                            tokio::spawn(async move {
                                hub_clone.handle_team_removed(&uid, &tid).await;
                            });
                        }
                    }
                    TEAM_RUN_COMPLETED_EVENT => {
                        if let Some(user_id) = event.data.get("user_id").and_then(serde_json::Value::as_str)
                            && let Ok(payload) = serde_json::from_value::<TeamRunPayload>(event.data.clone())
                        {
                            let uid = user_id.to_owned();
                            tokio::spawn(async move {
                                hub_clone.handle_team_run_completed(&uid, payload).await;
                            });
                        }
                    }
                    "inbox:item_resolved" => {
                        if event.data.get("resolved_by").and_then(serde_json::Value::as_str) == Some("desktop") {
                            let data = event.data.clone();
                            tokio::spawn(async move {
                                hub_clone.sync_desktop_inbox_resolution_to_telegram(data).await;
                            });
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    // -----------------------------------------------------------------------
    // Helpers: Telegram status, destination resolution, formatted send
    // -----------------------------------------------------------------------

    async fn telegram_status_snapshot(
        &self,
        owner_user_id: &str,
    ) -> (bool, bool, Option<String>, Option<String>) {
        let statuses = self
            .channel_manager
            .get_plugin_status(owner_user_id)
            .await
            .unwrap_or_default();
        let tg_plugin = statuses.into_iter().find(|p| p.plugin_type == "telegram");
        let enabled = tg_plugin.as_ref().is_some_and(|p| p.enabled);
        let connected = tg_plugin.as_ref().is_some_and(|p| p.connected);
        let bot_username = tg_plugin.and_then(|p| p.bot_username);

        let authorized_chat_id = self
            .channel_repo
            .get_all_users(owner_user_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|u| u.platform_type == "telegram")
            .max_by_key(|u| u.last_active.unwrap_or(u.authorized_at))
            .map(|u| u.platform_user_id);

        (enabled, connected, bot_username, authorized_chat_id)
    }

    pub async fn set_room_active_dm_target(
        &self,
        owner_user_id: &str,
        room_id: &str,
        active: bool,
    ) -> Result<(), aionui_db::DbError> {
        if active {
            let (_, _, _, auth_dm_chat) = self.telegram_status_snapshot(owner_user_id).await;
            if let Some(dm_chat) = auth_dm_chat {
                self.bridge_repo
                    .set_dm_target(
                        owner_user_id,
                        "telegram",
                        &dm_chat,
                        &dm_chat,
                        CustomTargetType::Room,
                        room_id,
                    )
                    .await?;
            }
        } else {
            let dms = self.bridge_repo.list_dm_targets_for_owner(owner_user_id).await?;
            for dm in dms {
                if dm.target_type == "room" && dm.target_id == room_id {
                    self.bridge_repo
                        .clear_dm_target(owner_user_id, &dm.plugin_id, &dm.chat_id)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn resolve_team_notify_settings(
        &self,
        owner_user_id: &str,
        team_id: &str,
        has_authorized_user: bool,
    ) -> (bool, CustomStreamMode) {
        if let Ok(Some(pref)) = self
            .bridge_repo
            .get_target_notify_pref(owner_user_id, CustomTargetType::Team, team_id)
            .await
        {
            return (pref.enabled, CustomStreamMode::parse(&pref.stream_mode));
        }
        (has_authorized_user, CustomStreamMode::Milestones)
    }

    /// Resolves the best outbound Telegram chat (`chat_id` or `chat_id#thread_id`) for a Team:
    /// 1. Bound Group / Forum Topic in `custom_channel_bindings`
    /// 2. Active DM target chat (`custom_channel_dm_targets`)
    /// 3. Authorized Telegram user's private 1-1 DM (`platform_user_id`)
    async fn resolve_team_telegram_destination(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Option<String> {
        if !self.channel_manager.is_plugin_running(owner_user_id, "telegram") {
            return None;
        }

        if let Ok(bindings) = self
            .bridge_repo
            .list_channel_bindings_for_target(owner_user_id, CustomTargetType::Team, team_id)
            .await
            && let Some(first) = bindings.first()
        {
            return Some(format_chat_with_thread(&first.chat_id, first.thread_id.as_deref()));
        }

        if let Ok(dm_targets) = self.bridge_repo.list_dm_targets_for_owner(owner_user_id).await {
            if let Some(exact) = dm_targets
                .iter()
                .find(|d| d.target_type == "team" && d.target_id == team_id)
            {
                return Some(exact.chat_id.clone());
            }
            if let Some(any_dm) = dm_targets.first() {
                return Some(any_dm.chat_id.clone());
            }
        }

        let (_, _, _, auth_dm_chat) = self.telegram_status_snapshot(owner_user_id).await;
        auth_dm_chat
    }

    async fn resolve_room_telegram_destination(&self, owner_user_id: &str, room_id: &str) -> Option<String> {
        if !self.channel_manager.is_plugin_running(owner_user_id, "telegram") {
            return None;
        }

        if let Ok(bindings) = self
            .bridge_repo
            .list_channel_bindings_for_target(owner_user_id, CustomTargetType::Room, room_id)
            .await
            && let Some(first) = bindings.first()
        {
            return Some(format_chat_with_thread(&first.chat_id, first.thread_id.as_deref()));
        }

        if let Ok(dm_targets) = self.bridge_repo.list_dm_targets_for_owner(owner_user_id).await
            && let Some(exact) = dm_targets
                .iter()
                .find(|d| d.target_type == "room" && d.target_id == room_id)
        {
            return Some(exact.chat_id.clone());
        }

        None
    }

    async fn send_formatted_telegram_message(
        &self,
        owner_user_id: &str,
        reply_chat: &str,
        markdown_text: &str,
        buttons: Option<Vec<Vec<ActionButton>>>,
        reply_to_message_id: Option<String>,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Result<Vec<String>, String> {
        let base_chat_id = split_chat_and_thread(reply_chat).0;
        let mut sent_ids = Vec::new();
        let locale = self.resolve_locale(owner_user_id).await;

        // If report exceeds 8,000 chars, attach full Markdown report as a `.md` document alongside a preview.
        if markdown_text.chars().count() > TELEGRAM_DOCUMENT_THRESHOLD_CHARS {
            let reports_dir = self.data_dir.join("shared-rooms").join("_reports");
            let _ = std::fs::create_dir_all(&reports_dir);
            let file_name = format!("report_{}_{}.md", sanitize_file_name(target_id), now_ms());
            let report_path = reports_dir.join(&file_name);
            if std::fs::write(&report_path, markdown_text.as_bytes()).is_ok() {
                let preview_chars: String = markdown_text.chars().take(1800).collect();
                let footnote = HubI18n::report_preview_footnote(locale, &file_name);
                let preview_md = format!("{preview_chars}{footnote}");
                let preview_html = markdown_to_telegram_html(&preview_md);
                let msg = UnifiedOutgoingMessage {
                    message_type: OutgoingMessageType::Text,
                    text: Some(preview_html),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: None,
                    keyboard: None,
                    image_url: None,
                    file_url: None,
                    file_name: None,
                    media_actions: None,
                    reply_to_message_id,
                    silent: None,
                };
                if let Ok(mid) = self
                    .channel_manager
                    .send_message(owner_user_id, "telegram", reply_chat, msg)
                    .await
                {
                    let _ = self
                        .bridge_repo
                        .record_msg_route("telegram", base_chat_id, &mid, target_type, target_id)
                        .await;
                    sent_ids.push(mid);
                }

                let doc_msg = UnifiedOutgoingMessage {
                    message_type: OutgoingMessageType::File,
                    text: Some(HubI18n::btn_full_report(locale).to_owned()),
                    parse_mode: None,
                    buttons,
                    keyboard: None,
                    image_url: None,
                    file_url: Some(report_path.to_string_lossy().into_owned()),
                    file_name: Some(file_name),
                    media_actions: None,
                    reply_to_message_id: None,
                    silent: None,
                };
                if let Ok(doc_mid) = self
                    .channel_manager
                    .send_message(owner_user_id, "telegram", reply_chat, doc_msg)
                    .await
                {
                    let _ = self
                        .bridge_repo
                        .record_msg_route("telegram", base_chat_id, &doc_mid, target_type, target_id)
                        .await;
                    sent_ids.push(doc_mid);
                }
                return Ok(sent_ids);
            }
        }

        let html = markdown_to_telegram_html(markdown_text);
        let chunks = chunk_telegram_html(&html, TELEGRAM_SAFE_CHUNK_CHARS);
        let total = chunks.len();

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let is_last = idx + 1 == total;
            let outgoing = UnifiedOutgoingMessage {
                message_type: if is_last && buttons.is_some() {
                    OutgoingMessageType::Buttons
                } else {
                    OutgoingMessageType::Text
                },
                text: Some(chunk),
                parse_mode: Some(ParseMode::HTML),
                buttons: if is_last { buttons.clone() } else { None },
                keyboard: None,
                image_url: None,
                file_url: None,
                file_name: None,
                media_actions: None,
                reply_to_message_id: if idx == 0 {
                    reply_to_message_id.clone()
                } else {
                    None
                },
                silent: None,
            };

            let mid = self
                .channel_manager
                .send_message(owner_user_id, "telegram", reply_chat, outgoing)
                .await
                .map_err(|e| e.to_string())?;
            let _ = self
                .bridge_repo
                .record_msg_route("telegram", base_chat_id, &mid, target_type, target_id)
                .await;
            sent_ids.push(mid);
        }

        Ok(sent_ids)
    }

    async fn send_validated_outbound_files(
        &self,
        owner_user_id: &str,
        reply_chat: &str,
        raw_files: &[String],
        team_workspace: Option<&str>,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Vec<String> {
        let base_chat_id = split_chat_and_thread(reply_chat).0;
        let mut delivered = Vec::new();

        for raw in raw_files {
            let valid_path = match validate_outbound_file_path(raw, team_workspace, &self.data_dir) {
                Ok(p) => p,
                Err(reason) => {
                    warn!(file = %raw, error = %reason, "outbound file rejected by path guard");
                    continue;
                }
            };
            let file_name = valid_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("attachment.bin")
                .to_owned();
            let path_str = valid_path.to_string_lossy().into_owned();
            let is_img = is_image_file_name(&file_name);

            let msg = UnifiedOutgoingMessage {
                message_type: if is_img {
                    OutgoingMessageType::Image
                } else {
                    OutgoingMessageType::File
                },
                text: Some(format!("📎 {file_name}")),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                image_url: if is_img { Some(path_str.clone()) } else { None },
                file_url: if !is_img { Some(path_str.clone()) } else { None },
                file_name: Some(file_name.clone()),
                media_actions: None,
                reply_to_message_id: None,
                silent: None,
            };

            if let Ok(mid) = self
                .channel_manager
                .send_message(owner_user_id, "telegram", reply_chat, msg)
                .await
            {
                let _ = self
                    .bridge_repo
                    .record_msg_route("telegram", base_chat_id, &mid, target_type, target_id)
                    .await;
                delivered.push(file_name);
            }
        }

        delivered
    }

    async fn make_team_quick_buttons(
        &self,
        owner_user_id: &str,
        team_id: &str,
        include_approve: bool,
        include_stop: bool,
    ) -> Option<Vec<Vec<ActionButton>>> {
        let locale = self.resolve_locale(owner_user_id).await;
        let mut row1 = Vec::new();
        if include_approve
            && let Ok(tok) = self
                .bridge_repo
                .create_action_token(
                    owner_user_id,
                    CustomTargetType::Team,
                    team_id,
                    "approve_continue",
                    &serde_json::json!({}),
                    None,
                )
                .await
        {
            row1.push(ActionButton {
                label: HubI18n::btn_approve_and_continue(locale).into(),
                action: format!("t:{tok}"),
                params: None,
            });
        }

        let mut row2 = Vec::new();
        if let Ok(tok) = self
            .bridge_repo
            .create_action_token(
                owner_user_id,
                CustomTargetType::Team,
                team_id,
                "view_tasks",
                &serde_json::json!({}),
                None,
            )
            .await
        {
            row2.push(ActionButton {
                label: HubI18n::btn_task_board(locale).into(),
                action: format!("t:{tok}"),
                params: None,
            });
        }
        if include_stop
            && let Ok(tok) = self
                .bridge_repo
                .create_action_token(
                    owner_user_id,
                    CustomTargetType::Team,
                    team_id,
                    "stop_run",
                    &serde_json::json!({}),
                    None,
                )
                .await
        {
            row2.push(ActionButton {
                label: HubI18n::btn_stop(locale).into(),
                action: format!("t:{tok}"),
                params: None,
            });
        }

        let mut rows = Vec::new();
        if !row1.is_empty() {
            rows.push(row1);
        }
        if !row2.is_empty() {
            rows.push(row2);
        }
        (!rows.is_empty()).then_some(rows)
    }

    fn find_lead_info(team: &TeamResponse) -> (String, String) {
        let lead = team
            .assistants
            .iter()
            .find(|a| {
                a.role.eq_ignore_ascii_case("lead")
                    || a.role.eq_ignore_ascii_case("leader")
                    || team.leader_assistant_id.as_deref() == Some(a.slot_id.as_str())
            })
            .or_else(|| team.assistants.first());
        match lead {
            Some(a) => (a.name.clone(), a.conversation_id.clone()),
            None => ("Leader".to_owned(), String::new()),
        }
    }

    async fn read_latest_lead_synthesis(
        &self,
        owner_user_id: &str,
        team: &TeamResponse,
        since_ms: i64,
    ) -> Option<String> {
        let (_, lead_conv_id) = Self::find_lead_info(team);
        if lead_conv_id.is_empty() {
            return None;
        }

        let page = self
            .conversation_repo
            .list_messages_page(
                owner_user_id,
                &lead_conv_id,
                &MessagePageParams {
                    limit: 40,
                    direction: MessagePageDirection::InitialLatest,
                },
            )
            .await
            .ok()?;

        let min_ts = since_ms.saturating_sub(2_000);
        for row in page.items.iter().rev() {
            if row.created_at < min_ts || row.hidden || row.r#type != "text" || row.position.as_deref() != Some("left")
            {
                continue;
            }
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&row.content) {
                if val.get("teammate_message").and_then(serde_json::Value::as_bool) == Some(true) {
                    continue;
                }
                if let Some(text) = val.get("content").and_then(serde_json::Value::as_str) {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.to_owned());
                    }
                }
            }
        }
        None
    }

    // -----------------------------------------------------------------------
    // EventBus lifecycle handlers
    // -----------------------------------------------------------------------

    async fn handle_team_removed(&self, owner_user_id: &str, team_id: &str) {
        self.active_runs.remove(team_id);
        match self.bridge_repo.cleanup_deleted_team(owner_user_id, team_id).await {
            Ok(affected_room_ids) => {
                for rid in affected_room_ids {
                    if let Ok(room_dto) = self.build_shared_room_dto(owner_user_id, &rid).await {
                        self.event_bus.broadcast(WebSocketMessage::new(
                            "sharedRoom.updated",
                            serde_json::json!({
                                "user_id": owner_user_id,
                                "room": room_dto,
                            }),
                        ));
                    }
                }
            }
            Err(err) => {
                warn!(team_id = %team_id, error = %err, "failed to clean up deleted team references in bridge repo");
            }
        }
    }

    async fn handle_member_runtime_failed(&self, owner_user_id: &str, payload: TeamAgentRuntimeStatusPayload) {
        let Ok(team) = self.team_service.get_team(owner_user_id, &payload.team_id).await else {
            return;
        };
        let member_name = team
            .assistants
            .iter()
            .find(|a| a.slot_id == payload.slot_id)
            .map(|a| a.name.clone())
            .unwrap_or_else(|| payload.slot_id.clone());
        let reason = payload
            .error
            .unwrap_or_else(|| "Runtime process exited or hit rate/quota limit".to_owned());

        let Some(reply_chat) = self
            .resolve_team_telegram_destination(owner_user_id, &payload.team_id)
            .await
        else {
            return;
        };

        let mut buttons = Vec::new();
        let locale = self.resolve_locale(owner_user_id).await;
        if let Ok(tok) = self
            .bridge_repo
            .create_action_token(
                owner_user_id,
                CustomTargetType::Team,
                &payload.team_id,
                "retry_member",
                &serde_json::json!({ "slot_id": payload.slot_id }),
                None,
            )
            .await
        {
            buttons.push(vec![ActionButton {
                label: HubI18n::btn_retry_member(locale, &member_name),
                action: format!("t:{tok}"),
                params: None,
            }]);
        }

        let alert_md = HubI18n::member_warning(locale, &team.name, &member_name, &reason);
        let _ = self
            .send_formatted_telegram_message(
                owner_user_id,
                &reply_chat,
                &alert_md,
                (!buttons.is_empty()).then_some(buttons),
                None,
                CustomTargetType::Team,
                &payload.team_id,
            )
            .await;
    }

    async fn handle_team_run_failed(&self, owner_user_id: &str, payload: TeamRunPayload) {
        let tracker = self.active_runs.remove(&payload.team_id).map(|(_, v)| v);
        let Ok(team) = self.team_service.get_team(owner_user_id, &payload.team_id).await else {
            return;
        };

        let reply_chat = match &tracker {
            Some(ActiveTeamRunTracker {
                origin: RunOrigin::Telegram { reply_chat, .. },
                ..
            }) => Some(reply_chat.clone()),
            _ => self.resolve_team_telegram_destination(owner_user_id, &payload.team_id).await,
        };

        if let Some(chat) = reply_chat {
            let (lead_name, _) = Self::find_lead_info(&team);
            let locale = self.resolve_locale(owner_user_id).await;
            let md = HubI18n::run_error_notice(locale, &team.name, &lead_name, &payload.team_run_id);
            let buttons = self
                .make_team_quick_buttons(owner_user_id, &payload.team_id, false, false)
                .await;
            let _ = self
                .send_formatted_telegram_message(
                    owner_user_id,
                    &chat,
                    &md,
                    buttons,
                    None,
                    CustomTargetType::Team,
                    &payload.team_id,
                )
                .await;
        }
    }

    pub fn classify_team_lead_wallet(team: &TeamResponse) -> (String, WalletType) {
        let lead = team
            .assistants
            .iter()
            .find(|a| {
                a.role.eq_ignore_ascii_case("lead")
                    || a.role.eq_ignore_ascii_case("leader")
                    || team.leader_assistant_id.as_deref() == Some(a.slot_id.as_str())
            })
            .or_else(|| team.assistants.first());

        let Some(agent) = lead else {
            return ("lead".to_owned(), WalletType::CliSubscription);
        };

        if agent.backend.trim().eq_ignore_ascii_case("aionrs") {
            return (agent.slot_id.clone(), WalletType::ApiKey);
        }

        let raw_backend = if !agent.backend.trim().is_empty()
            && !agent.backend.eq_ignore_ascii_case("custom")
            && !agent.backend.eq_ignore_ascii_case("acp")
        {
            agent.backend.trim()
        } else if !agent.assistant_backend.trim().is_empty()
            && !agent.assistant_backend.eq_ignore_ascii_case("custom")
            && !agent.assistant_backend.eq_ignore_ascii_case("acp")
        {
            agent.assistant_backend.trim()
        } else {
            agent.model.trim()
        };

        let normalized = raw_backend.to_lowercase();
        let backend_hint = if normalized == "aionrs" || normalized.contains('/') {
            "aionrs"
        } else if normalized.starts_with("claude") {
            "claude"
        } else if normalized.starts_with("codex") {
            "codex"
        } else if normalized.starts_with("gemini") {
            "gemini"
        } else if normalized.starts_with("antigravity") {
            "antigravity"
        } else {
            normalized.as_str()
        };

        (
            agent.slot_id.clone(),
            classify_agent_wallet(Some(backend_hint), None, false),
        )
    }

    async fn is_team_api_key_hard_blocked(
        &self,
        owner_user_id: &str,
        team: &TeamResponse,
    ) -> Option<(f64, f64)> {
        let (_, wallet_type) = Self::classify_team_lead_wallet(team);
        if wallet_type != WalletType::ApiKey {
            return None;
        }
        let Ok(wallet) = self.bridge_repo.get_or_create_cost_wallet(owner_user_id).await else {
            return None;
        };
        if wallet.is_hard_blocked() {
            Some((wallet.api_key_spent_usd, wallet.api_key_budget_limit_usd))
        } else {
            None
        }
    }

    async fn enforce_wallet_fuse_on_run_started(&self, owner_user_id: &str, payload: TeamRunPayload) {
        let Ok(team) = self.team_service.get_team(owner_user_id, &payload.team_id).await else {
            return;
        };
        if let Some((spent, limit)) = self.is_team_api_key_hard_blocked(owner_user_id, &team).await {
            self.active_runs.remove(&payload.team_id);
            let locale = self.resolve_locale(owner_user_id).await;
            let notice = HubI18n::wallet_hard_blocked_notice(locale, &team.name, spent, limit);
            let _ = self
                .team_service
                .cancel_run(
                    owner_user_id,
                    &payload.team_id,
                    &payload.team_run_id,
                    None,
                    Some(notice),
                )
                .await;
        }
    }

    async fn record_team_run_cost(
        &self,
        owner_user_id: &str,
        team: &TeamResponse,
        run_id: &str,
        synthesis_chars: usize,
    ) {
        let (agent_id, wallet_type) = Self::classify_team_lead_wallet(team);
        let tokens_out = ((synthesis_chars as i64) / 4).max(64);
        let tokens_in = 512_i64;
        let cost_usd = match wallet_type {
            WalletType::CliSubscription => 0.0,
            WalletType::ApiKey => ((tokens_in as f64 * 0.000003) + (tokens_out as f64 * 0.000015)).max(0.01),
        };

        if let Ok((wallet, newly_soft, newly_hard)) = self
            .bridge_repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: owner_user_id.to_owned(),
                run_id: run_id.to_owned(),
                team_id: team.id.clone(),
                agent_id,
                wallet_type: wallet_type.as_str().to_owned(),
                tokens_input: tokens_in,
                tokens_output: tokens_out,
                cost_usd,
            })
            .await
        {
            self.event_bus.broadcast(WebSocketMessage::new(
                "wallet:spend_updated",
                serde_json::json!({
                    "user_id": owner_user_id,
                    "spent_usd": wallet.api_key_spent_usd,
                    "budget_limit_usd": wallet.api_key_budget_limit_usd,
                    "cli_subscription_runs": wallet.cli_subscription_runs_count,
                    "api_key_runs": wallet.api_key_runs_count,
                    "soft_warning": wallet.is_soft_warning(),
                    "hard_blocked": wallet.is_hard_blocked(),
                }),
            ));

            if (newly_soft || newly_hard)
                && let Ok(items) = self
                    .bridge_repo
                    .list_action_inbox_items(owner_user_id, Some("pending"))
                    .await
                && let Some(latest_budget_card) = items.into_iter().find(|r| r.category == "budget_warning")
            {
                self.event_bus.broadcast(WebSocketMessage::new(
                    "inbox:item_added",
                    serde_json::json!({
                        "user_id": owner_user_id,
                        "item": ActionInboxCardDto::from_row(latest_budget_card),
                    }),
                ));
            }
        }
    }

    pub async fn sync_desktop_inbox_resolution_to_telegram(&self, data: serde_json::Value) {
        let Some(owner_user_id) = data.get("user_id").and_then(serde_json::Value::as_str) else {
            return;
        };
        let Some(card_id) = data.get("id").and_then(serde_json::Value::as_str) else {
            return;
        };
        let action = data
            .get("action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("resolved");

        let (tg_chat_opt, tg_msg_opt, title) = if let Ok(Some(card)) = self.bridge_repo.get_action_inbox_item(card_id).await {
            (
                card.telegram_chat_id,
                card.telegram_msg_id,
                card.title,
            )
        } else {
            (
                data.get("telegram_chat_id").and_then(serde_json::Value::as_str).map(str::to_owned),
                data.get("telegram_msg_id").and_then(serde_json::Value::as_str).map(str::to_owned),
                data.get("title").and_then(serde_json::Value::as_str).unwrap_or(card_id).to_owned(),
            )
        };

        let (Some(chat_id), Some(msg_id)) = (tg_chat_opt, tg_msg_opt) else {
            return;
        };

        let locale = self.resolve_locale(owner_user_id).await;
        let updated_md = HubI18n::inbox_resolved_on_desktop(locale, &title, action);
        let edit_msg = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Text,
            text: Some(markdown_to_telegram_html(&updated_md)),
            parse_mode: Some(ParseMode::HTML),
            buttons: None,
            keyboard: None,
            image_url: None,
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };
        let base_chat = split_chat_and_thread(&chat_id).0;
        let _ = self
            .channel_manager
            .edit_message(owner_user_id, "telegram", base_chat, &msg_id, edit_msg)
            .await;
    }

    async fn handle_team_run_completed(&self, owner_user_id: &str, payload: TeamRunPayload) {
        let tracker = self.active_runs.remove(&payload.team_id).map(|(_, v)| v);
        let Ok(team) = self.team_service.get_team(owner_user_id, &payload.team_id).await else {
            return;
        };

        let started_at_ms = tracker
            .as_ref()
            .map(|t| t.started_at_ms)
            .unwrap_or_else(|| now_ms().saturating_sub(60_000));
        let cached_synthesis = self
            .read_latest_lead_synthesis(owner_user_id, &team, started_at_ms)
            .await;
        self.record_team_run_cost(
            owner_user_id,
            &team,
            &payload.team_run_id,
            cached_synthesis.as_deref().map_or(0, str::len),
        )
        .await;

        // 1. Closed-Loop Shared Room Handoff Guarantee (Layer 2):
        // If this Team had any pending Shared Room handoff and went idle without calling `team_room_post`,
        // automatically mirror the Lead's final synthesis into the Shared Room (0 extra LLM turns!).
        if let Ok(pending_rooms) = self.bridge_repo.list_pending_handoffs_for_team(&payload.team_id).await
            && !pending_rooms.is_empty()
        {
            if let Some(ref synthesis) = cached_synthesis {
                for (room, _member) in pending_rooms {
                    let _ = self
                        .post_message_to_shared_room(PostRoomMessageArgs {
                            owner_user_id: &room.owner_user_id,
                            room_id: &room.id,
                            sender_type: "leader",
                            sender_team_id: Some(&payload.team_id),
                            content: synthesis,
                            explicit_mentions: Vec::new(),
                            source_files: Vec::new(),
                            reply_to_id: None,
                            origin_telegram_chat: None,
                            wake_targets: None,
                        })
                        .await;
                }
            } else {
                for (room, _) in pending_rooms {
                    let _ = self
                        .bridge_repo
                        .set_member_pending_handoff(&room.id, &payload.team_id, None, 0)
                        .await;
                }
            }
        }

        // 2. Source-Aware Smart Notification Filter for Team -> Telegram reporting
        if tracker.as_ref().is_some_and(|t| t.notify_completed_sent) {
            return;
        }

        let (_, _, _, auth_dm) = self.telegram_status_snapshot(owner_user_id).await;
        let (notify_enabled, stream_mode) = self
            .resolve_team_notify_settings(owner_user_id, &payload.team_id, auth_dm.is_some())
            .await;

        let (should_send, target_chat, reply_to_msg_id) = match tracker.as_ref().map(|t| &t.origin) {
            Some(RunOrigin::Telegram {
                reply_chat,
                reply_to_msg_id,
                ..
            }) => (true, Some(reply_chat.clone()), reply_to_msg_id.clone()),
            Some(RunOrigin::SharedRoom { .. }) => {
                // Already mirrored into Shared Room (which mirrors to room's Telegram if bound).
                (false, None, None)
            }
            Some(RunOrigin::Desktop { .. }) | None => {
                if !notify_enabled {
                    (false, None, None)
                } else {
                    let elapsed = now_ms().saturating_sub(started_at_ms);
                    let delegated = tracker.as_ref().is_some_and(|t| t.delegated);
                    let pass_filter = stream_mode == CustomStreamMode::FullStream
                        || delegated
                        || elapsed >= SMART_FILTER_DESKTOP_DURATION_MS;
                    let dest = if pass_filter {
                        self.resolve_team_telegram_destination(owner_user_id, &payload.team_id)
                            .await
                    } else {
                        None
                    };
                    (pass_filter && dest.is_some(), dest, None)
                }
            }
        };

        if !should_send {
            return;
        }
        let Some(reply_chat) = target_chat else {
            return;
        };
        let Some(synthesis) = self
            .read_latest_lead_synthesis(owner_user_id, &team, started_at_ms)
            .await
        else {
            return;
        };

        let (lead_name, _) = Self::find_lead_info(&team);
        let formatted_md = format!("✅ **[{team_name} · {lead_name} 👑]**\n\n{synthesis}", team_name = team.name);
        let buttons = self
            .make_team_quick_buttons(owner_user_id, &payload.team_id, false, false)
            .await;

        let _ = self
            .send_formatted_telegram_message(
                owner_user_id,
                &reply_chat,
                &formatted_md,
                buttons,
                reply_to_msg_id,
                CustomTargetType::Team,
                &payload.team_id,
            )
            .await;
    }

    // -----------------------------------------------------------------------
    // Inbound Telegram -> Team / Shared Room dispatch helpers
    // -----------------------------------------------------------------------

    async fn download_inbound_files(
        &self,
        owner_user_id: &str,
        msg: &UnifiedIncomingMessage,
        team_workspace: Option<&str>,
        fallback_id: &str,
    ) -> Vec<String> {
        let Some(attachments) = &msg.content.attachments else {
            return Vec::new();
        };
        let mut saved_paths = Vec::new();
        for att in attachments {
            let Some(file_id) = att.file_id.as_deref() else {
                continue;
            };
            match self
                .channel_manager
                .download_attachment(owner_user_id, "telegram", file_id)
                .await
            {
                Ok((bytes, remote_path)) => {
                    let raw_name = att
                        .file_name
                        .as_deref()
                        .or(remote_path.as_deref())
                        .unwrap_or("telegram_upload.bin");
                    if let Ok(local_path) =
                        save_inbound_telegram_attachment(&bytes, raw_name, team_workspace, &self.data_dir, fallback_id)
                    {
                        saved_paths.push(local_path.to_string_lossy().into_owned());
                    }
                }
                Err(err) => {
                    warn!(file_id = %file_id, error = %err, "failed to download inbound Telegram attachment");
                }
            }
        }
        saved_paths
    }

    async fn dispatch_telegram_message_to_team(
        &self,
        owner_user_id: &str,
        team_id: &str,
        msg: &UnifiedIncomingMessage,
    ) -> Result<bool, ChannelError> {
        let Ok(team) = self.team_service.get_team(owner_user_id, team_id).await else {
            return Ok(false);
        };
        let reply_chat = format_chat_with_thread(&msg.chat_id, msg.thread_id().as_deref());
        let saved_files = self
            .download_inbound_files(owner_user_id, msg, Some(&team.workspace), team_id)
            .await;

        let chat_file_refs: Option<Vec<ChatFileRef>> = if saved_files.is_empty() {
            None
        } else {
            Some(
                saved_files
                    .iter()
                    .map(|p| ChatFileRef::Local { path: p.clone() })
                    .collect(),
            )
        };

        let mut user_text = msg.content.text.trim().to_owned();
        let locale = self.resolve_locale(owner_user_id).await;
        if user_text.is_empty() && !saved_files.is_empty() {
            user_text = HubI18n::telegram_attachment_notice(locale, saved_files.len());
        }
        if user_text.is_empty() {
            return Ok(true);
        }

        if let Some((spent, limit)) = self.is_team_api_key_hard_blocked(owner_user_id, &team).await {
            let notice = HubI18n::wallet_hard_blocked_notice(locale, &team.name, spent, limit);
            let _ = self
                .send_formatted_telegram_message(
                    owner_user_id,
                    &reply_chat,
                    &notice,
                    None,
                    Some(msg.id.clone()),
                    CustomTargetType::Team,
                    team_id,
                )
                .await;
            return Ok(true);
        }

        self.active_runs.insert(
            team_id.to_owned(),
            ActiveTeamRunTracker {
                origin: RunOrigin::Telegram {
                    owner_user_id: owner_user_id.to_owned(),
                    plugin_id: "telegram".to_owned(),
                    reply_chat: reply_chat.clone(),
                    reply_to_msg_id: Some(msg.id.clone()),
                },
                started_at_ms: now_ms(),
                delegated: false,
                notify_completed_sent: false,
            },
        );

        match self
            .team_service
            .send_message(owner_user_id, team_id, &user_text, chat_file_refs)
            .await
        {
            Ok(_ack) => {
                let (lead_name, _) = Self::find_lead_info(&team);
                let ack_md = HubI18n::request_received_processing(locale, &team.name, &lead_name);
                let stop_btns = self.make_team_quick_buttons(owner_user_id, team_id, false, true).await;
                let _ = self
                    .send_formatted_telegram_message(
                        owner_user_id,
                        &reply_chat,
                        &ack_md,
                        stop_btns,
                        Some(msg.id.clone()),
                        CustomTargetType::Team,
                        team_id,
                    )
                    .await;
                Ok(true)
            }
            Err(err) => {
                self.active_runs.remove(team_id);
                let err_md = HubI18n::unable_to_send_team_message(locale, &team.name, &err.to_string());
                let _ = self
                    .send_formatted_telegram_message(
                        owner_user_id,
                        &reply_chat,
                        &err_md,
                        None,
                        Some(msg.id.clone()),
                        CustomTargetType::Team,
                        team_id,
                    )
                    .await;
                Ok(true)
            }
        }
    }

    async fn dispatch_telegram_message_to_room(
        &self,
        owner_user_id: &str,
        room_id: &str,
        msg: &UnifiedIncomingMessage,
    ) -> Result<bool, ChannelError> {
        let Ok(Some(_room)) = self.bridge_repo.get_shared_room(owner_user_id, room_id).await else {
            return Ok(false);
        };
        let reply_chat = format_chat_with_thread(&msg.chat_id, msg.thread_id().as_deref());
        let saved_files = self.download_inbound_files(owner_user_id, msg, None, room_id).await;
        let locale = self.resolve_locale(owner_user_id).await;

        let mut user_text = msg.content.text.trim().to_owned();
        if user_text.is_empty() && !saved_files.is_empty() {
            user_text = HubI18n::shared_room_attached_files(locale, saved_files.len());
        }
        if user_text.is_empty() {
            return Ok(true);
        }

        let res = self
            .post_message_to_shared_room(PostRoomMessageArgs {
                owner_user_id,
                room_id,
                sender_type: "user",
                sender_team_id: None,
                content: &user_text,
                explicit_mentions: Vec::new(),
                source_files: saved_files,
                reply_to_id: None,
                origin_telegram_chat: Some(reply_chat.clone()),
                wake_targets: None,
            })
            .await;

        if let Ok((_, routed)) = res
            && !routed.is_empty()
        {
            let woken_names: Vec<String> = routed.iter().map(|r| format!("`{}`", r.team_id)).collect();
            let ack = HubI18n::shared_room_routed_notice(locale, routed.len(), &woken_names.join(", "));
            let _ = self
                .send_formatted_telegram_message(
                    owner_user_id,
                    &reply_chat,
                    &ack,
                    None,
                    Some(msg.id.clone()),
                    CustomTargetType::Room,
                    room_id,
                )
                .await;
        }
        Ok(true)
    }

    async fn build_status_card_for_team(&self, owner_user_id: &str, team_id: &str) -> Result<(String, Option<Vec<Vec<ActionButton>>>), String> {
        let team = self
            .team_service
            .get_team(owner_user_id, team_id)
            .await
            .map_err(|e| e.to_string())?;
        let run_state = self.team_service.get_run_state(owner_user_id, team_id).await.ok();
        let tasks = self
            .team_service
            .list_team_tasks(owner_user_id, team_id, 25)
            .await
            .unwrap_or_default();

        let has_active_run = run_state.as_ref().and_then(|r| r.active_run.as_ref()).is_some();
        let locale = self.resolve_locale(owner_user_id).await;
        let run_badge = match run_state.as_ref().and_then(|r| r.active_run.as_ref()) {
            Some(r) => HubI18n::status_running(locale, &format!("{:?}", r.status)),
            None => HubI18n::status_idle(locale).to_owned(),
        };

        let mut lines = vec![
            HubI18n::status_board_header(locale, &team.name),
            HubI18n::status_board_status(locale, &run_badge),
        ];
        if !team.workspace.trim().is_empty() {
            lines.push(format!("• **Workspace:** `{}`", team.workspace));
        }

        lines.push(HubI18n::status_board_members(locale).to_owned());
        for a in &team.assistants {
            let role_icon = if a.role.eq_ignore_ascii_case("lead") || a.role.eq_ignore_ascii_case("leader") {
                "👑"
            } else {
                "🤖"
            };
            let st = a.status.as_deref().unwrap_or("idle");
            lines.push(format!("• {role_icon} **{}** (`{}`) — _{st}_", a.name, a.model));
        }

        if tasks.is_empty() {
            lines.push(HubI18n::taskboard_empty(locale).to_owned());
        } else {
            let completed = tasks.iter().filter(|t| t.status == "completed").count();
            let in_prog = tasks.iter().filter(|t| t.status == "in_progress").count();
            let pending = tasks.iter().filter(|t| t.status == "pending").count();
            lines.push(HubI18n::taskboard_counts(
                locale,
                tasks.len(),
                completed,
                in_prog,
                pending,
            ));
            for t in tasks.iter().take(8) {
                let icon = match t.status.as_str() {
                    "completed" => "✅",
                    "in_progress" => "🔄",
                    _ => "⏳",
                };
                let owner = t.owner.as_deref().unwrap_or("-");
                lines.push(format!("  {icon} `{}` {} _(owner: {owner})_", t.id, t.subject));
            }
        }

        let buttons = self
            .make_team_quick_buttons(owner_user_id, team_id, false, has_active_run)
            .await;
        Ok((lines.join("\n"), buttons))
    }

    async fn build_status_card_for_room(&self, owner_user_id: &str, room_id: &str) -> Result<(String, Option<Vec<Vec<ActionButton>>>), String> {
        let dto = self.build_shared_room_dto(owner_user_id, room_id).await?;
        let tasks = self.list_shared_room_task_dtos(owner_user_id, room_id).await?;
        let locale = self.resolve_locale(owner_user_id).await;

        let mut lines = vec![
            HubI18n::shared_room_header(locale, &dto.name),
            HubI18n::shared_room_hops(locale, dto.hop_count_since_human, dto.max_hops),
            HubI18n::shared_room_participating(locale).to_owned(),
        ];
        for m in &dto.members {
            let crown = if m.is_host {
                HubI18n::role_chair(locale)
            } else {
                HubI18n::role_leader(locale)
            };
            let handoff = if m.pending_handoff_msg_id.is_some() {
                HubI18n::handling_handoff_indicator(locale)
            } else {
                ""
            };
            lines.push(format!("• **@{}** ({crown}: {}){handoff}", m.team_name, m.leader_name));
        }

        if tasks.is_empty() {
            lines.push(HubI18n::cross_team_tasks_empty(locale).to_owned());
        } else {
            lines.push(HubI18n::cross_team_tasks_header(locale, tasks.len()));
            for t in tasks.iter().take(8) {
                let icon = match t.status.as_str() {
                    "completed" => "✅",
                    "in_progress" => "🔄",
                    _ => "⏳",
                };
                let blocked_note = if t.blocked_by.is_empty() {
                    String::new()
                } else {
                    HubI18n::blocked_tag(locale, &t.blocked_by.join(","))
                };
                lines.push(format!(
                    "  {icon} `{}` → **@{}**: {}{}",
                    t.id, t.to_team_name, t.title, blocked_note
                ));
            }
        }

        let mut rows = Vec::new();
        if dto.hop_count_since_human >= dto.max_hops
            && let Ok(tok) = self
                .bridge_repo
                .create_action_token(
                    owner_user_id,
                    CustomTargetType::Room,
                    room_id,
                    "allow_more_hops",
                    &serde_json::json!({}),
                    None,
                )
                .await
        {
            rows.push(vec![ActionButton {
                label: HubI18n::btn_allow_more_hops(locale).into(),
                action: format!("t:{tok}"),
                params: None,
            }]);
        }

        Ok((lines.join("\n"), (!rows.is_empty()).then_some(rows)))
    }

    // -----------------------------------------------------------------------
    // Shared Room Core Services (used by REST routes, Telegram & MCP/CLI tools)
    // -----------------------------------------------------------------------

    pub async fn build_shared_room_dto(&self, owner_user_id: &str, room_id: &str) -> Result<SharedRoomDto, String> {
        let room = self
            .bridge_repo
            .get_shared_room(owner_user_id, room_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Shared room not found: {room_id}"))?;

        self.hydrate_shared_room_dto(owner_user_id, room).await
    }

    pub async fn list_shared_room_dtos(&self, owner_user_id: &str) -> Result<Vec<SharedRoomDto>, String> {
        let rows = self
            .bridge_repo
            .list_shared_rooms(owner_user_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if let Ok(dto) = self.hydrate_shared_room_dto(owner_user_id, row).await {
                out.push(dto);
            }
        }
        Ok(out)
    }

    async fn hydrate_shared_room_dto(
        &self,
        owner_user_id: &str,
        room: CustomSharedRoomRow,
    ) -> Result<SharedRoomDto, String> {
        let teams = self
            .team_service
            .list_teams(owner_user_id)
            .await
            .unwrap_or_default();
        let team_map: HashMap<String, TeamResponse> = teams.into_iter().map(|t| (t.id.clone(), t)).collect();

        let member_rows = self
            .bridge_repo
            .list_shared_room_members(&room.id)
            .await
            .map_err(|e| e.to_string())?;

        let mut members = Vec::with_capacity(member_rows.len());
        for m in member_rows {
            let Some(t) = team_map.get(&m.team_id) else {
                continue;
            };
            let (leader_name, _) = Self::find_lead_info(t);
            let is_host = room.host_team_id.as_deref() == Some(m.team_id.as_str());
            members.push(SharedRoomMemberDto {
                team_id: m.team_id,
                team_name: t.name.clone(),
                workspace: t.workspace.clone(),
                leader_name,
                is_host,
                pending_handoff_msg_id: m.pending_handoff_msg_id,
            });
        }

        let bindings = self
            .bridge_repo
            .list_channel_bindings_for_target(owner_user_id, CustomTargetType::Room, &room.id)
            .await
            .unwrap_or_default();
        let first_binding = bindings.first();

        let is_active_dm_target = self
            .bridge_repo
            .list_dm_targets_for_owner(owner_user_id)
            .await
            .unwrap_or_default()
            .iter()
            .any(|d| d.target_type == "room" && d.target_id == room.id);

        Ok(SharedRoomDto {
            id: room.id,
            name: room.name,
            description: room.description,
            rules: room.rules,
            host_team_id: room.host_team_id,
            max_hops: room.max_hops,
            hop_count_since_human: room.hop_count_since_human,
            artifacts_dir: room.artifacts_dir,
            members,
            telegram_chat_id: first_binding.map(|b| b.chat_id.clone()),
            telegram_thread_id: first_binding.and_then(|b| b.thread_id.clone()),
            is_active_dm_target,
            created_at: room.created_at,
            updated_at: room.updated_at,
        })
    }

    pub fn map_room_message_dto(row: CustomSharedRoomMessageRow) -> SharedRoomMessageDto {
        let attachments: Vec<StagedRoomArtifact> = serde_json::from_str(&row.attachments_json).unwrap_or_default();
        let target_team_ids: Vec<String> = serde_json::from_str(&row.target_team_ids_json).unwrap_or_default();
        SharedRoomMessageDto {
            id: row.id,
            room_id: row.room_id,
            sender_type: row.sender_type,
            sender_team_id: row.sender_team_id,
            sender_label: row.sender_label,
            reply_to_id: row.reply_to_id,
            content: row.content,
            attachments,
            target_team_ids,
            hop_index: row.hop_index,
            created_at: row.created_at,
        }
    }

    pub async fn list_shared_room_task_dtos(
        &self,
        owner_user_id: &str,
        room_id: &str,
    ) -> Result<Vec<SharedRoomTaskDto>, String> {
        let teams = self
            .team_service
            .list_teams(owner_user_id)
            .await
            .unwrap_or_default();
        let name_map: HashMap<String, String> = teams.into_iter().map(|t| (t.id, t.name)).collect();

        let rows = self
            .bridge_repo
            .list_shared_room_tasks(room_id)
            .await
            .map_err(|e| e.to_string())?;
        Ok(rows
            .into_iter()
            .map(|r| Self::map_room_task_dto(r, &name_map))
            .collect())
    }

    fn map_room_task_dto(row: CustomSharedRoomTaskRow, name_map: &HashMap<String, String>) -> SharedRoomTaskDto {
        let blocked_by: Vec<String> = serde_json::from_str(&row.blocked_by_json).unwrap_or_default();
        let artifacts: Vec<StagedRoomArtifact> = serde_json::from_str(&row.artifact_paths_json).unwrap_or_default();
        let from_team_name = row
            .from_team_id
            .as_ref()
            .map(|id| name_map.get(id).cloned().unwrap_or_else(|| id.clone()));
        let to_team_name = name_map
            .get(&row.to_team_id)
            .cloned()
            .unwrap_or_else(|| row.to_team_id.clone());

        SharedRoomTaskDto {
            id: row.id,
            room_id: row.room_id,
            from_team_id: row.from_team_id,
            from_team_name,
            to_team_id: row.to_team_id,
            to_team_name,
            title: row.title,
            description: row.description,
            status: row.status,
            blocked_by,
            artifacts,
            result_summary: row.result_summary,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }

    async fn resolve_team_id_in_room(
        &self,
        owner_user_id: &str,
        room_id: &str,
        raw_team_ref: &str,
    ) -> Result<String, String> {
        let dto = self.build_shared_room_dto(owner_user_id, room_id).await?;
        let needle = raw_team_ref.trim().trim_start_matches('@').to_lowercase();
        for m in &dto.members {
            if m.team_id.eq_ignore_ascii_case(&needle)
                || m.team_name.to_lowercase() == needle
                || crate::bridge::judge::mention_slug(&m.team_name) == crate::bridge::judge::mention_slug(&needle)
            {
                return Ok(m.team_id.clone());
            }
        }
        Err(format!(
            "Team '{raw_team_ref}' is not a participant in room '{}'",
            dto.name
        ))
    }

    async fn resolve_default_room_for_team(&self, owner_user_id: &str, team_id: &str) -> Result<String, String> {
        let rooms = self
            .bridge_repo
            .list_shared_rooms_for_team(owner_user_id, team_id)
            .await
            .map_err(|e| e.to_string())?;
        match rooms.first() {
            Some(r) => Ok(r.id.clone()),
            None => Err(format!(
                "Team '{team_id}' is not currently added to any Shared Room"
            )),
        }
    }

    pub async fn post_message_to_shared_room(
        &self,
        args: PostRoomMessageArgs<'_>,
    ) -> Result<(SharedRoomMessageDto, Vec<RoutedTarget>), String> {
        let room_dto = self.build_shared_room_dto(args.owner_user_id, args.room_id).await?;

        if args.sender_type == "user" {
            let _ = self.bridge_repo.reset_room_hops(args.room_id).await;
        } else if args.sender_type == "leader"
            && let Some(stid) = args.sender_team_id
        {
            let _ = self
                .bridge_repo
                .set_member_pending_handoff(args.room_id, stid, None, 0)
                .await;
        }

        let locale = self.resolve_locale(args.owner_user_id).await;
        let sender_label = match (args.sender_type, args.sender_team_id) {
            ("user", _) => HubI18n::room_user_sender_label(locale).to_owned(),
            ("leader", Some(tid)) => room_dto
                .members
                .iter()
                .find(|m| m.team_id == tid)
                .map(|m| format!("{} · {} 👑", m.team_name, m.leader_name))
                .unwrap_or_else(|| format!("{tid} · Leader 👑")),
            _ => "System".to_owned(),
        };

        let source_ws = args.sender_team_id.and_then(|tid| {
            room_dto
                .members
                .iter()
                .find(|m| m.team_id == tid)
                .map(|m| m.workspace.as_str())
        });
        let recipient_workspaces: Vec<String> = room_dto
            .members
            .iter()
            .filter(|m| Some(m.team_id.as_str()) != args.sender_team_id && !m.workspace.trim().is_empty())
            .map(|m| m.workspace.clone())
            .collect();

        let allow_user_local_files = args.sender_type == "user" && args.sender_team_id.is_none();
        let artifact_batch = stage_room_artifacts_batch(
            args.room_id,
            &args.source_files,
            source_ws,
            &self.data_dir,
            &recipient_workspaces,
            allow_user_local_files,
        )?;
        let staged_artifacts = artifact_batch.artifacts;
        let recipient_inbox_paths = artifact_batch.recipient_inbox_paths;

        let reply_sender_team_id = if let Some(ref rid) = args.reply_to_id {
            self.bridge_repo
                .get_shared_room_message(args.room_id, rid)
                .await
                .ok()
                .flatten()
                .and_then(|m| m.sender_team_id)
        } else {
            None
        };

        let room_tasks = self
            .bridge_repo
            .list_shared_room_tasks(args.room_id)
            .await
            .unwrap_or_default();

        let participating: Vec<ParticipatingTeam> = room_dto
            .members
            .iter()
            .map(|m| ParticipatingTeam {
                team_id: m.team_id.clone(),
                name: m.team_name.clone(),
                role_in_room: if m.is_host {
                    "chair/host".to_owned()
                } else {
                    "member".to_owned()
                },
            })
            .collect();

        let mut routed_targets = if args.sender_type == "system" {
            Vec::new()
        } else if let Some(ref allowlist) = args.wake_targets
            && allowlist.is_empty()
        {
            // User explicitly muted all wake-preview chips in RoundedPillComposer
            Vec::new()
        } else {
            let providers = self
                .provider_repo
                .list(args.owner_user_id)
                .await
                .unwrap_or_default();
            let routing_input = RoomRoutingInput {
                sender_type: args.sender_type,
                sender_team_id: args.sender_team_id,
                content: args.content,
                explicit_mentions: &args.explicit_mentions,
                reply_to_sender_team_id: reply_sender_team_id.as_deref(),
                host_team_id: room_dto.host_team_id.as_deref(),
                members: &participating,
                room_tasks: &room_tasks,
            };
            resolve_room_targets(&routing_input, &providers, &self.encryption_key).await
        };

        // Apply Desktop wake-chip allowlist when user muted specific chips
        if let Some(ref allowlist) = args.wake_targets
            && !allowlist.is_empty()
        {
            let mut allowed_ids = Vec::new();
            for raw in allowlist {
                let needle = raw.trim().trim_start_matches('@').to_lowercase();
                if needle.is_empty() {
                    continue;
                }
                for m in &room_dto.members {
                    if (m.team_id.eq_ignore_ascii_case(&needle)
                        || m.team_name.to_lowercase() == needle
                        || mention_slug(&m.team_name) == mention_slug(&needle))
                        && !allowed_ids.contains(&m.team_id)
                    {
                        allowed_ids.push(m.team_id.clone());
                    }
                }
            }

            routed_targets.retain(|rt| allowed_ids.contains(&rt.team_id));

            // If user deliberately muted some chips (e.g., muted Host to wake a specific Member)
            // and default routing picked only muted teams, fall back to the unmuted chips.
            if routed_targets.is_empty() && allowed_ids.len() < room_dto.members.len() {
                routed_targets = allowed_ids
                    .into_iter()
                    .filter(|tid| !is_team_blocked_by_dependencies(tid, &room_tasks))
                    .map(|team_id| RoutedTarget {
                        team_id,
                        reason: "explicit:wake_chip".to_owned(),
                        tier: 1,
                    })
                    .collect();
            }
        }

        // Check `max_hops` loop guard for bot-to-bot turns
        let refreshed_room = self
            .bridge_repo
            .get_shared_room(args.owner_user_id, args.room_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Shared room not found: {}", args.room_id))?;

        let mut effective_targets = routed_targets;
        let mut hop_limit_triggered = false;
        let mut current_hop = refreshed_room.hop_count_since_human;

        if args.sender_type != "user" && !effective_targets.is_empty() {
            if refreshed_room.hop_count_since_human >= refreshed_room.max_hops {
                hop_limit_triggered = true;
                effective_targets.clear();
            } else if let Ok(next_hop) = self.bridge_repo.increment_room_hops(args.room_id).await {
                current_hop = next_hop;
            }
        }

        let target_ids: Vec<String> = effective_targets.iter().map(|t| t.team_id.clone()).collect();
        let msg_id = generate_prefixed_id("smsg");
        let saved_row = self
            .bridge_repo
            .insert_shared_room_message(CreateSharedRoomMessageParams {
                id: msg_id.clone(),
                room_id: args.room_id.to_owned(),
                sender_type: args.sender_type.to_owned(),
                sender_team_id: args.sender_team_id.map(ToOwned::to_owned),
                sender_label: sender_label.clone(),
                reply_to_id: args.reply_to_id,
                content: args.content.to_owned(),
                attachments_json: serde_json::to_string(&staged_artifacts).unwrap_or_else(|_| "[]".into()),
                target_team_ids_json: serde_json::to_string(&target_ids).unwrap_or_else(|_| "[]".into()),
                hop_index: current_hop,
            })
            .await
            .map_err(|e| e.to_string())?;

        let msg_dto = Self::map_room_message_dto(saved_row);
        let updated_room_dto = self.build_shared_room_dto(args.owner_user_id, args.room_id).await?;

        self.event_bus.broadcast(WebSocketMessage::new(
            "sharedRoom.messageCreated",
            serde_json::json!({
                "user_id": args.owner_user_id,
                "room_id": args.room_id,
                "message": msg_dto,
                "room": updated_room_dto,
            }),
        ));

        // Mirror to Telegram if this Shared Room is bound or active on DM
        if let Some(tg_dest) = self
            .resolve_room_telegram_destination(args.owner_user_id, args.room_id)
            .await
        {
            let is_same_user_echo =
                args.sender_type == "user" && args.origin_telegram_chat.as_deref() == Some(tg_dest.as_str());
            if !is_same_user_echo {
                let tg_md = format!(
                    "🏛 **[#{room_name}] {sender_label}**\n\n{body}",
                    room_name = room_dto.name,
                    body = args.content
                );
                let _ = self
                    .send_formatted_telegram_message(
                        args.owner_user_id,
                        &tg_dest,
                        &tg_md,
                        None,
                        None,
                        CustomTargetType::Room,
                        args.room_id,
                    )
                    .await;
                let staged_paths: Vec<String> = staged_artifacts.iter().map(|a| a.staged_path.clone()).collect();
                if !staged_paths.is_empty() {
                    let _ = self
                        .send_validated_outbound_files(
                            args.owner_user_id,
                            &tg_dest,
                            &staged_paths,
                            None,
                            CustomTargetType::Room,
                            args.room_id,
                        )
                        .await;
                }
            }
        }

        if hop_limit_triggered {
            self.emit_hop_limit_reached_notice(args.owner_user_id, &room_dto)
                .await;
            return Ok((msg_dto, Vec::new()));
        }

        // Dispatch wakeups to each routed Team's Lead (enforcing 100% Smart Budget Fuse for metered API-key teams)
        for target in &effective_targets {
            if let Ok(team_meta) = self.team_service.get_team(args.owner_user_id, &target.team_id).await
                && let Some((spent, limit)) = self.is_team_api_key_hard_blocked(args.owner_user_id, &team_meta).await
            {
                let fuse_notice = HubI18n::wallet_hard_blocked_notice(locale, &team_meta.name, spent, limit);
                if let Ok(saved_fuse) = self
                    .bridge_repo
                    .insert_shared_room_message(CreateSharedRoomMessageParams {
                        id: generate_prefixed_id("smsg"),
                        room_id: args.room_id.to_owned(),
                        sender_type: "system".to_owned(),
                        sender_team_id: None,
                        sender_label: "Budget Fuse".to_owned(),
                        reply_to_id: None,
                        content: fuse_notice,
                        attachments_json: "[]".to_owned(),
                        target_team_ids_json: "[]".to_owned(),
                        hop_index: current_hop,
                    })
                    .await
                {
                    let fuse_dto = Self::map_room_message_dto(saved_fuse);
                    self.event_bus.broadcast(WebSocketMessage::new(
                        "sharedRoom.messageCreated",
                        serde_json::json!({
                            "user_id": args.owner_user_id,
                            "room_id": args.room_id,
                            "message": fuse_dto,
                        }),
                    ));
                }
                continue;
            }

            let _ = self
                .bridge_repo
                .set_member_pending_handoff(args.room_id, &target.team_id, Some(&msg_id), current_hop)
                .await;

            self.active_runs.insert(
                target.team_id.clone(),
                ActiveTeamRunTracker {
                    origin: RunOrigin::SharedRoom {
                        owner_user_id: args.owner_user_id.to_owned(),
                        room_id: args.room_id.to_owned(),
                    },
                    started_at_ms: now_ms(),
                    delegated: false,
                    notify_completed_sent: false,
                },
            );

            let member_ws = room_dto
                .members
                .iter()
                .find(|m| m.team_id == target.team_id)
                .map(|m| m.workspace.clone())
                .unwrap_or_default();

            let local_inbox_files: Vec<String> = if staged_artifacts.is_empty() {
                Vec::new()
            } else {
                recipient_inbox_paths
                    .get(member_ws.trim())
                    .cloned()
                    .unwrap_or_else(|| staged_artifacts.iter().map(|a| a.staged_path.clone()).collect())
            };

            let files_note = if local_inbox_files.is_empty() {
                String::new()
            } else {
                format!(
                    "\nShared artifacts copied to your workspace inbox:\n- {}\n",
                    local_inbox_files.join("\n- ")
                )
            };

            let roster_summary: Vec<String> = room_dto
                .members
                .iter()
                .map(|m| format!("@{} (team_id={})", m.team_name, m.team_id))
                .collect();

            let wake_prompt = format!(
                "[SHARED ROOM #{room_name} | room_id=\"{room_id}\"]\n\
                 From: {sender_label}\n\
                 Routing: tier {tier} ({reason})\n\
                 Room Participants: {roster}\n\
                 {rules_line}\
                 \nMessage:\n{body}\n{files_note}\n\
                 ---\n\
                 [Action Required]: Coordinate with your team if needed, then post your response or deliverables back to Shared Room \"{room_name}\" using `team_room_post` (with `room_id`: \"{room_id}\"). To assign or update cross-team dependencies, use `team_room_task_create` / `team_room_task_update`.",
                room_name = room_dto.name,
                room_id = args.room_id,
                tier = target.tier,
                reason = target.reason,
                roster = roster_summary.join(", "),
                rules_line = if room_dto.rules.trim().is_empty() {
                    String::new()
                } else {
                    format!("Room Rules: {}\n", room_dto.rules.trim())
                },
                body = args.content,
            );

            let file_refs = if local_inbox_files.is_empty() {
                None
            } else {
                Some(
                    local_inbox_files
                        .into_iter()
                        .map(|path| ChatFileRef::Local { path })
                        .collect(),
                )
            };

            let team_svc = Arc::clone(&self.team_service);
            let uid = args.owner_user_id.to_owned();
            let tid = target.team_id.clone();
            tokio::spawn(async move {
                if let Err(err) = team_svc.send_message(&uid, &tid, &wake_prompt, file_refs).await {
                    warn!(team_id = %tid, error = %err, "failed to wake target Team Lead from Shared Room");
                }
            });
        }

        Ok((msg_dto, effective_targets))
    }

    async fn emit_hop_limit_reached_notice(&self, owner_user_id: &str, room_dto: &SharedRoomDto) {
        let locale = self.resolve_locale(owner_user_id).await;
        let notice_text = HubI18n::max_hops_paused_notice(locale, room_dto.max_hops);
        let msg_id = generate_prefixed_id("smsg");
        if let Ok(saved) = self
            .bridge_repo
            .insert_shared_room_message(CreateSharedRoomMessageParams {
                id: msg_id,
                room_id: room_dto.id.clone(),
                sender_type: "system".to_owned(),
                sender_team_id: None,
                sender_label: "Loop Guard".to_owned(),
                reply_to_id: None,
                content: notice_text.clone(),
                attachments_json: "[]".to_owned(),
                target_team_ids_json: "[]".to_owned(),
                hop_index: room_dto.hop_count_since_human,
            })
            .await
        {
            let msg_dto = Self::map_room_message_dto(saved);
            self.event_bus.broadcast(WebSocketMessage::new(
                "sharedRoom.messageCreated",
                serde_json::json!({
                    "user_id": owner_user_id,
                    "room_id": room_dto.id,
                    "message": msg_dto,
                    "hop_limit_reached": true,
                }),
            ));
        }

        let action_token = self
            .bridge_repo
            .create_action_token(
                owner_user_id,
                CustomTargetType::Room,
                &room_dto.id,
                "allow_more_hops",
                &serde_json::json!({}),
                None,
            )
            .await
            .ok();

        let mut tg_chat_for_inbox: Option<String> = None;
        let mut tg_msg_for_inbox: Option<String> = None;

        if let Some(tg_dest) = self.resolve_room_telegram_destination(owner_user_id, &room_dto.id).await {
            let mut buttons = Vec::new();
            if let Some(ref tok) = action_token {
                buttons.push(vec![ActionButton {
                    label: HubI18n::btn_allow_more_hops(locale).into(),
                    action: format!("t:{tok}"),
                    params: None,
                }]);
            }
            if let Ok(sent_ids) = self
                .send_formatted_telegram_message(
                    owner_user_id,
                    &tg_dest,
                    &format!("🏛 **[#{}]**\n{}", room_dto.name, notice_text),
                    (!buttons.is_empty()).then_some(buttons),
                    None,
                    CustomTargetType::Room,
                    &room_dto.id,
                )
                .await
            {
                tg_chat_for_inbox = Some(tg_dest);
                tg_msg_for_inbox = sent_ids.into_iter().last();
            }
        }

        let inbox_title = match locale {
            HubLocale::ViVn => format!("Chạm giới hạn vòng thảo luận tại #{}", room_dto.name),
            HubLocale::EnUs => format!("Max-Hops Loop Limit Reached in #{}", room_dto.name),
        };
        if let Ok(inbox_row) = self
            .bridge_repo
            .create_action_inbox_item(CreateActionInboxParams {
                id: None,
                owner_user_id: owner_user_id.to_owned(),
                category: "max_hops".to_owned(),
                title: inbox_title,
                summary: notice_text,
                details_json: serde_json::json!({
                    "room_id": room_dto.id,
                    "room_name": room_dto.name,
                    "current_hops": room_dto.hop_count_since_human,
                    "max_hops": room_dto.max_hops,
                })
                .to_string(),
                target_type: Some("room".to_owned()),
                target_id: room_dto.id.clone(),
                action_token,
                telegram_chat_id: tg_chat_for_inbox,
                telegram_msg_id: tg_msg_for_inbox,
            })
            .await
        {
            self.event_bus.broadcast(WebSocketMessage::new(
                "inbox:item_added",
                serde_json::json!({
                    "user_id": owner_user_id,
                    "item": ActionInboxCardDto::from_row(inbox_row),
                }),
            ));
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_cross_team_task(
        &self,
        owner_user_id: &str,
        room_id: &str,
        from_team_id: Option<&str>,
        to_team_ref: &str,
        title: &str,
        description: &str,
        blocked_by: Vec<String>,
        files: Vec<String>,
    ) -> Result<SharedRoomTaskDto, String> {
        let room_dto = self.build_shared_room_dto(owner_user_id, room_id).await?;
        let to_team_id = self
            .resolve_team_id_in_room(owner_user_id, room_id, to_team_ref)
            .await?;

        let source_ws = from_team_id.and_then(|tid| {
            room_dto
                .members
                .iter()
                .find(|m| m.team_id == tid)
                .map(|m| m.workspace.as_str())
        });
        let recipient_ws: Vec<String> = room_dto
            .members
            .iter()
            .filter(|m| m.team_id == to_team_id && !m.workspace.trim().is_empty())
            .map(|m| m.workspace.clone())
            .collect();

        let existing_tasks = self
            .bridge_repo
            .list_shared_room_tasks(room_id)
            .await
            .unwrap_or_default();
        let task_id = generate_prefixed_id("rtask");
        validate_task_dependencies(&task_id, &blocked_by, &existing_tasks)?;

        let allow_user_local_files = from_team_id.is_none();
        let staged = stage_room_artifacts_batch(
            room_id,
            &files,
            source_ws,
            &self.data_dir,
            &recipient_ws,
            allow_user_local_files,
        )?
        .artifacts;

        let created = self
            .bridge_repo
            .create_shared_room_task(CreateSharedRoomTaskParams {
                id: task_id.clone(),
                room_id: room_id.to_owned(),
                from_team_id: from_team_id.map(ToOwned::to_owned),
                to_team_id: to_team_id.clone(),
                title: title.trim().to_owned(),
                description: description.to_owned(),
                blocked_by_json: serde_json::to_string(&blocked_by).unwrap_or_else(|_| "[]".into()),
                artifact_paths_json: serde_json::to_string(&staged).unwrap_or_else(|_| "[]".into()),
            })
            .await
            .map_err(|e| e.to_string())?;

        let name_map: HashMap<String, String> = room_dto
            .members
            .iter()
            .map(|m| (m.team_id.clone(), m.team_name.clone()))
            .collect();
        let dto = Self::map_room_task_dto(created.clone(), &name_map);

        self.event_bus.broadcast(WebSocketMessage::new(
            "sharedRoom.taskChanged",
            serde_json::json!({
                "user_id": owner_user_id,
                "room_id": room_id,
                "task": dto,
            }),
        ));

        // If this specific task is NOT gated by unfinished `blocked_by` tasks, wake the assignee Team's Lead immediately!
        let all_tasks = self
            .bridge_repo
            .list_shared_room_tasks(room_id)
            .await
            .unwrap_or_default();
        if !is_task_blocked(&created, &all_tasks) {
            let wake_msg = format!(
                "[CROSS-TEAM TASK ASSIGNED in #{room_name} | room_id=\"{room_id}\" | task_id=\"{task_id}\"]\n\
                 Title: {title}\n\
                 Description: {description}\n\
                 Please execute with your team and call `team_room_task_update` (task_id=\"{task_id}\", status=\"completed\") + `team_room_post` when finished.",
                room_name = room_dto.name,
            );
            let _ = self
                .bridge_repo
                .set_member_pending_handoff(room_id, &to_team_id, Some(&task_id), room_dto.hop_count_since_human)
                .await;
            let team_svc = Arc::clone(&self.team_service);
            let uid = owner_user_id.to_owned();
            let tid = to_team_id.clone();
            tokio::spawn(async move {
                let _ = team_svc.send_message(&uid, &tid, &wake_msg, None).await;
            });
        }

        Ok(dto)
    }

    pub async fn update_cross_team_task(
        &self,
        owner_user_id: &str,
        task_id: &str,
        actor_team_id: Option<&str>,
        status: Option<String>,
        description: Option<String>,
        result_summary: Option<String>,
        files: Vec<String>,
    ) -> Result<SharedRoomTaskDto, String> {
        let existing = self
            .bridge_repo
            .get_shared_room_task(task_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Shared room task not found: {task_id}"))?;

        // Feature F19: Human-Only Done Guard
        // Automated agents (actor_team_id.is_some()) are strictly forbidden from transitioning tasks to 'done' or 'completed'.
        // Only human users (actor_team_id = None) have the authority to finalize tasks.
        if let Some(ref st) = status {
            let normalized = st.trim().to_lowercase();
            if (normalized == "done" || normalized == "completed") && actor_team_id.is_some() {
                return Err(
                    "Forbidden: Only human users can transition a task to Done. Agents may only transition tasks up to 'in_review' or 'blocked'."
                        .to_string(),
                );
            }
        }

        let room_dto = self
            .build_shared_room_dto(owner_user_id, &existing.room_id)
            .await?;

        let before_tasks = self
            .bridge_repo
            .list_shared_room_tasks(&existing.room_id)
            .await
            .unwrap_or_default();

        let mut merged_artifacts: Vec<StagedRoomArtifact> =
            serde_json::from_str(&existing.artifact_paths_json).unwrap_or_default();
        if !files.is_empty() {
            let source_ws = actor_team_id.and_then(|tid| {
                room_dto
                    .members
                    .iter()
                    .find(|m| m.team_id == tid)
                    .map(|m| m.workspace.as_str())
            });
            let recipient_ws: Vec<String> = room_dto
                .members
                .iter()
                .filter(|m| Some(m.team_id.as_str()) != actor_team_id && !m.workspace.trim().is_empty())
                .map(|m| m.workspace.clone())
                .collect();
            let allow_user_local_files = actor_team_id.is_none();
            let newly_staged = stage_room_artifacts_batch(
                &existing.room_id,
                &files,
                source_ws,
                &self.data_dir,
                &recipient_ws,
                allow_user_local_files,
            )?
            .artifacts;
            merged_artifacts.extend(newly_staged);
        }

        let updated = self
            .bridge_repo
            .update_shared_room_task(
                task_id,
                UpdateSharedRoomTaskParams {
                    status: status.clone(),
                    description,
                    blocked_by_json: None,
                    artifact_paths_json: Some(
                        serde_json::to_string(&merged_artifacts).unwrap_or_else(|_| "[]".into()),
                    ),
                    result_summary: result_summary.clone(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        let name_map: HashMap<String, String> = room_dto
            .members
            .iter()
            .map(|m| (m.team_id.clone(), m.team_name.clone()))
            .collect();
        let dto = Self::map_room_task_dto(updated.clone(), &name_map);

        self.event_bus.broadcast(WebSocketMessage::new(
            "sharedRoom.taskChanged",
            serde_json::json!({
                "user_id": owner_user_id,
                "room_id": existing.room_id,
                "task": dto,
            }),
        ));

        // Closed-Loop Layer 3: If this task newly transitioned to `completed`, wake any downstream Team
        // whose `blocked_by` task just became unblocked!
        if existing.status != "completed" && updated.status == "completed" {
            let after_tasks = self
                .bridge_repo
                .list_shared_room_tasks(&existing.room_id)
                .await
                .unwrap_or_default();

            for candidate in &after_tasks {
                if matches!(candidate.status.as_str(), "completed" | "cancelled") {
                    continue;
                }
                let deps: Vec<String> = serde_json::from_str(&candidate.blocked_by_json).unwrap_or_default();
                if !deps.iter().any(|d| d == task_id) {
                    continue;
                }
                let was_blocked = before_tasks
                    .iter()
                    .find(|t| t.id == candidate.id)
                    .is_some_and(|before_cand| is_task_blocked(before_cand, &before_tasks));
                let now_blocked = is_task_blocked(candidate, &after_tasks);
                if was_blocked && !now_blocked {
                    let _ = self
                        .bridge_repo
                        .set_member_pending_handoff(
                            &existing.room_id,
                            &candidate.to_team_id,
                            Some(&candidate.id),
                            room_dto.hop_count_since_human,
                        )
                        .await;

                    let summary_line = updated
                        .result_summary
                        .as_deref()
                        .map(|s| format!("Upstream Summary: {s}\n"))
                        .unwrap_or_default();
                    let wake_prompt = format!(
                        "[CROSS-TEAM TASK UNBLOCKED in #{room_name} | room_id=\"{room_id}\"]\n\
                         Upstream dependency `{upstream_id}` (\"{upstream_title}\") is now COMPLETED!\n\
                         {summary_line}\
                         Your Cross-Team Task `{task_id}` (\"{task_title}\") is now UNBLOCKED and ready to start.\n\
                         Check `.shared-inbox/{safe_room}/` for any upstream artifacts, execute with your team, and call `team_room_task_update` + `team_room_post` when done.",
                        room_name = room_dto.name,
                        room_id = existing.room_id,
                        upstream_id = updated.id,
                        upstream_title = updated.title,
                        task_id = candidate.id,
                        task_title = candidate.title,
                        safe_room = sanitize_file_name(&existing.room_id),
                    );
                    let team_svc = Arc::clone(&self.team_service);
                    let uid = owner_user_id.to_owned();
                    let tid = candidate.to_team_id.clone();
                    tokio::spawn(async move {
                        let _ = team_svc.send_message(&uid, &tid, &wake_prompt, None).await;
                    });
                }
            }
        }

        Ok(dto)
    }
}

// ===========================================================================
// Trait 1: `ChannelTeamBridgePort` (for `aionui-channel`)
// ===========================================================================

#[async_trait]
impl ChannelTeamBridgePort for TelegramTeamHub {
    async fn handle_bridge_command(
        &self,
        owner_user_id: &str,
        msg: &UnifiedIncomingMessage,
        command: &str,
        args: &str,
    ) -> Result<Option<ActionResponse>, ChannelError> {
        let trimmed_args = args.trim();
        let locale = self.resolve_locale(owner_user_id).await;

        match command {
            "/team" | "/teams" => {
                let teams = self
                    .team_service
                    .list_teams(owner_user_id)
                    .await
                    .map_err(|e| ChannelError::PlatformApi(e.to_string()))?;

                if teams.is_empty() {
                    return Ok(Some(ActionResponse {
                        text: Some(HubI18n::no_teams_info(locale).into()),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }));
                }

                if !trimmed_args.is_empty() {
                    let lower = trimmed_args.to_lowercase();
                    let matched = teams
                        .iter()
                        .find(|t| t.name.to_lowercase() == lower || t.id.eq_ignore_ascii_case(&lower))
                        .or_else(|| teams.iter().find(|t| t.name.to_lowercase().contains(&lower)));

                    if let Some(team) = matched {
                        let (lead_name, _) = Self::find_lead_info(team);
                        if msg.is_group_chat() {
                            self.bridge_repo
                                .upsert_channel_binding(UpsertChannelBindingParams {
                                    owner_user_id: owner_user_id.to_owned(),
                                    plugin_id: "telegram".to_owned(),
                                    chat_id: msg.chat_id.clone(),
                                    thread_id: msg.thread_id(),
                                    target_type: CustomTargetType::Team,
                                    target_id: team.id.clone(),
                                    stream_mode: CustomStreamMode::Milestones,
                                })
                                .await?;
                            let html = markdown_to_telegram_html(&HubI18n::bind_topic_info(
                                locale,
                                &team.name,
                                &lead_name,
                            ));
                            return Ok(Some(ActionResponse {
                                text: Some(html),
                                parse_mode: Some(ParseMode::HTML),
                                buttons: self
                                    .make_team_quick_buttons(owner_user_id, &team.id, false, false)
                                    .await,
                                keyboard: None,
                                behavior: ActionBehavior::Send,
                                toast: None,
                                edit_message_id: None,
                            }));
                        } else {
                            self.bridge_repo
                                .set_dm_target(
                                    owner_user_id,
                                    "telegram",
                                    &msg.user.id,
                                    &msg.chat_id,
                                    CustomTargetType::Team,
                                    &team.id,
                                )
                                .await?;
                            let html = markdown_to_telegram_html(&HubI18n::switch_team_info(
                                locale,
                                &team.name,
                                &lead_name,
                                team.assistants.len(),
                            ));
                            return Ok(Some(ActionResponse {
                                text: Some(html),
                                parse_mode: Some(ParseMode::HTML),
                                buttons: self
                                    .make_team_quick_buttons(owner_user_id, &team.id, false, false)
                                    .await,
                                keyboard: None,
                                behavior: ActionBehavior::Send,
                                toast: None,
                                edit_message_id: None,
                            }));
                        }
                    }
                }

                let active_dm = self
                    .bridge_repo
                    .get_dm_target(owner_user_id, "telegram", &msg.chat_id)
                    .await?;
                let mut rows: Vec<Vec<ActionButton>> = Vec::new();
                let mut current_row: Vec<ActionButton> = Vec::new();

                for team in teams.iter().take(16) {
                    let is_current = active_dm
                        .as_ref()
                        .is_some_and(|d| d.target_type == "team" && d.target_id == team.id);
                    let prefix = if is_current { "✅ " } else { "👥 " };
                    let tok = self
                        .bridge_repo
                        .create_action_token(
                            owner_user_id,
                            CustomTargetType::Team,
                            &team.id,
                            "switch_team",
                            &serde_json::json!({
                                "chat_id": msg.chat_id,
                                "thread_id": msg.thread_id(),
                                "is_group": msg.is_group_chat(),
                                "tg_user_id": msg.user.id,
                            }),
                            None,
                        )
                        .await?;
                    current_row.push(ActionButton {
                        label: format!("{prefix}{}", team.name),
                        action: format!("t:{tok}"),
                        params: None,
                    });
                    if current_row.len() == 2 {
                        rows.push(std::mem::take(&mut current_row));
                    }
                }
                if !current_row.is_empty() {
                    rows.push(current_row);
                }

                if !msg.is_group_chat() {
                    let solo_tok = self
                        .bridge_repo
                        .create_action_token(
                            owner_user_id,
                            CustomTargetType::Team,
                            "solo",
                            "switch_solo",
                            &serde_json::json!({ "chat_id": msg.chat_id }),
                            None,
                        )
                        .await?;
                    rows.push(vec![ActionButton {
                        label: HubI18n::btn_switch_solo(locale).into(),
                        action: format!("t:{solo_tok}"),
                        params: None,
                    }]);
                }

                let header = if msg.is_group_chat() {
                    HubI18n::select_team_bind_prompt(locale)
                } else {
                    HubI18n::select_team_chat_prompt(locale)
                };

                Ok(Some(ActionResponse {
                    text: Some(markdown_to_telegram_html(header)),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: Some(rows),
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                }))
            }

            "/room" | "/rooms" => {
                let rooms = self.bridge_repo.list_shared_rooms(owner_user_id).await?;
                if rooms.is_empty() {
                    let html = markdown_to_telegram_html(HubI18n::no_shared_rooms_info(locale));
                    return Ok(Some(ActionResponse {
                        text: Some(html),
                        parse_mode: Some(ParseMode::HTML),
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }));
                }

                if !trimmed_args.is_empty() {
                    let lower = trimmed_args.to_lowercase();
                    let matched = rooms
                        .iter()
                        .find(|r| r.name.to_lowercase() == lower || r.id.eq_ignore_ascii_case(&lower))
                        .or_else(|| rooms.iter().find(|r| r.name.to_lowercase().contains(&lower)));
                    if let Some(room) = matched {
                        if msg.is_group_chat() {
                            self.bridge_repo
                                .upsert_channel_binding(UpsertChannelBindingParams {
                                    owner_user_id: owner_user_id.to_owned(),
                                    plugin_id: "telegram".to_owned(),
                                    chat_id: msg.chat_id.clone(),
                                    thread_id: msg.thread_id(),
                                    target_type: CustomTargetType::Room,
                                    target_id: room.id.clone(),
                                    stream_mode: CustomStreamMode::Milestones,
                                })
                                .await?;
                        } else {
                            self.bridge_repo
                                .set_dm_target(
                                    owner_user_id,
                                    "telegram",
                                    &msg.user.id,
                                    &msg.chat_id,
                                    CustomTargetType::Room,
                                    &room.id,
                                )
                                .await?;
                        }
                        let html = markdown_to_telegram_html(&HubI18n::shared_room_joined_info(
                            locale,
                            &room.name,
                        ));
                        return Ok(Some(ActionResponse {
                            text: Some(html),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        }));
                    }
                }

                let mut rows = Vec::new();
                for room in rooms.iter().take(12) {
                    let tok = self
                        .bridge_repo
                        .create_action_token(
                            owner_user_id,
                            CustomTargetType::Room,
                            &room.id,
                            "switch_room",
                            &serde_json::json!({
                                "chat_id": msg.chat_id,
                                "thread_id": msg.thread_id(),
                                "is_group": msg.is_group_chat(),
                                "tg_user_id": msg.user.id,
                            }),
                            None,
                        )
                        .await?;
                    rows.push(vec![ActionButton {
                        label: format!("🏛 {}", room.name),
                        action: format!("t:{tok}"),
                        params: None,
                    }]);
                }

                Ok(Some(ActionResponse {
                    text: Some(markdown_to_telegram_html(HubI18n::select_shared_room_prompt(locale))),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: Some(rows),
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                }))
            }

            "/solo" => {
                self.bridge_repo
                    .clear_dm_target(owner_user_id, "telegram", &msg.chat_id)
                    .await?;
                Ok(Some(ActionResponse {
                    text: Some(markdown_to_telegram_html(HubI18n::switched_to_solo_info(locale))),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                }))
            }

            "/status" => {
                let target = if let Some(b) = self
                    .bridge_repo
                    .resolve_channel_binding_for_chat(
                        owner_user_id,
                        "telegram",
                        &msg.chat_id,
                        msg.thread_id().as_deref(),
                    )
                    .await?
                {
                    Some((b.target_type, b.target_id))
                } else if !msg.is_group_chat() {
                    self.bridge_repo
                        .get_dm_target(owner_user_id, "telegram", &msg.chat_id)
                        .await?
                        .map(|d| (d.target_type, d.target_id))
                } else {
                    None
                };

                let Some((ttype, tid)) = target else {
                    return Ok(None);
                };

                let card_res = if ttype == "room" {
                    self.build_status_card_for_room(owner_user_id, &tid).await
                } else {
                    self.build_status_card_for_team(owner_user_id, &tid).await
                };

                match card_res {
                    Ok((md, buttons)) => Ok(Some(ActionResponse {
                        text: Some(markdown_to_telegram_html(&md)),
                        parse_mode: Some(ParseMode::HTML),
                        buttons,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    })),
                    Err(_) => Ok(None),
                }
            }

            "/stop" => {
                let target = if let Some(b) = self
                    .bridge_repo
                    .resolve_channel_binding_for_chat(
                        owner_user_id,
                        "telegram",
                        &msg.chat_id,
                        msg.thread_id().as_deref(),
                    )
                    .await?
                {
                    Some((b.target_type, b.target_id))
                } else if !msg.is_group_chat() {
                    self.bridge_repo
                        .get_dm_target(owner_user_id, "telegram", &msg.chat_id)
                        .await?
                        .map(|d| (d.target_type, d.target_id))
                } else {
                    None
                };

                let Some((ttype, tid)) = target else {
                    return Ok(Some(ActionResponse {
                        text: Some(HubI18n::no_bound_team_info(locale).into()),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }));
                };

                let mut stopped_names = Vec::new();
                if ttype == "team" {
                    if let Ok(rs) = self.team_service.get_run_state(owner_user_id, &tid).await
                        && let Some(run) = rs.active_run
                    {
                        let _ = self
                            .team_service
                            .cancel_run(
                                owner_user_id,
                                &tid,
                                &run.team_run_id,
                                None,
                                Some("Stopped from Telegram /stop".into()),
                            )
                            .await;
                        stopped_names.push(tid);
                    }
                } else if let Ok(members) = self.bridge_repo.list_shared_room_members(&tid).await {
                    for m in members {
                        if let Ok(rs) = self.team_service.get_run_state(owner_user_id, &m.team_id).await
                            && let Some(run) = rs.active_run
                        {
                            let _ = self
                                .team_service
                                .cancel_run(
                                    owner_user_id,
                                    &m.team_id,
                                    &run.team_run_id,
                                    None,
                                    Some("Stopped from Telegram /stop".into()),
                                )
                                .await;
                            stopped_names.push(m.team_id);
                        }
                    }
                }

                let reply_md = if stopped_names.is_empty() {
                    HubI18n::no_active_runs_to_stop(locale).to_owned()
                } else {
                    HubI18n::stopped_active_runs_info(locale, stopped_names.len())
                };
                Ok(Some(ActionResponse {
                    text: Some(markdown_to_telegram_html(&reply_md)),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                }))
            }

            "/compact" => {
                let bound_team_id = if let Some(b) = self
                    .bridge_repo
                    .resolve_channel_binding_for_chat(
                        owner_user_id,
                        "telegram",
                        &msg.chat_id,
                        msg.thread_id().as_deref(),
                    )
                    .await?
                {
                    (b.target_type == "team").then_some(b.target_id)
                } else if !msg.is_group_chat() {
                    self.bridge_repo
                        .get_dm_target(owner_user_id, "telegram", &msg.chat_id)
                        .await?
                        .and_then(|d| (d.target_type == "team").then_some(d.target_id))
                } else {
                    None
                };

                let Some(team_id) = bound_team_id else {
                    return Ok(Some(ActionResponse {
                        text: Some(HubI18n::no_bound_team_info(locale).into()),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }));
                };

                let Some(orch) = self.orchestrator.get() else {
                    return Ok(None);
                };

                let team = self
                    .team_service
                    .get_team(owner_user_id, &team_id)
                    .await
                    .map_err(|e| ChannelError::PlatformApi(e.to_string()))?;

                let compacted = orch
                    .compact_team_context(
                        owner_user_id,
                        &team_id,
                        crate::bridge::context_compactor::CompactTeamContextRequest::default(),
                    )
                    .await
                    .map_err(|e| ChannelError::PlatformApi(format!("{e:?}")))?;

                let avg_after = if compacted.context_status.slots.is_empty() {
                    0
                } else {
                    let sum: i64 = compacted.context_status.slots.iter().map(|s| s.usage_percent).sum();
                    sum / (compacted.context_status.slots.len() as i64)
                };

                let md = HubI18n::compact_completed_markdown(locale, &team.name, compacted.outcomes.len(), avg_after);
                Ok(Some(ActionResponse {
                    text: Some(markdown_to_telegram_html(&md)),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                }))
            }

            "/model" | "/models" => {
                let bound_team_id = if let Some(b) = self
                    .bridge_repo
                    .resolve_channel_binding_for_chat(
                        owner_user_id,
                        "telegram",
                        &msg.chat_id,
                        msg.thread_id().as_deref(),
                    )
                    .await?
                {
                    (b.target_type == "team").then_some(b.target_id)
                } else if !msg.is_group_chat() {
                    self.bridge_repo
                        .get_dm_target(owner_user_id, "telegram", &msg.chat_id)
                        .await?
                        .and_then(|d| (d.target_type == "team").then_some(d.target_id))
                } else {
                    None
                };

                let Some(team_id) = bound_team_id else {
                    return Ok(Some(ActionResponse {
                        text: Some(HubI18n::no_bound_team_info(locale).into()),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }));
                };

                let Some(orch) = self.orchestrator.get() else {
                    return Ok(None);
                };

                let team = self
                    .team_service
                    .get_team(owner_user_id, &team_id)
                    .await
                    .map_err(|e| ChannelError::PlatformApi(e.to_string()))?;

                let catalog = orch
                    .list_connected_models(owner_user_id)
                    .await
                    .map_err(|e| ChannelError::PlatformApi(format!("{e:?}")))?;

                // Collect candidate (backend, provider_id, model_id, short_label)
                let mut candidates: Vec<(String, Option<String>, String, String)> = Vec::new();
                for cli in &catalog.cli_agents {
                    for m in cli.models.iter().take(2) {
                        let short = if m.id == cli.backend {
                            cli.name.clone()
                        } else {
                            format!("{}/{}", cli.backend, m.id)
                        };
                        candidates.push((cli.backend.clone(), None, m.id.clone(), short));
                    }
                }
                for prov in &catalog.api_providers {
                    for m in prov.models.iter().take(3) {
                        let short_model = m.id.rsplit('/').next().unwrap_or(&m.id);
                        let short = format!("{}:{}", prov.name, short_model);
                        candidates.push((
                            "aionrs".to_owned(),
                            prov.provider_id.clone().or_else(|| Some(prov.id.clone())),
                            m.id.clone(),
                            short,
                        ));
                    }
                }

                if candidates.is_empty() {
                    return Ok(Some(ActionResponse {
                        text: Some(markdown_to_telegram_html(HubI18n::no_connected_models_info(locale))),
                        parse_mode: Some(ParseMode::HTML),
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }));
                }

                let mut rows: Vec<Vec<ActionButton>> = Vec::new();
                for (backend, provider_id, model_id, short_label) in candidates.into_iter().take(8) {
                    let short_capped: String = short_label.chars().take(22).collect();
                    let all_tok = self
                        .bridge_repo
                        .create_action_token(
                            owner_user_id,
                            CustomTargetType::Team,
                            &team_id,
                            "switch_team_model",
                            &serde_json::json!({
                                "scope": "all",
                                "backend": backend,
                                "provider_id": provider_id,
                                "model_id": model_id,
                                "label": short_label,
                            }),
                            None,
                        )
                        .await?;
                    let workers_tok = self
                        .bridge_repo
                        .create_action_token(
                            owner_user_id,
                            CustomTargetType::Team,
                            &team_id,
                            "switch_team_model",
                            &serde_json::json!({
                                "scope": "teammates_only",
                                "backend": backend,
                                "provider_id": provider_id,
                                "model_id": model_id,
                                "label": short_label,
                            }),
                            None,
                        )
                        .await?;

                    rows.push(vec![
                        ActionButton {
                            label: HubI18n::btn_switch_all_members(locale, &short_capped),
                            action: format!("t:{all_tok}"),
                            params: None,
                        },
                        ActionButton {
                            label: HubI18n::btn_switch_teammates_only(locale, &short_capped),
                            action: format!("t:{workers_tok}"),
                            params: None,
                        },
                    ]);
                }

                if let Ok(compact_tok) = self
                    .bridge_repo
                    .create_action_token(
                        owner_user_id,
                        CustomTargetType::Team,
                        &team_id,
                        "compact_team_now",
                        &serde_json::json!({}),
                        None,
                    )
                    .await
                {
                    rows.push(vec![ActionButton {
                        label: HubI18n::btn_compact_now(locale).into(),
                        action: format!("t:{compact_tok}"),
                        params: None,
                    }]);
                }

                let current_roster = team
                    .assistants
                    .iter()
                    .map(|a| {
                        let icon = if a.role.eq_ignore_ascii_case("lead") || a.role.eq_ignore_ascii_case("leader") {
                            "👑"
                        } else {
                            "🤖"
                        };
                        format!("• {icon} **{}**: `{}/{}`", a.name, a.backend, a.model)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let prompt_md = HubI18n::model_picker_prompt(locale, &team.name, &current_roster);
                Ok(Some(ActionResponse {
                    text: Some(markdown_to_telegram_html(&prompt_md)),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: Some(rows),
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                }))
            }

            _ => Ok(None),
        }
    }

    async fn handle_bridge_token(
        &self,
        owner_user_id: &str,
        token: &str,
        context: &ActionContext,
        raw_meta: Option<&serde_json::Value>,
    ) -> Result<ActionResponse, ChannelError> {
        let locale = self.resolve_locale(owner_user_id).await;
        let Some(row) = self.bridge_repo.get_valid_action_token(token).await? else {
            return Ok(ActionResponse {
                text: None,
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Answer,
                toast: Some(HubI18n::action_expired(locale).into()),
                edit_message_id: None,
            });
        };

        if row.owner_user_id != owner_user_id {
            return Err(ChannelError::UserNotAuthorized("Cross-account token rejected".into()));
        }

        let payload: serde_json::Value = serde_json::from_str(&row.payload_json).unwrap_or_default();
        let thread_id = payload
            .get("thread_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                raw_meta
                    .and_then(|m| m.get("message_thread_id"))
                    .and_then(serde_json::Value::as_i64)
                    .map(|n| n.to_string())
            });
        let is_group = payload
            .get("is_group")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or_else(|| context.chat_id.starts_with('-'));

        match row.action_kind.as_str() {
            "switch_team" => {
                let team = self
                    .team_service
                    .get_team(owner_user_id, &row.target_id)
                    .await
                    .map_err(|e| ChannelError::PlatformApi(e.to_string()))?;
                let (lead_name, _) = Self::find_lead_info(&team);

                if is_group {
                    self.bridge_repo
                        .upsert_channel_binding(UpsertChannelBindingParams {
                            owner_user_id: owner_user_id.to_owned(),
                            plugin_id: "telegram".to_owned(),
                            chat_id: context.chat_id.clone(),
                            thread_id,
                            target_type: CustomTargetType::Team,
                            target_id: team.id.clone(),
                            stream_mode: CustomStreamMode::Milestones,
                        })
                        .await?;
                } else {
                    self.bridge_repo
                        .set_dm_target(
                            owner_user_id,
                            "telegram",
                            &context.user_id,
                            &context.chat_id,
                            CustomTargetType::Team,
                            &team.id,
                        )
                        .await?;
                }

                let text = markdown_to_telegram_html(&HubI18n::connected_team_notice(
                    locale,
                    &team.name,
                    &lead_name,
                ));
                Ok(ActionResponse {
                    text: Some(text),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: self
                        .make_team_quick_buttons(owner_user_id, &team.id, false, false)
                        .await,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: Some(HubI18n::selected_team_toast(locale, &team.name)),
                    edit_message_id: None,
                })
            }

            "switch_room" => {
                let room = self
                    .bridge_repo
                    .get_shared_room(owner_user_id, &row.target_id)
                    .await?
                    .ok_or_else(|| ChannelError::SessionNotFound("Shared room not found".into()))?;

                if is_group {
                    self.bridge_repo
                        .upsert_channel_binding(UpsertChannelBindingParams {
                            owner_user_id: owner_user_id.to_owned(),
                            plugin_id: "telegram".to_owned(),
                            chat_id: context.chat_id.clone(),
                            thread_id,
                            target_type: CustomTargetType::Room,
                            target_id: room.id.clone(),
                            stream_mode: CustomStreamMode::Milestones,
                        })
                        .await?;
                } else {
                    self.bridge_repo
                        .set_dm_target(
                            owner_user_id,
                            "telegram",
                            &context.user_id,
                            &context.chat_id,
                            CustomTargetType::Room,
                            &room.id,
                        )
                        .await?;
                }

                let text = markdown_to_telegram_html(&HubI18n::in_shared_room_notice(locale, &room.name));
                Ok(ActionResponse {
                    text: Some(text),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: Some(HubI18n::opened_room_toast(locale, &room.name)),
                    edit_message_id: None,
                })
            }

            "switch_solo" => {
                self.bridge_repo
                    .clear_dm_target(owner_user_id, "telegram", &context.chat_id)
                    .await?;
                Ok(ActionResponse {
                    text: Some(markdown_to_telegram_html(
                        HubI18n::switched_to_default_solo_notice(locale),
                    )),
                    parse_mode: Some(ParseMode::HTML),
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                })
            }

            "view_tasks" => {
                let card = if row.target_type == "room" {
                    self.build_status_card_for_room(owner_user_id, &row.target_id).await
                } else {
                    self.build_status_card_for_team(owner_user_id, &row.target_id).await
                };
                match card {
                    Ok((md, buttons)) => Ok(ActionResponse {
                        text: Some(markdown_to_telegram_html(&md)),
                        parse_mode: Some(ParseMode::HTML),
                        buttons,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }),
                    Err(e) => Ok(ActionResponse {
                        text: Some(HubI18n::read_taskboard_error(locale, &e)),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    }),
                }
            }

            // Mutating one-shot actions guarded by `consume_action_token_once`
            "approve_continue" | "stop_run" | "retry_member" | "allow_more_hops"
            | "worktree_merge" | "inbox_resolve" | "inbox_choice" | "budget_ack"
            | "switch_team_model" | "compact_team_now" => {
                let consumed = self.bridge_repo.consume_action_token_once(token).await?;
                if !consumed {
                    return Ok(ActionResponse {
                        text: None,
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Answer,
                        toast: Some(HubI18n::action_already_processed(locale).into()),
                        edit_message_id: None,
                    });
                }

                // If this action token was attached to an Action Inbox card, mark it resolved
                let resolved_inbox_item = self
                    .bridge_repo
                    .resolve_action_inbox_item_by_token(token, row.action_kind.as_str(), "telegram")
                    .await
                    .ok()
                    .flatten();
                if let Some(ref inbox_item) = resolved_inbox_item {
                    self.event_bus.broadcast(WebSocketMessage::new(
                        "inbox:item_resolved",
                        serde_json::json!({
                            "user_id": owner_user_id,
                            "id": inbox_item.id,
                            "action": row.action_kind,
                            "resolved_by": "telegram",
                        }),
                    ));
                }

                match row.action_kind.as_str() {
                    "approve_continue" => {
                        let reply_chat = format_chat_with_thread(&context.chat_id, thread_id.as_deref());
                        self.active_runs.insert(
                            row.target_id.clone(),
                            ActiveTeamRunTracker {
                                origin: RunOrigin::Telegram {
                                    owner_user_id: owner_user_id.to_owned(),
                                    plugin_id: "telegram".to_owned(),
                                    reply_chat,
                                    reply_to_msg_id: context.message_id.clone(),
                                },
                                started_at_ms: now_ms(),
                                delegated: false,
                                notify_completed_sent: false,
                            },
                        );
                        let default_prompt = HubI18n::default_approval_prompt(locale);
                        let approval_msg = payload
                            .get("prompt")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or(default_prompt);
                        let _ = self
                            .team_service
                            .send_message(owner_user_id, &row.target_id, approval_msg, None)
                            .await;
                        Ok(ActionResponse {
                            text: Some(markdown_to_telegram_html(
                                HubI18n::approval_confirmed_markdown(locale),
                            )),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        })
                    }

                    "stop_run" => {
                        if let Ok(rs) = self.team_service.get_run_state(owner_user_id, &row.target_id).await
                            && let Some(run) = rs.active_run
                        {
                            let _ = self
                                .team_service
                                .cancel_run(
                                    owner_user_id,
                                    &row.target_id,
                                    &run.team_run_id,
                                    None,
                                    Some("Stopped via Telegram inline button".into()),
                                )
                                .await;
                        }
                        Ok(ActionResponse {
                            text: Some(markdown_to_telegram_html(HubI18n::run_stopped_markdown(locale))),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        })
                    }

                    "retry_member" => {
                        let slot_id = payload
                            .get("slot_id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        if !slot_id.is_empty()
                            && self
                                .team_service
                                .restart_agent_runtime(owner_user_id, &row.target_id, slot_id)
                                .await
                                .is_err()
                        {
                            let _ = self
                                .team_service
                                .attach_agent_runtime(owner_user_id, &row.target_id, slot_id)
                                .await;
                        }
                        Ok(ActionResponse {
                            text: Some(markdown_to_telegram_html(
                                HubI18n::member_runtime_restarted_markdown(locale),
                            )),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        })
                    }

                    "allow_more_hops" => {
                        let _ = self
                            .bridge_repo
                            .extend_room_hops_allowance(&row.target_id, 5)
                            .await?;
                        if let Ok(room_dto) = self.build_shared_room_dto(owner_user_id, &row.target_id).await {
                            self.event_bus.broadcast(WebSocketMessage::new(
                                "sharedRoom.updated",
                                serde_json::json!({
                                    "user_id": owner_user_id,
                                    "room": room_dto,
                                }),
                            ));
                        }
                        Ok(ActionResponse {
                            text: Some(markdown_to_telegram_html(
                                HubI18n::more_hops_granted_markdown(locale),
                            )),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        })
                    }

                    "worktree_merge" => {
                        let inbox_details: serde_json::Value = resolved_inbox_item
                            .as_ref()
                            .and_then(|item| serde_json::from_str(&item.details_json).ok())
                            .unwrap_or_default();
                        let lease_id_opt = payload
                            .get("lease_id")
                            .and_then(serde_json::Value::as_str)
                            .or_else(|| inbox_details.get("lease_id").and_then(serde_json::Value::as_str))
                            .or_else(|| (row.target_type == "worktree").then_some(row.target_id.as_str()));

                        if let Some(lease_id) = lease_id_opt
                            && let Ok(Some(lease)) = self.bridge_repo.get_worktree_lease(lease_id).await
                        {
                            let commit_msg = format!(
                                "feat({}): squash merge worktree {}",
                                lease.team_id.as_deref().unwrap_or("team"),
                                lease.lease_id
                            );
                            let engine = GitWorktreeEngine::new();
                            if let Ok(merge_res) = engine
                                .squash_merge(
                                    std::path::Path::new(&lease.repo_path),
                                    std::path::Path::new(&lease.worktree_path),
                                    &lease.branch,
                                    &lease.base_branch,
                                    &commit_msg,
                                    Some("User"),
                                    Some("user@local"),
                                    &lease.lease_id,
                                )
                                .await
                            {
                                let _ = self
                                    .bridge_repo
                                    .update_worktree_status(
                                        lease_id,
                                        "merged",
                                        merge_res.leftovers_ref.as_deref(),
                                    )
                                    .await;
                            }
                        }

                        let text = match locale {
                            HubLocale::ViVn => "✅ **Đã duyệt gộp nhánh Git Worktree qua Telegram!**",
                            HubLocale::EnUs => "✅ **Approved Git Worktree squash merge via Telegram!**",
                        };
                        Ok(ActionResponse {
                            text: Some(markdown_to_telegram_html(text)),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        })
                    }

                    "inbox_resolve" | "inbox_choice" | "budget_ack" => {
                        let text = match locale {
                            HubLocale::ViVn => "✅ **Đã ghi nhận phản hồi của anh qua Telegram!**",
                            HubLocale::EnUs => "✅ **Recorded your response via Telegram!**",
                        };
                        Ok(ActionResponse {
                            text: Some(markdown_to_telegram_html(text)),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: ActionBehavior::Send,
                            toast: None,
                            edit_message_id: None,
                        })
                    }

                    "compact_team_now" => {
                        let orch = self
                            .orchestrator
                            .get()
                            .ok_or_else(|| ChannelError::PlatformApi("TeamHubOrchestrator not initialized".into()))?;
                        let team = self
                            .team_service
                            .get_team(owner_user_id, &row.target_id)
                            .await
                            .map_err(|e| ChannelError::PlatformApi(e.to_string()))?;
                        let compacted = orch
                            .compact_team_context(
                                owner_user_id,
                                &row.target_id,
                                crate::bridge::context_compactor::CompactTeamContextRequest::default(),
                            )
                            .await
                            .map_err(|e| ChannelError::PlatformApi(format!("{e:?}")))?;
                        let avg_after = if compacted.context_status.slots.is_empty() {
                            0
                        } else {
                            let sum: i64 = compacted.context_status.slots.iter().map(|s| s.usage_percent).sum();
                            sum / (compacted.context_status.slots.len() as i64)
                        };
                        let html = markdown_to_telegram_html(&HubI18n::compact_completed_markdown(
                            locale,
                            &team.name,
                            compacted.outcomes.len(),
                            avg_after,
                        ));
                        if let Some(ref mid) = context.message_id {
                            let base_chat = split_chat_and_thread(&context.chat_id).0;
                            let _ = self
                                .channel_manager
                                .edit_message(
                                    owner_user_id,
                                    "telegram",
                                    base_chat,
                                    mid,
                                    UnifiedOutgoingMessage {
                                        message_type: OutgoingMessageType::Text,
                                        text: Some(html.clone()),
                                        parse_mode: Some(ParseMode::HTML),
                                        buttons: None,
                                        keyboard: None,
                                        image_url: None,
                                        file_url: None,
                                        file_name: None,
                                        media_actions: None,
                                        reply_to_message_id: None,
                                        silent: None,
                                    },
                                )
                                .await;
                        }
                        Ok(ActionResponse {
                            text: Some(html),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: if context.message_id.is_some() {
                                ActionBehavior::Edit
                            } else {
                                ActionBehavior::Send
                            },
                            toast: None,
                            edit_message_id: context.message_id.clone(),
                        })
                    }

                    "switch_team_model" => {
                        let orch = self
                            .orchestrator
                            .get()
                            .ok_or_else(|| ChannelError::PlatformApi("TeamHubOrchestrator not initialized".into()))?;
                        let scope = payload
                            .get("scope")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("all")
                            .to_owned();
                        let backend = payload
                            .get("backend")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        let provider_id = payload
                            .get("provider_id")
                            .and_then(serde_json::Value::as_str)
                            .filter(|s| !s.is_empty())
                            .map(str::to_owned);
                        let model_id = payload
                            .get("model_id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        let label = payload
                            .get("label")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or(&model_id)
                            .to_owned();

                        let switched = orch
                            .switch_team_models(
                                owner_user_id,
                                &row.target_id,
                                crate::bridge::context_compactor::SwitchTeamModelsRequest {
                                    scope: Some(scope.clone()),
                                    target: Some(crate::bridge::context_compactor::ModelTargetSpec {
                                        backend,
                                        provider_id,
                                        model_id,
                                    }),
                                    updates: Vec::new(),
                                    force_interrupt: Some(false),
                                },
                            )
                            .await
                            .map_err(|e| ChannelError::PlatformApi(format!("{e:?}")))?;

                        let switched_count = switched.results.iter().filter(|r| r.status == "switched").count();
                        let queued_count = switched
                            .results
                            .iter()
                            .filter(|r| r.status == "queued_after_turn")
                            .count();
                        let scope_label = if scope == "teammates_only" {
                            HubI18n::scope_teammates_only_label(locale)
                        } else {
                            HubI18n::scope_all_members_label(locale)
                        };
                        let html = markdown_to_telegram_html(&HubI18n::model_switched_markdown(
                            locale,
                            &switched.team.name,
                            scope_label,
                            &label,
                            switched_count,
                            queued_count,
                        ));

                        if let Some(ref mid) = context.message_id {
                            let base_chat = split_chat_and_thread(&context.chat_id).0;
                            let _ = self
                                .channel_manager
                                .edit_message(
                                    owner_user_id,
                                    "telegram",
                                    base_chat,
                                    mid,
                                    UnifiedOutgoingMessage {
                                        message_type: OutgoingMessageType::Text,
                                        text: Some(html.clone()),
                                        parse_mode: Some(ParseMode::HTML),
                                        buttons: None,
                                        keyboard: None,
                                        image_url: None,
                                        file_url: None,
                                        file_name: None,
                                        media_actions: None,
                                        reply_to_message_id: None,
                                        silent: None,
                                    },
                                )
                                .await;
                        }

                        Ok(ActionResponse {
                            text: Some(html),
                            parse_mode: Some(ParseMode::HTML),
                            buttons: None,
                            keyboard: None,
                            behavior: if context.message_id.is_some() {
                                ActionBehavior::Edit
                            } else {
                                ActionBehavior::Send
                            },
                            toast: None,
                            edit_message_id: context.message_id.clone(),
                        })
                    }

                    _ => unreachable!(),
                }
            }

            other => Ok(ActionResponse {
                text: None,
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Answer,
                toast: Some(HubI18n::unknown_action_toast(locale, other)),
                edit_message_id: None,
            }),
        }
    }

    async fn try_dispatch_to_target(
        &self,
        owner_user_id: &str,
        msg: &UnifiedIncomingMessage,
    ) -> Result<bool, ChannelError> {
        // Priority 1: Swipe-Reply to any outbound `[Team · Leader 👑]` or `[#Room]` message!
        if let Some(reply_id) = msg.reply_to_message_id.as_deref()
            && let Some(route) = self
                .bridge_repo
                .resolve_msg_route("telegram", &msg.chat_id, reply_id)
                .await?
        {
            debug!(
                reply_id = %reply_id,
                target_type = %route.target_type,
                target_id = %route.target_id,
                "routing Telegram reply directly via custom_channel_msg_routes"
            );
            return if route.target_type == "room" {
                self.dispatch_telegram_message_to_room(owner_user_id, &route.target_id, msg)
                    .await
            } else {
                self.dispatch_telegram_message_to_team(owner_user_id, &route.target_id, msg)
                    .await
            };
        }

        // Priority 2: Explicit Group / Supergroup Forum Topic binding
        if let Some(binding) = self
            .bridge_repo
            .resolve_channel_binding_for_chat(owner_user_id, "telegram", &msg.chat_id, msg.thread_id().as_deref())
            .await?
        {
            return if binding.target_type == "room" {
                self.dispatch_telegram_message_to_room(owner_user_id, &binding.target_id, msg)
                    .await
            } else {
                self.dispatch_telegram_message_to_team(owner_user_id, &binding.target_id, msg)
                    .await
            };
        }

        // Priority 3: Active 1-1 DM target (selected via `/team`, `/room`, or AionUi toggle)
        if !msg.is_group_chat()
            && let Some(dm) = self
                .bridge_repo
                .get_dm_target(owner_user_id, "telegram", &msg.chat_id)
                .await?
        {
            return if dm.target_type == "room" {
                self.dispatch_telegram_message_to_room(owner_user_id, &dm.target_id, msg)
                    .await
            } else {
                self.dispatch_telegram_message_to_team(owner_user_id, &dm.target_id, msg)
                    .await
            };
        }

        Ok(false)
    }

    async fn list_team_bridge_configs(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<TeamTelegramBridgeConfigDto>, ChannelError> {
        let teams = self
            .team_service
            .list_teams(owner_user_id)
            .await
            .map_err(|e| ChannelError::PlatformApi(e.to_string()))?;

        let (enabled, connected, bot_username, auth_dm_chat) =
            self.telegram_status_snapshot(owner_user_id).await;
        let has_auth_user = auth_dm_chat.is_some();

        let prefs: HashMap<String, _> = self
            .bridge_repo
            .list_target_notify_prefs(owner_user_id, CustomTargetType::Team)
            .await?
            .into_iter()
            .map(|p| (p.target_id.clone(), p))
            .collect();

        let bindings = self
            .bridge_repo
            .list_channel_bindings_for_owner(owner_user_id)
            .await?;
        let dm_targets = self.bridge_repo.list_dm_targets_for_owner(owner_user_id).await?;

        let mut out = Vec::with_capacity(teams.len());
        for team in teams {
            let pref = prefs.get(&team.id);
            let notify_enabled = pref.map(|p| p.enabled).unwrap_or(has_auth_user);
            let stream_mode = pref
                .map(|p| p.stream_mode.clone())
                .unwrap_or_else(|| CustomStreamMode::Milestones.as_str().to_owned());

            let group_binding = bindings
                .iter()
                .find(|b| b.target_type == "team" && b.target_id == team.id);
            let is_active_dm_target = dm_targets
                .iter()
                .any(|d| d.target_type == "team" && d.target_id == team.id);

            out.push(TeamTelegramBridgeConfigDto {
                team_id: team.id,
                telegram_plugin_enabled: enabled,
                telegram_connected: connected,
                bot_username: bot_username.clone(),
                has_authorized_user: has_auth_user,
                notify_enabled,
                stream_mode,
                is_active_dm_target,
                group_chat_id: group_binding.map(|b| b.chat_id.clone()),
                group_thread_id: group_binding.and_then(|b| b.thread_id.clone()),
            });
        }
        Ok(out)
    }

    async fn get_team_bridge_config(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<TeamTelegramBridgeConfigDto, ChannelError> {
        let _ = self
            .team_service
            .get_team(owner_user_id, team_id)
            .await
            .map_err(|_| ChannelError::SessionNotFound(format!("Team not found: {team_id}")))?;

        let (enabled, connected, bot_username, auth_dm_chat) =
            self.telegram_status_snapshot(owner_user_id).await;
        let has_auth_user = auth_dm_chat.is_some();
        let (notify_enabled, stream_mode) = self
            .resolve_team_notify_settings(owner_user_id, team_id, has_auth_user)
            .await;

        let bindings = self
            .bridge_repo
            .list_channel_bindings_for_target(owner_user_id, CustomTargetType::Team, team_id)
            .await?;
        let group_binding = bindings.first();

        let is_active_dm_target = self
            .bridge_repo
            .list_dm_targets_for_owner(owner_user_id)
            .await?
            .iter()
            .any(|d| d.target_type == "team" && d.target_id == team_id);

        Ok(TeamTelegramBridgeConfigDto {
            team_id: team_id.to_owned(),
            telegram_plugin_enabled: enabled,
            telegram_connected: connected,
            bot_username,
            has_authorized_user: has_auth_user,
            notify_enabled,
            stream_mode: stream_mode.as_str().to_owned(),
            is_active_dm_target,
            group_chat_id: group_binding.map(|b| b.chat_id.clone()),
            group_thread_id: group_binding.and_then(|b| b.thread_id.clone()),
        })
    }

    async fn upsert_team_bridge_config(
        &self,
        owner_user_id: &str,
        team_id: &str,
        req: UpsertTeamTelegramBridgeRequest,
    ) -> Result<TeamTelegramBridgeConfigDto, ChannelError> {
        let current = self.get_team_bridge_config(owner_user_id, team_id).await?;

        let next_notify = req.notify_enabled.unwrap_or(current.notify_enabled);
        let next_mode = req
            .stream_mode
            .as_deref()
            .map(CustomStreamMode::parse)
            .unwrap_or_else(|| CustomStreamMode::parse(&current.stream_mode));

        self.bridge_repo
            .upsert_target_notify_pref(
                owner_user_id,
                CustomTargetType::Team,
                team_id,
                next_notify,
                next_mode,
            )
            .await?;

        if req.clear_group_binding == Some(true) {
            self.bridge_repo
                .delete_channel_bindings_for_target(owner_user_id, CustomTargetType::Team, team_id)
                .await?;
        } else if let Some(chat_id) = req.group_chat_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            self.bridge_repo
                .delete_channel_bindings_for_target(owner_user_id, CustomTargetType::Team, team_id)
                .await?;
            self.bridge_repo
                .upsert_channel_binding(UpsertChannelBindingParams {
                    owner_user_id: owner_user_id.to_owned(),
                    plugin_id: "telegram".to_owned(),
                    chat_id: chat_id.to_owned(),
                    thread_id: req.group_thread_id.clone(),
                    target_type: CustomTargetType::Team,
                    target_id: team_id.to_owned(),
                    stream_mode: next_mode,
                })
                .await?;
        }

        if let Some(set_dm) = req.set_active_dm_target {
            let (_, _, _, auth_dm_chat) = self.telegram_status_snapshot(owner_user_id).await;
            if set_dm {
                if let Some(dm_chat) = auth_dm_chat {
                    self.bridge_repo
                        .set_dm_target(
                            owner_user_id,
                            "telegram",
                            &dm_chat,
                            &dm_chat,
                            CustomTargetType::Team,
                            team_id,
                        )
                        .await?;
                }
            } else {
                let dms = self.bridge_repo.list_dm_targets_for_owner(owner_user_id).await?;
                for dm in dms {
                    if dm.target_type == "team" && dm.target_id == team_id {
                        self.bridge_repo
                            .clear_dm_target(owner_user_id, &dm.plugin_id, &dm.chat_id)
                            .await?;
                    }
                }
            }
        }

        self.get_team_bridge_config(owner_user_id, team_id).await
    }

    async fn delete_team_group_binding(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<TeamTelegramBridgeConfigDto, ChannelError> {
        self.bridge_repo
            .delete_channel_bindings_for_target(owner_user_id, CustomTargetType::Team, team_id)
            .await?;
        self.get_team_bridge_config(owner_user_id, team_id).await
    }
}

// ===========================================================================
// Trait 2: `TeamExternalBridgePort` (for `aionui-team` MCP + CLI tools)
// ===========================================================================

#[async_trait]
impl TeamExternalBridgePort for TelegramTeamHub {
    async fn notify_user(
        &self,
        user_id: &str,
        team_id: &str,
        kind: &str,
        summary: &str,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String> {
        let team = self
            .team_service
            .get_team(user_id, team_id)
            .await
            .map_err(|e| e.to_string())?;
        let (lead_name, _) = Self::find_lead_info(&team);
        let locale = self.resolve_locale(user_id).await;
        let badge = HubI18n::task_status_badge(locale, kind);

        if kind == "completed"
            && let Some(mut entry) = self.active_runs.get_mut(team_id)
        {
            entry.notify_completed_sent = true;
        }

        // Broadcast realtime toast/event to Desktop UI as well
        self.event_bus.broadcast(WebSocketMessage::new(
            "team.proactiveNotification",
            serde_json::json!({
                "user_id": user_id,
                "team_id": team_id,
                "team_name": team.name,
                "kind": kind,
                "summary": summary,
                "files": files,
                "created_at": now_ms(),
            }),
        ));

        let reply_chat = match self.active_runs.get(team_id).map(|e| e.origin.clone()) {
            Some(RunOrigin::Telegram { reply_chat, .. }) => Some(reply_chat),
            _ => self.resolve_team_telegram_destination(user_id, team_id).await,
        };

        let is_actionable = matches!(kind, "approval_needed" | "decision" | "blocker" | "blocked");

        let Some(dest_chat) = reply_chat else {
            if is_actionable {
                let tok = self
                    .bridge_repo
                    .create_action_token(
                        user_id,
                        CustomTargetType::Team,
                        team_id,
                        "approve_continue",
                        &serde_json::json!({}),
                        None,
                    )
                    .await
                    .ok();
                let inbox_title = format!("[{} · {} 👑] {}", team.name, lead_name, badge);
                if let Ok(inbox_row) = self
                    .bridge_repo
                    .create_action_inbox_item(CreateActionInboxParams {
                        id: None,
                        owner_user_id: user_id.to_owned(),
                        category: "multiple_choice".to_owned(),
                        title: inbox_title,
                        summary: summary.to_owned(),
                        details_json: serde_json::json!({
                            "team_id": team_id,
                            "team_name": team.name,
                            "kind": kind,
                            "options": ["Approve & Continue", "Hold / Review"],
                            "recommended_index": 0,
                        })
                        .to_string(),
                        target_type: Some("team".to_owned()),
                        target_id: team_id.to_owned(),
                        action_token: tok,
                        telegram_chat_id: None,
                        telegram_msg_id: None,
                    })
                    .await
                {
                    self.event_bus.broadcast(WebSocketMessage::new(
                        "inbox:item_added",
                        serde_json::json!({
                            "user_id": user_id,
                            "item": ActionInboxCardDto::from_row(inbox_row),
                        }),
                    ));
                }
            }
            return Ok(serde_json::json!({
                "delivered_telegram": false,
                "delivered_desktop": true,
                "note": "No active Telegram destination connected; notification delivered to Desktop UI."
            }));
        };

        let md = format!(
            "**[{team_name} · {lead_name} 👑]** — _{badge}_\n\n{summary}",
            team_name = team.name
        );
        let include_approve = is_actionable;
        let include_stop = kind != "completed";
        let buttons = self
            .make_team_quick_buttons(user_id, team_id, include_approve, include_stop)
            .await;
        let approve_tok = buttons
            .as_ref()
            .and_then(|rows| rows.first())
            .and_then(|r| r.first())
            .and_then(|b| b.action.strip_prefix("t:"))
            .map(str::to_owned);

        let sent_ids = self
            .send_formatted_telegram_message(
                user_id,
                &dest_chat,
                &md,
                buttons,
                None,
                CustomTargetType::Team,
                team_id,
            )
            .await?;

        if is_actionable {
            let inbox_title = format!("[{} · {} 👑] {}", team.name, lead_name, badge);
            if let Ok(inbox_row) = self
                .bridge_repo
                .create_action_inbox_item(CreateActionInboxParams {
                    id: None,
                    owner_user_id: user_id.to_owned(),
                    category: "multiple_choice".to_owned(),
                    title: inbox_title,
                    summary: summary.to_owned(),
                    details_json: serde_json::json!({
                        "team_id": team_id,
                        "team_name": team.name,
                        "kind": kind,
                        "options": ["Approve & Continue", "Hold / Review"],
                        "recommended_index": 0,
                    })
                    .to_string(),
                    target_type: Some("team".to_owned()),
                    target_id: team_id.to_owned(),
                    action_token: approve_tok,
                    telegram_chat_id: Some(dest_chat.clone()),
                    telegram_msg_id: sent_ids.last().cloned(),
                })
                .await
            {
                self.event_bus.broadcast(WebSocketMessage::new(
                    "inbox:item_added",
                    serde_json::json!({
                        "user_id": user_id,
                        "item": ActionInboxCardDto::from_row(inbox_row),
                    }),
                ));
            }
        }

        let delivered_files = self
            .send_validated_outbound_files(
                user_id,
                &dest_chat,
                &files,
                Some(&team.workspace),
                CustomTargetType::Team,
                team_id,
            )
            .await;

        info!(
            team_id = %team_id,
            kind = %kind,
            chunks = sent_ids.len(),
            files = delivered_files.len(),
            "proactive team_notify_user delivered to Telegram"
        );

        Ok(serde_json::json!({
            "delivered_telegram": true,
            "delivered_desktop": true,
            "message_ids": sent_ids,
            "delivered_files": delivered_files,
        }))
    }

    async fn room_read(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let resolved_room_id = match room_id.map(str::trim).filter(|s| !s.is_empty()) {
            Some(id) => id.to_owned(),
            None => self.resolve_default_room_for_team(user_id, team_id).await?,
        };

        let room_dto = self.build_shared_room_dto(user_id, &resolved_room_id).await?;
        let messages: Vec<SharedRoomMessageDto> = self
            .bridge_repo
            .list_shared_room_messages(&resolved_room_id, limit.clamp(1, 200) as i64)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(Self::map_room_message_dto)
            .collect();
        let tasks = self
            .list_shared_room_task_dtos(user_id, &resolved_room_id)
            .await?;

        Ok(serde_json::json!({
            "room": room_dto,
            "messages": messages,
            "tasks": tasks,
        }))
    }

    async fn room_post(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        message: &str,
        mentions: Vec<String>,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String> {
        let resolved_room_id = match room_id.map(str::trim).filter(|s| !s.is_empty()) {
            Some(id) => id.to_owned(),
            None => self.resolve_default_room_for_team(user_id, team_id).await?,
        };

        let (msg_dto, routed) = self
            .post_message_to_shared_room(PostRoomMessageArgs {
                owner_user_id: user_id,
                room_id: &resolved_room_id,
                sender_type: "leader",
                sender_team_id: Some(team_id),
                content: message,
                explicit_mentions: mentions,
                source_files: files,
                reply_to_id: None,
                origin_telegram_chat: None,
                wake_targets: None,
            })
            .await?;

        Ok(serde_json::json!({
            "posted": true,
            "room_id": resolved_room_id,
            "message_id": msg_dto.id,
            "woken_targets": routed,
        }))
    }

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
    ) -> Result<serde_json::Value, String> {
        let resolved_room_id = match room_id.map(str::trim).filter(|s| !s.is_empty()) {
            Some(id) => id.to_owned(),
            None => self.resolve_default_room_for_team(user_id, team_id).await?,
        };

        let task_dto = self
            .create_cross_team_task(
                user_id,
                &resolved_room_id,
                Some(team_id),
                to_team,
                title,
                description.unwrap_or(""),
                blocked_by,
                files,
            )
            .await?;

        Ok(serde_json::json!({
            "created": true,
            "task": task_dto,
        }))
    }

    async fn room_task_update(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        status: Option<&str>,
        result_summary: Option<&str>,
        files: Vec<String>,
    ) -> Result<serde_json::Value, String> {
        // Feature F19: Human-Only Done Guard
        if let Some(s) = status {
            let normalized = s.trim().to_lowercase();
            if normalized == "done" || normalized == "completed" {
                return Err(
                    "Forbidden: Only human users can transition a task to Done. Agents may only transition tasks up to 'in_review' or 'blocked'."
                        .to_string(),
                );
            }
        }

        let task_dto = self
            .update_cross_team_task(
                user_id,
                task_id,
                Some(team_id),
                status.map(ToOwned::to_owned),
                None,
                result_summary.map(ToOwned::to_owned),
                files,
            )
            .await?;

        Ok(serde_json::json!({
            "updated": true,
            "task": task_dto,
        }))
    }

    async fn pending_room_handoff_banner(&self, _user_id: &str, team_id: &str) -> Option<String> {
        let pending = self
            .bridge_repo
            .list_pending_handoffs_for_team(team_id)
            .await
            .ok()?;
        if pending.is_empty() {
            return None;
        }
        let mut lines = Vec::new();
        for (room, member) in pending {
            lines.push(format!(
                "[PENDING ROOM HANDOFF -> #{name} (room_id=\"{id}\", msg_id=\"{msg_id}\")]: When your team finishes this work or reaches a decision, call `team_room_post` (room_id=\"{id}\") to deliver your response/files back to the Shared Room.",
                name = room.name,
                id = room.id,
                msg_id = member.pending_handoff_msg_id.unwrap_or_default(),
            ));
        }
        Some(lines.join("\n"))
    }

    fn on_team_delegation(&self, team_id: &str) {
        if let Some(mut entry) = self.active_runs.get_mut(team_id) {
            entry.delegated = true;
        }
    }

    async fn pending_slot_handover_banner(
        &self,
        _user_id: &str,
        team_id: &str,
        slot_id: &str,
        _conversation_id: &str,
    ) -> Option<String> {
        let anchor = self
            .bridge_repo
            .consume_pending_slot_handover(team_id, slot_id)
            .await
            .ok()??;
        let trimmed = anchor.anchored_summary_md.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(format!("## Anchored Session Handover\n\n{trimmed}"))
    }

    fn on_slot_turn_settled(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        conversation_id: &str,
    ) {
        let Some(orch) = self.orchestrator.get().cloned() else {
            return;
        };
        let uid = user_id.to_owned();
        let tid = team_id.to_owned();
        let sid = slot_id.to_owned();
        let cid = conversation_id.to_owned();
        tokio::spawn(async move {
            orch.handle_slot_turn_settled(&uid, &tid, &sid, &cid).await;
        });
    }
}

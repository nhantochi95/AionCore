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
    CreateActionInboxParams, RecordCostRunParams, SlotWalletFacts, WalletType, classify_slot_wallet_facts,
    estimate_api_token_cost_usd, slot_has_custom_api_key,
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
    RejectedOutboundFile, StagedRoomArtifact, is_image_file_name, partition_outbound_files, sanitize_file_name,
    save_inbound_telegram_attachment, stage_room_artifacts_batch_async, stage_room_oversized_message_async,
};
use crate::bridge::judge::{
    ParticipatingTeam, RoomRoutingHeuristics, RoomRoutingInput, RoutedTarget, is_task_blocked,
    is_team_blocked_by_dependencies, mention_slug, resolve_room_targets_smart, validate_task_dependencies,
};
use crate::bridge::locales::{HubI18n, HubLocale};
use crate::routes::inbox::ActionInboxCardDto;

/// Minimum run duration (45s) for Desktop-initiated runs to trigger a proactive Telegram summary
/// when no teammates were delegated and `stream_mode == "milestones"`.
const SMART_FILTER_DESKTOP_DURATION_MS: i64 = 45_000;

/// Threshold above which outbound Telegram file uploads are staged asynchronously (`512 KiB`).
const OUTBOUND_INLINE_BYTE_CAP: u64 = 512 * 1024;
/// Maximum concurrency for outbound Telegram attachment uploads.
const OUTBOUND_UPLOAD_CONCURRENCY: usize = 2;
/// Startup garbage-collection max age for orphaned `<data_dir>/outbound-staging` files (`15 min`).
const OUTBOUND_STAGING_MAX_AGE_SECS: u64 = 15 * 60;
/// Maximum wait duration when draining in-flight staged Telegram uploads on shutdown.
const OUTBOUND_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

struct TempStagedFile(PathBuf);

impl Drop for TempStagedFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct InflightUploadGuard {
    counter: Arc<std::sync::atomic::AtomicUsize>,
    drained: Arc<tokio::sync::Notify>,
}

impl Drop for InflightUploadGuard {
    fn drop(&mut self) {
        if self.counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
}

#[derive(Debug, Clone, Default)]
struct OutboundFilesDispatchResult {
    delivered_files: Vec<String>,
    queued_files: Vec<String>,
    rejected_files: Vec<RejectedOutboundFile>,
}

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
    pub live_steering: bool,
}

#[derive(Debug, Clone)]
struct PendingCrossTeamTaskWake {
    task_id: String,
    title: String,
    description: String,
    room_name: String,
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
    http_client: reqwest::Client,
    pending_task_wakes: Arc<DashMap<(String, String), PendingCrossTeamTaskWake>>,
    delivered_handoff_banners: DashMap<(String, String), String>,
    last_room_read_cursor: DashMap<(String, String), String>,
    outbound_upload_semaphore: Arc<tokio::sync::Semaphore>,
    inflight_outbound_uploads: Arc<std::sync::atomic::AtomicUsize>,
    outbound_uploads_drained: Arc<tokio::sync::Notify>,
    outbound_staging_gc_started: std::sync::atomic::AtomicBool,
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
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(4))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
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
            http_client,
            pending_task_wakes: Arc::new(DashMap::new()),
            delivered_handoff_banners: DashMap::new(),
            last_room_read_cursor: DashMap::new(),
            outbound_upload_semaphore: Arc::new(tokio::sync::Semaphore::new(OUTBOUND_UPLOAD_CONCURRENCY)),
            inflight_outbound_uploads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            outbound_uploads_drained: Arc::new(tokio::sync::Notify::new()),
            outbound_staging_gc_started: std::sync::atomic::AtomicBool::new(false),
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

    /// Sweeps `<data_dir>/outbound-staging` once on startup to remove orphaned staged files older than 15 minutes.
    pub fn spawn_outbound_staging_gc(&self) {
        if self
            .outbound_staging_gc_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let staging_dir = self.data_dir.join("outbound-staging");
        tokio::task::spawn_blocking(move || {
            let Ok(entries) = std::fs::read_dir(&staging_dir) else {
                return;
            };
            let max_age = std::time::Duration::from_secs(OUTBOUND_STAGING_MAX_AGE_SECS);
            let now = std::time::SystemTime::now();
            for entry in entries.flatten() {
                let path = entry.path();
                let Ok(meta) = std::fs::symlink_metadata(&path) else {
                    continue;
                };
                if !meta.file_type().is_file() {
                    continue;
                }
                let is_stale = meta
                    .modified()
                    .ok()
                    .and_then(|mtime| now.duration_since(mtime).ok())
                    .is_some_and(|age| age > max_age);
                if is_stale {
                    let _ = std::fs::remove_file(&path);
                }
            }
        });
    }

    /// Waits up to 5 seconds for in-flight background Telegram attachment uploads to complete.
    pub async fn drain_outbound_uploads(&self) {
        let wait_loop = async {
            loop {
                let notified = self.outbound_uploads_drained.notified();
                if self
                    .inflight_outbound_uploads
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0
                {
                    break;
                }
                notified.await;
            }
        };
        if tokio::time::timeout(OUTBOUND_DRAIN_TIMEOUT, wait_loop)
            .await
            .is_err()
        {
            warn!(
                remaining = self
                    .inflight_outbound_uploads
                    .load(std::sync::atomic::Ordering::Acquire),
                "timed out after 5s waiting for outbound Telegram uploads to drain"
            );
        }
    }

    /// Spawns the background EventBus watcher that handles:
    /// 1. Desktop-origin Team run tracking (`team.runStarted`)
    /// 2. Immediate crash / rate-limit / quota alerts (`team.agentRuntimeStatusChanged` / `team.runFailed`)
    /// 3. Closed-loop Shared Room synthesis mirroring + Smart Notification Filter (`team.runCompleted`)
    pub fn spawn_event_watcher(self: &Arc<Self>) {
        self.spawn_outbound_staging_gc();
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
                            if let Some(orch) = hub_clone.team_hub_orchestrator() {
                                orch.reset_team_run_turn_hops(&payload.team_id);
                            }
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
                            if let Some(orch) = hub_clone.team_hub_orchestrator() {
                                orch.reset_team_run_turn_hops(&payload.team_id);
                            }
                            let uid = user_id.to_owned();
                            tokio::spawn(async move {
                                hub_clone.handle_team_run_failed(&uid, payload).await;
                            });
                        }
                    }
                    TEAM_RUN_CANCELLED_EVENT => {
                        if let Ok(payload) = serde_json::from_value::<TeamRunPayload>(event.data.clone()) {
                            if let Some(orch) = hub_clone.team_hub_orchestrator() {
                                orch.reset_team_run_turn_hops(&payload.team_id);
                            }
                            hub_clone.active_runs.remove(&payload.team_id);
                        }
                    }
                    TEAM_REMOVED_EVENT => {
                        if let Some(user_id) = event.data.get("user_id").and_then(serde_json::Value::as_str)
                            && let Some(team_id) = event.data.get("team_id").and_then(serde_json::Value::as_str)
                        {
                            if let Some(orch) = hub_clone.team_hub_orchestrator() {
                                orch.reset_team_run_turn_hops(team_id);
                            }
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
                            if let Some(orch) = hub_clone.team_hub_orchestrator() {
                                orch.reset_team_run_turn_hops(&payload.team_id);
                            }
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
            hub.drain_outbound_uploads().await;
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

    async fn partition_outbound_files_async(
        &self,
        raw_files: Vec<String>,
        team_workspace: Option<String>,
    ) -> (Vec<(PathBuf, String, u64)>, Vec<RejectedOutboundFile>) {
        if raw_files.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let data_dir = self.data_dir.clone();
        match tokio::task::spawn_blocking(move || {
            partition_outbound_files(&raw_files, team_workspace.as_deref(), &data_dir)
        })
        .await
        {
            Ok((valid, rejected)) => {
                for r in &rejected {
                    warn!(file = %r.path, error = %r.reason, "outbound file rejected by path guard");
                }
                (valid, rejected)
            }
            Err(join_err) => {
                let reason = format!("Outbound file validation task panicked: {join_err}");
                let rejected = Vec::new();
                warn!(error = %reason, "outbound file validation task failed");
                (Vec::new(), rejected)
            }
        }
    }

    async fn send_validated_outbound_files(
        &self,
        owner_user_id: &str,
        reply_chat: &str,
        raw_files: &[String],
        team_workspace: Option<&str>,
        target_type: CustomTargetType,
        target_id: &str,
        reply_to_message_id: Option<&str>,
    ) -> OutboundFilesDispatchResult {
        let (valid, rejected) = self
            .partition_outbound_files_async(raw_files.to_vec(), team_workspace.map(str::to_owned))
            .await;
        self.dispatch_prevalidated_outbound_files(
            owner_user_id,
            reply_chat,
            valid,
            rejected,
            target_type,
            target_id,
            reply_to_message_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_prevalidated_outbound_files(
        &self,
        owner_user_id: &str,
        reply_chat: &str,
        valid_files: Vec<(PathBuf, String, u64)>,
        mut rejected_files: Vec<RejectedOutboundFile>,
        target_type: CustomTargetType,
        target_id: &str,
        reply_to_message_id: Option<&str>,
    ) -> OutboundFilesDispatchResult {
        if valid_files.is_empty() {
            return OutboundFilesDispatchResult {
                delivered_files: Vec::new(),
                queued_files: Vec::new(),
                rejected_files,
            };
        }

        let base_chat_id = split_chat_and_thread(reply_chat).0.to_owned();
        let total_bytes: u64 = valid_files.iter().map(|(_, _, sz)| *sz).sum();
        let should_stage_async = valid_files.len() > 1 || total_bytes > OUTBOUND_INLINE_BYTE_CAP;

        if should_stage_async {
            self.spawn_outbound_staging_gc();
            let staging_dir = self.data_dir.join("outbound-staging");
            let stage_result = tokio::task::spawn_blocking(move || {
                let mut staged_items: Vec<(TempStagedFile, String)> = Vec::with_capacity(valid_files.len());
                let mut copy_rejections: Vec<RejectedOutboundFile> = Vec::new();
                if let Err(e) = std::fs::create_dir_all(&staging_dir) {
                    for (src, _, _) in valid_files {
                        copy_rejections.push(RejectedOutboundFile {
                            path: src.to_string_lossy().into_owned(),
                            reason: format!(
                                "Failed to create outbound staging directory '{}': {e}",
                                staging_dir.display()
                            ),
                        });
                    }
                    return (staged_items, copy_rejections);
                }
                for (src, safe_name, _) in valid_files {
                    let dest = staging_dir.join(format!("{}_{}", generate_prefixed_id("outf"), safe_name));
                    match std::fs::copy(&src, &dest) {
                        Ok(_) => staged_items.push((TempStagedFile(dest), safe_name)),
                        Err(e) => copy_rejections.push(RejectedOutboundFile {
                            path: src.to_string_lossy().into_owned(),
                            reason: format!("Failed to stage outbound attachment '{}': {e}", src.display()),
                        }),
                    }
                }
                (staged_items, copy_rejections)
            })
            .await;

            let (staged_items, copy_rejections) = match stage_result {
                Ok(pair) => pair,
                Err(join_err) => {
                    warn!(error = %join_err, "outbound staging copy task panicked");
                    return OutboundFilesDispatchResult {
                        delivered_files: Vec::new(),
                        queued_files: Vec::new(),
                        rejected_files,
                    };
                }
            };
            rejected_files.extend(copy_rejections);
            let queued_files: Vec<String> = staged_items.iter().map(|(_, name)| name.clone()).collect();

            if !staged_items.is_empty() {
                self.inflight_outbound_uploads
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                let inflight_guard = InflightUploadGuard {
                    counter: Arc::clone(&self.inflight_outbound_uploads),
                    drained: Arc::clone(&self.outbound_uploads_drained),
                };
                let semaphore = Arc::clone(&self.outbound_upload_semaphore);
                let channel_manager = Arc::clone(&self.channel_manager);
                let bridge_repo = Arc::clone(&self.bridge_repo);
                let uid = owner_user_id.to_owned();
                let chat = reply_chat.to_owned();
                let tid = target_id.to_owned();
                let reply_to = reply_to_message_id.map(str::to_owned);

                tokio::spawn(async move {
                    let _inflight = inflight_guard;
                    for (staged_file, safe_name) in staged_items {
                        let _permit = semaphore.acquire().await;
                        let path_str = staged_file.0.to_string_lossy().into_owned();
                        let is_img = is_image_file_name(&safe_name);
                        let msg = UnifiedOutgoingMessage {
                            message_type: if is_img {
                                OutgoingMessageType::Image
                            } else {
                                OutgoingMessageType::File
                            },
                            text: Some(format!("📎 {safe_name}")),
                            parse_mode: None,
                            buttons: None,
                            keyboard: None,
                            image_url: if is_img { Some(path_str.clone()) } else { None },
                            file_url: if !is_img { Some(path_str) } else { None },
                            file_name: Some(safe_name.clone()),
                            media_actions: None,
                            reply_to_message_id: reply_to.clone(),
                            silent: None,
                        };

                        match channel_manager.send_message(&uid, "telegram", &chat, msg).await {
                            Ok(mid) => {
                                let _ = bridge_repo
                                    .record_msg_route("telegram", &base_chat_id, &mid, target_type, &tid)
                                    .await;
                            }
                            Err(err) => {
                                warn!(
                                    file = %safe_name,
                                    error = %err,
                                    "staged background Telegram attachment upload failed"
                                );
                                let fallback_notice = UnifiedOutgoingMessage {
                                    message_type: OutgoingMessageType::Text,
                                    text: Some(format!("⚠️ Failed to upload attachment '{safe_name}'.")),
                                    parse_mode: None,
                                    buttons: None,
                                    keyboard: None,
                                    image_url: None,
                                    file_url: None,
                                    file_name: None,
                                    media_actions: None,
                                    reply_to_message_id: reply_to.clone(),
                                    silent: None,
                                };
                                let _ = channel_manager
                                    .send_message(&uid, "telegram", &chat, fallback_notice)
                                    .await;
                            }
                        }
                        drop(staged_file);
                    }
                });
            }

            return OutboundFilesDispatchResult {
                delivered_files: Vec::new(),
                queued_files,
                rejected_files,
            };
        }

        // Fast inline path: single validated file <= 512 KiB
        let mut delivered_files = Vec::with_capacity(1);
        for (valid_path, safe_name, _) in valid_files {
            let _permit = self.outbound_upload_semaphore.acquire().await;
            let path_str = valid_path.to_string_lossy().into_owned();
            let is_img = is_image_file_name(&safe_name);
            let msg = UnifiedOutgoingMessage {
                message_type: if is_img {
                    OutgoingMessageType::Image
                } else {
                    OutgoingMessageType::File
                },
                text: Some(format!("📎 {safe_name}")),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                image_url: if is_img { Some(path_str.clone()) } else { None },
                file_url: if !is_img { Some(path_str) } else { None },
                file_name: Some(safe_name.clone()),
                media_actions: None,
                reply_to_message_id: reply_to_message_id.map(str::to_owned),
                silent: None,
            };

            match self
                .channel_manager
                .send_message(owner_user_id, "telegram", reply_chat, msg)
                .await
            {
                Ok(mid) => {
                    let _ = self
                        .bridge_repo
                        .record_msg_route("telegram", &base_chat_id, &mid, target_type, target_id)
                        .await;
                    delivered_files.push(safe_name);
                }
                Err(err) => {
                    warn!(file = %safe_name, error = %err, "inline Telegram attachment upload failed");
                    rejected_files.push(RejectedOutboundFile {
                        path: valid_path.to_string_lossy().into_owned(),
                        reason: format!("Telegram upload failed: {err}"),
                    });
                    let fallback_notice = UnifiedOutgoingMessage {
                        message_type: OutgoingMessageType::Text,
                        text: Some(format!("⚠️ Failed to upload attachment '{safe_name}'.")),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        image_url: None,
                        file_url: None,
                        file_name: None,
                        media_actions: None,
                        reply_to_message_id: reply_to_message_id.map(str::to_owned),
                        silent: None,
                    };
                    let _ = self
                        .channel_manager
                        .send_message(owner_user_id, "telegram", reply_chat, fallback_notice)
                        .await;
                }
            }
        }

        OutboundFilesDispatchResult {
            delivered_files,
            queued_files: Vec::new(),
            rejected_files,
        }
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
        let (_, name, conv_id) = Self::find_lead_slot_info(team);
        (name, conv_id)
    }

    fn find_lead_slot_info(team: &TeamResponse) -> (String, String, String) {
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
            Some(a) => (a.slot_id.clone(), a.name.clone(), a.conversation_id.clone()),
            None => ("lead".to_owned(), "Leader".to_owned(), String::new()),
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
        let member = team.assistants.iter().find(|a| a.slot_id == payload.slot_id);
        let member_name = member
            .map(|a| a.name.clone())
            .unwrap_or_else(|| payload.slot_id.clone());
        let is_worker_slot = member.is_some_and(|a| {
            !a.role.eq_ignore_ascii_case("lead")
                && !a.role.eq_ignore_ascii_case("leader")
                && team.leader_assistant_id.as_deref() != Some(a.slot_id.as_str())
        });
        let reason = payload
            .error
            .unwrap_or_else(|| "Runtime process exited or hit rate/quota limit".to_owned());

        // 1. Always surface Desktop WebSocket alert + ActionInbox card even when Telegram is unbound
        self.event_bus.broadcast(WebSocketMessage::new(
            "team:slot_quota_alert",
            serde_json::json!({
                "user_id": owner_user_id,
                "team_id": payload.team_id,
                "team_name": team.name,
                "slot_id": payload.slot_id,
                "member_name": member_name,
                "error": reason,
            }),
        ));

        let lower_reason = reason.to_lowercase();
        let is_quota_or_rate_err = lower_reason.contains("quota")
            || lower_reason.contains("rate")
            || lower_reason.contains("429")
            || lower_reason.contains("exhaust")
            || lower_reason.contains("credit")
            || lower_reason.contains("billing");

        let (fb_backend, fb_model_id, fb_display) =
            self.select_best_free_cli_fallback(owner_user_id).await;
        let fallback_target_json = serde_json::json!({
            "backend": fb_backend,
            "provider_id": serde_json::Value::Null,
            "model_id": fb_model_id,
        });

        let mut auto_fell_back = false;
        if is_quota_or_rate_err
            && is_worker_slot
            && let Ok(pref) = self.bridge_repo.get_or_default_team_context_pref(&payload.team_id).await
            && pref.is_auto_fallback_free_cli_enabled()
        {
            let _ = self
                .execute_quota_fallback_resolution(
                    owner_user_id,
                    &payload.team_id,
                    Some(&payload.slot_id),
                    Some(&fallback_target_json),
                )
                .await;
            auto_fell_back = true;
        }

        let failed_model = member
            .map(|a| a.model.clone())
            .unwrap_or_else(|| "API Model".to_owned());
        let token_action_kind = if is_quota_or_rate_err {
            "quota_fallback"
        } else {
            "retry_member"
        };
        let action_tok = self
            .bridge_repo
            .create_action_token(
                owner_user_id,
                CustomTargetType::Team,
                &payload.team_id,
                token_action_kind,
                &serde_json::json!({
                    "slot_id": payload.slot_id,
                    "fallback_target": fallback_target_json,
                }),
                None,
            )
            .await
            .ok();

        let mut tg_chat_saved: Option<String> = None;
        let mut tg_msg_saved: Option<String> = None;
        if let Some(reply_chat) = self
            .resolve_team_telegram_destination(owner_user_id, &payload.team_id)
            .await
        {
            let mut buttons = Vec::new();
            let locale = self.resolve_locale(owner_user_id).await;
            if let Some(ref tok) = action_tok {
                let btn_label = if is_quota_or_rate_err {
                    match locale {
                        HubLocale::ViVn => format!("Chuyển {} sang CLI $0 & Thử lại", member_name),
                        HubLocale::EnUs => format!("Switch {} to $0 CLI & Retry", member_name),
                    }
                } else {
                    HubI18n::btn_retry_member(locale, &member_name)
                };
                buttons.push(vec![ActionButton {
                    label: btn_label,
                    action: format!("t:{tok}"),
                    params: None,
                }]);
            }

            let alert_md = HubI18n::member_warning(locale, &team.name, &member_name, &reason);
            if let Ok(sent_ids) = self
                .send_formatted_telegram_message(
                    owner_user_id,
                    &reply_chat,
                    &alert_md,
                    (!buttons.is_empty()).then_some(buttons),
                    None,
                    CustomTargetType::Team,
                    &payload.team_id,
                )
                .await
                && let Some(last_mid) = sent_ids.last()
            {
                tg_chat_saved = Some(reply_chat);
                tg_msg_saved = Some(last_mid.clone());
            }
        }

        if let Ok(card) = self
            .bridge_repo
            .create_action_inbox_item(CreateActionInboxParams {
                id: None,
                owner_user_id: owner_user_id.to_owned(),
                category: if is_quota_or_rate_err {
                    "quota_fallback".to_owned()
                } else {
                    "quota_alert".to_owned()
                },
                title: format!("{} · {} runtime alert", team.name, member_name),
                summary: if auto_fell_back {
                    format!("{reason} (Auto-switched slot to $0 Free CLI)")
                } else {
                    reason.clone()
                },
                details_json: serde_json::json!({
                    "team_id": payload.team_id,
                    "team_name": team.name,
                    "slot_id": payload.slot_id,
                    "member_name": member_name,
                    "failed_model": failed_model,
                    "fallback_model": fb_display,
                    "fallback_target": fallback_target_json,
                    "error": reason,
                    "auto_fallback_applied": auto_fell_back,
                })
                .to_string(),
                target_type: Some("team".to_owned()),
                target_id: payload.team_id.clone(),
                action_token: action_tok,
                telegram_chat_id: tg_chat_saved,
                telegram_msg_id: tg_msg_saved,
            })
            .await
        {
            self.event_bus.broadcast(WebSocketMessage::new(
                "inbox:item_added",
                serde_json::json!({
                    "user_id": owner_user_id,
                    "item": ActionInboxCardDto::from_row(card),
                }),
            ));
        }
    }

    async fn select_best_free_cli_fallback(&self, owner_user_id: &str) -> (String, String, String) {
        if let Some(orch) = self.team_hub_orchestrator()
            && let Ok(catalog) = orch.list_connected_models(owner_user_id).await
        {
            let preferred_order = ["claude", "codex", "gemini", "antigravity"];
            for pref_backend in preferred_order {
                if let Some(group) = catalog.cli_agents.iter().find(|g| {
                    g.wallet_type == "cli_subscription"
                        && g.backend.eq_ignore_ascii_case(pref_backend)
                        && !g.models.is_empty()
                }) {
                    let first = &group.models[0];
                    return (
                        group.backend.clone(),
                        first.id.clone(),
                        format!("{} ($0 CLI)", first.label),
                    );
                }
            }
            if let Some(group) = catalog
                .cli_agents
                .iter()
                .find(|g| g.wallet_type == "cli_subscription" && !g.models.is_empty())
            {
                let first = &group.models[0];
                return (
                    group.backend.clone(),
                    first.id.clone(),
                    format!("{} ($0 CLI)", first.label),
                );
            }
        }
        (
            "claude".to_owned(),
            "claude-sonnet-4-6".to_owned(),
            "Claude Sonnet 4.6 ($0 CLI)".to_owned(),
        )
    }

    /// Executes the `$0 CLI` model switch + runtime retry when a `quota_fallback` card is approved
    /// from either Desktop Action Inbox (`POST /api/inbox/{id}/resolve`) or Telegram inline button.
    pub async fn execute_quota_fallback_resolution(
        &self,
        owner_user_id: &str,
        team_id: &str,
        slot_id: Option<&str>,
        explicit_target: Option<&serde_json::Value>,
    ) -> Result<(), String> {
        let orch = self
            .team_hub_orchestrator()
            .ok_or_else(|| "TeamHubOrchestrator not initialized".to_owned())?;

        let target_obj = explicit_target.and_then(|v| {
            if v.get("backend").is_some() {
                Some(v)
            } else {
                v.get("fallback_target")
            }
        });
        let (backend, provider_id, model_id) = if let Some(obj) = target_obj
            && let Some(b) = obj.get("backend").and_then(serde_json::Value::as_str)
            && !b.trim().is_empty()
        {
            let pid = obj
                .get("provider_id")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_owned);
            let mid = obj
                .get("model_id")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| b.to_owned());
            (b.to_owned(), pid, mid)
        } else {
            let (b, mid, _) = self.select_best_free_cli_fallback(owner_user_id).await;
            (b, None, mid)
        };

        let trimmed_slot = slot_id.map(str::trim).filter(|s| !s.is_empty());
        if let Some(sid) = trimmed_slot {
            orch.switch_team_models(
                owner_user_id,
                team_id,
                crate::bridge::context_compactor::SwitchTeamModelsRequest {
                    scope: Some("slots".to_owned()),
                    preset: None,
                    target: None,
                    updates: vec![crate::bridge::context_compactor::PerSlotModelUpdate {
                        slot_id: sid.to_owned(),
                        backend,
                        provider_id,
                        model_id,
                        target: None,
                    }],
                    force_interrupt: Some(true),
                },
            )
            .await
            .map_err(|e| format!("{e:?}"))?;

            if self
                .team_service
                .restart_agent_runtime(owner_user_id, team_id, sid)
                .await
                .is_err()
            {
                let _ = self
                    .team_service
                    .attach_agent_runtime(owner_user_id, team_id, sid)
                    .await;
            }
        } else {
            orch.apply_quota_preset(
                owner_user_id,
                team_id,
                crate::bridge::context_compactor::ApplyQuotaPresetRequest {
                    preset: Some("smart_split".to_owned()),
                    force_interrupt: Some(true),
                },
            )
            .await
            .map_err(|e| format!("{e:?}"))?;
        }

        Ok(())
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

    pub fn classify_slot_agent_wallet(agent: &aionui_api_types::TeamAgentResponse) -> WalletType {
        Self::classify_slot_agent_wallet_with_key(
            agent,
            slot_has_custom_api_key(None, None, None),
        )
    }

    pub fn classify_slot_agent_wallet_with_key(
        agent: &aionui_api_types::TeamAgentResponse,
        has_custom_api_key: bool,
    ) -> WalletType {
        classify_slot_wallet_facts(&SlotWalletFacts {
            slot_backend: Some(&agent.backend),
            assistant_backend: Some(&agent.assistant_backend),
            extra_backend: None,
            cli_path: None,
            provider_id: None,
            model_id: Some(&agent.model),
            auth_methods_json: None,
            has_custom_api_key,
        })
    }

    async fn classify_slot_agent_wallet_live(
        &self,
        owner_user_id: &str,
        agent: &aionui_api_types::TeamAgentResponse,
    ) -> WalletType {
        let has_custom_api_key = self
            .bridge_repo
            .slot_has_custom_api_key_in_db(
                owner_user_id,
                &agent.conversation_id,
                &agent.backend,
                agent.assistant_id.as_deref(),
            )
            .await;
        Self::classify_slot_agent_wallet_with_key(agent, has_custom_api_key)
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

        (agent.slot_id.clone(), Self::classify_slot_agent_wallet(agent))
    }

    pub fn has_any_api_key_slot(team: &TeamResponse) -> bool {
        team.assistants
            .iter()
            .any(|a| Self::classify_slot_agent_wallet(a) == WalletType::ApiKey)
    }

    async fn is_team_api_key_hard_blocked(
        &self,
        owner_user_id: &str,
        team: &TeamResponse,
    ) -> Option<(f64, f64)> {
        let mut has_paid_slot = false;
        for member in &team.assistants {
            if self.classify_slot_agent_wallet_live(owner_user_id, member).await == WalletType::ApiKey {
                has_paid_slot = true;
                break;
            }
        }
        if !has_paid_slot {
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
        let (lead_slot_id, _) = Self::classify_team_lead_wallet(team);
        let mut has_paid_slot = false;
        let mut slot_wallets = Vec::with_capacity(team.assistants.len());
        for member in &team.assistants {
            let w = self.classify_slot_agent_wallet_live(owner_user_id, member).await;
            if w == WalletType::ApiKey {
                has_paid_slot = true;
            }
            slot_wallets.push(w);
        }
        let effective_wallet = if has_paid_slot {
            WalletType::ApiKey
        } else {
            WalletType::CliSubscription
        };

        let slot_econs: HashMap<String, _> = self
            .bridge_repo
            .list_team_slot_economy(&team.id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.slot_id.clone(), r))
            .collect();

        let mut measured_in_tokens = 0_i64;
        let mut measured_out_tokens = 0_i64;
        let mut measured_api_cost_usd = 0.0_f64;

        for (member, slot_wallet) in team.assistants.iter().zip(slot_wallets.into_iter()) {
            let (_, total_bytes) = self
                .bridge_repo
                .estimate_conversation_tokens_fast(&member.conversation_id)
                .await
                .unwrap_or((0, 0));
            let prev_bytes = slot_econs
                .get(&member.slot_id)
                .map(|e| e.last_seen_char_sum)
                .unwrap_or(0);
            let wake_chars = slot_econs
                .get(&member.slot_id)
                .map(|e| e.delivered_wake_chars)
                .unwrap_or(0);
            let delta_bytes = (total_bytes - prev_bytes).max(0);
            let slot_in = ((delta_bytes / 8) + (wake_chars.max(0) / 4)).max(if delta_bytes > 0 { 16 } else { 0 });
            let slot_out = (delta_bytes / 8).max(if delta_bytes > 0 { 16 } else { 0 });
            measured_in_tokens += slot_in;
            measured_out_tokens += slot_out;
            if slot_wallet == WalletType::ApiKey {
                measured_api_cost_usd += estimate_api_token_cost_usd(
                    WalletType::ApiKey,
                    &member.model,
                    slot_in.max(64),
                    slot_out.max(((synthesis_chars as i64) / 4).max(32)),
                );
            }
        }

        let synth_tokens_out = ((synthesis_chars as i64) / 4).max(32);
        let final_tokens_in = measured_in_tokens.max(synth_tokens_out.max(64));
        let final_tokens_out = measured_out_tokens.max(synth_tokens_out);
        let cost_usd = match effective_wallet {
            WalletType::CliSubscription => 0.0,
            WalletType::ApiKey => {
                if measured_api_cost_usd > 0.0 {
                    measured_api_cost_usd.max(0.0001)
                } else {
                    estimate_api_token_cost_usd(
                        WalletType::ApiKey,
                        team.assistants
                            .first()
                            .map(|a| a.model.as_str())
                            .unwrap_or("api"),
                        final_tokens_in,
                        final_tokens_out,
                    )
                    .max(0.0001)
                }
            }
        };

        if let Ok((wallet, newly_soft, newly_hard)) = self
            .bridge_repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: owner_user_id.to_owned(),
                run_id: run_id.to_owned(),
                team_id: team.id.clone(),
                agent_id: lead_slot_id,
                wallet_type: effective_wallet.as_str().to_owned(),
                tokens_input: final_tokens_in,
                tokens_output: final_tokens_out,
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
                            live_steering: false,
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
                live_steering: false,
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
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let teams = self
            .team_service
            .list_teams(owner_user_id)
            .await
            .unwrap_or_default();
        let team_map: HashMap<String, TeamResponse> = teams.into_iter().map(|t| (t.id.clone(), t)).collect();
        let active_dm_room_ids: std::collections::HashSet<String> = self
            .bridge_repo
            .list_dm_targets_for_owner(owner_user_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|d| d.target_type == "room")
            .map(|d| d.target_id)
            .collect();

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if let Ok(dto) = self
                .hydrate_shared_room_dto_with_maps(owner_user_id, row, &team_map, &active_dm_room_ids)
                .await
            {
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
        let active_dm_room_ids: std::collections::HashSet<String> = self
            .bridge_repo
            .list_dm_targets_for_owner(owner_user_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|d| d.target_type == "room")
            .map(|d| d.target_id)
            .collect();
        self.hydrate_shared_room_dto_with_maps(owner_user_id, room, &team_map, &active_dm_room_ids)
            .await
    }

    async fn hydrate_shared_room_dto_with_maps(
        &self,
        owner_user_id: &str,
        room: CustomSharedRoomRow,
        team_map: &HashMap<String, TeamResponse>,
        active_dm_room_ids: &std::collections::HashSet<String>,
    ) -> Result<SharedRoomDto, String> {
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

        let is_active_dm_target = active_dm_room_ids.contains(&room.id);

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

    pub async fn hydrate_room_task_dto(
        &self,
        owner_user_id: &str,
        row: CustomSharedRoomTaskRow,
    ) -> SharedRoomTaskDto {
        let teams = self
            .team_service
            .list_teams(owner_user_id)
            .await
            .unwrap_or_default();
        let name_map: HashMap<String, String> = teams.into_iter().map(|t| (t.id, t.name)).collect();
        Self::map_room_task_dto(row, &name_map)
    }

    pub fn map_room_task_dto(row: CustomSharedRoomTaskRow, name_map: &HashMap<String, String>) -> SharedRoomTaskDto {
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
        let (msg_dto, targets, _) = self.post_message_to_shared_room_with_rejections(args).await?;
        Ok((msg_dto, targets))
    }

    async fn post_message_to_shared_room_with_rejections(
        &self,
        args: PostRoomMessageArgs<'_>,
    ) -> Result<(SharedRoomMessageDto, Vec<RoutedTarget>, Vec<RejectedOutboundFile>), String> {
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
        let artifact_batch = stage_room_artifacts_batch_async(
            args.room_id.to_owned(),
            args.source_files.clone(),
            source_ws.map(ToOwned::to_owned),
            self.data_dir.clone(),
            recipient_workspaces.clone(),
            allow_user_local_files,
        )
        .await?;
        let staged_artifacts = artifact_batch.artifacts;
        let recipient_inbox_paths = artifact_batch.recipient_inbox_paths;
        let rejected_files = artifact_batch.rejected_files;

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

        let recent_msgs = if args.sender_type == "user" {
            self.bridge_repo
                .list_shared_room_messages(args.room_id, 6)
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let latest_leader_pair: Option<(String, i64)> = recent_msgs
            .iter()
            .rev()
            .find(|m| m.sender_type == "leader" && m.sender_team_id.is_some())
            .and_then(|m| m.sender_team_id.as_ref().map(|tid| (tid.clone(), m.created_at)));

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
            let api_wallet_hard_blocked = self
                .bridge_repo
                .get_or_create_cost_wallet(args.owner_user_id)
                .await
                .map(|w| w.is_hard_blocked())
                .unwrap_or(false);
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
            let heuristics = RoomRoutingHeuristics {
                member_role_tags: &[],
                last_leader_speaker: latest_leader_pair
                    .as_ref()
                    .map(|(tid, ts)| (tid.as_str(), *ts)),
                now_ms: now_ms(),
                api_wallet_hard_blocked,
            };
            let (targets, judge_usage) = resolve_room_targets_smart(
                &routing_input,
                &heuristics,
                &providers,
                &self.encryption_key,
                &self.http_client,
            )
            .await;

            if let Some(usage) = judge_usage {
                let judge_cost = estimate_api_token_cost_usd(
                    WalletType::ApiKey,
                    &usage.model,
                    usage.prompt_tokens,
                    usage.completion_tokens,
                );
                if let Ok((wallet, _, _)) = self
                    .bridge_repo
                    .record_cost_run(RecordCostRunParams {
                        owner_user_id: args.owner_user_id.to_owned(),
                        run_id: generate_prefixed_id("jrun"),
                        team_id: args.room_id.to_owned(),
                        agent_id: format!("room-judge:{}", usage.model),
                        wallet_type: WalletType::ApiKey.as_str().to_owned(),
                        tokens_input: usage.prompt_tokens,
                        tokens_output: usage.completion_tokens,
                        cost_usd: judge_cost,
                    })
                    .await
                {
                    self.event_bus.broadcast(WebSocketMessage::new(
                        "wallet:spend_updated",
                        serde_json::json!({
                            "user_id": args.owner_user_id,
                            "spent_usd": wallet.api_key_spent_usd,
                            "budget_limit_usd": wallet.api_key_budget_limit_usd,
                            "cli_subscription_runs": wallet.cli_subscription_runs_count,
                            "api_key_runs": wallet.api_key_runs_count,
                            "soft_warning": wallet.is_soft_warning(),
                            "hard_blocked": wallet.is_hard_blocked(),
                        }),
                    ));
                }
            }
            targets
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
        let mut updated_room_dto = room_dto.clone();
        updated_room_dto.hop_count_since_human = current_hop;
        updated_room_dto.updated_at = msg_dto.created_at;
        if args.sender_type == "leader"
            && let Some(stid) = args.sender_team_id
            && let Some(member) = updated_room_dto.members.iter_mut().find(|m| m.team_id == stid)
        {
            member.pending_handoff_msg_id = None;
        }

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
                let sent_room_ids = self
                    .send_formatted_telegram_message(
                        args.owner_user_id,
                        &tg_dest,
                        &tg_md,
                        None,
                        None,
                        CustomTargetType::Room,
                        args.room_id,
                    )
                    .await
                    .unwrap_or_default();
                let staged_paths: Vec<String> = staged_artifacts.iter().map(|a| a.staged_path.clone()).collect();
                if !staged_paths.is_empty() {
                    let fallback_team_ws = if source_ws.is_none()
                        && let Some(tid) = args.sender_team_id
                    {
                        self.team_service
                            .get_team(args.owner_user_id, tid)
                            .await
                            .ok()
                            .map(|team| team.workspace)
                    } else {
                        None
                    };
                    let effective_sender_ws = source_ws
                        .or(fallback_team_ws.as_deref())
                        .filter(|w| !w.trim().is_empty());
                    let _ = self
                        .send_validated_outbound_files(
                            args.owner_user_id,
                            &tg_dest,
                            &staged_paths,
                            effective_sender_ws,
                            CustomTargetType::Room,
                            args.room_id,
                            sent_room_ids.last().map(String::as_str),
                        )
                        .await;
                }
            }
        }

        if hop_limit_triggered {
            self.emit_hop_limit_reached_notice(args.owner_user_id, &room_dto)
                .await;
            return Ok((msg_dto, Vec::new(), rejected_files));
        }

        // Offload oversized Shared Room message (> 2,400 chars) to `.shared-inbox/` once for all woken targets
        let oversized_staged = if !effective_targets.is_empty() && args.content.chars().count() > 2_400 {
            let saved_chars = args.content.chars().count().saturating_sub(1_200) as i64;
            for t in &effective_targets {
                let _ = self
                    .bridge_repo
                    .increment_team_economy_savings(&t.team_id, (saved_chars / 4).max(1), 0)
                    .await;
            }
            stage_room_oversized_message_async(
                args.room_id.to_owned(),
                msg_id.clone(),
                args.content.to_owned(),
                self.data_dir.clone(),
                recipient_workspaces.clone(),
            )
            .await
            .ok()
        } else {
            None
        };

        let roster_summary: Vec<String> = room_dto
            .members
            .iter()
            .map(|m| format!("@{} (team_id={})", m.team_name, m.team_id))
            .collect();

        // Dispatch wakeups to each routed Team's Lead (enforcing 100% Smart Budget Fuse for metered API-key teams)
        for target in &effective_targets {
            let team_meta_opt = self.team_service.get_team(args.owner_user_id, &target.team_id).await.ok();
            if let Some(ref team_meta) = team_meta_opt
                && let Some((spent, limit)) = self.is_team_api_key_hard_blocked(args.owner_user_id, team_meta).await
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
            // Suppress duplicate `[PENDING ROOM HANDOFF]` banner on Turn 1 (wake_prompt already carries room_id & instructions)
            self.delivered_handoff_banners
                .insert((args.room_id.to_owned(), target.team_id.clone()), msg_id.clone());

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

            let mut local_inbox_files: Vec<String> = if staged_artifacts.is_empty() {
                Vec::new()
            } else {
                recipient_inbox_paths
                    .get(member_ws.trim())
                    .cloned()
                    .unwrap_or_else(|| staged_artifacts.iter().map(|a| a.staged_path.clone()).collect())
            };

            let rendered_body = if let Some((ref fallback_path, ref per_ws)) = oversized_staged {
                let delivered_md = per_ws
                    .get(member_ws.trim())
                    .cloned()
                    .unwrap_or_else(|| fallback_path.clone());
                if !local_inbox_files.contains(&delivered_md) {
                    local_inbox_files.push(delivered_md.clone());
                }
                let excerpt: String = args.content.chars().take(1_200).collect();
                format!(
                    "{excerpt}…\n[Full message ({len} chars) staged at `{delivered_md}`]",
                    len = args.content.chars().count()
                )
            } else {
                args.content.to_owned()
            };

            let coalesced_task_note = if let Some((_, pending_task)) = self
                .pending_task_wakes
                .remove(&(args.room_id.to_owned(), target.team_id.clone()))
            {
                let _ = self
                    .bridge_repo
                    .increment_team_economy_savings(&target.team_id, 220, 1)
                    .await;
                let desc_line = if pending_task.description.trim().is_empty() {
                    String::new()
                } else {
                    format!(" — {}", pending_task.description.trim())
                };
                format!(
                    "\n[Coalesced Cross-Team Task: task_id=\"{}\" title=\"{}\"{} -> call `team_room_task_update` with status=\"in_review\" when ready]\n",
                    pending_task.task_id, pending_task.title, desc_line
                )
            } else {
                String::new()
            };

            let files_note = if local_inbox_files.is_empty() {
                String::new()
            } else {
                format!(
                    "\nShared artifacts in workspace inbox:\n- {}\n",
                    local_inbox_files.join("\n- ")
                )
            };

            // Cache-friendly static room header FIRST; dynamic per-turn msg_id/sender/body LAST
            let wake_prompt = format!(
                "[SHARED ROOM #{room_name} | room_id=\"{room_id}\"]\n\
                 Room Participants: {roster}\n\
                 {rules_line}\
                 [Protocol]: Coordinate with your team if needed, then deliver back via `team_room_post` (room_id=\"{room_id}\"). For cross-team tasks, set status=\"in_review\" via `team_room_task_update` (Done is human-only).\n\
                 ---\n\
                 msg_id: {msg_id} | From: {sender_label} | Routing: tier {tier} ({reason})\n\
                 {coalesced_task_note}\
                 Message:\n{rendered_body}\n{files_note}",
                room_name = room_dto.name,
                room_id = args.room_id,
                roster = roster_summary.join(", "),
                rules_line = if room_dto.rules.trim().is_empty() {
                    String::new()
                } else {
                    format!("Room Rules: {}\n", room_dto.rules.trim())
                },
                tier = target.tier,
                reason = target.reason,
            );

            if let Some(ref team_meta) = team_meta_opt {
                let (lead_slot_id, _, _) = Self::find_lead_slot_info(team_meta);
                let _ = self
                    .bridge_repo
                    .record_slot_wake_delivery(
                        &target.team_id,
                        &lead_slot_id,
                        args.owner_user_id,
                        "lead",
                        wake_prompt.chars().count() as i64,
                    )
                    .await;
            }

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
            let live_steering = args.live_steering && args.sender_type == "user";
            let lead_slot_opt = team_meta_opt.as_ref().map(|tm| Self::find_lead_slot_info(tm).0);
            tokio::spawn(async move {
                if live_steering
                    && let Some(ref lead_slot) = lead_slot_opt
                    && team_svc.is_slot_turn_running(&tid, lead_slot).await
                {
                    let _ = team_svc
                        .interrupt_or_restart_slot_for_maintenance(&uid, &tid, lead_slot, true)
                        .await;
                }
                if let Err(err) = team_svc.send_message(&uid, &tid, &wake_prompt, file_refs).await {
                    warn!(team_id = %tid, error = %err, "failed to wake target Team Lead from Shared Room");
                }
            });
        }

        Ok((msg_dto, effective_targets, rejected_files))
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
        let (dto, _) = self
            .create_cross_team_task_with_rejections(
                owner_user_id,
                room_id,
                from_team_id,
                to_team_ref,
                title,
                description,
                blocked_by,
                files,
            )
            .await?;
        Ok(dto)
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_cross_team_task_with_rejections(
        &self,
        owner_user_id: &str,
        room_id: &str,
        from_team_id: Option<&str>,
        to_team_ref: &str,
        title: &str,
        description: &str,
        blocked_by: Vec<String>,
        files: Vec<String>,
    ) -> Result<(SharedRoomTaskDto, Vec<RejectedOutboundFile>), String> {
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
        let stage_batch = stage_room_artifacts_batch_async(
            room_id.to_owned(),
            files,
            source_ws.map(ToOwned::to_owned),
            self.data_dir.clone(),
            recipient_ws,
            allow_user_local_files,
        )
        .await?;
        let staged = stage_batch.artifacts;
        let rejected_files = stage_batch.rejected_files;

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

        // If this specific task is NOT gated by unfinished `blocked_by` tasks, schedule a 650ms coalescable
        // wake on the assignee Team's Lead (so `team_room_task_create` + `team_room_post` merge into 1 turn!)
        let all_tasks = self
            .bridge_repo
            .list_shared_room_tasks(room_id)
            .await
            .unwrap_or_default();
        if !is_task_blocked(&created, &all_tasks) {
            let _ = self
                .bridge_repo
                .set_member_pending_handoff(room_id, &to_team_id, Some(&task_id), room_dto.hop_count_since_human)
                .await;

            // Enforce max_hops for agent-originated task wakes
            let hop_blocked = from_team_id.is_some() && room_dto.hop_count_since_human >= room_dto.max_hops;
            let fuse_blocked = if let Ok(team_meta) = self.team_service.get_team(owner_user_id, &to_team_id).await {
                self.is_team_api_key_hard_blocked(owner_user_id, &team_meta)
                    .await
                    .is_some()
            } else {
                false
            };

            if !hop_blocked && !fuse_blocked {
                self.pending_task_wakes.insert(
                    (room_id.to_owned(), to_team_id.clone()),
                    PendingCrossTeamTaskWake {
                        task_id: task_id.clone(),
                        title: title.trim().to_owned(),
                        description: description.to_owned(),
                        room_name: room_dto.name.clone(),
                    },
                );

                let pending_map = Arc::clone(&self.pending_task_wakes);
                let team_svc = Arc::clone(&self.team_service);
                let bridge_repo = Arc::clone(&self.bridge_repo);
                let uid = owner_user_id.to_owned();
                let rid = room_id.to_owned();
                let tid = to_team_id.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(650)).await;
                    let Some((_, pending)) = pending_map.remove(&(rid.clone(), tid.clone())) else {
                        // Coalesced into a companion `team_room_post` wake!
                        return;
                    };
                    let wake_msg = format!(
                        "[CROSS-TEAM TASK ASSIGNED in #{room_name} | room_id=\"{rid}\"]\n\
                         [Protocol]: Execute with your team, then call `team_room_task_update` (task_id=\"{task_id}\", status=\"in_review\") + `team_room_post` (Done is human-only).\n\
                         ---\n\
                         task_id: {task_id}\n\
                         Title: {title}\n\
                         Description: {desc}",
                        room_name = pending.room_name,
                        task_id = pending.task_id,
                        title = pending.title,
                        desc = pending.description,
                    );
                    if let Ok(team_meta) = team_svc.get_team(&uid, &tid).await {
                        let (lead_slot_id, _, _) = Self::find_lead_slot_info(&team_meta);
                        let _ = bridge_repo
                            .record_slot_wake_delivery(
                                &tid,
                                &lead_slot_id,
                                &uid,
                                "lead",
                                wake_msg.chars().count() as i64,
                            )
                            .await;
                    }
                    let _ = team_svc.send_message(&uid, &tid, &wake_msg, None).await;
                });
            }
        }

        Ok((dto, rejected_files))
    }

    #[allow(clippy::too_many_arguments)]
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
        let (dto, _) = self
            .update_cross_team_task_with_rejections(
                owner_user_id,
                task_id,
                actor_team_id,
                status,
                description,
                result_summary,
                files,
            )
            .await?;
        Ok(dto)
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_cross_team_task_with_rejections(
        &self,
        owner_user_id: &str,
        task_id: &str,
        actor_team_id: Option<&str>,
        status: Option<String>,
        description: Option<String>,
        result_summary: Option<String>,
        files: Vec<String>,
    ) -> Result<(SharedRoomTaskDto, Vec<RejectedOutboundFile>), String> {
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
        let mut rejected_files = Vec::new();
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
            let newly_staged = stage_room_artifacts_batch_async(
                existing.room_id.clone(),
                files,
                source_ws.map(ToOwned::to_owned),
                self.data_dir.clone(),
                recipient_ws,
                allow_user_local_files,
            )
            .await?;
            merged_artifacts.extend(newly_staged.artifacts);
            rejected_files = newly_staged.rejected_files;
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

        // Feature F19: When an agent submits `status == "in_review"`, emit a human-only `approve_done`
        // ActionInbox card (synced with optional Telegram inline Approve button) so a human can finalize `completed`.
        if existing.status != "in_review" && updated.status == "in_review" {
            let approve_token = self
                .bridge_repo
                .create_action_token(
                    owner_user_id,
                    CustomTargetType::Room,
                    &existing.room_id,
                    "approve_done",
                    &serde_json::json!({
                        "task_id": updated.id,
                        "room_id": existing.room_id,
                    }),
                    None,
                )
                .await
                .ok();

            let mut tg_chat_saved: Option<String> = None;
            let mut tg_msg_saved: Option<String> = None;
            if let (Some(tok), Some(tg_dest)) = (
                approve_token.as_deref(),
                self.resolve_room_telegram_destination(owner_user_id, &existing.room_id)
                    .await,
            ) {
                let summary_snip = updated
                    .result_summary
                    .as_deref()
                    .unwrap_or("Ready for human review");
                let review_md = format!(
                    "📋 **[#{} · Task Ready for Review]**\n**{}**\n\n{}",
                    room_dto.name, updated.title, summary_snip
                );
                let btns = vec![vec![ActionButton {
                    label: "Approve Done".to_owned(),
                    action: format!("t:{tok}"),
                    params: None,
                }]];
                if let Ok(sent_ids) = self
                    .send_formatted_telegram_message(
                        owner_user_id,
                        &tg_dest,
                        &review_md,
                        Some(btns),
                        None,
                        CustomTargetType::Room,
                        &existing.room_id,
                    )
                    .await
                    && let Some(last_mid) = sent_ids.last()
                {
                    tg_chat_saved = Some(tg_dest);
                    tg_msg_saved = Some(last_mid.clone());
                }
            }

            if let Ok(card) = self
                .bridge_repo
                .create_action_inbox_item(CreateActionInboxParams {
                    id: None,
                    owner_user_id: owner_user_id.to_owned(),
                    category: "approve_done".to_owned(),
                    title: format!("Review Done: {}", updated.title),
                    summary: updated
                        .result_summary
                        .clone()
                        .unwrap_or_else(|| format!("Task '{}' in #{} is ready for human Done approval.", updated.title, room_dto.name)),
                    details_json: serde_json::json!({
                        "task_id": updated.id,
                        "task_title": updated.title,
                        "title": updated.title,
                        "room_id": existing.room_id,
                        "room_name": room_dto.name,
                        "to_team_id": updated.to_team_id,
                        "to_team_name": dto.to_team_name,
                        "team_name": dto.to_team_name,
                        "result_summary": updated.result_summary,
                    })
                    .to_string(),
                    target_type: Some("room".to_owned()),
                    target_id: existing.room_id.clone(),
                    action_token: approve_token,
                    telegram_chat_id: tg_chat_saved,
                    telegram_msg_id: tg_msg_saved,
                })
                .await
            {
                self.event_bus.broadcast(WebSocketMessage::new(
                    "inbox:item_added",
                    serde_json::json!({
                        "user_id": owner_user_id,
                        "item": ActionInboxCardDto::from_row(card),
                    }),
                ));
            }
        }

        // Closed-Loop Layer 3: If this task newly transitioned into a dependency-satisfying state
        // (`in_review`, `completed`, or `done`), wake any downstream Team whose `blocked_by` task just unblocked!
        let was_satisfying = matches!(existing.status.as_str(), "in_review" | "completed" | "done");
        let now_satisfying = matches!(updated.status.as_str(), "in_review" | "completed" | "done");
        if !was_satisfying && now_satisfying {
            let after_tasks = self
                .bridge_repo
                .list_shared_room_tasks(&existing.room_id)
                .await
                .unwrap_or_default();

            for candidate in &after_tasks {
                if matches!(candidate.status.as_str(), "in_review" | "completed" | "done" | "cancelled") {
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

                    // Respect room max_hops and Paid API 100% Safety Fuse before waking downstream team
                    if actor_team_id.is_some() && room_dto.hop_count_since_human >= room_dto.max_hops {
                        continue;
                    }
                    let downstream_team = self
                        .team_service
                        .get_team(owner_user_id, &candidate.to_team_id)
                        .await
                        .ok();
                    if let Some(ref tm) = downstream_team
                        && self.is_team_api_key_hard_blocked(owner_user_id, tm).await.is_some()
                    {
                        continue;
                    }

                    let summary_line = updated
                        .result_summary
                        .as_deref()
                        .map(|s| format!("Upstream Summary: {s}\n"))
                        .unwrap_or_default();
                    let wake_prompt = format!(
                        "[CROSS-TEAM TASK UNBLOCKED in #{room_name} | room_id=\"{room_id}\"]\n\
                         [Protocol]: Check `.shared-inbox/{safe_room}/` for upstream artifacts, execute with your team, and call `team_room_task_update` (status=\"in_review\") + `team_room_post` when ready.\n\
                         ---\n\
                         Upstream `{upstream_id}` (\"{upstream_title}\") status: {upstream_status}\n\
                         {summary_line}\
                         Unblocked Task `{task_id}` (\"{task_title}\")",
                        room_name = room_dto.name,
                        room_id = existing.room_id,
                        upstream_id = updated.id,
                        upstream_title = updated.title,
                        upstream_status = updated.status,
                        task_id = candidate.id,
                        task_title = candidate.title,
                        safe_room = sanitize_file_name(&existing.room_id),
                    );
                    if let Some(ref tm) = downstream_team {
                        let (lead_slot_id, _, _) = Self::find_lead_slot_info(tm);
                        let _ = self
                            .bridge_repo
                            .record_slot_wake_delivery(
                                &candidate.to_team_id,
                                &lead_slot_id,
                                owner_user_id,
                                "lead",
                                wake_prompt.chars().count() as i64,
                            )
                            .await;
                    }
                    let team_svc = Arc::clone(&self.team_service);
                    let uid = owner_user_id.to_owned();
                    let tid = candidate.to_team_id.clone();
                    tokio::spawn(async move {
                        let _ = team_svc.send_message(&uid, &tid, &wake_prompt, None).await;
                    });
                }
            }
        }

        Ok((dto, rejected_files))
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
            "approve_continue" | "stop_run" | "retry_member" | "quota_fallback" | "allow_more_hops"
            | "worktree_merge" | "approve_done" | "inbox_resolve" | "inbox_choice" | "budget_ack"
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

                    "quota_fallback" => {
                        let inbox_details: serde_json::Value = resolved_inbox_item
                            .as_ref()
                            .and_then(|item| serde_json::from_str(&item.details_json).ok())
                            .unwrap_or_default();
                        let slot_id_opt = payload
                            .get("slot_id")
                            .and_then(serde_json::Value::as_str)
                            .or_else(|| inbox_details.get("slot_id").and_then(serde_json::Value::as_str));
                        let fb_target_opt = payload
                            .get("fallback_target")
                            .or_else(|| inbox_details.get("fallback_target"));

                        let _ = self
                            .execute_quota_fallback_resolution(
                                owner_user_id,
                                &row.target_id,
                                slot_id_opt,
                                fb_target_opt,
                            )
                            .await;

                        let done_md = match locale {
                            HubLocale::ViVn => "✅ **Đã chuyển sang CLI $0 và khởi động lại phiên chạy qua Telegram!**",
                            HubLocale::EnUs => "✅ **Switched slot to $0 CLI and restarted runtime via Telegram!**",
                        };
                        let html = markdown_to_telegram_html(done_md);
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

                    "approve_done" => {
                        let inbox_details: serde_json::Value = resolved_inbox_item
                            .as_ref()
                            .and_then(|item| serde_json::from_str(&item.details_json).ok())
                            .unwrap_or_default();
                        let task_id_opt = payload
                            .get("task_id")
                            .and_then(serde_json::Value::as_str)
                            .or_else(|| inbox_details.get("task_id").and_then(serde_json::Value::as_str));
                        if let Some(tid) = task_id_opt {
                            let _ = self
                                .update_cross_team_task(
                                    owner_user_id,
                                    tid,
                                    None,
                                    Some("completed".to_owned()),
                                    None,
                                    None,
                                    Vec::new(),
                                )
                                .await;
                        }
                        let text = match locale {
                            HubLocale::ViVn => "✅ **Đã duyệt hoàn tất (Done) tác vụ liên team qua Telegram!**",
                            HubLocale::EnUs => "✅ **Approved cross-team task as Done via Telegram!**",
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
                                    preset: None,
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

        let (valid_files, rejected_files) = self
            .partition_outbound_files_async(files, Some(team.workspace.clone()))
            .await;
        let validated_desktop_files: Vec<String> = valid_files
            .iter()
            .map(|(path, _, _)| path.to_string_lossy().into_owned())
            .collect();

        // Broadcast realtime toast/event to Desktop UI as well (only validated files)
        self.event_bus.broadcast(WebSocketMessage::new(
            "team.proactiveNotification",
            serde_json::json!({
                "user_id": user_id,
                "team_id": team_id,
                "team_name": team.name,
                "kind": kind,
                "summary": summary,
                "files": validated_desktop_files,
                "rejected_files": rejected_files,
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
                "delivered_files": Vec::<String>::new(),
                "queued_files": Vec::<String>::new(),
                "rejected_files": rejected_files,
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

        let file_dispatch = self
            .dispatch_prevalidated_outbound_files(
                user_id,
                &dest_chat,
                valid_files,
                rejected_files,
                CustomTargetType::Team,
                team_id,
                sent_ids.last().map(String::as_str),
            )
            .await;

        info!(
            team_id = %team_id,
            kind = %kind,
            chunks = sent_ids.len(),
            delivered = file_dispatch.delivered_files.len(),
            queued = file_dispatch.queued_files.len(),
            rejected = file_dispatch.rejected_files.len(),
            "proactive team_notify_user delivered to Telegram"
        );

        Ok(serde_json::json!({
            "delivered_telegram": true,
            "delivered_desktop": true,
            "message_ids": sent_ids,
            "delivered_files": file_dispatch.delivered_files,
            "queued_files": file_dispatch.queued_files,
            "rejected_files": file_dispatch.rejected_files,
        }))
    }

    async fn room_read(
        &self,
        user_id: &str,
        team_id: &str,
        room_id: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        self.room_read_delta(user_id, team_id, room_id, limit, None, false)
            .await
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
        let resolved_room_id = match room_id.map(str::trim).filter(|s| !s.is_empty()) {
            Some(id) => id.to_owned(),
            None => self.resolve_default_room_for_team(user_id, team_id).await?,
        };

        let room_dto = self.build_shared_room_dto(user_id, &resolved_room_id).await?;
        let capped_limit = limit.clamp(1, 50) as i64;
        let fetched_messages: Vec<SharedRoomMessageDto> = self
            .bridge_repo
            .list_shared_room_messages(&resolved_room_id, capped_limit)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(Self::map_room_message_dto)
            .collect();

        let explicit_cursor = since_msg_id.map(str::trim).filter(|c| !c.is_empty()).map(str::to_owned);
        let cursor_key = (resolved_room_id.clone(), team_id.to_owned());
        let prev_cursor = self.last_room_read_cursor.get(&cursor_key).map(|v| v.clone());
        if let Some(last_msg) = fetched_messages.last() {
            self.last_room_read_cursor
                .insert(cursor_key, last_msg.id.clone());
        }

        // Honor explicit `since_msg_id` directly (even if caught up -> empty delta),
        // otherwise fall back to per-team `prev_cursor` when newer messages exist.
        let mut delta_applied = false;
        let effective_cursor: Option<String>;
        let selected_messages: Vec<SharedRoomMessageDto> = if let Some(cur_id) = explicit_cursor {
            effective_cursor = Some(cur_id.clone());
            if let Some(pos) = fetched_messages.iter().position(|m| m.id == cur_id) {
                delta_applied = true;
                fetched_messages[(pos + 1)..].to_vec()
            } else {
                fetched_messages
            }
        } else if let Some(ref cur_id) = prev_cursor
            && let Some(pos) = fetched_messages.iter().position(|m| m.id == *cur_id)
            && pos + 1 < fetched_messages.len()
        {
            effective_cursor = Some(cur_id.clone());
            delta_applied = true;
            fetched_messages[(pos + 1)..].to_vec()
        } else {
            effective_cursor = prev_cursor;
            fetched_messages
        };

        // Digest cap: truncate individual message bodies to <= 800 chars in MCP room_read to prevent context bloat
        let mut chars_saved = 0_i64;
        let messages: Vec<SharedRoomMessageDto> = selected_messages
            .into_iter()
            .map(|mut m| {
                let char_len = m.content.chars().count();
                if char_len > 800 {
                    chars_saved += (char_len - 800) as i64;
                    let snippet: String = m.content.chars().take(800).collect();
                    m.content = format!("{snippet}… [truncated {} chars]", char_len - 800);
                }
                m
            })
            .collect();

        if chars_saved > 0 || delta_applied {
            let bonus_saved = (chars_saved / 4) + if delta_applied { 120 } else { 0 };
            let _ = self
                .bridge_repo
                .increment_team_economy_savings(team_id, bonus_saved.max(1), 0)
                .await;
        }

        let all_tasks = self
            .list_shared_room_task_dtos(user_id, &resolved_room_id)
            .await?;
        let total_tasks = all_tasks.len();
        let (returned_tasks, completed_tasks_omitted) = if include_completed_tasks {
            (all_tasks, 0)
        } else {
            let active_tasks: Vec<SharedRoomTaskDto> = all_tasks
                .into_iter()
                .filter(|t| !matches!(t.status.as_str(), "completed" | "done" | "cancelled"))
                .collect();
            let omitted = total_tasks.saturating_sub(active_tasks.len());
            (active_tasks, omitted)
        };

        Ok(serde_json::json!({
            "room": room_dto,
            "messages": messages,
            "tasks": returned_tasks,
            "delta_from_cursor": effective_cursor,
            "delta_applied": delta_applied,
            "completed_tasks_omitted": completed_tasks_omitted,
            "include_completed_tasks": include_completed_tasks,
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

        let (msg_dto, routed, rejected_files) = self
            .post_message_to_shared_room_with_rejections(PostRoomMessageArgs {
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
                live_steering: false,
            })
            .await?;

        Ok(serde_json::json!({
            "posted": true,
            "room_id": resolved_room_id,
            "message_id": msg_dto.id,
            "woken_targets": routed,
            "rejected_files": rejected_files,
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

        let (task_dto, rejected_files) = self
            .create_cross_team_task_with_rejections(
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
            "rejected_files": rejected_files,
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

        let (task_dto, rejected_files) = self
            .update_cross_team_task_with_rejections(
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
            "rejected_files": rejected_files,
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
            let msg_id = member.pending_handoff_msg_id.unwrap_or_default();
            // Turn-1 deduplication: skip repeating the handoff banner on the very wake turn where `wake_prompt`
            // already delivered the room context and `team_room_post` instructions!
            if let Some((_, delivered_msg_id)) = self
                .delivered_handoff_banners
                .remove(&(room.id.clone(), team_id.to_owned()))
                && (delivered_msg_id == msg_id || delivered_msg_id.is_empty())
            {
                let _ = self
                    .bridge_repo
                    .increment_team_economy_savings(team_id, 45, 0)
                    .await;
                continue;
            }
            // Cache-friendly static instruction prefix first; dynamic IDs at the tail
            lines.push(format!(
                "[PENDING ROOM HANDOFF]: When your team finishes this work or reaches a decision, call `team_room_post` to deliver your response/files back to the Shared Room | room=\"#{name}\" room_id=\"{id}\" msg_id=\"{msg_id}\"",
                name = room.name,
                id = room.id,
            ));
        }
        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    fn on_team_delegation(&self, team_id: &str) {
        if let Some(mut entry) = self.active_runs.get_mut(team_id) {
            entry.delegated = true;
        }
    }

    async fn has_pending_slot_handover(
        &self,
        _user_id: &str,
        team_id: &str,
        slot_id: &str,
        _conversation_id: &str,
    ) -> bool {
        self.bridge_repo
            .get_team_context_anchor(team_id, slot_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|anchor| anchor.pending_handover != 0)
    }

    async fn slot_requires_fresh_role_prompt(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        conversation_id: &str,
    ) -> bool {
        if self
            .has_pending_slot_handover(user_id, team_id, slot_id, conversation_id)
            .await
        {
            return true;
        }
        self.bridge_repo
            .slot_has_empty_acp_session(user_id, conversation_id)
            .await
            .ok()
            .flatten()
            .unwrap_or(false)
    }

    async fn rearm_slot_handover(
        &self,
        _user_id: &str,
        team_id: &str,
        slot_id: &str,
        _conversation_id: &str,
    ) {
        let _ = self
            .bridge_repo
            .rearm_pending_slot_handover(team_id, slot_id)
            .await;
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
        Some(format!(
            "## Anchored Session Handover\n\n{trimmed}\n\n\
             > **Compaction Continuity Note**: Your prior CLI session was rolled over into this fresh context window. \
             Follow your `<team_Prompt>` role & tool rules above (when using Team CLI transport, pipe JSON via stdin to \
             `$env:AIONUI_HELPER_BIN` on Windows PowerShell or `\"$AIONUI_HELPER_BIN\"` on POSIX sh)."
        ))
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

    fn on_slot_wake_delivered(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        _conversation_id: &str,
        delivered_chars: usize,
        coalesced_wakes_saved: u64,
    ) {
        let bridge_repo = Arc::clone(&self.bridge_repo);
        let uid = user_id.to_owned();
        let tid = team_id.to_owned();
        let sid = slot_id.to_owned();
        let chars = delivered_chars as i64;
        let saved_wakes = coalesced_wakes_saved as i64;
        tokio::spawn(async move {
            if chars > 0 {
                let _ = bridge_repo
                    .record_slot_wake_delivery(&tid, &sid, &uid, "member", chars)
                    .await;
            }
            if saved_wakes > 0 {
                let _ = bridge_repo
                    .increment_team_economy_savings(&tid, saved_wakes * 350, saved_wakes)
                    .await;
            }
        });
    }
}

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use tracing::info;

use crate::error::DbError;

pub const CUSTOM_BRIDGE_SCHEMA_VERSION: &str = "1";
/// Default expiry for Telegram inline button tokens (7 days).
pub const DEFAULT_ACTION_TOKEN_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustomTargetType {
    Team,
    Room,
}

impl CustomTargetType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Team => "team",
            Self::Room => "room",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "team" => Some(Self::Team),
            "room" => Some(Self::Room),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CustomStreamMode {
    #[default]
    Milestones,
    FullStream,
}

impl CustomStreamMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Milestones => "milestones",
            Self::FullStream => "full_stream",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw.trim() {
            "full_stream" => Self::FullStream,
            _ => Self::Milestones,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CustomTargetNotifyPrefRow {
    pub owner_user_id: String,
    pub target_type: String,
    pub target_id: String,
    pub enabled: bool,
    pub stream_mode: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CustomChannelBindingRow {
    pub id: String,
    pub owner_user_id: String,
    pub plugin_id: String,
    pub chat_id: String,
    pub thread_id: Option<String>,
    pub target_type: String,
    pub target_id: String,
    pub stream_mode: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomChannelDmTargetRow {
    pub owner_user_id: String,
    pub plugin_id: String,
    pub user_id: String,
    pub chat_id: String,
    pub target_type: String,
    pub target_id: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomChannelMsgRouteRow {
    pub plugin_id: String,
    pub chat_id: String,
    pub telegram_msg_id: String,
    pub target_type: String,
    pub target_id: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomChannelActionTokenRow {
    pub token: String,
    pub owner_user_id: String,
    pub target_type: String,
    pub target_id: String,
    pub action_kind: String,
    pub payload_json: String,
    pub expires_at: i64,
    pub consumed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomSharedRoomRow {
    pub id: String,
    pub owner_user_id: String,
    pub name: String,
    pub description: String,
    pub rules: String,
    pub host_team_id: Option<String>,
    pub max_hops: i64,
    pub hop_count_since_human: i64,
    pub artifacts_dir: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomSharedRoomMemberRow {
    pub room_id: String,
    pub team_id: String,
    pub joined_at: i64,
    pub pending_handoff_msg_id: Option<String>,
    pub pending_handoff_hop_index: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomSharedRoomMessageRow {
    pub id: String,
    pub room_id: String,
    pub sender_type: String,
    pub sender_team_id: Option<String>,
    pub sender_label: String,
    pub reply_to_id: Option<String>,
    pub content: String,
    pub attachments_json: String,
    pub target_team_ids_json: String,
    pub hop_index: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomSharedRoomTaskRow {
    pub id: String,
    pub room_id: String,
    pub from_team_id: Option<String>,
    pub to_team_id: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub blocked_by_json: String,
    pub artifact_paths_json: String,
    pub result_summary: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct UpsertChannelBindingParams {
    pub owner_user_id: String,
    pub plugin_id: String,
    pub chat_id: String,
    pub thread_id: Option<String>,
    pub target_type: CustomTargetType,
    pub target_id: String,
    pub stream_mode: CustomStreamMode,
}

#[derive(Debug, Clone)]
pub struct CreateSharedRoomParams {
    pub id: String,
    pub owner_user_id: String,
    pub name: String,
    pub description: String,
    pub rules: String,
    pub host_team_id: Option<String>,
    pub max_hops: i64,
    pub artifacts_dir: String,
    pub team_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateSharedRoomParams {
    pub name: Option<String>,
    pub description: Option<String>,
    pub rules: Option<String>,
    pub host_team_id: Option<Option<String>>,
    pub max_hops: Option<i64>,
    pub team_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct CreateSharedRoomMessageParams {
    pub id: String,
    pub room_id: String,
    pub sender_type: String,
    pub sender_team_id: Option<String>,
    pub sender_label: String,
    pub reply_to_id: Option<String>,
    pub content: String,
    pub attachments_json: String,
    pub target_team_ids_json: String,
    pub hop_index: i64,
}

#[derive(Debug, Clone)]
pub struct CreateSharedRoomTaskParams {
    pub id: String,
    pub room_id: String,
    pub from_team_id: Option<String>,
    pub to_team_id: String,
    pub title: String,
    pub description: String,
    pub blocked_by_json: String,
    pub artifact_paths_json: String,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateSharedRoomTaskParams {
    pub status: Option<String>,
    pub description: Option<String>,
    pub blocked_by_json: Option<String>,
    pub artifact_paths_json: Option<String>,
    pub result_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomGitWorktreeRow {
    pub id: String,
    pub lease_id: String,
    pub owner_user_id: String,
    pub room_id: Option<String>,
    pub team_id: Option<String>,
    pub repo_path: String,
    pub worktree_path: String,
    pub branch: String,
    pub base_branch: String,
    pub status: String,
    pub leftovers_ref: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl CustomGitWorktreeRow {
    pub fn branch_name(&self) -> &str {
        &self.branch
    }
}

#[derive(Debug, Clone)]
pub struct CreateGitWorktreeParams {
    pub id: Option<String>,
    pub lease_id: String,
    pub owner_user_id: String,
    pub room_id: Option<String>,
    pub team_id: Option<String>,
    pub repo_path: String,
    pub worktree_path: String,
    pub branch: String,
    pub base_branch: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletType {
    ApiKey,
    CliSubscription,
}

impl WalletType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ApiKey => "api_key",
            Self::CliSubscription => "cli_subscription",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_lowercase().as_str() {
            "cli_subscription" | "cli" | "subscription" => Self::CliSubscription,
            _ => Self::ApiKey,
        }
    }
}

pub fn classify_agent_wallet(
    backend: Option<&str>,
    auth_methods_json: Option<&str>,
    has_custom_api_key: bool,
) -> WalletType {
    if has_custom_api_key {
        return WalletType::ApiKey;
    }
    let b = backend.unwrap_or("").trim().to_lowercase();
    if matches!(
        b.as_str(),
        "codex" | "antigravity" | "agy" | "gemini" | "gemini-cli" | "claude-code"
    ) {
        return WalletType::CliSubscription;
    }
    if b == "claude" {
        if let Some(raw) = auth_methods_json {
            if raw.contains("\"oauth\"") || raw.contains("subscription") {
                return WalletType::CliSubscription;
            }
        }
        // Claude CLI defaults to OAuth subscription when no custom API key is configured
        return WalletType::CliSubscription;
    }
    WalletType::ApiKey
}

#[derive(Debug, Clone, Default)]
pub struct SlotWalletFacts<'a> {
    pub slot_backend: Option<&'a str>,
    pub assistant_backend: Option<&'a str>,
    pub extra_backend: Option<&'a str>,
    pub cli_path: Option<&'a str>,
    pub provider_id: Option<&'a str>,
    pub model_id: Option<&'a str>,
    pub auth_methods_json: Option<&'a str>,
    pub has_custom_api_key: bool,
}

pub fn is_cli_oauth_backend_slug(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "claude" | "claude-code" | "codex" | "gemini" | "gemini-cli" | "antigravity" | "agy"
    )
}

pub fn is_seeded_cli_model_id(raw: &str) -> bool {
    let m = raw.trim().to_ascii_lowercase();
    if m.is_empty() {
        return false;
    }
    m.starts_with("claude")
        || m.starts_with("codex")
        || m.starts_with("gemini")
        || m.starts_with("antigravity")
        || matches!(
            m.as_str(),
            "default"
                | "opus"
                | "sonnet"
                | "haiku"
                | "fable"
                | "gpt-6.1-sol"
                | "gpt-5.4"
                | "gpt-5.3-codex"
                | "gpt-5.4-mini"
                | "gpt-oss-120b-medium"
        )
}

/// Authoritative per-slot wallet classifier resolving across `slot_backend`, `assistant_backend`,
/// `conversations.extra`, `cli_path`, `provider_id`, and seeded CLI short model IDs (`opus`,
/// `sonnet`, `haiku`, `default`, `gpt-6.1-sol`, `gpt-5.4`, etc.) so `$0` CLI subscription slots
/// spawned via ACP are never misclassified as `WalletType::ApiKey`, while Paid API (`aionrs` /
/// custom `provider_id` / `has_custom_api_key`) slots are always metered accurately.
pub fn classify_slot_wallet_facts(facts: &SlotWalletFacts<'_>) -> WalletType {
    if facts.has_custom_api_key {
        return WalletType::ApiKey;
    }
    if facts
        .provider_id
        .map(str::trim)
        .filter(|p| {
            !p.is_empty()
                && !matches!(
                    p.to_ascii_lowercase().as_str(),
                    "cli" | "oauth" | "builtin" | "subscription"
                )
        })
        .is_some()
    {
        return WalletType::ApiKey;
    }

    for candidate in [
        facts.slot_backend,
        facts.assistant_backend,
        facts.extra_backend,
    ]
    .into_iter()
    .flatten()
    {
        let b = candidate.trim().to_ascii_lowercase();
        if b.is_empty() || b == "acp" {
            continue;
        }
        if b == "aionrs" {
            return WalletType::ApiKey;
        }
        if is_cli_oauth_backend_slug(&b) {
            return WalletType::CliSubscription;
        }
        return classify_agent_wallet(Some(&b), facts.auth_methods_json, false);
    }

    if let Some(cli_raw) = facts.cli_path.map(str::trim).filter(|s| !s.is_empty()) {
        let file_name = std::path::Path::new(cli_raw)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(cli_raw)
            .to_ascii_lowercase();
        let stem = file_name
            .trim_end_matches(".exe")
            .trim_end_matches(".cmd")
            .trim_end_matches(".bat")
            .trim_end_matches(".ps1");
        if is_cli_oauth_backend_slug(stem) {
            return WalletType::CliSubscription;
        }
    }

    if let Some(mid) = facts.model_id.map(str::trim).filter(|s| !s.is_empty())
        && is_seeded_cli_model_id(mid)
    {
        return WalletType::CliSubscription;
    }

    WalletType::ApiKey
}

/// Estimates USD API spend for a single turn/call from real input/output tokens and model tier.
/// `WalletType::CliSubscription` is strictly `$0.00` (`AGENTS.md` §3).
pub fn estimate_api_token_cost_usd(
    wallet_type: WalletType,
    model_id: &str,
    tokens_in: i64,
    tokens_out: i64,
) -> f64 {
    if wallet_type == WalletType::CliSubscription {
        return 0.0;
    }
    let t_in = tokens_in.max(0) as f64;
    let t_out = tokens_out.max(0) as f64;
    let m = model_id.trim().to_ascii_lowercase();
    let (in_per_million, out_per_million) = if m.contains("opus") || m.contains("o1") || m.contains("gpt-5-pro") {
        (15.0, 75.0)
    } else if m.contains("flash")
        || m.contains("mini")
        || m.contains("haiku")
        || m.contains("nano")
        || m.contains("small")
    {
        (0.40, 1.60)
    } else {
        (3.00, 15.00)
    };
    (t_in * (in_per_million / 1_000_000.0)) + (t_out * (out_per_million / 1_000_000.0))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomActionInboxRow {
    pub id: String,
    pub owner_user_id: String,
    pub category: String,
    pub title: String,
    pub summary: String,
    pub details_json: String,
    pub status: String,
    pub target_type: String,
    pub target_id: String,
    pub action_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub telegram_msg_id: Option<String>,
    pub resolution_result: Option<String>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl CustomActionInboxRow {
    pub fn is_pending(&self) -> bool {
        self.status == "pending"
    }

    pub fn is_resolved(&self) -> bool {
        self.status == "resolved"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct CustomCostWalletRow {
    pub owner_user_id: String,
    pub api_key_budget_limit_usd: f64,
    pub api_key_spent_usd: f64,
    pub cli_subscription_runs_count: i64,
    pub api_key_runs_count: i64,
    pub soft_warning_triggered: i64,
    pub hard_block_triggered: i64,
    pub updated_at: i64,
}

impl CustomCostWalletRow {
    pub fn is_soft_warning(&self) -> bool {
        self.soft_warning_triggered == 1
    }

    pub fn is_hard_blocked(&self) -> bool {
        self.hard_block_triggered == 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct CustomCostLedgerRow {
    pub id: String,
    pub owner_user_id: String,
    pub run_id: String,
    pub team_id: String,
    pub agent_id: String,
    pub wallet_type: String,
    pub tokens_input: i64,
    pub tokens_output: i64,
    pub cost_usd: f64,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct CreateActionInboxParams {
    pub id: Option<String>,
    pub owner_user_id: String,
    pub category: String,
    pub title: String,
    pub summary: String,
    pub details_json: String,
    pub target_type: Option<String>,
    pub target_id: String,
    pub action_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub telegram_msg_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RecordCostRunParams {
    pub owner_user_id: String,
    pub run_id: String,
    pub team_id: String,
    pub agent_id: String,
    pub wallet_type: String,
    pub tokens_input: i64,
    pub tokens_output: i64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomTeamContextAnchorRow {
    pub team_id: String,
    pub slot_id: String,
    pub conversation_id: String,
    pub anchored_summary_md: String,
    pub files_touched_json: String,
    pub last_compacted_msg_id: Option<String>,
    pub compaction_count: i64,
    pub pending_handover: i64,
    pub updated_at: i64,
}

impl CustomTeamContextAnchorRow {
    pub fn has_pending_handover(&self) -> bool {
        self.pending_handover != 0
    }
}

#[derive(Debug, Clone)]
pub struct UpsertTeamContextAnchorParams {
    pub team_id: String,
    pub slot_id: String,
    pub conversation_id: String,
    pub anchored_summary_md: String,
    pub files_touched_json: String,
    pub last_compacted_msg_id: Option<String>,
    pub compaction_count: i64,
    pub pending_handover: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomTeamContextPrefRow {
    pub team_id: String,
    pub auto_compact_enabled: i64,
    pub threshold_pct: i64,
    #[serde(default = "default_economy_mode")]
    #[sqlx(default)]
    pub economy_mode: String,
    #[serde(default = "default_max_context_tokens")]
    #[sqlx(default)]
    pub max_context_tokens: i64,
    #[serde(default)]
    #[sqlx(default)]
    pub tokens_saved_estimate: i64,
    #[serde(default)]
    #[sqlx(default)]
    pub coalesced_wakes_saved: i64,
    #[serde(default = "default_max_turn_hops_per_run")]
    #[sqlx(default)]
    pub max_turn_hops_per_run: i64,
    #[serde(default)]
    #[sqlx(default)]
    pub auto_fallback_free_cli: i64,
    pub updated_at: i64,
}

pub fn default_economy_mode() -> String {
    "lean".to_owned()
}

pub fn default_max_context_tokens() -> i64 {
    160_000
}

pub fn default_max_turn_hops_per_run() -> i64 {
    24
}

impl CustomTeamContextPrefRow {
    pub fn is_auto_compact_enabled(&self) -> bool {
        self.auto_compact_enabled != 0
    }

    pub fn effective_economy_mode(&self) -> &str {
        match self.economy_mode.trim() {
            "balanced" => "balanced",
            "verbose" => "verbose",
            _ => "lean",
        }
    }

    pub fn effective_max_context_tokens(&self) -> i64 {
        if self.max_context_tokens > 0 {
            self.max_context_tokens
        } else {
            default_max_context_tokens()
        }
    }

    pub fn is_auto_fallback_free_cli_enabled(&self) -> bool {
        self.auto_fallback_free_cli != 0
    }
}

#[derive(Debug, Clone, Default)]
pub struct UpsertTeamContextPrefParams {
    pub team_id: String,
    pub auto_compact_enabled: Option<bool>,
    pub threshold_pct: Option<i64>,
    pub economy_mode: Option<String>,
    pub max_context_tokens: Option<i64>,
    pub max_turn_hops_per_run: Option<i64>,
    pub auto_fallback_free_cli: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct CustomTeamSlotEconomyRow {
    pub team_id: String,
    pub slot_id: String,
    pub owner_user_id: String,
    pub role: String,
    pub wallet_type: String,
    pub backend: String,
    pub model_id: String,
    pub wakeup_turns: i64,
    pub input_tokens_total: i64,
    pub output_tokens_total: i64,
    #[serde(default)]
    #[sqlx(default)]
    pub cached_read_tokens_total: i64,
    pub delivered_wake_chars: i64,
    pub unbilled_wake_chars: i64,
    pub estimated_cost_usd: f64,
    pub compaction_count: i64,
    pub compaction_saved_tokens: i64,
    pub coalesced_wakes_saved: i64,
    pub last_seen_conv_id: String,
    pub last_seen_msg_count: i64,
    pub last_seen_char_sum: i64,
    pub last_seen_usage_tokens: i64,
    #[serde(default)]
    #[sqlx(default)]
    pub last_seen_meta_seq: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct SettleSlotEconomyParams {
    pub team_id: String,
    pub slot_id: String,
    pub owner_user_id: String,
    pub role: String,
    pub conversation_id: String,
    pub wallet_type: WalletType,
    pub backend: String,
    pub model_id: String,
    pub current_msg_count: i64,
    pub current_char_sum: i64,
    pub reported_total_tokens: Option<i64>,
    pub reported_input_tokens: Option<i64>,
    pub reported_output_tokens: Option<i64>,
    pub reported_cached_read_tokens: Option<i64>,
    pub reported_meta_seq: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotTurnDeltaResult {
    pub delta_input_tokens: i64,
    pub delta_output_tokens: i64,
    #[serde(default)]
    pub delta_cached_read_tokens: i64,
    pub delta_cost_usd: f64,
    pub row: CustomTeamSlotEconomyRow,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct CustomTeamSlotSwapRow {
    pub team_id: String,
    pub slot_id: String,
    pub target_backend: String,
    pub target_provider_id: Option<String>,
    pub target_model_id: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct AnchorMessageSourceRow {
    pub id: String,
    pub msg_id: Option<String>,
    pub r#type: String,
    pub content: String,
    pub position: Option<String>,
    pub hidden: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct AnchorMailboxSourceRow {
    pub from_agent_id: String,
    pub to_agent_id: String,
    pub content: String,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
pub struct AnchorTeamTaskSourceRow {
    pub id: String,
    pub subject: String,
    pub status: String,
    pub owner: Option<String>,
    pub blocked_by: String,
}

/// Idempotently creates all fork-specific `custom_*` tables and indexes
/// without modifying `_sqlx_migrations`, keeping upstream merges conflict-free.
pub async fn ensure_custom_bridge_tables(pool: &SqlitePool) -> Result<(), DbError> {
    let ddl_statements = [
        r#"
        CREATE TABLE IF NOT EXISTS custom_schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_target_notify_prefs (
            owner_user_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            stream_mode TEXT NOT NULL DEFAULT 'milestones',
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (owner_user_id, target_type, target_id)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_channel_bindings (
            id TEXT PRIMARY KEY,
            owner_user_id TEXT NOT NULL,
            plugin_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            thread_id TEXT NOT NULL DEFAULT '',
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            stream_mode TEXT NOT NULL DEFAULT 'milestones',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_channel_bindings_chat_thread
            ON custom_channel_bindings(owner_user_id, plugin_id, chat_id, thread_id)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_channel_bindings_target
            ON custom_channel_bindings(owner_user_id, target_type, target_id)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_channel_dm_targets (
            owner_user_id TEXT NOT NULL,
            plugin_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (owner_user_id, plugin_id, chat_id)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_channel_msg_routes (
            plugin_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            telegram_msg_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (plugin_id, chat_id, telegram_msg_id)
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_channel_msg_routes_created
            ON custom_channel_msg_routes(created_at)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_channel_action_tokens (
            token TEXT PRIMARY KEY,
            owner_user_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            action_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            consumed_at INTEGER
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_channel_action_tokens_expires
            ON custom_channel_action_tokens(expires_at)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_shared_rooms (
            id TEXT PRIMARY KEY,
            owner_user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            rules TEXT NOT NULL DEFAULT '',
            host_team_id TEXT,
            max_hops INTEGER NOT NULL DEFAULT 5,
            hop_count_since_human INTEGER NOT NULL DEFAULT 0,
            artifacts_dir TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_shared_rooms_owner
            ON custom_shared_rooms(owner_user_id, updated_at DESC)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_shared_room_members (
            room_id TEXT NOT NULL,
            team_id TEXT NOT NULL,
            joined_at INTEGER NOT NULL,
            pending_handoff_msg_id TEXT,
            pending_handoff_hop_index INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (room_id, team_id)
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_shared_room_members_team
            ON custom_shared_room_members(team_id)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_shared_room_messages (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            sender_type TEXT NOT NULL,
            sender_team_id TEXT,
            sender_label TEXT NOT NULL DEFAULT '',
            reply_to_id TEXT,
            content TEXT NOT NULL,
            attachments_json TEXT NOT NULL DEFAULT '[]',
            target_team_ids_json TEXT NOT NULL DEFAULT '[]',
            hop_index INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_shared_room_messages_room_time
            ON custom_shared_room_messages(room_id, created_at ASC)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_shared_room_tasks (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            from_team_id TEXT,
            to_team_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            blocked_by_json TEXT NOT NULL DEFAULT '[]',
            artifact_paths_json TEXT NOT NULL DEFAULT '[]',
            result_summary TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_shared_room_tasks_room
            ON custom_shared_room_tasks(room_id, updated_at DESC)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_shared_room_tasks_assignee
            ON custom_shared_room_tasks(to_team_id, status)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_git_worktrees (
            id TEXT PRIMARY KEY,
            lease_id TEXT NOT NULL,
            owner_user_id TEXT NOT NULL,
            room_id TEXT,
            team_id TEXT,
            repo_path TEXT NOT NULL,
            worktree_path TEXT NOT NULL,
            branch TEXT NOT NULL,
            base_branch TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            leftovers_ref TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_git_worktrees_lease
            ON custom_git_worktrees(lease_id)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_git_worktrees_team
            ON custom_git_worktrees(team_id, status)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_git_worktrees_owner
            ON custom_git_worktrees(owner_user_id, status)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_git_worktrees_path
            ON custom_git_worktrees(worktree_path)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_git_worktrees_room
            ON custom_git_worktrees(room_id, status)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_action_inbox (
            id TEXT PRIMARY KEY,
            owner_user_id TEXT NOT NULL,
            category TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            details_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            target_type TEXT NOT NULL DEFAULT 'room',
            target_id TEXT NOT NULL,
            action_token TEXT,
            telegram_chat_id TEXT,
            telegram_msg_id TEXT,
            resolution_result TEXT,
            resolved_by TEXT,
            resolved_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_action_inbox_pending
            ON custom_action_inbox(owner_user_id, status, created_at DESC)
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_action_inbox_target
            ON custom_action_inbox(target_type, target_id)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_cost_wallets (
            owner_user_id TEXT PRIMARY KEY,
            api_key_budget_limit_usd REAL NOT NULL DEFAULT 50.0,
            api_key_spent_usd REAL NOT NULL DEFAULT 0.0,
            cli_subscription_runs_count INTEGER NOT NULL DEFAULT 0,
            api_key_runs_count INTEGER NOT NULL DEFAULT 0,
            soft_warning_triggered INTEGER NOT NULL DEFAULT 0,
            hard_block_triggered INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_cost_ledger (
            id TEXT PRIMARY KEY,
            owner_user_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            team_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            wallet_type TEXT NOT NULL,
            tokens_input INTEGER NOT NULL DEFAULT 0,
            tokens_output INTEGER NOT NULL DEFAULT 0,
            cost_usd REAL NOT NULL DEFAULT 0.0,
            created_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_cost_ledger_team
            ON custom_cost_ledger(owner_user_id, team_id, created_at DESC)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_team_context_anchors (
            team_id TEXT NOT NULL,
            slot_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            anchored_summary_md TEXT NOT NULL DEFAULT '',
            files_touched_json TEXT NOT NULL DEFAULT '[]',
            last_compacted_msg_id TEXT,
            compaction_count INTEGER NOT NULL DEFAULT 0,
            pending_handover INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (team_id, slot_id)
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_team_context_anchors_conv
            ON custom_team_context_anchors(conversation_id)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_team_context_prefs (
            team_id TEXT PRIMARY KEY,
            auto_compact_enabled INTEGER NOT NULL DEFAULT 1,
            threshold_pct INTEGER NOT NULL DEFAULT 80,
            economy_mode TEXT NOT NULL DEFAULT 'lean',
            max_context_tokens INTEGER NOT NULL DEFAULT 160000,
            tokens_saved_estimate INTEGER NOT NULL DEFAULT 0,
            coalesced_wakes_saved INTEGER NOT NULL DEFAULT 0,
            max_turn_hops_per_run INTEGER NOT NULL DEFAULT 24,
            auto_fallback_free_cli INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_team_slot_economy (
            team_id TEXT NOT NULL,
            slot_id TEXT NOT NULL,
            owner_user_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'teammate',
            wallet_type TEXT NOT NULL DEFAULT 'cli_subscription',
            backend TEXT NOT NULL DEFAULT '',
            model_id TEXT NOT NULL DEFAULT '',
            wakeup_turns INTEGER NOT NULL DEFAULT 0,
            input_tokens_total INTEGER NOT NULL DEFAULT 0,
            output_tokens_total INTEGER NOT NULL DEFAULT 0,
            cached_read_tokens_total INTEGER NOT NULL DEFAULT 0,
            delivered_wake_chars INTEGER NOT NULL DEFAULT 0,
            unbilled_wake_chars INTEGER NOT NULL DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
            compaction_count INTEGER NOT NULL DEFAULT 0,
            compaction_saved_tokens INTEGER NOT NULL DEFAULT 0,
            coalesced_wakes_saved INTEGER NOT NULL DEFAULT 0,
            last_seen_conv_id TEXT NOT NULL DEFAULT '',
            last_seen_msg_count INTEGER NOT NULL DEFAULT 0,
            last_seen_char_sum INTEGER NOT NULL DEFAULT 0,
            last_seen_usage_tokens INTEGER NOT NULL DEFAULT 0,
            last_seen_meta_seq INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (team_id, slot_id)
        )
        "#,
        r#"
        CREATE INDEX IF NOT EXISTS idx_custom_team_slot_economy_owner
            ON custom_team_slot_economy(owner_user_id, updated_at DESC)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS custom_team_slot_swaps (
            team_id TEXT NOT NULL,
            slot_id TEXT NOT NULL,
            target_backend TEXT NOT NULL,
            target_provider_id TEXT,
            target_model_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (team_id, slot_id)
        )
        "#,
    ];

    for ddl in ddl_statements {
        sqlx::query(ddl).execute(pool).await.map_err(DbError::Query)?;
    }

    // Idempotently evolve `custom_team_context_prefs` on upgraded databases without touching `_sqlx_migrations`.
    let existing_pref_cols: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('custom_team_context_prefs')",
    )
    .fetch_all(pool)
    .await
    .map_err(DbError::Query)?;

    let pref_alter_specs: &[(&str, &str)] = &[
        (
            "economy_mode",
            "ALTER TABLE custom_team_context_prefs ADD COLUMN economy_mode TEXT NOT NULL DEFAULT 'lean'",
        ),
        (
            "max_context_tokens",
            "ALTER TABLE custom_team_context_prefs ADD COLUMN max_context_tokens INTEGER NOT NULL DEFAULT 160000",
        ),
        (
            "tokens_saved_estimate",
            "ALTER TABLE custom_team_context_prefs ADD COLUMN tokens_saved_estimate INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "coalesced_wakes_saved",
            "ALTER TABLE custom_team_context_prefs ADD COLUMN coalesced_wakes_saved INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "max_turn_hops_per_run",
            "ALTER TABLE custom_team_context_prefs ADD COLUMN max_turn_hops_per_run INTEGER NOT NULL DEFAULT 24",
        ),
        (
            "auto_fallback_free_cli",
            "ALTER TABLE custom_team_context_prefs ADD COLUMN auto_fallback_free_cli INTEGER NOT NULL DEFAULT 0",
        ),
    ];

    for (col_name, alter_sql) in pref_alter_specs {
        if !existing_pref_cols.iter().any(|c| c == col_name) {
            sqlx::query(alter_sql).execute(pool).await.map_err(DbError::Query)?;
        }
    }

    let existing_econ_cols: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('custom_team_slot_economy')",
    )
    .fetch_all(pool)
    .await
    .map_err(DbError::Query)?;

    if !existing_econ_cols.iter().any(|c| c == "cached_read_tokens_total") {
        sqlx::query(
            "ALTER TABLE custom_team_slot_economy ADD COLUMN cached_read_tokens_total INTEGER NOT NULL DEFAULT 0",
        )
        .execute(pool)
        .await
        .map_err(DbError::Query)?;
    }
    if !existing_econ_cols.iter().any(|c| c == "last_seen_meta_seq") {
        sqlx::query(
            "ALTER TABLE custom_team_slot_economy ADD COLUMN last_seen_meta_seq INTEGER NOT NULL DEFAULT 0",
        )
        .execute(pool)
        .await
        .map_err(DbError::Query)?;
    }

    // Seed cold-start model catalogs for direct CLI agents (antigravity, claude, codex, gemini)
    // when `available_models` has not been populated yet, so the New Chat (/guid) and
    // conversation header model pickers are immediately selectable on first launch.
    let cli_catalog_seeds: &[(&str, &str, &str)] = &[
        (
            "antigravity",
            r#"{"available_models":[{"id":"gemini-4-argon-high","label":"Gemini 4 Argon (High)"},{"id":"gemini-4-argon-medium","label":"Gemini 4 Argon (Medium)"},{"id":"gemini-4-argon-low","label":"Gemini 4 Argon (Low)"},{"id":"gemini-3.8-flash-high","label":"Gemini 3.8 Flash (High)"},{"id":"gemini-3.8-flash-medium","label":"Gemini 3.8 Flash (Medium)"},{"id":"gemini-3.8-flash-low","label":"Gemini 3.8 Flash (Low)"},{"id":"claude-sonnet-4-6","label":"Claude Sonnet 4.6 (Thinking)"},{"id":"claude-opus-4-6-thinking","label":"Claude Opus 4.6 (Thinking)"},{"id":"gpt-oss-120b-medium","label":"GPT-OSS 120B (Medium)"}],"current_model_id":"gemini-4-argon-high"}"#,
            r#"[{"id":"mode","category":"mode","type":"select","currentValue":"default","options":[{"value":"default","name":"Default"},{"value":"accept-edits","name":"Accept Edits"},{"value":"plan","name":"Plan"},{"value":"yolo","name":"Full Auto"}]},{"id":"model","category":"model","type":"select","currentValue":"gemini-4-argon-high","options":[{"value":"gemini-4-argon-high","name":"Gemini 4 Argon (High)"},{"value":"gemini-4-argon-medium","name":"Gemini 4 Argon (Medium)"},{"value":"gemini-4-argon-low","name":"Gemini 4 Argon (Low)"},{"value":"gemini-3.8-flash-high","name":"Gemini 3.8 Flash (High)"},{"value":"gemini-3.8-flash-medium","name":"Gemini 3.8 Flash (Medium)"},{"value":"gemini-3.8-flash-low","name":"Gemini 3.8 Flash (Low)"},{"value":"claude-sonnet-4-6","name":"Claude Sonnet 4.6 (Thinking)"},{"value":"claude-opus-4-6-thinking","name":"Claude Opus 4.6 (Thinking)"},{"value":"gpt-oss-120b-medium","name":"GPT-OSS 120B (Medium)"}]}]"#,
        ),
        (
            "claude",
            r#"{"available_models":[{"id":"default","label":"Default (recommended)"},{"id":"opus","label":"Opus"},{"id":"sonnet","label":"Sonnet"},{"id":"haiku","label":"Haiku"},{"id":"fable","label":"Fable"}],"current_model_id":"default"}"#,
            r#"[{"id":"model","category":"model","type":"select","currentValue":"default","options":[{"value":"default","name":"Default (recommended)"},{"value":"opus","name":"Opus"},{"value":"sonnet","name":"Sonnet"},{"value":"haiku","name":"Haiku"},{"value":"fable","name":"Fable"}]}]"#,
        ),
        (
            "codex",
            r#"{"available_models":[{"id":"gpt-6.1-sol","label":"GPT-6.1 Sol"},{"id":"gpt-5.4","label":"GPT-5.4"},{"id":"gpt-5.3-codex","label":"GPT-5.3 Codex"}],"current_model_id":"gpt-6.1-sol"}"#,
            r#"[{"id":"model","category":"model","type":"select","currentValue":"gpt-6.1-sol","options":[{"value":"gpt-6.1-sol","name":"GPT-6.1 Sol"},{"value":"gpt-5.4","name":"GPT-5.4"},{"value":"gpt-5.3-codex","name":"GPT-5.3 Codex"}]}]"#,
        ),
        (
            "gemini",
            r#"{"available_models":[{"id":"default","label":"Default (Gemini 2.5 Pro)"},{"id":"gemini-2.5-pro","label":"Gemini 2.5 Pro"},{"id":"gemini-2.5-flash","label":"Gemini 2.5 Flash"}],"current_model_id":"default"}"#,
            r#"[{"id":"model","category":"model","type":"select","currentValue":"default","options":[{"value":"default","name":"Default (Gemini 2.5 Pro)"},{"value":"gemini-2.5-pro","name":"Gemini 2.5 Pro"},{"value":"gemini-2.5-flash","name":"Gemini 2.5 Flash"}]}]"#,
        ),
        (
            "gemini-cli",
            r#"{"available_models":[{"id":"default","label":"Default (Gemini 2.5 Pro)"},{"id":"gemini-2.5-pro","label":"Gemini 2.5 Pro"},{"id":"gemini-2.5-flash","label":"Gemini 2.5 Flash"}],"current_model_id":"default"}"#,
            r#"[{"id":"model","category":"model","type":"select","currentValue":"default","options":[{"value":"default","name":"Default (Gemini 2.5 Pro)"},{"value":"gemini-2.5-pro","name":"Gemini 2.5 Pro"},{"value":"gemini-2.5-flash","name":"Gemini 2.5 Flash"}]}]"#,
        ),
    ];

    for (backend, available_models_json, config_options_json) in cli_catalog_seeds {
        let _ = sqlx::query(
            r#"
            UPDATE agent_metadata
               SET available_models = ?,
                   config_options = COALESCE(config_options, ?)
             WHERE backend = ?
               AND (available_models IS NULL OR TRIM(available_models) = '')
            "#,
        )
        .bind(available_models_json)
        .bind(config_options_json)
        .bind(backend)
        .execute(pool)
        .await;
    }

    let now = aionui_common::now_ms();
    sqlx::query(
        r#"
        INSERT INTO custom_schema_meta (key, value, updated_at)
        VALUES ('custom_bridge_schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        "#,
    )
    .bind(CUSTOM_BRIDGE_SCHEMA_VERSION)
    .bind(now)
    .execute(pool)
    .await
    .map_err(DbError::Query)?;

    info!(
        version = CUSTOM_BRIDGE_SCHEMA_VERSION,
        "custom_bridge_schema tables verified"
    );
    Ok(())
}

fn normalize_thread_id(thread_id: Option<&str>) -> String {
    thread_id.map(str::trim).filter(|s| !s.is_empty()).unwrap_or("").to_owned()
}

fn decode_thread_id(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[derive(FromRow)]
struct RawChannelBindingRow {
    id: String,
    owner_user_id: String,
    plugin_id: String,
    chat_id: String,
    thread_id: String,
    target_type: String,
    target_id: String,
    stream_mode: String,
    created_at: i64,
    updated_at: i64,
}

impl From<RawChannelBindingRow> for CustomChannelBindingRow {
    fn from(raw: RawChannelBindingRow) -> Self {
        Self {
            id: raw.id,
            owner_user_id: raw.owner_user_id,
            plugin_id: raw.plugin_id,
            chat_id: raw.chat_id,
            thread_id: decode_thread_id(raw.thread_id),
            target_type: raw.target_type,
            target_id: raw.target_id,
            stream_mode: raw.stream_mode,
            created_at: raw.created_at,
            updated_at: raw.updated_at,
        }
    }
}

#[derive(FromRow)]
struct RawTargetNotifyPrefRow {
    owner_user_id: String,
    target_type: String,
    target_id: String,
    enabled: i64,
    stream_mode: String,
    updated_at: i64,
}

impl From<RawTargetNotifyPrefRow> for CustomTargetNotifyPrefRow {
    fn from(raw: RawTargetNotifyPrefRow) -> Self {
        Self {
            owner_user_id: raw.owner_user_id,
            target_type: raw.target_type,
            target_id: raw.target_id,
            enabled: raw.enabled != 0,
            stream_mode: raw.stream_mode,
            updated_at: raw.updated_at,
        }
    }
}

/// Repository over the `custom_*` bridge & shared-room tables.
#[derive(Clone, Debug)]
pub struct SqliteCustomBridgeRepository {
    pool: SqlitePool,
}

impl SqliteCustomBridgeRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // -----------------------------------------------------------------------
    // Per-target notification preferences (enabled + stream_mode)
    // -----------------------------------------------------------------------

    pub async fn get_target_notify_pref(
        &self,
        owner_user_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Result<Option<CustomTargetNotifyPrefRow>, DbError> {
        let row = sqlx::query_as::<_, RawTargetNotifyPrefRow>(
            r#"
            SELECT owner_user_id, target_type, target_id, enabled, stream_mode, updated_at
            FROM custom_target_notify_prefs
            WHERE owner_user_id = ? AND target_type = ? AND target_id = ?
            "#,
        )
        .bind(owner_user_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(Into::into))
    }

    pub async fn list_target_notify_prefs(
        &self,
        owner_user_id: &str,
        target_type: CustomTargetType,
    ) -> Result<Vec<CustomTargetNotifyPrefRow>, DbError> {
        let rows = sqlx::query_as::<_, RawTargetNotifyPrefRow>(
            r#"
            SELECT owner_user_id, target_type, target_id, enabled, stream_mode, updated_at
            FROM custom_target_notify_prefs
            WHERE owner_user_id = ? AND target_type = ?
            "#,
        )
        .bind(owner_user_id)
        .bind(target_type.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn upsert_target_notify_pref(
        &self,
        owner_user_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
        enabled: bool,
        stream_mode: CustomStreamMode,
    ) -> Result<CustomTargetNotifyPrefRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_target_notify_prefs (owner_user_id, target_type, target_id, enabled, stream_mode, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_user_id, target_type, target_id) DO UPDATE SET
                enabled = excluded.enabled,
                stream_mode = excluded.stream_mode,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(owner_user_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .bind(if enabled { 1_i64 } else { 0_i64 })
        .bind(stream_mode.as_str())
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(CustomTargetNotifyPrefRow {
            owner_user_id: owner_user_id.to_owned(),
            target_type: target_type.as_str().to_owned(),
            target_id: target_id.to_owned(),
            enabled,
            stream_mode: stream_mode.as_str().to_owned(),
            updated_at: now,
        })
    }

    // -----------------------------------------------------------------------
    // Channel Chat / Topic Bindings
    // -----------------------------------------------------------------------

    pub async fn upsert_channel_binding(
        &self,
        params: UpsertChannelBindingParams,
    ) -> Result<CustomChannelBindingRow, DbError> {
        let now = aionui_common::now_ms();
        let id = aionui_common::generate_prefixed_id("cbind");
        let norm_thread = normalize_thread_id(params.thread_id.as_deref());

        sqlx::query(
            r#"
            INSERT INTO custom_channel_bindings (
                id, owner_user_id, plugin_id, chat_id, thread_id,
                target_type, target_id, stream_mode, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_user_id, plugin_id, chat_id, thread_id) DO UPDATE SET
                target_type = excluded.target_type,
                target_id = excluded.target_id,
                stream_mode = excluded.stream_mode,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&id)
        .bind(&params.owner_user_id)
        .bind(&params.plugin_id)
        .bind(&params.chat_id)
        .bind(&norm_thread)
        .bind(params.target_type.as_str())
        .bind(&params.target_id)
        .bind(params.stream_mode.as_str())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.find_channel_binding_exact(
            &params.owner_user_id,
            &params.plugin_id,
            &params.chat_id,
            params.thread_id.as_deref(),
        )
        .await?
        .ok_or_else(|| DbError::NotFound("channel binding missing after upsert".into()))
    }

    pub async fn find_channel_binding_exact(
        &self,
        owner_user_id: &str,
        plugin_id: &str,
        chat_id: &str,
        thread_id: Option<&str>,
    ) -> Result<Option<CustomChannelBindingRow>, DbError> {
        let norm_thread = normalize_thread_id(thread_id);
        let row = sqlx::query_as::<_, RawChannelBindingRow>(
            r#"
            SELECT id, owner_user_id, plugin_id, chat_id, thread_id,
                   target_type, target_id, stream_mode, created_at, updated_at
            FROM custom_channel_bindings
            WHERE owner_user_id = ? AND plugin_id = ? AND chat_id = ? AND thread_id = ?
            "#,
        )
        .bind(owner_user_id)
        .bind(plugin_id)
        .bind(chat_id)
        .bind(norm_thread)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(Into::into))
    }

    /// Resolves a binding for an incoming message: checks exact `(chat_id, thread_id)` first,
    /// then falls back to chat-wide `(chat_id, '')` when `thread_id` is present.
    pub async fn resolve_channel_binding_for_chat(
        &self,
        owner_user_id: &str,
        plugin_id: &str,
        chat_id: &str,
        thread_id: Option<&str>,
    ) -> Result<Option<CustomChannelBindingRow>, DbError> {
        if let Some(tid) = thread_id.map(str::trim).filter(|s| !s.is_empty())
            && let Some(exact) = self
                .find_channel_binding_exact(owner_user_id, plugin_id, chat_id, Some(tid))
                .await?
        {
            return Ok(Some(exact));
        }
        self.find_channel_binding_exact(owner_user_id, plugin_id, chat_id, None)
            .await
    }

    pub async fn list_channel_bindings_for_owner(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<CustomChannelBindingRow>, DbError> {
        let rows = sqlx::query_as::<_, RawChannelBindingRow>(
            r#"
            SELECT id, owner_user_id, plugin_id, chat_id, thread_id,
                   target_type, target_id, stream_mode, created_at, updated_at
            FROM custom_channel_bindings
            WHERE owner_user_id = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_channel_bindings_for_target(
        &self,
        owner_user_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Result<Vec<CustomChannelBindingRow>, DbError> {
        let rows = sqlx::query_as::<_, RawChannelBindingRow>(
            r#"
            SELECT id, owner_user_id, plugin_id, chat_id, thread_id,
                   target_type, target_id, stream_mode, created_at, updated_at
            FROM custom_channel_bindings
            WHERE owner_user_id = ? AND target_type = ? AND target_id = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(owner_user_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn delete_channel_bindings_for_target(
        &self,
        owner_user_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Result<u64, DbError> {
        let res = sqlx::query(
            "DELETE FROM custom_channel_bindings WHERE owner_user_id = ? AND target_type = ? AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(res.rows_affected())
    }

    pub async fn delete_channel_binding_by_id(&self, owner_user_id: &str, id: &str) -> Result<bool, DbError> {
        let res = sqlx::query("DELETE FROM custom_channel_bindings WHERE owner_user_id = ? AND id = ?")
            .bind(owner_user_id)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(DbError::Query)?;
        Ok(res.rows_affected() > 0)
    }

    // -----------------------------------------------------------------------
    // DM 1-1 Active Target
    // -----------------------------------------------------------------------

    pub async fn set_dm_target(
        &self,
        owner_user_id: &str,
        plugin_id: &str,
        user_id: &str,
        chat_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Result<CustomChannelDmTargetRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_channel_dm_targets (
                owner_user_id, plugin_id, user_id, chat_id, target_type, target_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_user_id, plugin_id, chat_id) DO UPDATE SET
                user_id = excluded.user_id,
                target_type = excluded.target_type,
                target_id = excluded.target_id,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(owner_user_id)
        .bind(plugin_id)
        .bind(user_id)
        .bind(chat_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(CustomChannelDmTargetRow {
            owner_user_id: owner_user_id.to_owned(),
            plugin_id: plugin_id.to_owned(),
            user_id: user_id.to_owned(),
            chat_id: chat_id.to_owned(),
            target_type: target_type.as_str().to_owned(),
            target_id: target_id.to_owned(),
            updated_at: now,
        })
    }

    pub async fn clear_dm_target(&self, owner_user_id: &str, plugin_id: &str, chat_id: &str) -> Result<bool, DbError> {
        let res = sqlx::query(
            "DELETE FROM custom_channel_dm_targets WHERE owner_user_id = ? AND plugin_id = ? AND chat_id = ?",
        )
        .bind(owner_user_id)
        .bind(plugin_id)
        .bind(chat_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn get_dm_target(
        &self,
        owner_user_id: &str,
        plugin_id: &str,
        chat_id: &str,
    ) -> Result<Option<CustomChannelDmTargetRow>, DbError> {
        sqlx::query_as::<_, CustomChannelDmTargetRow>(
            r#"
            SELECT owner_user_id, plugin_id, user_id, chat_id, target_type, target_id, updated_at
            FROM custom_channel_dm_targets
            WHERE owner_user_id = ? AND plugin_id = ? AND chat_id = ?
            "#,
        )
        .bind(owner_user_id)
        .bind(plugin_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_dm_targets_for_owner(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<CustomChannelDmTargetRow>, DbError> {
        sqlx::query_as::<_, CustomChannelDmTargetRow>(
            r#"
            SELECT owner_user_id, plugin_id, user_id, chat_id, target_type, target_id, updated_at
            FROM custom_channel_dm_targets
            WHERE owner_user_id = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    // -----------------------------------------------------------------------
    // Outbound Message -> Target Reply Routing (`custom_channel_msg_routes`)
    // -----------------------------------------------------------------------

    pub async fn record_msg_route(
        &self,
        plugin_id: &str,
        chat_id: &str,
        telegram_msg_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
    ) -> Result<(), DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_channel_msg_routes (
                plugin_id, chat_id, telegram_msg_id, target_type, target_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(plugin_id, chat_id, telegram_msg_id) DO UPDATE SET
                target_type = excluded.target_type,
                target_id = excluded.target_id,
                created_at = excluded.created_at
            "#,
        )
        .bind(plugin_id)
        .bind(chat_id)
        .bind(telegram_msg_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        // Opportunistically prune message route entries older than 30 days.
        let cutoff = now.saturating_sub(30 * 24 * 60 * 60 * 1000);
        let _ = sqlx::query("DELETE FROM custom_channel_msg_routes WHERE created_at < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await;

        Ok(())
    }

    pub async fn resolve_msg_route(
        &self,
        plugin_id: &str,
        chat_id: &str,
        telegram_msg_id: &str,
    ) -> Result<Option<CustomChannelMsgRouteRow>, DbError> {
        sqlx::query_as::<_, CustomChannelMsgRouteRow>(
            r#"
            SELECT plugin_id, chat_id, telegram_msg_id, target_type, target_id, created_at
            FROM custom_channel_msg_routes
            WHERE plugin_id = ? AND chat_id = ? AND telegram_msg_id = ?
            "#,
        )
        .bind(plugin_id)
        .bind(chat_id)
        .bind(telegram_msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    // -----------------------------------------------------------------------
    // 12-byte Compact Callback Action Tokens (`t:<10-char>`)
    // -----------------------------------------------------------------------

    pub async fn create_action_token(
        &self,
        owner_user_id: &str,
        target_type: CustomTargetType,
        target_id: &str,
        action_kind: &str,
        payload_json: &serde_json::Value,
        ttl_ms: Option<i64>,
    ) -> Result<String, DbError> {
        let now = aionui_common::now_ms();
        let expires_at = now.saturating_add(ttl_ms.unwrap_or(DEFAULT_ACTION_TOKEN_TTL_MS));
        let token = aionui_common::generate_id_with_length(Some(10));
        let payload_str = serde_json::to_string(payload_json).unwrap_or_else(|_| "{}".to_owned());

        sqlx::query(
            r#"
            INSERT INTO custom_channel_action_tokens (
                token, owner_user_id, target_type, target_id, action_kind, payload_json, expires_at, consumed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            "#,
        )
        .bind(&token)
        .bind(owner_user_id)
        .bind(target_type.as_str())
        .bind(target_id)
        .bind(action_kind)
        .bind(payload_str)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        let _ = sqlx::query("DELETE FROM custom_channel_action_tokens WHERE expires_at < ?")
            .bind(now)
            .execute(&self.pool)
            .await;

        Ok(token)
    }

    /// Reads an action token if it exists and has not expired.
    pub async fn get_valid_action_token(&self, token: &str) -> Result<Option<CustomChannelActionTokenRow>, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query_as::<_, CustomChannelActionTokenRow>(
            r#"
            SELECT token, owner_user_id, target_type, target_id, action_kind, payload_json, expires_at, consumed_at
            FROM custom_channel_action_tokens
            WHERE token = ? AND expires_at > ?
            "#,
        )
        .bind(token)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    /// Atomically marks a one-shot action token as consumed.
    /// Returns `Ok(true)` if this call consumed it, or `Ok(false)` if it was already consumed/expired.
    pub async fn consume_action_token_once(&self, token: &str) -> Result<bool, DbError> {
        let now = aionui_common::now_ms();
        let res = sqlx::query(
            r#"
            UPDATE custom_channel_action_tokens
            SET consumed_at = ?
            WHERE token = ? AND consumed_at IS NULL AND expires_at > ?
            "#,
        )
        .bind(now)
        .bind(token)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(res.rows_affected() == 1)
    }

    // -----------------------------------------------------------------------
    // Shared Rooms (Chặng 2 — Leader Hub)
    // -----------------------------------------------------------------------

    pub async fn create_shared_room(&self, params: CreateSharedRoomParams) -> Result<CustomSharedRoomRow, DbError> {
        let now = aionui_common::now_ms();
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        let host_team_id = params
            .host_team_id
            .or_else(|| params.team_ids.first().cloned());

        sqlx::query(
            r#"
            INSERT INTO custom_shared_rooms (
                id, owner_user_id, name, description, rules, host_team_id,
                max_hops, hop_count_since_human, artifacts_dir, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            "#,
        )
        .bind(&params.id)
        .bind(&params.owner_user_id)
        .bind(&params.name)
        .bind(&params.description)
        .bind(&params.rules)
        .bind(&host_team_id)
        .bind(params.max_hops.max(1))
        .bind(&params.artifacts_dir)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        for team_id in &params.team_ids {
            sqlx::query(
                r#"
                INSERT OR IGNORE INTO custom_shared_room_members (
                    room_id, team_id, joined_at, pending_handoff_msg_id, pending_handoff_hop_index
                )
                VALUES (?, ?, ?, NULL, 0)
                "#,
            )
            .bind(&params.id)
            .bind(team_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        }

        tx.commit().await.map_err(DbError::Query)?;

        self.get_shared_room(&params.owner_user_id, &params.id)
            .await?
            .ok_or_else(|| DbError::NotFound("shared room missing after insert".into()))
    }

    pub async fn get_shared_room(
        &self,
        owner_user_id: &str,
        room_id: &str,
    ) -> Result<Option<CustomSharedRoomRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomRow>(
            r#"
            SELECT id, owner_user_id, name, description, rules, host_team_id,
                   max_hops, hop_count_since_human, artifacts_dir, created_at, updated_at
            FROM custom_shared_rooms
            WHERE owner_user_id = ? AND id = ?
            "#,
        )
        .bind(owner_user_id)
        .bind(room_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn get_shared_room_by_id(&self, room_id: &str) -> Result<Option<CustomSharedRoomRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomRow>(
            r#"
            SELECT id, owner_user_id, name, description, rules, host_team_id,
                   max_hops, hop_count_since_human, artifacts_dir, created_at, updated_at
            FROM custom_shared_rooms
            WHERE id = ?
            "#,
        )
        .bind(room_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_shared_rooms(&self, owner_user_id: &str) -> Result<Vec<CustomSharedRoomRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomRow>(
            r#"
            SELECT id, owner_user_id, name, description, rules, host_team_id,
                   max_hops, hop_count_since_human, artifacts_dir, created_at, updated_at
            FROM custom_shared_rooms
            WHERE owner_user_id = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_shared_rooms_for_team(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<Vec<CustomSharedRoomRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomRow>(
            r#"
            SELECT r.id, r.owner_user_id, r.name, r.description, r.rules, r.host_team_id,
                   r.max_hops, r.hop_count_since_human, r.artifacts_dir, r.created_at, r.updated_at
            FROM custom_shared_rooms r
            INNER JOIN custom_shared_room_members m ON m.room_id = r.id
            WHERE r.owner_user_id = ? AND m.team_id = ?
            ORDER BY r.updated_at DESC
            "#,
        )
        .bind(owner_user_id)
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn update_shared_room(
        &self,
        owner_user_id: &str,
        room_id: &str,
        params: UpdateSharedRoomParams,
    ) -> Result<CustomSharedRoomRow, DbError> {
        let existing = self
            .get_shared_room(owner_user_id, room_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("shared room not found: {room_id}")))?;

        let now = aionui_common::now_ms();
        let name = params.name.unwrap_or(existing.name);
        let description = params.description.unwrap_or(existing.description);
        let rules = params.rules.unwrap_or(existing.rules);
        let mut host_team_id = params.host_team_id.unwrap_or(existing.host_team_id);
        if let Some(ref team_ids) = params.team_ids
            && let Some(ref hid) = host_team_id
            && !team_ids.iter().any(|id| id == hid)
        {
            host_team_id = team_ids.first().cloned();
        }
        let max_hops = params.max_hops.unwrap_or(existing.max_hops).max(1);

        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        sqlx::query(
            r#"
            UPDATE custom_shared_rooms
            SET name = ?, description = ?, rules = ?, host_team_id = ?, max_hops = ?, updated_at = ?
            WHERE owner_user_id = ? AND id = ?
            "#,
        )
        .bind(&name)
        .bind(&description)
        .bind(&rules)
        .bind(&host_team_id)
        .bind(max_hops)
        .bind(now)
        .bind(owner_user_id)
        .bind(room_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        if let Some(team_ids) = params.team_ids {
            let existing_members: Vec<String> = sqlx::query_scalar(
                "SELECT team_id FROM custom_shared_room_members WHERE room_id = ?",
            )
            .bind(room_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(DbError::Query)?;

            for old_tid in existing_members {
                if !team_ids.iter().any(|tid| tid == &old_tid) {
                    sqlx::query("DELETE FROM custom_shared_room_members WHERE room_id = ? AND team_id = ?")
                        .bind(room_id)
                        .bind(&old_tid)
                        .execute(&mut *tx)
                        .await
                        .map_err(DbError::Query)?;
                }
            }

            for team_id in &team_ids {
                sqlx::query(
                    r#"
                    INSERT OR IGNORE INTO custom_shared_room_members (
                        room_id, team_id, joined_at, pending_handoff_msg_id, pending_handoff_hop_index
                    )
                    VALUES (?, ?, ?, NULL, 0)
                    "#,
                )
                .bind(room_id)
                .bind(team_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
            }
        }

        tx.commit().await.map_err(DbError::Query)?;

        self.get_shared_room(owner_user_id, room_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("shared room not found: {room_id}")))
    }

    pub async fn delete_shared_room(&self, owner_user_id: &str, room_id: &str) -> Result<bool, DbError> {
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;
        let res = sqlx::query("DELETE FROM custom_shared_rooms WHERE owner_user_id = ? AND id = ?")
            .bind(owner_user_id)
            .bind(room_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;

        if res.rows_affected() == 0 {
            tx.rollback().await.map_err(DbError::Query)?;
            return Ok(false);
        }

        sqlx::query("DELETE FROM custom_shared_room_members WHERE room_id = ?")
            .bind(room_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        sqlx::query("DELETE FROM custom_shared_room_messages WHERE room_id = ?")
            .bind(room_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        sqlx::query("DELETE FROM custom_shared_room_tasks WHERE room_id = ?")
            .bind(room_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        sqlx::query(
            "DELETE FROM custom_channel_bindings WHERE owner_user_id = ? AND target_type = 'room' AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(room_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;
        sqlx::query(
            "DELETE FROM custom_channel_dm_targets WHERE owner_user_id = ? AND target_type = 'room' AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(room_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;
        sqlx::query(
            "DELETE FROM custom_target_notify_prefs WHERE owner_user_id = ? AND target_type = 'room' AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(room_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        tx.commit().await.map_err(DbError::Query)?;
        Ok(true)
    }

    /// Cleans up all bridge bindings, DM pointers, notify prefs, and Shared Room memberships
    /// referencing a deleted `team_id`, returning the IDs of any Shared Rooms that were updated.
    pub async fn cleanup_deleted_team(&self, owner_user_id: &str, team_id: &str) -> Result<Vec<String>, DbError> {
        let now = aionui_common::now_ms();
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        sqlx::query(
            "DELETE FROM custom_target_notify_prefs WHERE owner_user_id = ? AND target_type = 'team' AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(team_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        sqlx::query(
            "DELETE FROM custom_channel_bindings WHERE owner_user_id = ? AND target_type = 'team' AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(team_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        sqlx::query(
            "DELETE FROM custom_channel_dm_targets WHERE owner_user_id = ? AND target_type = 'team' AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(team_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        sqlx::query("DELETE FROM custom_team_context_anchors WHERE team_id = ?")
            .bind(team_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;

        sqlx::query("DELETE FROM custom_team_context_prefs WHERE team_id = ?")
            .bind(team_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;

        sqlx::query("DELETE FROM custom_team_slot_swaps WHERE team_id = ?")
            .bind(team_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;

        let mut affected_room_ids: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT r.id
            FROM custom_shared_rooms r
            LEFT JOIN custom_shared_room_members m ON m.room_id = r.id
            WHERE r.owner_user_id = ? AND (m.team_id = ? OR r.host_team_id = ?)
            "#,
        )
        .bind(owner_user_id)
        .bind(team_id)
        .bind(team_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(DbError::Query)?;
        affected_room_ids.dedup();

        sqlx::query(
            r#"
            DELETE FROM custom_shared_room_members
            WHERE team_id = ? AND room_id IN (SELECT id FROM custom_shared_rooms WHERE owner_user_id = ?)
            "#,
        )
        .bind(team_id)
        .bind(owner_user_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        for rid in &affected_room_ids {
            let current_host: Option<Option<String>> = sqlx::query_scalar(
                "SELECT host_team_id FROM custom_shared_rooms WHERE owner_user_id = ? AND id = ?",
            )
            .bind(owner_user_id)
            .bind(rid)
            .fetch_optional(&mut *tx)
            .await
            .map_err(DbError::Query)?;

            if current_host.flatten().as_deref() == Some(team_id) {
                let replacement_host: Option<String> = sqlx::query_scalar(
                    "SELECT team_id FROM custom_shared_room_members WHERE room_id = ? ORDER BY joined_at ASC, rowid ASC LIMIT 1",
                )
                .bind(rid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(DbError::Query)?;

                sqlx::query(
                    "UPDATE custom_shared_rooms SET host_team_id = ?, updated_at = ? WHERE owner_user_id = ? AND id = ?",
                )
                .bind(replacement_host)
                .bind(now)
                .bind(owner_user_id)
                .bind(rid)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
            } else {
                sqlx::query("UPDATE custom_shared_rooms SET updated_at = ? WHERE owner_user_id = ? AND id = ?")
                    .bind(now)
                    .bind(owner_user_id)
                    .bind(rid)
                    .execute(&mut *tx)
                    .await
                    .map_err(DbError::Query)?;
            }
        }

        tx.commit().await.map_err(DbError::Query)?;
        Ok(affected_room_ids)
    }

    pub async fn list_shared_room_members(&self, room_id: &str) -> Result<Vec<CustomSharedRoomMemberRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomMemberRow>(
            r#"
            SELECT room_id, team_id, joined_at, pending_handoff_msg_id, pending_handoff_hop_index
            FROM custom_shared_room_members
            WHERE room_id = ?
            ORDER BY joined_at ASC
            "#,
        )
        .bind(room_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_pending_handoffs_for_team(
        &self,
        team_id: &str,
    ) -> Result<Vec<(CustomSharedRoomRow, CustomSharedRoomMemberRow)>, DbError> {
        let members = sqlx::query_as::<_, CustomSharedRoomMemberRow>(
            r#"
            SELECT room_id, team_id, joined_at, pending_handoff_msg_id, pending_handoff_hop_index
            FROM custom_shared_room_members
            WHERE team_id = ? AND pending_handoff_msg_id IS NOT NULL
            ORDER BY joined_at ASC
            "#,
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)?;

        let mut out = Vec::with_capacity(members.len());
        for member in members {
            if let Some(room) = self.get_shared_room_by_id(&member.room_id).await? {
                out.push((room, member));
            }
        }
        Ok(out)
    }

    pub async fn set_member_pending_handoff(
        &self,
        room_id: &str,
        team_id: &str,
        msg_id: Option<&str>,
        hop_index: i64,
    ) -> Result<(), DbError> {
        sqlx::query(
            r#"
            UPDATE custom_shared_room_members
            SET pending_handoff_msg_id = ?, pending_handoff_hop_index = ?
            WHERE room_id = ? AND team_id = ?
            "#,
        )
        .bind(msg_id)
        .bind(hop_index)
        .bind(room_id)
        .bind(team_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;
        Ok(())
    }

    pub async fn reset_room_hops(&self, room_id: &str) -> Result<(), DbError> {
        let now = aionui_common::now_ms();
        sqlx::query("UPDATE custom_shared_rooms SET hop_count_since_human = 0, updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(room_id)
            .execute(&self.pool)
            .await
            .map_err(DbError::Query)?;
        Ok(())
    }

    pub async fn increment_room_hops(&self, room_id: &str) -> Result<i64, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            "UPDATE custom_shared_rooms SET hop_count_since_human = hop_count_since_human + 1, updated_at = ? WHERE id = ?",
        )
        .bind(now)
        .bind(room_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        let hops: i64 = sqlx::query_scalar("SELECT hop_count_since_human FROM custom_shared_rooms WHERE id = ?")
            .bind(room_id)
            .fetch_one(&self.pool)
            .await
            .map_err(DbError::Query)?;
        Ok(hops)
    }

    pub async fn extend_room_hops_allowance(&self, room_id: &str, grant_hops: i64) -> Result<CustomSharedRoomRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            UPDATE custom_shared_rooms
            SET hop_count_since_human = MAX(0, hop_count_since_human - ?),
                updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(grant_hops.max(1))
        .bind(now)
        .bind(room_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_shared_room_by_id(room_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("shared room not found: {room_id}")))
    }

    // -----------------------------------------------------------------------
    // Shared Room Messages
    // -----------------------------------------------------------------------

    pub async fn insert_shared_room_message(
        &self,
        params: CreateSharedRoomMessageParams,
    ) -> Result<CustomSharedRoomMessageRow, DbError> {
        let now = aionui_common::now_ms();
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        sqlx::query(
            r#"
            INSERT INTO custom_shared_room_messages (
                id, room_id, sender_type, sender_team_id, sender_label,
                reply_to_id, content, attachments_json, target_team_ids_json, hop_index, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&params.id)
        .bind(&params.room_id)
        .bind(&params.sender_type)
        .bind(&params.sender_team_id)
        .bind(&params.sender_label)
        .bind(&params.reply_to_id)
        .bind(&params.content)
        .bind(&params.attachments_json)
        .bind(&params.target_team_ids_json)
        .bind(params.hop_index)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        sqlx::query("UPDATE custom_shared_rooms SET updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(&params.room_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;

        tx.commit().await.map_err(DbError::Query)?;

        Ok(CustomSharedRoomMessageRow {
            id: params.id,
            room_id: params.room_id,
            sender_type: params.sender_type,
            sender_team_id: params.sender_team_id,
            sender_label: params.sender_label,
            reply_to_id: params.reply_to_id,
            content: params.content,
            attachments_json: params.attachments_json,
            target_team_ids_json: params.target_team_ids_json,
            hop_index: params.hop_index,
            created_at: now,
        })
    }

    pub async fn get_shared_room_message(
        &self,
        room_id: &str,
        message_id: &str,
    ) -> Result<Option<CustomSharedRoomMessageRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomMessageRow>(
            r#"
            SELECT id, room_id, sender_type, sender_team_id, sender_label,
                   reply_to_id, content, attachments_json, target_team_ids_json, hop_index, created_at
            FROM custom_shared_room_messages
            WHERE room_id = ? AND id = ?
            "#,
        )
        .bind(room_id)
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_shared_room_messages(
        &self,
        room_id: &str,
        limit: i64,
    ) -> Result<Vec<CustomSharedRoomMessageRow>, DbError> {
        let clamped = limit.clamp(1, 500);
        let mut rows = sqlx::query_as::<_, CustomSharedRoomMessageRow>(
            r#"
            SELECT id, room_id, sender_type, sender_team_id, sender_label,
                   reply_to_id, content, attachments_json, target_team_ids_json, hop_index, created_at
            FROM custom_shared_room_messages
            WHERE room_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            "#,
        )
        .bind(room_id)
        .bind(clamped)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)?;

        rows.reverse();
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Cross-Team Tasks (`custom_shared_room_tasks`)
    // -----------------------------------------------------------------------

    pub async fn create_shared_room_task(
        &self,
        params: CreateSharedRoomTaskParams,
    ) -> Result<CustomSharedRoomTaskRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_shared_room_tasks (
                id, room_id, from_team_id, to_team_id, title, description,
                status, blocked_by_json, artifact_paths_json, result_summary, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, NULL, ?, ?)
            "#,
        )
        .bind(&params.id)
        .bind(&params.room_id)
        .bind(&params.from_team_id)
        .bind(&params.to_team_id)
        .bind(&params.title)
        .bind(&params.description)
        .bind(&params.blocked_by_json)
        .bind(&params.artifact_paths_json)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(CustomSharedRoomTaskRow {
            id: params.id,
            room_id: params.room_id,
            from_team_id: params.from_team_id,
            to_team_id: params.to_team_id,
            title: params.title,
            description: params.description,
            status: "pending".to_owned(),
            blocked_by_json: params.blocked_by_json,
            artifact_paths_json: params.artifact_paths_json,
            result_summary: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn get_shared_room_task(&self, task_id: &str) -> Result<Option<CustomSharedRoomTaskRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomTaskRow>(
            r#"
            SELECT id, room_id, from_team_id, to_team_id, title, description,
                   status, blocked_by_json, artifact_paths_json, result_summary, created_at, updated_at
            FROM custom_shared_room_tasks
            WHERE id = ?
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_shared_room_tasks(&self, room_id: &str) -> Result<Vec<CustomSharedRoomTaskRow>, DbError> {
        sqlx::query_as::<_, CustomSharedRoomTaskRow>(
            r#"
            SELECT id, room_id, from_team_id, to_team_id, title, description,
                   status, blocked_by_json, artifact_paths_json, result_summary, created_at, updated_at
            FROM custom_shared_room_tasks
            WHERE room_id = ?
            ORDER BY created_at ASC
            "#,
        )
        .bind(room_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn update_shared_room_task(
        &self,
        task_id: &str,
        params: UpdateSharedRoomTaskParams,
    ) -> Result<CustomSharedRoomTaskRow, DbError> {
        let existing = self
            .get_shared_room_task(task_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("shared room task not found: {task_id}")))?;

        let now = aionui_common::now_ms();
        let status = params.status.unwrap_or(existing.status);
        let description = params.description.unwrap_or(existing.description);
        let blocked_by_json = params.blocked_by_json.unwrap_or(existing.blocked_by_json);
        let artifact_paths_json = params.artifact_paths_json.unwrap_or(existing.artifact_paths_json);
        let result_summary = params.result_summary.or(existing.result_summary);

        sqlx::query(
            r#"
            UPDATE custom_shared_room_tasks
            SET status = ?, description = ?, blocked_by_json = ?, artifact_paths_json = ?, result_summary = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&status)
        .bind(&description)
        .bind(&blocked_by_json)
        .bind(&artifact_paths_json)
        .bind(&result_summary)
        .bind(now)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_shared_room_task(task_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("shared room task not found: {task_id}")))
    }

    // -----------------------------------------------------------------------
    // Git Worktree Lease Tracking (F07 & F23)
    // -----------------------------------------------------------------------

    pub async fn create_worktree_lease(
        &self,
        params: CreateGitWorktreeParams,
    ) -> Result<CustomGitWorktreeRow, DbError> {
        let now = aionui_common::now_ms();
        let id = params.id.unwrap_or_else(|| params.lease_id.clone());
        sqlx::query(
            r#"
            INSERT INTO custom_git_worktrees (
                id, lease_id, owner_user_id, room_id, team_id, repo_path, worktree_path,
                branch, base_branch, status, leftovers_ref, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', NULL, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(&params.lease_id)
        .bind(&params.owner_user_id)
        .bind(&params.room_id)
        .bind(&params.team_id)
        .bind(&params.repo_path)
        .bind(&params.worktree_path)
        .bind(&params.branch)
        .bind(&params.base_branch)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(CustomGitWorktreeRow {
            id,
            lease_id: params.lease_id,
            owner_user_id: params.owner_user_id,
            room_id: params.room_id,
            team_id: params.team_id,
            repo_path: params.repo_path,
            worktree_path: params.worktree_path,
            branch: params.branch,
            base_branch: params.base_branch,
            status: "active".to_string(),
            leftovers_ref: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn get_worktree_lease(
        &self,
        id_or_lease_id: &str,
    ) -> Result<Option<CustomGitWorktreeRow>, DbError> {
        sqlx::query_as::<_, CustomGitWorktreeRow>(
            r#"
            SELECT id, lease_id, owner_user_id, room_id, team_id, repo_path, worktree_path,
                   branch, base_branch, status, leftovers_ref, created_at, updated_at
            FROM custom_git_worktrees
            WHERE id = ? OR lease_id = ?
            LIMIT 1
            "#,
        )
        .bind(id_or_lease_id)
        .bind(id_or_lease_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn get_active_worktree_for_team(
        &self,
        team_id: &str,
    ) -> Result<Option<CustomGitWorktreeRow>, DbError> {
        sqlx::query_as::<_, CustomGitWorktreeRow>(
            r#"
            SELECT id, lease_id, owner_user_id, room_id, team_id, repo_path, worktree_path,
                   branch, base_branch, status, leftovers_ref, created_at, updated_at
            FROM custom_git_worktrees
            WHERE team_id = ? AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(team_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn get_active_worktree_for_room(
        &self,
        room_id: &str,
    ) -> Result<Option<CustomGitWorktreeRow>, DbError> {
        sqlx::query_as::<_, CustomGitWorktreeRow>(
            r#"
            SELECT id, lease_id, owner_user_id, room_id, team_id, repo_path, worktree_path,
                   branch, base_branch, status, leftovers_ref, created_at, updated_at
            FROM custom_git_worktrees
            WHERE room_id = ? AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(room_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_worktrees(
        &self,
        owner_user_id: &str,
        status: Option<&str>,
    ) -> Result<Vec<CustomGitWorktreeRow>, DbError> {
        match status {
            Some(s) => sqlx::query_as::<_, CustomGitWorktreeRow>(
                r#"
                SELECT id, lease_id, owner_user_id, room_id, team_id, repo_path, worktree_path,
                       branch, base_branch, status, leftovers_ref, created_at, updated_at
                FROM custom_git_worktrees
                WHERE owner_user_id = ? AND status = ?
                ORDER BY updated_at DESC
                "#,
            )
            .bind(owner_user_id)
            .bind(s)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::Query),
            None => sqlx::query_as::<_, CustomGitWorktreeRow>(
                r#"
                SELECT id, lease_id, owner_user_id, room_id, team_id, repo_path, worktree_path,
                       branch, base_branch, status, leftovers_ref, created_at, updated_at
                FROM custom_git_worktrees
                WHERE owner_user_id = ?
                ORDER BY updated_at DESC
                "#,
            )
            .bind(owner_user_id)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::Query),
        }
    }

    pub async fn update_worktree_status(
        &self,
        id_or_lease_id: &str,
        status: &str,
        leftovers_ref: Option<&str>,
    ) -> Result<bool, DbError> {
        let now = aionui_common::now_ms();
        let res = sqlx::query(
            r#"
            UPDATE custom_git_worktrees
            SET status = ?,
                leftovers_ref = COALESCE(?, leftovers_ref),
                updated_at = ?
            WHERE id = ? OR lease_id = ?
            "#,
        )
        .bind(status)
        .bind(leftovers_ref)
        .bind(now)
        .bind(id_or_lease_id)
        .bind(id_or_lease_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(res.rows_affected() > 0)
    }

    pub async fn delete_worktree_lease(&self, id_or_lease_id: &str) -> Result<bool, DbError> {
        let res = sqlx::query("DELETE FROM custom_git_worktrees WHERE id = ? OR lease_id = ?")
            .bind(id_or_lease_id)
            .bind(id_or_lease_id)
            .execute(&self.pool)
            .await
            .map_err(DbError::Query)?;

        Ok(res.rows_affected() > 0)
    }

    // --- Action Inbox Methods ---

    pub async fn create_action_inbox_item(
        &self,
        p: CreateActionInboxParams,
    ) -> Result<CustomActionInboxRow, DbError> {
        let now = aionui_common::now_ms();
        let id = p
            .id
            .unwrap_or_else(|| aionui_common::generate_prefixed_id("inbox"));
        let target_type = p.target_type.unwrap_or_else(|| "room".to_string());

        sqlx::query(
            r#"
            INSERT INTO custom_action_inbox (
                id, owner_user_id, category, title, summary, details_json,
                status, target_type, target_id, action_token, telegram_chat_id,
                telegram_msg_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(&p.owner_user_id)
        .bind(&p.category)
        .bind(&p.title)
        .bind(&p.summary)
        .bind(&p.details_json)
        .bind(&target_type)
        .bind(&p.target_id)
        .bind(&p.action_token)
        .bind(&p.telegram_chat_id)
        .bind(&p.telegram_msg_id)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_action_inbox_item(&id)
            .await?
            .ok_or_else(|| DbError::NotFound(id))
    }

    pub async fn get_action_inbox_item(
        &self,
        id: &str,
    ) -> Result<Option<CustomActionInboxRow>, DbError> {
        sqlx::query_as::<_, CustomActionInboxRow>(
            "SELECT * FROM custom_action_inbox WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_action_inbox_items(
        &self,
        owner_user_id: &str,
        status: Option<&str>,
    ) -> Result<Vec<CustomActionInboxRow>, DbError> {
        match status {
            Some(s) => sqlx::query_as::<_, CustomActionInboxRow>(
                "SELECT * FROM custom_action_inbox WHERE owner_user_id = ? AND status = ? ORDER BY created_at DESC",
            )
            .bind(owner_user_id)
            .bind(s)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::Query),
            None => sqlx::query_as::<_, CustomActionInboxRow>(
                "SELECT * FROM custom_action_inbox WHERE owner_user_id = ? ORDER BY created_at DESC",
            )
            .bind(owner_user_id)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::Query),
        }
    }

    pub async fn count_pending_inbox_items(
        &self,
        owner_user_id: &str,
    ) -> Result<i64, DbError> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM custom_action_inbox WHERE owner_user_id = ? AND status = 'pending'",
        )
        .bind(owner_user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(DbError::Query)?;
        Ok(count)
    }

    /// Atomically transitions an inbox item from 'pending' to 'resolved'.
    /// Returns Ok(true) if transition succeeded, or Ok(false) if it was already resolved.
    pub async fn resolve_action_inbox_item(
        &self,
        id: &str,
        resolution_result: &str,
        resolved_by: &str,
    ) -> Result<bool, DbError> {
        let now = aionui_common::now_ms();
        let res = sqlx::query(
            r#"
            UPDATE custom_action_inbox
            SET status = 'resolved',
                resolution_result = ?,
                resolved_by = ?,
                resolved_at = ?,
                updated_at = ?
            WHERE id = ? AND status = 'pending'
            "#,
        )
        .bind(resolution_result)
        .bind(resolved_by)
        .bind(now)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(res.rows_affected() > 0)
    }

    /// Atomically resolves an inbox item by action token (used by Telegram callback query).
    pub async fn resolve_action_inbox_item_by_token(
        &self,
        token: &str,
        resolution_result: &str,
        resolved_by: &str,
    ) -> Result<Option<CustomActionInboxRow>, DbError> {
        let now = aionui_common::now_ms();
        let res = sqlx::query(
            r#"
            UPDATE custom_action_inbox
            SET status = 'resolved',
                resolution_result = ?,
                resolved_by = ?,
                resolved_at = ?,
                updated_at = ?
            WHERE action_token = ? AND status = 'pending'
            "#,
        )
        .bind(resolution_result)
        .bind(resolved_by)
        .bind(now)
        .bind(now)
        .bind(token)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        if res.rows_affected() > 0 {
            sqlx::query_as::<_, CustomActionInboxRow>(
                "SELECT * FROM custom_action_inbox WHERE action_token = ?",
            )
            .bind(token)
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::Query)
        } else {
            Ok(None)
        }
    }

    // --- Dual Cost Wallet & Ledger Methods ---

    pub async fn get_or_create_cost_wallet(
        &self,
        owner_user_id: &str,
    ) -> Result<CustomCostWalletRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_cost_wallets (
                owner_user_id, api_key_budget_limit_usd, api_key_spent_usd,
                cli_subscription_runs_count, api_key_runs_count,
                soft_warning_triggered, hard_block_triggered, updated_at
            ) VALUES (?, 50.0, 0.0, 0, 0, 0, 0, ?)
            ON CONFLICT(owner_user_id) DO NOTHING
            "#,
        )
        .bind(owner_user_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        sqlx::query_as::<_, CustomCostWalletRow>(
            "SELECT * FROM custom_cost_wallets WHERE owner_user_id = ?",
        )
        .bind(owner_user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn record_cost_run(
        &self,
        p: RecordCostRunParams,
    ) -> Result<(CustomCostWalletRow, bool, bool), DbError> {
        let now = aionui_common::now_ms();
        let ledger_id = aionui_common::generate_prefixed_id("cld");

        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        sqlx::query(
            r#"
            INSERT INTO custom_cost_ledger (
                id, owner_user_id, run_id, team_id, agent_id, wallet_type,
                tokens_input, tokens_output, cost_usd, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&ledger_id)
        .bind(&p.owner_user_id)
        .bind(&p.run_id)
        .bind(&p.team_id)
        .bind(&p.agent_id)
        .bind(&p.wallet_type)
        .bind(p.tokens_input)
        .bind(p.tokens_output)
        .bind(p.cost_usd)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        let current = sqlx::query_as::<_, CustomCostWalletRow>(
            "SELECT * FROM custom_cost_wallets WHERE owner_user_id = ?",
        )
        .bind(&p.owner_user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DbError::Query)?
        .unwrap_or(CustomCostWalletRow {
            owner_user_id: p.owner_user_id.clone(),
            api_key_budget_limit_usd: 50.0,
            api_key_spent_usd: 0.0,
            cli_subscription_runs_count: 0,
            api_key_runs_count: 0,
            soft_warning_triggered: 0,
            hard_block_triggered: 0,
            updated_at: now,
        });

        let is_cli = p.wallet_type == "cli_subscription";
        let new_spent = if is_cli {
            current.api_key_spent_usd
        } else {
            current.api_key_spent_usd + p.cost_usd
        };
        let new_cli_runs = if is_cli {
            current.cli_subscription_runs_count + 1
        } else {
            current.cli_subscription_runs_count
        };
        let new_api_runs = if is_cli {
            current.api_key_runs_count
        } else {
            current.api_key_runs_count + 1
        };

        let newly_soft_warned = !is_cli
            && current.soft_warning_triggered == 0
            && new_spent >= (0.8 * current.api_key_budget_limit_usd);

        let newly_hard_blocked = !is_cli
            && current.hard_block_triggered == 0
            && new_spent >= (1.0 * current.api_key_budget_limit_usd);

        let soft_flag = if newly_soft_warned || current.soft_warning_triggered == 1 {
            1
        } else {
            0
        };
        let hard_flag = if newly_hard_blocked || current.hard_block_triggered == 1 {
            1
        } else {
            0
        };

        sqlx::query(
            r#"
            INSERT INTO custom_cost_wallets (
                owner_user_id, api_key_budget_limit_usd, api_key_spent_usd,
                cli_subscription_runs_count, api_key_runs_count,
                soft_warning_triggered, hard_block_triggered, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_user_id) DO UPDATE SET
                api_key_spent_usd = excluded.api_key_spent_usd,
                cli_subscription_runs_count = excluded.cli_subscription_runs_count,
                api_key_runs_count = excluded.api_key_runs_count,
                soft_warning_triggered = excluded.soft_warning_triggered,
                hard_block_triggered = excluded.hard_block_triggered,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&p.owner_user_id)
        .bind(current.api_key_budget_limit_usd)
        .bind(new_spent)
        .bind(new_cli_runs)
        .bind(new_api_runs)
        .bind(soft_flag)
        .bind(hard_flag)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        if newly_soft_warned {
            let warn_id = format!("inbox_cst_80_{}", &ledger_id);
            let title = format!(
                "Soft Budget Warning: 80% API Key Cap Reached (${:.2} / ${:.2})",
                new_spent, current.api_key_budget_limit_usd
            );
            let summary = "CLI subscription runs (Claude/Codex) remain free ($0.00). Only pay-per-token API key usage has reached 80% of limit.".to_string();
            let details = serde_json::json!({
                "budget_limit_usd": current.api_key_budget_limit_usd,
                "spent_usd": new_spent,
                "percentage": 80.0,
                "wallet_type": "api_key",
                "threshold": 80
            })
            .to_string();

            sqlx::query(
                r#"
                INSERT INTO custom_action_inbox (
                    id, owner_user_id, category, title, summary, details_json,
                    status, target_type, target_id, created_at, updated_at
                ) VALUES (?, ?, 'budget_warning', ?, ?, ?, 'pending', 'wallet', 'dual_cost_wallet', ?, ?)
                "#,
            )
            .bind(warn_id)
            .bind(&p.owner_user_id)
            .bind(title)
            .bind(summary)
            .bind(details)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        }

        if newly_hard_blocked {
            let block_id = format!("inbox_cst_100_{}", &ledger_id);
            let title = format!(
                "100% Hard Budget Fuse: Paused API Key Agents (${:.2} / ${:.2})",
                new_spent, current.api_key_budget_limit_usd
            );
            let summary = "API Key budget reached 100%. Paid API Key agents are paused. CLI subscription runs ($0.00) continue executing normally.".to_string();
            let details = serde_json::json!({
                "budget_limit_usd": current.api_key_budget_limit_usd,
                "spent_usd": new_spent,
                "percentage": 100.0,
                "wallet_type": "api_key",
                "threshold": 100
            })
            .to_string();

            sqlx::query(
                r#"
                INSERT INTO custom_action_inbox (
                    id, owner_user_id, category, title, summary, details_json,
                    status, target_type, target_id, created_at, updated_at
                ) VALUES (?, ?, 'budget_warning', ?, ?, ?, 'pending', 'wallet', 'dual_cost_wallet', ?, ?)
                "#,
            )
            .bind(block_id)
            .bind(&p.owner_user_id)
            .bind(title)
            .bind(summary)
            .bind(details)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        }

        tx.commit().await.map_err(DbError::Query)?;

        let updated_row = self.get_or_create_cost_wallet(&p.owner_user_id).await?;
        Ok((updated_row, newly_soft_warned, newly_hard_blocked))
    }

    pub async fn update_budget_limit(
        &self,
        owner_user_id: &str,
        limit: f64,
    ) -> Result<CustomCostWalletRow, DbError> {
        let now = aionui_common::now_ms();
        let current = self.get_or_create_cost_wallet(owner_user_id).await?;
        let limit = limit.max(0.0);

        let hard_flag = if current.api_key_spent_usd >= limit { 1 } else { 0 };
        let soft_flag = if current.api_key_spent_usd >= 0.8 * limit { 1 } else { 0 };

        sqlx::query(
            r#"
            UPDATE custom_cost_wallets
            SET api_key_budget_limit_usd = ?,
                soft_warning_triggered = ?,
                hard_block_triggered = ?,
                updated_at = ?
            WHERE owner_user_id = ?
            "#,
        )
        .bind(limit)
        .bind(soft_flag)
        .bind(hard_flag)
        .bind(now)
        .bind(owner_user_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_or_create_cost_wallet(owner_user_id).await
    }

    pub async fn reset_cost_wallet(
        &self,
        owner_user_id: &str,
    ) -> Result<CustomCostWalletRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            UPDATE custom_cost_wallets
            SET api_key_spent_usd = 0.0,
                cli_subscription_runs_count = 0,
                api_key_runs_count = 0,
                soft_warning_triggered = 0,
                hard_block_triggered = 0,
                updated_at = ?
            WHERE owner_user_id = ?
            "#,
        )
        .bind(now)
        .bind(owner_user_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_or_create_cost_wallet(owner_user_id).await
    }

    pub async fn is_wallet_hard_blocked(&self, owner_user_id: &str) -> Result<bool, DbError> {
        let wallet = self.get_or_create_cost_wallet(owner_user_id).await?;
        Ok(wallet.hard_block_triggered == 1)
    }

    // -----------------------------------------------------------------------
    // Team Context Compaction Anchors, Prefs & Cross-Engine Hot-Swaps
    // -----------------------------------------------------------------------

    pub async fn get_team_context_anchor(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<Option<CustomTeamContextAnchorRow>, DbError> {
        sqlx::query_as::<_, CustomTeamContextAnchorRow>(
            r#"
            SELECT team_id, slot_id, conversation_id, anchored_summary_md, files_touched_json,
                   last_compacted_msg_id, compaction_count, pending_handover, updated_at
            FROM custom_team_context_anchors
            WHERE team_id = ? AND slot_id = ?
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_team_context_anchors(
        &self,
        team_id: &str,
    ) -> Result<Vec<CustomTeamContextAnchorRow>, DbError> {
        sqlx::query_as::<_, CustomTeamContextAnchorRow>(
            r#"
            SELECT team_id, slot_id, conversation_id, anchored_summary_md, files_touched_json,
                   last_compacted_msg_id, compaction_count, pending_handover, updated_at
            FROM custom_team_context_anchors
            WHERE team_id = ?
            "#,
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn upsert_team_context_anchor(
        &self,
        params: UpsertTeamContextAnchorParams,
    ) -> Result<CustomTeamContextAnchorRow, DbError> {
        let now = aionui_common::now_ms();
        let pending_flag = if params.pending_handover { 1_i64 } else { 0_i64 };
        sqlx::query(
            r#"
            INSERT INTO custom_team_context_anchors (
                team_id, slot_id, conversation_id, anchored_summary_md, files_touched_json,
                last_compacted_msg_id, compaction_count, pending_handover, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, slot_id) DO UPDATE SET
                conversation_id = excluded.conversation_id,
                anchored_summary_md = excluded.anchored_summary_md,
                files_touched_json = excluded.files_touched_json,
                last_compacted_msg_id = excluded.last_compacted_msg_id,
                compaction_count = excluded.compaction_count,
                pending_handover = excluded.pending_handover,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&params.team_id)
        .bind(&params.slot_id)
        .bind(&params.conversation_id)
        .bind(&params.anchored_summary_md)
        .bind(&params.files_touched_json)
        .bind(&params.last_compacted_msg_id)
        .bind(params.compaction_count)
        .bind(pending_flag)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_team_context_anchor(&params.team_id, &params.slot_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("context anchor {}:{}", params.team_id, params.slot_id)))
    }

    pub async fn consume_pending_slot_handover(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<Option<CustomTeamContextAnchorRow>, DbError> {
        let now = aionui_common::now_ms();
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;
        let res = sqlx::query(
            r#"
            UPDATE custom_team_context_anchors
            SET pending_handover = 0, updated_at = ?
            WHERE team_id = ? AND slot_id = ? AND pending_handover = 1
            "#,
        )
        .bind(now)
        .bind(team_id)
        .bind(slot_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        if res.rows_affected() == 0 {
            tx.rollback().await.map_err(DbError::Query)?;
            return Ok(None);
        }

        let row = sqlx::query_as::<_, CustomTeamContextAnchorRow>(
            r#"
            SELECT team_id, slot_id, conversation_id, anchored_summary_md, files_touched_json,
                   last_compacted_msg_id, compaction_count, pending_handover, updated_at
            FROM custom_team_context_anchors
            WHERE team_id = ? AND slot_id = ?
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        tx.commit().await.map_err(DbError::Query)?;
        Ok(row)
    }

    /// Re-arms `pending_handover = 1` in SQLite when a wake that consumed the handover banner
    /// fails to start or is aborted before the turn executes, ensuring durability across restarts.
    pub async fn rearm_pending_slot_handover(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<bool, DbError> {
        let now = aionui_common::now_ms();
        let res = sqlx::query(
            r#"
            UPDATE custom_team_context_anchors
            SET pending_handover = 1, updated_at = ?
            WHERE team_id = ? AND slot_id = ?
              AND TRIM(anchored_summary_md) != ''
            "#,
        )
        .bind(now)
        .bind(team_id)
        .bind(slot_id)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;
        Ok(res.rows_affected() > 0)
    }

    /// Returns `Some(true)` if an `acp_session` row exists for `conversation_id` with a cleared
    /// (`NULL` or blank) `session_id`, `Some(false)` if it holds a live resume anchor, or `None`
    /// if no `acp_session` row exists for the conversation.
    pub async fn slot_has_empty_acp_session(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<bool>, DbError> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            r#"
            SELECT a.session_id
            FROM acp_session a
            WHERE a.conversation_id = ?
              AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = a.conversation_id AND c.user_id = ?)
            "#,
        )
        .bind(conversation_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(|(sid,)| sid.as_deref().map(str::trim).filter(|s| !s.is_empty()).is_none()))
    }

    pub async fn get_or_default_team_context_pref(
        &self,
        team_id: &str,
    ) -> Result<CustomTeamContextPrefRow, DbError> {
        let existing = sqlx::query_as::<_, CustomTeamContextPrefRow>(
            r#"
            SELECT team_id, auto_compact_enabled, threshold_pct,
                   economy_mode, max_context_tokens, tokens_saved_estimate,
                   coalesced_wakes_saved, max_turn_hops_per_run, auto_fallback_free_cli,
                   updated_at
            FROM custom_team_context_prefs
            WHERE team_id = ?
            "#,
        )
        .bind(team_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)?;

        if let Some(row) = existing {
            return Ok(row);
        }

        Ok(CustomTeamContextPrefRow {
            team_id: team_id.to_owned(),
            auto_compact_enabled: 1,
            threshold_pct: 80,
            economy_mode: default_economy_mode(),
            max_context_tokens: default_max_context_tokens(),
            tokens_saved_estimate: 0,
            coalesced_wakes_saved: 0,
            max_turn_hops_per_run: default_max_turn_hops_per_run(),
            auto_fallback_free_cli: 0,
            updated_at: aionui_common::now_ms(),
        })
    }

    pub async fn upsert_team_context_pref(
        &self,
        team_id: &str,
        auto_compact_enabled: bool,
        threshold_pct: i64,
    ) -> Result<CustomTeamContextPrefRow, DbError> {
        self.upsert_team_context_pref_ext(UpsertTeamContextPrefParams {
            team_id: team_id.to_owned(),
            auto_compact_enabled: Some(auto_compact_enabled),
            threshold_pct: Some(threshold_pct),
            ..Default::default()
        })
        .await
    }

    pub async fn upsert_team_context_pref_ext(
        &self,
        params: UpsertTeamContextPrefParams,
    ) -> Result<CustomTeamContextPrefRow, DbError> {
        let now = aionui_common::now_ms();
        let current = self
            .get_or_default_team_context_pref(&params.team_id)
            .await?;

        let enabled_flag = params
            .auto_compact_enabled
            .map(|b| if b { 1_i64 } else { 0_i64 })
            .unwrap_or(current.auto_compact_enabled);
        let clamped_threshold = params
            .threshold_pct
            .unwrap_or(current.threshold_pct)
            .clamp(30, 99);
        let next_mode = params
            .economy_mode
            .as_deref()
            .map(str::trim)
            .filter(|s| matches!(*s, "lean" | "balanced" | "verbose"))
            .unwrap_or_else(|| current.effective_economy_mode())
            .to_owned();
        let next_max_tokens = params
            .max_context_tokens
            .unwrap_or(current.effective_max_context_tokens())
            .clamp(8_000, 2_000_000);
        let next_max_hops = params
            .max_turn_hops_per_run
            .unwrap_or(current.max_turn_hops_per_run)
            .clamp(4, 200);
        let next_auto_fallback = params
            .auto_fallback_free_cli
            .map(|b| if b { 1_i64 } else { 0_i64 })
            .unwrap_or(current.auto_fallback_free_cli);

        sqlx::query(
            r#"
            INSERT INTO custom_team_context_prefs (
                team_id, auto_compact_enabled, threshold_pct,
                economy_mode, max_context_tokens, tokens_saved_estimate,
                coalesced_wakes_saved, max_turn_hops_per_run, auto_fallback_free_cli,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                auto_compact_enabled = excluded.auto_compact_enabled,
                threshold_pct = excluded.threshold_pct,
                economy_mode = excluded.economy_mode,
                max_context_tokens = excluded.max_context_tokens,
                max_turn_hops_per_run = excluded.max_turn_hops_per_run,
                auto_fallback_free_cli = excluded.auto_fallback_free_cli,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&params.team_id)
        .bind(enabled_flag)
        .bind(clamped_threshold)
        .bind(next_mode)
        .bind(next_max_tokens)
        .bind(current.tokens_saved_estimate)
        .bind(current.coalesced_wakes_saved)
        .bind(next_max_hops)
        .bind(next_auto_fallback)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_or_default_team_context_pref(&params.team_id).await
    }

    pub async fn increment_team_economy_savings(
        &self,
        team_id: &str,
        saved_tokens_delta: i64,
        coalesced_wakes_delta: i64,
    ) -> Result<(), DbError> {
        let tok_delta = saved_tokens_delta.max(0);
        let wake_delta = coalesced_wakes_delta.max(0);
        if tok_delta == 0 && wake_delta == 0 {
            return Ok(());
        }
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_team_context_prefs (
                team_id, auto_compact_enabled, threshold_pct,
                economy_mode, max_context_tokens, tokens_saved_estimate,
                coalesced_wakes_saved, max_turn_hops_per_run, auto_fallback_free_cli,
                updated_at
            ) VALUES (?, 1, 80, 'lean', 160000, ?, ?, 24, 0, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                tokens_saved_estimate = custom_team_context_prefs.tokens_saved_estimate + excluded.tokens_saved_estimate,
                coalesced_wakes_saved = custom_team_context_prefs.coalesced_wakes_saved + excluded.coalesced_wakes_saved,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(team_id)
        .bind(tok_delta)
        .bind(wake_delta)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;
        Ok(())
    }

    pub async fn get_pending_slot_swap(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<Option<CustomTeamSlotSwapRow>, DbError> {
        sqlx::query_as::<_, CustomTeamSlotSwapRow>(
            r#"
            SELECT team_id, slot_id, target_backend, target_provider_id, target_model_id, created_at
            FROM custom_team_slot_swaps
            WHERE team_id = ? AND slot_id = ?
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_pending_slot_swaps(
        &self,
        team_id: &str,
    ) -> Result<Vec<CustomTeamSlotSwapRow>, DbError> {
        sqlx::query_as::<_, CustomTeamSlotSwapRow>(
            r#"
            SELECT team_id, slot_id, target_backend, target_provider_id, target_model_id, created_at
            FROM custom_team_slot_swaps
            WHERE team_id = ?
            "#,
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn upsert_pending_slot_swap(
        &self,
        team_id: &str,
        slot_id: &str,
        target_backend: &str,
        target_provider_id: Option<&str>,
        target_model_id: &str,
    ) -> Result<CustomTeamSlotSwapRow, DbError> {
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_team_slot_swaps (
                team_id, slot_id, target_backend, target_provider_id, target_model_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, slot_id) DO UPDATE SET
                target_backend = excluded.target_backend,
                target_provider_id = excluded.target_provider_id,
                target_model_id = excluded.target_model_id,
                created_at = excluded.created_at
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .bind(target_backend)
        .bind(target_provider_id)
        .bind(target_model_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        self.get_pending_slot_swap(team_id, slot_id)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("pending slot swap {team_id}:{slot_id}")))
    }

    pub async fn take_pending_slot_swap(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<Option<CustomTeamSlotSwapRow>, DbError> {
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;
        let row = sqlx::query_as::<_, CustomTeamSlotSwapRow>(
            r#"
            SELECT team_id, slot_id, target_backend, target_provider_id, target_model_id, created_at
            FROM custom_team_slot_swaps
            WHERE team_id = ? AND slot_id = ?
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        if row.is_some() {
            sqlx::query("DELETE FROM custom_team_slot_swaps WHERE team_id = ? AND slot_id = ?")
                .bind(team_id)
                .bind(slot_id)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
        }

        tx.commit().await.map_err(DbError::Query)?;
        Ok(row)
    }

    pub async fn clear_pending_slot_swap(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<bool, DbError> {
        let res = sqlx::query("DELETE FROM custom_team_slot_swaps WHERE team_id = ? AND slot_id = ?")
            .bind(team_id)
            .bind(slot_id)
            .execute(&self.pool)
            .await
            .map_err(DbError::Query)?;
        Ok(res.rows_affected() > 0)
    }

    /// Atomically rebinds a conversation's engine (`conversations.type`, `model`, `extra`, `updated_at`),
    /// updates/creates `acp_session` (`session_id = NULL`, clears `runtime.context_usage`),
    /// and synchronizes `conversation_assistant_snapshots.agent_id` in a single SQLite transaction.
    pub async fn rebind_conversation_engine(
        &self,
        user_id: &str,
        conversation_id: &str,
        new_type: &str,
        new_model_json: Option<&str>,
        new_extra_json: &str,
    ) -> Result<bool, DbError> {
        let now = aionui_common::now_ms();
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        let upd = sqlx::query(
            r#"
            UPDATE conversations
            SET type = ?, model = ?, extra = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
            "#,
        )
        .bind(new_type)
        .bind(new_model_json)
        .bind(new_extra_json)
        .bind(now)
        .bind(conversation_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        if upd.rows_affected() == 0 {
            tx.rollback().await.map_err(DbError::Query)?;
            return Ok(false);
        }

        let extra_val: serde_json::Value =
            serde_json::from_str(new_extra_json).unwrap_or_else(|_| serde_json::json!({}));
        let extra_agent_id = extra_val
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim();
        let extra_agent_source = extra_val
            .get("agent_source")
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("builtin");
        let extra_model_id = extra_val
            .get("current_model_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let existing_cfg: Option<String> = sqlx::query_scalar(
            "SELECT session_config FROM acp_session WHERE conversation_id = ?",
        )
        .bind(conversation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        let mut cfg_json: serde_json::Value = existing_cfg
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(obj) = cfg_json.as_object_mut() {
            let rt = obj
                .entry("runtime")
                .or_insert_with(|| serde_json::json!({}));
            if let Some(rt_obj) = rt.as_object_mut() {
                rt_obj.remove("context_usage");
                if let Some(mid) = extra_model_id {
                    rt_obj.insert("current_model_id".into(), serde_json::Value::String(mid.to_owned()));
                } else {
                    rt_obj.remove("current_model_id");
                }
            }
        }
        let cleaned_cfg_str = serde_json::to_string(&cfg_json).unwrap_or_else(|_| "{}".into());

        if existing_cfg.is_some() {
            if !extra_agent_id.is_empty() {
                sqlx::query(
                    r#"
                    UPDATE acp_session
                    SET agent_source = ?, agent_id = ?, session_id = NULL, session_config = ?, last_active_at = ?
                    WHERE conversation_id = ?
                    "#,
                )
                .bind(extra_agent_source)
                .bind(extra_agent_id)
                .bind(&cleaned_cfg_str)
                .bind(now)
                .bind(conversation_id)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
            } else {
                sqlx::query(
                    r#"
                    UPDATE acp_session
                    SET session_id = NULL, session_config = ?, last_active_at = ?
                    WHERE conversation_id = ?
                    "#,
                )
                .bind(&cleaned_cfg_str)
                .bind(now)
                .bind(conversation_id)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
            }
        } else if (new_type == "acp" || new_type == "antigravity") && !extra_agent_id.is_empty() {
            sqlx::query(
                r#"
                INSERT INTO acp_session (
                    conversation_id, agent_source, agent_id, session_id, session_status, session_config, last_active_at
                ) VALUES (?, ?, ?, NULL, 'idle', ?, ?)
                "#,
            )
            .bind(conversation_id)
            .bind(extra_agent_source)
            .bind(extra_agent_id)
            .bind(&cleaned_cfg_str)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        }

        if !extra_agent_id.is_empty() {
            sqlx::query(
                r#"
                UPDATE conversation_assistant_snapshots
                SET agent_id = ?, resolved_model_id = COALESCE(?, resolved_model_id)
                WHERE conversation_id = ?
                "#,
            )
            .bind(extra_agent_id)
            .bind(extra_model_id)
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        }

        tx.commit().await.map_err(DbError::Query)?;
        Ok(true)
    }

    /// Clears `acp_session.session_id = NULL` and removes `runtime.context_usage` from `session_config`
    /// on rollover so the next turn opens `SessionSpec::Fresh` with a reset token gauge.
    pub async fn clear_acp_context_usage_on_rollover(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<bool, DbError> {
        let now = aionui_common::now_ms();
        let mut tx = self.pool.begin().await.map_err(DbError::Query)?;

        let existing_cfg: Option<String> = sqlx::query_scalar(
            r#"
            SELECT a.session_config
            FROM acp_session a
            WHERE a.conversation_id = ?
              AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = a.conversation_id AND c.user_id = ?)
            "#,
        )
        .bind(conversation_id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        let Some(raw_cfg) = existing_cfg else {
            tx.rollback().await.map_err(DbError::Query)?;
            return Ok(false);
        };

        let mut cfg_json: serde_json::Value =
            serde_json::from_str(&raw_cfg).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(rt_obj) = cfg_json
            .as_object_mut()
            .and_then(|o| o.get_mut("runtime"))
            .and_then(serde_json::Value::as_object_mut)
        {
            rt_obj.remove("context_usage");
        }
        let cleaned_cfg = serde_json::to_string(&cfg_json).unwrap_or_else(|_| "{}".into());

        let upd = sqlx::query(
            r#"
            UPDATE acp_session
            SET session_id = NULL, session_config = ?, last_active_at = ?
            WHERE conversation_id = ?
            "#,
        )
        .bind(cleaned_cfg)
        .bind(now)
        .bind(conversation_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        tx.commit().await.map_err(DbError::Query)?;
        Ok(upd.rows_affected() > 0)
    }

    pub async fn list_conversation_messages_for_anchor(
        &self,
        conversation_id: &str,
    ) -> Result<Vec<AnchorMessageSourceRow>, DbError> {
        sqlx::query_as::<_, AnchorMessageSourceRow>(
            r#"
            SELECT id, msg_id, type, content, position, hidden
            FROM messages
            WHERE conversation_id = ?
            ORDER BY created_at ASC, rowid ASC
            "#,
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_recent_slot_mailbox_for_anchor(
        &self,
        team_id: &str,
        slot_id: &str,
        limit: i64,
    ) -> Result<Vec<AnchorMailboxSourceRow>, DbError> {
        sqlx::query_as::<_, AnchorMailboxSourceRow>(
            r#"
            SELECT from_agent_id, to_agent_id, content, summary
            FROM mailbox
            WHERE team_id = ? AND (to_agent_id = ? OR from_agent_id = ?)
            ORDER BY created_at DESC
            LIMIT ?
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .bind(slot_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_active_team_tasks_for_anchor(
        &self,
        team_id: &str,
        limit: i64,
    ) -> Result<Vec<AnchorTeamTaskSourceRow>, DbError> {
        sqlx::query_as::<_, AnchorTeamTaskSourceRow>(
            r#"
            SELECT id, subject, status, owner, blocked_by
            FROM team_tasks
            WHERE team_id = ? AND status != 'deleted'
            ORDER BY updated_at DESC
            LIMIT ?
            "#,
        )
        .bind(team_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn count_conversation_messages(&self, conversation_id: &str) -> Result<i64, DbError> {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages WHERE conversation_id = ?")
            .bind(conversation_id)
            .fetch_one(&self.pool)
            .await
            .map_err(DbError::Query)
    }

    /// Fast SQL aggregate returning `(message_count, total_content_byte_len)` without loading
    /// historical `MessageRow` strings into Rust heap memory on every turn settle.
    pub async fn estimate_conversation_tokens_fast(
        &self,
        conversation_id: &str,
    ) -> Result<(i64, i64), DbError> {
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(content)), 0) FROM messages WHERE conversation_id = ?",
        )
        .bind(conversation_id)
        .fetch_one(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn get_team_slot_economy(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<Option<CustomTeamSlotEconomyRow>, DbError> {
        sqlx::query_as::<_, CustomTeamSlotEconomyRow>(
            r#"
            SELECT team_id, slot_id, owner_user_id, role, wallet_type, backend, model_id,
                   wakeup_turns, input_tokens_total, output_tokens_total, cached_read_tokens_total,
                   delivered_wake_chars, unbilled_wake_chars, estimated_cost_usd,
                   compaction_count, compaction_saved_tokens, coalesced_wakes_saved,
                   last_seen_conv_id, last_seen_msg_count, last_seen_char_sum,
                   last_seen_usage_tokens, last_seen_meta_seq, updated_at
            FROM custom_team_slot_economy
            WHERE team_id = ? AND slot_id = ?
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    pub async fn list_team_slot_economy(
        &self,
        team_id: &str,
    ) -> Result<Vec<CustomTeamSlotEconomyRow>, DbError> {
        sqlx::query_as::<_, CustomTeamSlotEconomyRow>(
            r#"
            SELECT team_id, slot_id, owner_user_id, role, wallet_type, backend, model_id,
                   wakeup_turns, input_tokens_total, output_tokens_total, cached_read_tokens_total,
                   delivered_wake_chars, unbilled_wake_chars, estimated_cost_usd,
                   compaction_count, compaction_saved_tokens, coalesced_wakes_saved,
                   last_seen_conv_id, last_seen_msg_count, last_seen_char_sum,
                   last_seen_usage_tokens, last_seen_meta_seq, updated_at
            FROM custom_team_slot_economy
            WHERE team_id = ?
            ORDER BY role ASC, slot_id ASC
            "#,
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(DbError::Query)
    }

    /// Records unpersisted wake/banner chars (`persist_user_message: false`) delivered to a slot
    /// so both context-window gauges and per-turn token metering include the real prompt payload.
    pub async fn record_slot_wake_delivery(
        &self,
        team_id: &str,
        slot_id: &str,
        owner_user_id: &str,
        role: &str,
        wake_chars: i64,
    ) -> Result<(), DbError> {
        let chars = wake_chars.max(0);
        if chars == 0 {
            return Ok(());
        }
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_team_slot_economy (
                team_id, slot_id, owner_user_id, role, delivered_wake_chars, unbilled_wake_chars, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, slot_id) DO UPDATE SET
                owner_user_id = excluded.owner_user_id,
                role = excluded.role,
                delivered_wake_chars = custom_team_slot_economy.delivered_wake_chars + excluded.delivered_wake_chars,
                unbilled_wake_chars = custom_team_slot_economy.unbilled_wake_chars + excluded.unbilled_wake_chars,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .bind(owner_user_id)
        .bind(role)
        .bind(chars)
        .bind(chars)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;
        Ok(())
    }

    /// Computes real per-turn input/output token deltas for a settled slot turn, persists cumulative
    /// slot economy telemetry, and returns the exact delta for Dual Cost Wallet ledger accounting.
    pub async fn settle_slot_economy_turn(
        &self,
        params: SettleSlotEconomyParams,
    ) -> Result<SlotTurnDeltaResult, DbError> {
        let now = aionui_common::now_ms();
        let prev = self
            .get_team_slot_economy(&params.team_id, &params.slot_id)
            .await?;

        let same_conv = prev
            .as_ref()
            .is_some_and(|r| r.last_seen_conv_id == params.conversation_id);
        let prev_char_sum = if same_conv {
            prev.as_ref().map(|r| r.last_seen_char_sum).unwrap_or(0)
        } else {
            0
        };
        let prev_usage_tokens = if same_conv {
            prev.as_ref().map(|r| r.last_seen_usage_tokens).unwrap_or(0)
        } else {
            0
        };
        let prev_meta_seq = if same_conv {
            prev.as_ref().map(|r| r.last_seen_meta_seq).unwrap_or(0)
        } else {
            0
        };
        let unbilled_wake_chars = prev.as_ref().map(|r| r.unbilled_wake_chars).unwrap_or(0);
        let delivered_wake_chars = prev.as_ref().map(|r| r.delivered_wake_chars).unwrap_or(0);

        let char_delta = (params.current_char_sum - prev_char_sum).max(0);
        let est_out_from_chars = (char_delta / 4).max(32);
        let est_wake_in_tokens = (unbilled_wake_chars / 4).max(0);
        // Active conversation context re-read on this turn + newly delivered wake prompt + MCP tool schema overhead (~450 tok)
        let est_context_in_tokens = ((prev_char_sum + delivered_wake_chars) / 4)
            .saturating_add(est_wake_in_tokens)
            .saturating_add(450)
            .max(128);

        let rep_cached = params.reported_cached_read_tokens.unwrap_or(0).max(0);
        let (delta_in, delta_out, delta_cached, next_usage_tokens, next_meta_seq) = match (
            params.reported_input_tokens.filter(|&v| v >= 0),
            params.reported_output_tokens.filter(|&v| v >= 0),
            params.reported_total_tokens.filter(|&v| v > 0),
        ) {
            (Some(rep_in), Some(rep_out), total_opt) if rep_in > 0 || rep_out > 0 || rep_cached > 0 => {
                // Codex `last.inputTokens` includes `cachedInputTokens`, whereas Claude and Antigravity
                // report uncached `inputTokens` separately from `cachedReadTokens`. Normalize so
                // `input_tokens_total` always tracks uncached prompt tokens and `input + cached_read`
                // equals total prompt volume across all backends.
                let uncached_in = if params.backend.eq_ignore_ascii_case("codex") && rep_in >= rep_cached {
                    rep_in - rep_cached
                } else {
                    rep_in
                };
                let total = total_opt.unwrap_or(uncached_in + rep_out + rep_cached);
                let meta_is_stale = match params.reported_meta_seq.filter(|&s| s > 0) {
                    Some(seq) => same_conv && prev_meta_seq > 0 && seq == prev_meta_seq,
                    None => same_conv && prev_usage_tokens > 0 && total == prev_usage_tokens,
                };
                if meta_is_stale {
                    // Snapshot did not advance on this turn (e.g. mid-turn error before UsageDelta);
                    // do not re-add the previous turn's `_meta` counters.
                    (est_context_in_tokens, est_out_from_chars, 0, prev_usage_tokens, prev_meta_seq)
                } else {
                    let seq_out = params.reported_meta_seq.filter(|&s| s > 0).unwrap_or(prev_meta_seq);
                    (uncached_in, rep_out, rep_cached, total, seq_out)
                }
            }
            (_, _, Some(total)) if !(same_conv && prev_usage_tokens > 0 && total == prev_usage_tokens) => {
                let delta_total = if total > prev_usage_tokens && prev_usage_tokens > 0 {
                    (total - prev_usage_tokens).max(est_out_from_chars)
                } else {
                    total.max(est_out_from_chars)
                };
                let d_out = est_out_from_chars.min(delta_total);
                let d_in = (delta_total - d_out).max(128);
                (d_in, d_out.max(16), 0, total, prev_meta_seq)
            }
            _ => (est_context_in_tokens, est_out_from_chars, 0, prev_usage_tokens, prev_meta_seq),
        };

        let delta_cost_usd = estimate_api_token_cost_usd(
            params.wallet_type,
            &params.model_id,
            delta_in,
            delta_out,
        );

        sqlx::query(
            r#"
            INSERT INTO custom_team_slot_economy (
                team_id, slot_id, owner_user_id, role, wallet_type, backend, model_id,
                wakeup_turns, input_tokens_total, output_tokens_total, cached_read_tokens_total,
                delivered_wake_chars, unbilled_wake_chars, estimated_cost_usd,
                last_seen_conv_id, last_seen_msg_count, last_seen_char_sum,
                last_seen_usage_tokens, last_seen_meta_seq, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, slot_id) DO UPDATE SET
                owner_user_id = excluded.owner_user_id,
                role = excluded.role,
                wallet_type = excluded.wallet_type,
                backend = excluded.backend,
                model_id = excluded.model_id,
                wakeup_turns = custom_team_slot_economy.wakeup_turns + 1,
                input_tokens_total = custom_team_slot_economy.input_tokens_total + excluded.input_tokens_total,
                output_tokens_total = custom_team_slot_economy.output_tokens_total + excluded.output_tokens_total,
                cached_read_tokens_total = custom_team_slot_economy.cached_read_tokens_total + excluded.cached_read_tokens_total,
                unbilled_wake_chars = 0,
                estimated_cost_usd = custom_team_slot_economy.estimated_cost_usd + excluded.estimated_cost_usd,
                last_seen_conv_id = excluded.last_seen_conv_id,
                last_seen_msg_count = excluded.last_seen_msg_count,
                last_seen_char_sum = excluded.last_seen_char_sum,
                last_seen_usage_tokens = excluded.last_seen_usage_tokens,
                last_seen_meta_seq = excluded.last_seen_meta_seq,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&params.team_id)
        .bind(&params.slot_id)
        .bind(&params.owner_user_id)
        .bind(&params.role)
        .bind(params.wallet_type.as_str())
        .bind(&params.backend)
        .bind(&params.model_id)
        .bind(delta_in)
        .bind(delta_out)
        .bind(delta_cached)
        .bind(delivered_wake_chars)
        .bind(delta_cost_usd)
        .bind(&params.conversation_id)
        .bind(params.current_msg_count)
        .bind(params.current_char_sum)
        .bind(next_usage_tokens)
        .bind(next_meta_seq)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        let row = self
            .get_team_slot_economy(&params.team_id, &params.slot_id)
            .await?
            .ok_or_else(|| {
                DbError::NotFound(format!(
                    "slot economy {}:{}",
                    params.team_id, params.slot_id
                ))
            })?;

        Ok(SlotTurnDeltaResult {
            delta_input_tokens: delta_in,
            delta_output_tokens: delta_out,
            delta_cached_read_tokens: delta_cached,
            delta_cost_usd,
            row,
        })
    }

    pub async fn record_slot_compaction_savings(
        &self,
        team_id: &str,
        slot_id: &str,
        owner_user_id: &str,
        saved_tokens: i64,
    ) -> Result<(), DbError> {
        let saved = saved_tokens.max(0);
        let now = aionui_common::now_ms();
        sqlx::query(
            r#"
            INSERT INTO custom_team_slot_economy (
                team_id, slot_id, owner_user_id, compaction_count, compaction_saved_tokens,
                delivered_wake_chars, unbilled_wake_chars, last_seen_usage_tokens, last_seen_meta_seq, updated_at
            ) VALUES (?, ?, ?, 1, ?, 0, 0, 0, 0, ?)
            ON CONFLICT(team_id, slot_id) DO UPDATE SET
                owner_user_id = excluded.owner_user_id,
                compaction_count = custom_team_slot_economy.compaction_count + 1,
                compaction_saved_tokens = custom_team_slot_economy.compaction_saved_tokens + excluded.compaction_saved_tokens,
                delivered_wake_chars = 0,
                unbilled_wake_chars = 0,
                last_seen_usage_tokens = 0,
                last_seen_meta_seq = 0,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(team_id)
        .bind(slot_id)
        .bind(owner_user_id)
        .bind(saved)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(DbError::Query)?;

        if saved > 0 {
            let _ = self.increment_team_economy_savings(team_id, saved, 0).await;
        }
        Ok(())
    }

    pub async fn list_cost_ledger_entries(
        &self,
        owner_user_id: &str,
        team_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<CustomCostLedgerRow>, DbError> {
        let clamped = limit.clamp(1, 200);
        if let Some(tid) = team_id.map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query_as::<_, CustomCostLedgerRow>(
                r#"
                SELECT id, owner_user_id, run_id, team_id, agent_id, wallet_type,
                       tokens_input, tokens_output, cost_usd, created_at
                FROM custom_cost_ledger
                WHERE owner_user_id = ? AND team_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                "#,
            )
            .bind(owner_user_id)
            .bind(tid)
            .bind(clamped)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::Query)
        } else {
            sqlx::query_as::<_, CustomCostLedgerRow>(
                r#"
                SELECT id, owner_user_id, run_id, team_id, agent_id, wallet_type,
                       tokens_input, tokens_output, cost_usd, created_at
                FROM custom_cost_ledger
                WHERE owner_user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                "#,
            )
            .bind(owner_user_id)
            .bind(clamped)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::Query)
        }
    }

    pub async fn get_latest_conversation_message_id(
        &self,
        conversation_id: &str,
    ) -> Result<Option<String>, DbError> {
        sqlx::query_scalar(
            "SELECT id FROM messages WHERE conversation_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
        )
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::Query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::init_database_memory;

    #[tokio::test]
    async fn custom_bridge_tables_are_idempotent_and_leave_sqlx_migrations_untouched() {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();

        let before_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();

        ensure_custom_bridge_tables(&pool).await.unwrap();
        ensure_custom_bridge_tables(&pool).await.unwrap();

        let after_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(before_count, after_count);
    }

    #[tokio::test]
    async fn bindings_reply_routes_and_action_tokens_round_trip() {
        let db = init_database_memory().await.unwrap();
        let repo = SqliteCustomBridgeRepository::new(db.pool().clone());

        let binding = repo
            .upsert_channel_binding(UpsertChannelBindingParams {
                owner_user_id: "system_default_user".into(),
                plugin_id: "telegram_default".into(),
                chat_id: "-100123456".into(),
                thread_id: Some("42".into()),
                target_type: CustomTargetType::Team,
                target_id: "team-nhantown".into(),
                stream_mode: CustomStreamMode::Milestones,
            })
            .await
            .unwrap();
        assert_eq!(binding.thread_id.as_deref(), Some("42"));

        let resolved = repo
            .resolve_channel_binding_for_chat("system_default_user", "telegram_default", "-100123456", Some("42"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.target_id, "team-nhantown");

        repo.record_msg_route(
            "telegram_default",
            "998877",
            "501",
            CustomTargetType::Team,
            "team-vibeclip",
        )
        .await
        .unwrap();
        let route = repo
            .resolve_msg_route("telegram_default", "998877", "501")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(route.target_id, "team-vibeclip");

        let token = repo
            .create_action_token(
                "system_default_user",
                CustomTargetType::Team,
                "team-nhantown",
                "approve_continue",
                &serde_json::json!({"prompt": "Đã duyệt, triển khai đi"}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(token.len(), 10);
        assert!(format!("t:{token}").len() <= 64);

        assert!(repo.consume_action_token_once(&token).await.unwrap());
        assert!(
            !repo.consume_action_token_once(&token).await.unwrap(),
            "second consume must fail idempotently"
        );
    }

    #[tokio::test]
    async fn shared_room_crud_and_tasks_work() {
        let db = init_database_memory().await.unwrap();
        let repo = SqliteCustomBridgeRepository::new(db.pool().clone());

        let room = repo
            .create_shared_room(CreateSharedRoomParams {
                id: "room-1".into(),
                owner_user_id: "system_default_user".into(),
                name: "Phòng Liên Team Sản Phẩm".into(),
                description: "Kết nối Backend + Frontend".into(),
                rules: "".into(),
                host_team_id: None,
                max_hops: 5,
                artifacts_dir: "/tmp/room-1/artifacts".into(),
                team_ids: vec!["team-a".into(), "team-b".into()],
            })
            .await
            .unwrap();
        assert_eq!(room.host_team_id.as_deref(), Some("team-a"));

        let members = repo.list_shared_room_members("room-1").await.unwrap();
        assert_eq!(members.len(), 2);

        let task = repo
            .create_shared_room_task(CreateSharedRoomTaskParams {
                id: "rtask-1".into(),
                room_id: "room-1".into(),
                from_team_id: Some("team-a".into()),
                to_team_id: "team-b".into(),
                title: "Implement UI from OpenAPI spec".into(),
                description: "Read spec.yaml".into(),
                blocked_by_json: "[\"rtask-0\"]".into(),
                artifact_paths_json: "[]".into(),
            })
            .await
            .unwrap();
        assert_eq!(task.status, "pending");

        let updated = repo
            .update_shared_room_task(
                "rtask-1",
                UpdateSharedRoomTaskParams {
                    status: Some("completed".into()),
                    result_summary: Some("Done".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.status, "completed");
        assert_eq!(updated.result_summary.as_deref(), Some("Done"));
    }

    #[tokio::test]
    async fn custom_git_worktrees_crud_and_lifecycle_work() {
        let db = init_database_memory().await.unwrap();
        let repo = SqliteCustomBridgeRepository::new(db.pool().clone());

        let lease = repo
            .create_worktree_lease(CreateGitWorktreeParams {
                id: None,
                lease_id: "lease-wt-1".into(),
                owner_user_id: "system_default_user".into(),
                room_id: Some("room-1".into()),
                team_id: Some("team-a".into()),
                repo_path: "/workspace/repo".into(),
                worktree_path: "/workspace/repo/.aionui/worktrees/lease-wt-1".into(),
                branch: "wt/lease-wt-1".into(),
                base_branch: "feat/hybrid-command-hub".into(),
            })
            .await
            .unwrap();

        assert_eq!(lease.id, "lease-wt-1");
        assert_eq!(lease.lease_id, "lease-wt-1");
        assert_eq!(lease.status, "active");
        assert_eq!(lease.branch_name(), "wt/lease-wt-1");

        let fetched = repo.get_worktree_lease("lease-wt-1").await.unwrap().unwrap();
        assert_eq!(fetched.worktree_path, "/workspace/repo/.aionui/worktrees/lease-wt-1");

        let team_active = repo.get_active_worktree_for_team("team-a").await.unwrap().unwrap();
        assert_eq!(team_active.lease_id, "lease-wt-1");

        let room_active = repo.get_active_worktree_for_room("room-1").await.unwrap().unwrap();
        assert_eq!(room_active.lease_id, "lease-wt-1");

        let (idx_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_custom_git_worktrees_room'"
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(idx_count, 1, "idx_custom_git_worktrees_room must exist in SQLite catalog");

        let list = repo.list_worktrees("system_default_user", Some("active")).await.unwrap();
        assert_eq!(list.len(), 1);

        let updated = repo
            .update_worktree_status("lease-wt-1", "merged", Some("refs/aionui/leftovers/lease-wt-1_12345"))
            .await
            .unwrap();
        assert!(updated);

        let fetched_updated = repo.get_worktree_lease("lease-wt-1").await.unwrap().unwrap();
        assert_eq!(fetched_updated.status, "merged");
        assert_eq!(
            fetched_updated.leftovers_ref.as_deref(),
            Some("refs/aionui/leftovers/lease-wt-1_12345")
        );

        assert!(repo.get_active_worktree_for_team("team-a").await.unwrap().is_none());

        let deleted = repo.delete_worktree_lease("lease-wt-1").await.unwrap();
        assert!(deleted);
        assert!(repo.get_worktree_lease("lease-wt-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_action_inbox_lifecycle_and_atomic_resolution() {
        let db = init_database_memory().await.unwrap();
        let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
        let owner = "test_user_inbox";

        let pending_init = repo.count_pending_inbox_items(owner).await.unwrap();
        assert_eq!(pending_init, 0);

        let card = repo
            .create_action_inbox_item(CreateActionInboxParams {
                id: Some("inbox_test_1".into()),
                owner_user_id: owner.into(),
                category: "worktree_merge".into(),
                title: "Merge Worktree wt/feat-1".into(),
                summary: "Squash merge worktree wt/feat-1".into(),
                details_json: r#"{"lease_id":"lease-1"}"#.into(),
                target_type: Some("room".into()),
                target_id: "room-1".into(),
                action_token: Some("tok_merge_123".into()),
                telegram_chat_id: Some("-100123".into()),
                telegram_msg_id: Some("456".into()),
            })
            .await
            .unwrap();

        assert_eq!(card.id, "inbox_test_1");
        assert_eq!(card.status, "pending");
        assert!(card.is_pending());
        assert!(!card.is_resolved());

        let fetched = repo.get_action_inbox_item("inbox_test_1").await.unwrap().unwrap();
        assert_eq!(fetched.title, "Merge Worktree wt/feat-1");

        let pending_count = repo.count_pending_inbox_items(owner).await.unwrap();
        assert_eq!(pending_count, 1);

        let items = repo.list_action_inbox_items(owner, Some("pending")).await.unwrap();
        assert_eq!(items.len(), 1);

        // Atomic resolution: first attempt succeeds
        let resolved = repo
            .resolve_action_inbox_item("inbox_test_1", "approved", "desktop")
            .await
            .unwrap();
        assert!(resolved, "First resolution should succeed");

        // Atomic resolution idempotency: second attempt fails
        let second_resolve = repo
            .resolve_action_inbox_item("inbox_test_1", "approved", "telegram")
            .await
            .unwrap();
        assert!(!second_resolve, "Second resolution must fail due to status != pending");

        let updated_card = repo.get_action_inbox_item("inbox_test_1").await.unwrap().unwrap();
        assert_eq!(updated_card.status, "resolved");
        assert_eq!(updated_card.resolved_by.as_deref(), Some("desktop"));
        assert_eq!(updated_card.resolution_result.as_deref(), Some("approved"));
        assert!(updated_card.is_resolved());

        let pending_after = repo.count_pending_inbox_items(owner).await.unwrap();
        assert_eq!(pending_after, 0);

        // Test token resolution for Telegram
        let card2 = repo
            .create_action_inbox_item(CreateActionInboxParams {
                id: Some("inbox_test_2".into()),
                owner_user_id: owner.into(),
                category: "choice".into(),
                title: "Choose Architecture".into(),
                summary: "Option A vs B".into(),
                details_json: r#"{"options":["A","B"]}"#.into(),
                target_type: Some("room".into()),
                target_id: "room-1".into(),
                action_token: Some("tok_choice_xyz".into()),
                telegram_chat_id: None,
                telegram_msg_id: None,
            })
            .await
            .unwrap();
        assert_eq!(card2.status, "pending");

        let tok_res = repo
            .resolve_action_inbox_item_by_token("tok_choice_xyz", "option_a", "telegram")
            .await
            .unwrap();
        assert!(tok_res.is_some());
        assert_eq!(tok_res.unwrap().status, "resolved");

        // Second token resolution returns None
        let tok_res_again = repo
            .resolve_action_inbox_item_by_token("tok_choice_xyz", "option_a", "telegram")
            .await
            .unwrap();
        assert!(tok_res_again.is_none());
    }

    #[tokio::test]
    async fn test_dual_cost_wallet_ledger_and_budget_fuse() {
        // Classification tests
        assert_eq!(
            classify_agent_wallet(Some("claude"), None, false),
            WalletType::CliSubscription
        );
        assert_eq!(
            classify_agent_wallet(Some("claude"), Some(r#"{"type":"oauth"}"#), false),
            WalletType::CliSubscription
        );
        assert_eq!(
            classify_agent_wallet(Some("codex"), None, false),
            WalletType::CliSubscription
        );
        assert_eq!(
            classify_agent_wallet(Some("antigravity"), None, false),
            WalletType::CliSubscription
        );
        assert_eq!(
            classify_agent_wallet(Some("claude"), None, true),
            WalletType::ApiKey
        );
        assert_eq!(
            classify_agent_wallet(Some("openai"), None, false),
            WalletType::ApiKey
        );

        let db = init_database_memory().await.unwrap();
        let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
        let owner = "test_user_wallet";

        let wallet = repo.get_or_create_cost_wallet(owner).await.unwrap();
        assert_eq!(wallet.api_key_budget_limit_usd, 50.0);
        assert_eq!(wallet.api_key_spent_usd, 0.0);
        assert_eq!(wallet.cli_subscription_runs_count, 0);
        assert_eq!(wallet.api_key_runs_count, 0);
        assert_eq!(wallet.soft_warning_triggered, 0);
        assert_eq!(wallet.hard_block_triggered, 0);

        // 1. Record CLI Subscription run ($0.00 additional cost)
        let (w1, soft1, hard1) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: owner.into(),
                run_id: "run-cli-1".into(),
                team_id: "team-1".into(),
                agent_id: "claude-sub".into(),
                wallet_type: "cli_subscription".into(),
                tokens_input: 10_000,
                tokens_output: 2_000,
                cost_usd: 0.0,
            })
            .await
            .unwrap();

        assert_eq!(w1.cli_subscription_runs_count, 1);
        assert_eq!(w1.api_key_runs_count, 0);
        assert_eq!(w1.api_key_spent_usd, 0.0);
        assert!(!soft1);
        assert!(!hard1);

        // 2. Record API Key run ($30.00 out of $50.00 limit = 60%, below 80%)
        let (w2, soft2, hard2) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: owner.into(),
                run_id: "run-api-1".into(),
                team_id: "team-1".into(),
                agent_id: "gpt-paid".into(),
                wallet_type: "api_key".into(),
                tokens_input: 5_000,
                tokens_output: 1_000,
                cost_usd: 30.0,
            })
            .await
            .unwrap();

        assert_eq!(w2.cli_subscription_runs_count, 1);
        assert_eq!(w2.api_key_runs_count, 1);
        assert_eq!(w2.api_key_spent_usd, 30.0);
        assert!(!soft2);
        assert!(!hard2);

        // 3. Record second API Key run ($12.00, total $42.00 >= 80% of $50 = $40)
        let (w3, soft3, hard3) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: owner.into(),
                run_id: "run-api-2".into(),
                team_id: "team-1".into(),
                agent_id: "gpt-paid".into(),
                wallet_type: "api_key".into(),
                tokens_input: 2_000,
                tokens_output: 500,
                cost_usd: 12.0,
            })
            .await
            .unwrap();

        assert_eq!(w3.api_key_spent_usd, 42.0);
        assert!(soft3, "80% soft warning must trigger");
        assert_eq!(w3.soft_warning_triggered, 1);
        assert!(!hard3);
        assert!(!repo.is_wallet_hard_blocked(owner).await.unwrap());

        // Check Action Inbox received soft warning item
        let inbox_items = repo.list_action_inbox_items(owner, Some("pending")).await.unwrap();
        assert_eq!(inbox_items.len(), 1);
        assert_eq!(inbox_items[0].category, "budget_warning");
        assert!(inbox_items[0].title.contains("80%"));

        // 4. Record third API Key run ($10.00, total $52.00 >= 100% of $50 = $50)
        let (w4, soft4, hard4) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: owner.into(),
                run_id: "run-api-3".into(),
                team_id: "team-1".into(),
                agent_id: "gpt-paid".into(),
                wallet_type: "api_key".into(),
                tokens_input: 1_500,
                tokens_output: 400,
                cost_usd: 10.0,
            })
            .await
            .unwrap();

        assert_eq!(w4.api_key_spent_usd, 52.0);
        assert!(!soft4, "Already soft warned previously");
        assert!(hard4, "100% hard block must trigger");
        assert_eq!(w4.hard_block_triggered, 1);
        assert!(repo.is_wallet_hard_blocked(owner).await.unwrap());

        // Check Action Inbox received hard fuse item
        let inbox_items_all = repo.list_action_inbox_items(owner, Some("pending")).await.unwrap();
        assert_eq!(inbox_items_all.len(), 2);
        assert!(inbox_items_all.iter().any(|i| i.title.contains("100%")));

        // 5. Update budget limit to $100 -> unblocks hard fuse ($52 < $100 and $52 < $80)
        let w_updated = repo.update_budget_limit(owner, 100.0).await.unwrap();
        assert_eq!(w_updated.api_key_budget_limit_usd, 100.0);
        assert_eq!(w_updated.hard_block_triggered, 0);
        assert_eq!(w_updated.soft_warning_triggered, 0);
        assert!(!repo.is_wallet_hard_blocked(owner).await.unwrap());

        // 6. Reset cost wallet
        let w_reset = repo.reset_cost_wallet(owner).await.unwrap();
        assert_eq!(w_reset.api_key_spent_usd, 0.0);
        assert_eq!(w_reset.cli_subscription_runs_count, 0);
        assert_eq!(w_reset.api_key_runs_count, 0);
        assert_eq!(w_reset.soft_warning_triggered, 0);
        assert_eq!(w_reset.hard_block_triggered, 0);
    }

    #[tokio::test]
    async fn slot_wallet_facts_and_economy_schema_upgrade_work() {
        // Seeded ACP short model IDs (`opus`, `sonnet`, `haiku`, `default`, `gpt-6.1-sol`, `gpt-5.4`, `gpt-oss-120b-medium`)
        // MUST resolve to `$0` `WalletType::CliSubscription` when no custom API key or paid provider is bound!
        for short_cli_model in [
            "default",
            "opus",
            "sonnet",
            "haiku",
            "fable",
            "gpt-6.1-sol",
            "gpt-5.4",
            "gpt-5.3-codex",
            "gpt-5.4-mini",
            "gemini-2.5-pro",
            "gemini-4-argon-high",
            "gpt-oss-120b-medium",
        ] {
            let w = classify_slot_wallet_facts(&SlotWalletFacts {
                slot_backend: Some("acp"),
                model_id: Some(short_cli_model),
                ..Default::default()
            });
            assert_eq!(
                w,
                WalletType::CliSubscription,
                "seeded CLI model '{short_cli_model}' on ACP must classify as CliSubscription"
            );
            assert_eq!(estimate_api_token_cost_usd(w, short_cli_model, 50_000, 8_000), 0.0);
        }

        // Paid `aionrs` or custom `provider_id` or `has_custom_api_key` MUST classify as `WalletType::ApiKey`
        // even if the model name starts with `claude` or `gemini`!
        assert_eq!(
            classify_slot_wallet_facts(&SlotWalletFacts {
                slot_backend: Some("aionrs"),
                provider_id: Some("prov-openrouter"),
                model_id: Some("claude-sonnet-4-5"),
                ..Default::default()
            }),
            WalletType::ApiKey
        );
        assert_eq!(
            classify_slot_wallet_facts(&SlotWalletFacts {
                slot_backend: Some("claude"),
                has_custom_api_key: true,
                model_id: Some("opus"),
                ..Default::default()
            }),
            WalletType::ApiKey
        );

        let db = init_database_memory().await.unwrap();
        let repo = SqliteCustomBridgeRepository::new(db.pool().clone());

        let pref = repo.get_or_default_team_context_pref("team-econ-1").await.unwrap();
        assert_eq!(pref.effective_economy_mode(), "lean");
        assert_eq!(pref.effective_max_context_tokens(), 160_000);

        repo.record_slot_wake_delivery("team-econ-1", "slot-lead", "system_default_user", "lead", 4_800)
            .await
            .unwrap();
        let delta = repo
            .settle_slot_economy_turn(SettleSlotEconomyParams {
                team_id: "team-econ-1".into(),
                slot_id: "slot-lead".into(),
                owner_user_id: "system_default_user".into(),
                role: "lead".into(),
                conversation_id: "conv-econ-lead".into(),
                wallet_type: WalletType::CliSubscription,
                backend: "claude".into(),
                model_id: "opus".into(),
                current_msg_count: 2,
                current_char_sum: 2_400,
                reported_total_tokens: None,
                reported_input_tokens: None,
                reported_output_tokens: None,
                reported_cached_read_tokens: None,
                reported_meta_seq: None,
            })
            .await
            .unwrap();
        assert!(delta.delta_input_tokens > 1_000);
        assert_eq!(delta.delta_cost_usd, 0.0);
        assert_eq!(delta.row.wakeup_turns, 1);

        // Verify `_meta` real token + `cached_read_tokens` telemetry across warm turns,
        // including equal `total_tokens` with advancing `reported_meta_seq` and Codex cache normalization.
        let t1 = repo
            .settle_slot_economy_turn(SettleSlotEconomyParams {
                team_id: "team-econ-1".into(),
                slot_id: "slot-agy".into(),
                owner_user_id: "system_default_user".into(),
                role: "teammate".into(),
                conversation_id: "conv-econ-agy".into(),
                wallet_type: WalletType::CliSubscription,
                backend: "antigravity".into(),
                model_id: "gemini-3.8-flash-low".into(),
                current_msg_count: 2,
                current_char_sum: 1_800,
                reported_total_tokens: Some(14_236),
                reported_input_tokens: Some(13_811),
                reported_output_tokens: Some(425),
                reported_cached_read_tokens: Some(0),
                reported_meta_seq: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(t1.row.input_tokens_total, 13_811);
        assert_eq!(t1.row.output_tokens_total, 425);
        assert_eq!(t1.row.cached_read_tokens_total, 0);

        let t2 = repo
            .settle_slot_economy_turn(SettleSlotEconomyParams {
                team_id: "team-econ-1".into(),
                slot_id: "slot-agy".into(),
                owner_user_id: "system_default_user".into(),
                role: "teammate".into(),
                conversation_id: "conv-econ-agy".into(),
                wallet_type: WalletType::CliSubscription,
                backend: "antigravity".into(),
                model_id: "gemini-3.8-flash-low".into(),
                current_msg_count: 4,
                current_char_sum: 3_100,
                reported_total_tokens: Some(15_650),
                reported_input_tokens: Some(1_240),
                reported_output_tokens: Some(310),
                reported_cached_read_tokens: Some(14_100),
                reported_meta_seq: Some(11),
            })
            .await
            .unwrap();
        assert_eq!(t2.row.input_tokens_total, 13_811 + 1_240);
        assert_eq!(t2.row.output_tokens_total, 425 + 310);
        assert_eq!(t2.row.cached_read_tokens_total, 14_100);
        assert_eq!(t2.row.estimated_cost_usd, 0.0);

        // Consecutive turn with identical `reported_total_tokens` (15_650) MUST still count
        // when `reported_meta_seq` advances (11 -> 12), and MUST NOT re-add when seq is stale (12 == 12).
        let t3_equal_total = repo
            .settle_slot_economy_turn(SettleSlotEconomyParams {
                team_id: "team-econ-1".into(),
                slot_id: "slot-agy".into(),
                owner_user_id: "system_default_user".into(),
                role: "teammate".into(),
                conversation_id: "conv-econ-agy".into(),
                wallet_type: WalletType::CliSubscription,
                backend: "antigravity".into(),
                model_id: "gemini-3.8-flash-low".into(),
                current_msg_count: 6,
                current_char_sum: 3_900,
                reported_total_tokens: Some(15_650),
                reported_input_tokens: Some(1_050),
                reported_output_tokens: Some(500),
                reported_cached_read_tokens: Some(14_100),
                reported_meta_seq: Some(12),
            })
            .await
            .unwrap();
        assert_eq!(t3_equal_total.row.input_tokens_total, 13_811 + 1_240 + 1_050);
        assert_eq!(t3_equal_total.row.cached_read_tokens_total, 14_100 + 14_100);

        let t4_stale_seq = repo
            .settle_slot_economy_turn(SettleSlotEconomyParams {
                team_id: "team-econ-1".into(),
                slot_id: "slot-agy".into(),
                owner_user_id: "system_default_user".into(),
                role: "teammate".into(),
                conversation_id: "conv-econ-agy".into(),
                wallet_type: WalletType::CliSubscription,
                backend: "antigravity".into(),
                model_id: "gemini-3.8-flash-low".into(),
                current_msg_count: 7,
                current_char_sum: 4_100,
                reported_total_tokens: Some(16_000),
                reported_input_tokens: Some(1_050),
                reported_output_tokens: Some(500),
                reported_cached_read_tokens: Some(14_100),
                reported_meta_seq: Some(12),
            })
            .await
            .unwrap();
        assert_eq!(
            t4_stale_seq.row.cached_read_tokens_total,
            28_200,
            "stale _meta seq must not double-count cached_read_tokens"
        );

        // Codex includes `cachedInputTokens` inside `inputTokens` (e.g. 11024 = 18 uncached + 11006 cached)
        let codex_turn = repo
            .settle_slot_economy_turn(SettleSlotEconomyParams {
                team_id: "team-econ-1".into(),
                slot_id: "slot-codex".into(),
                owner_user_id: "system_default_user".into(),
                role: "teammate".into(),
                conversation_id: "conv-econ-codex".into(),
                wallet_type: WalletType::CliSubscription,
                backend: "codex".into(),
                model_id: "gpt-5.3-codex".into(),
                current_msg_count: 2,
                current_char_sum: 900,
                reported_total_tokens: Some(11_030),
                reported_input_tokens: Some(11_024),
                reported_output_tokens: Some(6),
                reported_cached_read_tokens: Some(11_006),
                reported_meta_seq: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(codex_turn.row.input_tokens_total, 18);
        assert_eq!(codex_turn.row.cached_read_tokens_total, 11_006);

        repo.record_slot_compaction_savings("team-econ-1", "slot-lead", "system_default_user", 42_000)
            .await
            .unwrap();
        let updated_pref = repo.get_or_default_team_context_pref("team-econ-1").await.unwrap();
        assert_eq!(updated_pref.tokens_saved_estimate, 42_000);
    }
}

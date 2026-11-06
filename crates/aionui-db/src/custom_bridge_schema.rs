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
    ];

    for ddl in ddl_statements {
        sqlx::query(ddl).execute(pool).await.map_err(DbError::Query)?;
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
        let host_team_id = params.host_team_id.unwrap_or(existing.host_team_id);
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
            sqlx::query("DELETE FROM custom_shared_room_members WHERE room_id = ?")
                .bind(room_id)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
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

        tx.commit().await.map_err(DbError::Query)?;
        Ok(true)
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
}

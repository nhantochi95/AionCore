//! 3-Tier Context Compaction (`AnchoredHandoverEngine`), Connected Models Catalog,
//! Quick Member Provisioning, and Cross-Engine Hot-Swap Orchestrator.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aionui_ai_agent::AgentRegistry;
use aionui_api_types::{
    AddAgentRequest, AgentManagementStatus, AssistantResponse, CreateAssistantRequest, TeamAgentResponse, TeamResponse,
    WebSocketMessage,
};
use aionui_assistant::AssistantService;
use aionui_common::{AgentType, ApiError, ProviderWithModel};
use aionui_conversation::ConversationService;
use aionui_db::custom_bridge_schema::{
    CreateActionInboxParams, SettleSlotEconomyParams, SlotWalletFacts, UpsertTeamContextPrefParams, WalletType,
    classify_slot_wallet_facts, slot_has_custom_api_key,
};
use aionui_db::{
    IAgentMetadataRepository, IConversationRepository, IProviderRepository, ITeamRepository,
    SqliteCustomBridgeRepository, UpdateTeamParams, UpsertTeamContextAnchorParams,
};
use aionui_realtime::{BroadcastEventBus, EventBroadcaster};
use aionui_team::TeamSessionService;
use aionui_team::types::Team;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

pub const MAX_ANCHORED_SUMMARY_CHARS: usize = 6_000;
pub const DEFAULT_CONTEXT_WINDOW_TOKENS: i64 = 200_000;

// ---------------------------------------------------------------------------
// Public Frozen HTTP & WS DTOs (`/api/team-hub/*`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectedModelOptionDto {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub is_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectedEngineCatalogItemDto {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    pub engine_kind: String, // "cli" | "api_provider"
    pub backend: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub platform: Option<String>,
    pub agent_id: String,
    pub assistant_id: String,
    #[serde(default)]
    pub icon: Option<String>,
    pub status: String,
    pub wallet_type: String, // "cli_subscription" | "api_key"
    pub supports_native_compact: bool,
    pub models: Vec<ConnectedModelOptionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct QuotaRoutingPresetDto {
    pub id: String,
    pub label: String,
    pub description: String,
    #[serde(default)]
    pub lead_target: Option<ModelTargetSpec>,
    #[serde(default)]
    pub worker_target: Option<ModelTargetSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ConnectedModelsCatalogDto {
    pub cli_agents: Vec<ConnectedEngineCatalogItemDto>,
    pub api_providers: Vec<ConnectedEngineCatalogItemDto>,
    #[serde(default)]
    pub recommended_presets: Vec<QuotaRoutingPresetDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuickCreateMemberRequest {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub role_prompt: Option<String>,
    #[serde(default)]
    pub backend: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub save_to_catalog: Option<bool>,
    #[serde(default)]
    pub team_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickCreateMemberResponse {
    pub name: String,
    pub assistant_name: String,
    pub assistant_id: String,
    pub agent_id: String,
    pub backend: String,
    pub provider_id: Option<String>,
    pub model_id: String,
    pub wallet_type: String,
    pub saved_to_catalog: bool,
    pub added_agent: Option<TeamAgentResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ModelTargetSpec {
    #[serde(default)]
    pub backend: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlotContextStatusDto {
    pub slot_id: String,
    pub name: String,
    pub role: String,
    pub conversation_id: String,
    pub backend: String,
    pub provider_id: Option<String>,
    pub model_id: String,
    pub wallet_type: String,
    pub used_tokens: i64,
    pub max_tokens: i64,
    pub usage_percent: i64,
    pub level: String, // "normal" (<70) | "warning" (70..=84) | "critical" (>=85)
    pub compaction_count: i64,
    pub auto_compact_enabled: bool,
    pub has_anchored_summary: bool,
    pub pending_model_swap: Option<ModelTargetSpec>,
    #[serde(default)]
    pub effective_compact_limit_tokens: i64,
    #[serde(default)]
    pub wakeup_turns: i64,
    #[serde(default)]
    pub input_tokens_total: i64,
    #[serde(default)]
    pub output_tokens_total: i64,
    #[serde(default)]
    pub cached_read_tokens_total: i64,
    #[serde(default)]
    pub estimated_cost_usd: f64,
    #[serde(default)]
    pub compaction_saved_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TeamContextStatusResponse {
    pub team_id: String,
    pub auto_compact_enabled: bool,
    pub auto_compact_threshold_pct: i64,
    #[serde(default = "default_economy_mode")]
    pub economy_mode: String,
    #[serde(default = "default_max_context_tokens")]
    pub max_context_tokens: i64,
    #[serde(default)]
    pub tokens_saved_estimate: i64,
    #[serde(default)]
    pub coalesced_wakes_saved: i64,
    #[serde(default = "default_max_turn_hops")]
    pub max_turn_hops_per_run: i64,
    #[serde(default)]
    pub auto_fallback_free_cli: bool,
    #[serde(default)]
    pub enforce_hard_cap_on_cli: bool,
    #[serde(default)]
    pub total_wakeup_turns: i64,
    #[serde(default)]
    pub total_input_tokens: i64,
    #[serde(default)]
    pub total_output_tokens: i64,
    #[serde(default)]
    pub total_cached_read_tokens: i64,
    #[serde(default)]
    pub total_estimated_cost_usd: f64,
    pub slots: Vec<SlotContextStatusDto>,
}

fn default_economy_mode() -> String {
    "lean".to_owned()
}

fn default_max_context_tokens() -> i64 {
    160_000
}

fn default_max_turn_hops() -> i64 {
    24
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactTeamContextRequest {
    #[serde(default)]
    pub slot_id: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub auto_compact_enabled: Option<bool>,
    #[serde(default)]
    pub threshold_pct: Option<i64>,
    #[serde(default)]
    pub economy_mode: Option<String>,
    #[serde(default)]
    pub max_context_tokens: Option<i64>,
    #[serde(default)]
    pub max_turn_hops_per_run: Option<i64>,
    #[serde(default)]
    pub auto_fallback_free_cli: Option<bool>,
    #[serde(default)]
    pub enforce_hard_cap_on_cli: Option<bool>,
    #[serde(default)]
    pub force_interrupt: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotCompactionOutcomeDto {
    pub slot_id: String,
    pub name: String,
    pub role: String,
    pub conversation_id: String,
    pub strategy: String,
    pub strategy_used: String,
    pub before_tokens: i64,
    pub after_tokens: i64,
    pub previous_tokens: i64,
    pub new_tokens: i64,
    pub before_percent: i64,
    pub after_percent: i64,
    pub compaction_count: i64,
    pub summary_chars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTeamContextResponse {
    pub team_id: String,
    pub auto_compact_enabled: bool,
    pub auto_compact_threshold_pct: i64,
    pub economy_mode: String,
    pub max_context_tokens: i64,
    pub tokens_saved_estimate: i64,
    pub coalesced_wakes_saved: i64,
    pub outcomes: Vec<SlotCompactionOutcomeDto>,
    pub context_status: TeamContextStatusResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PerSlotModelUpdate {
    pub slot_id: String,
    #[serde(default)]
    pub backend: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub target: Option<ModelTargetSpec>,
}

impl PerSlotModelUpdate {
    pub fn effective_target(&self) -> ModelTargetSpec {
        if let Some(ref t) = self.target
            && !t.backend.trim().is_empty()
        {
            return t.clone();
        }
        ModelTargetSpec {
            backend: self.backend.clone(),
            provider_id: self.provider_id.clone(),
            model_id: self.model_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SwitchTeamModelsRequest {
    #[serde(default)]
    pub scope: Option<String>, // "all" | "teammates_only" | "slots"
    #[serde(default)]
    pub preset: Option<String>, // "smart_split" | "quota_saver" | "all_free_cli"
    #[serde(default)]
    pub target: Option<ModelTargetSpec>,
    #[serde(default)]
    pub updates: Vec<PerSlotModelUpdate>,
    #[serde(default)]
    pub force_interrupt: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApplyQuotaPresetRequest {
    #[serde(default)]
    pub preset: Option<String>, // Defaults to "smart_split" on `{}` empty body
    #[serde(default)]
    pub force_interrupt: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotModelSwitchResultDto {
    pub slot_id: String,
    pub name: String,
    pub role: String,
    pub conversation_id: String,
    pub previous_backend: String,
    pub previous_model_id: String,
    pub previous_model: String,
    pub backend: String,
    pub new_backend: String,
    pub provider_id: Option<String>,
    pub new_provider_id: Option<String>,
    pub model_id: String,
    pub new_model: String,
    pub wallet_type: String,
    pub status: String, // "switched" | "applied" | "queued_after_turn"
    pub handover_injected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchTeamModelsResponse {
    pub team_id: String,
    pub scope: String,
    pub lead_wallet_type: String,
    pub results: Vec<SlotModelSwitchResultDto>,
    pub outcomes: Vec<SlotModelSwitchResultDto>,
    pub team: TeamResponse,
    pub context_status: TeamContextStatusResponse,
}

// ---------------------------------------------------------------------------
// Deterministic SQLite `AnchoredHandoverEngine` (Zero LLM Token Cost)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AnchoredExtractionResult {
    pub summary_md: String,
    pub files_touched: Vec<String>,
    pub latest_msg_id: Option<String>,
    pub new_message_count: usize,
}

pub struct AnchoredHandoverEngine;

impl AnchoredHandoverEngine {
    /// Scrubs API keys and auth tokens (`sk-[A-Za-z0-9_-]+`, `Bearer <tok>`, `api[_-]?key[:=]<tok>`,
    /// and Telegram BotFather tokens `(?:bot)?\d{6,15}(?::|%3[Aa])[A-Za-z0-9_-]{30,64}`)
    /// from deterministic summaries before persisting or injecting into prompts.
    pub fn scrub_secrets(input: &str) -> String {
        let step0 = Self::scrub_botfather_tokens(input);
        let step1 = Self::scrub_sk_tokens(&step0);
        let step2 = Self::scrub_bearer_tokens(&step1);
        Self::scrub_api_key_assignments(&step2)
    }

    fn scrub_botfather_tokens(input: &str) -> String {
        let bytes = input.as_bytes();
        let len = bytes.len();
        let mut out = String::with_capacity(len);
        let mut i = 0;

        while i < len {
            let candidate = if i + 4 <= len && &bytes[i..i + 3] == b"bot" && bytes[i + 3].is_ascii_digit() {
                Some((true, i + 3))
            } else if bytes[i].is_ascii_digit() && (i == 0 || !bytes[i - 1].is_ascii_digit()) {
                Some((false, i))
            } else {
                None
            };

            if let Some((has_bot_prefix, digits_start)) = candidate {
                let mut digits_end = digits_start;
                while digits_end < len && bytes[digits_end].is_ascii_digit() {
                    digits_end += 1;
                }
                if (6..=15).contains(&(digits_end - digits_start)) {
                    let sec_start_opt = if digits_end < len && bytes[digits_end] == b':' {
                        Some(digits_end + 1)
                    } else if digits_end + 3 <= len
                        && bytes[digits_end] == b'%'
                        && bytes[digits_end + 1] == b'3'
                        && matches!(bytes[digits_end + 2], b'A' | b'a')
                    {
                        Some(digits_end + 3)
                    } else {
                        None
                    };

                    if let Some(sec_start) = sec_start_opt {
                        let mut sec_end = sec_start;
                        while sec_end < len
                            && (bytes[sec_end].is_ascii_alphanumeric()
                                || bytes[sec_end] == b'_'
                                || bytes[sec_end] == b'-')
                        {
                            sec_end += 1;
                        }
                        if (30..=64).contains(&(sec_end - sec_start)) {
                            out.push_str(if has_bot_prefix { "bot[REDACTED]" } else { "[REDACTED]" });
                            i = sec_end;
                            continue;
                        }
                    }
                }
            }

            let ch = input[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
        out
    }

    fn scrub_sk_tokens(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = String::with_capacity(input.len());
        let mut i = 0;

        while i < bytes.len() {
            if i + 4 <= bytes.len()
                && &bytes[i..i + 3] == b"sk-"
                && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
                && (bytes[i + 3].is_ascii_alphanumeric() || bytes[i + 3] == b'_' || bytes[i + 3] == b'-')
            {
                let mut end = i + 3;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_' || bytes[end] == b'-')
                {
                    end += 1;
                }
                if end - (i + 3) >= 3 {
                    out.push_str("[REDACTED_SECRET]");
                    i = end;
                    continue;
                }
            }
            let ch = input[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
        out
    }

    fn scrub_bearer_tokens(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = String::with_capacity(input.len());
        let mut i = 0;

        while i < bytes.len() {
            if i + 7 <= bytes.len()
                && bytes[i..i + 6].eq_ignore_ascii_case(b"bearer")
                && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
                && bytes[i + 6].is_ascii_whitespace()
            {
                let mut ws_end = i + 6;
                while ws_end < bytes.len() && bytes[ws_end].is_ascii_whitespace() && bytes[ws_end] != b'\n' {
                    ws_end += 1;
                }
                let mut tok_end = ws_end;
                while tok_end < bytes.len() && !bytes[tok_end].is_ascii_whitespace() {
                    if matches!(bytes[tok_end], b'"' | b'\'' | b',' | b';' | b')' | b']' | b'}') {
                        break;
                    }
                    tok_end += 1;
                }
                if tok_end > ws_end {
                    out.push_str("Bearer [REDACTED_TOKEN]");
                    i = tok_end;
                    continue;
                }
            }
            let ch = input[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
        out
    }

    fn scrub_api_key_assignments(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = String::with_capacity(input.len());
        let mut i = 0;

        while i < bytes.len() {
            let prefix_len = if i + 7 <= bytes.len()
                && (bytes[i..i + 7].eq_ignore_ascii_case(b"api_key")
                    || bytes[i..i + 7].eq_ignore_ascii_case(b"api-key"))
            {
                Some(7)
            } else if i + 6 <= bytes.len() && bytes[i..i + 6].eq_ignore_ascii_case(b"apikey") {
                Some(6)
            } else {
                None
            };

            if let Some(plen) = prefix_len
                && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
            {
                let mut p = i + plen;
                // Allow closing quote right after key name in JSON like `"api_key": "secret"`
                if p < bytes.len() && (bytes[p] == b'"' || bytes[p] == b'\'') {
                    p += 1;
                }
                while p < bytes.len() && bytes[p].is_ascii_whitespace() && bytes[p] != b'\n' {
                    p += 1;
                }
                if p < bytes.len() && (bytes[p] == b':' || bytes[p] == b'=') {
                    p += 1;
                    while p < bytes.len() && bytes[p].is_ascii_whitespace() && bytes[p] != b'\n' {
                        p += 1;
                    }
                    let quote = if p < bytes.len() && (bytes[p] == b'"' || bytes[p] == b'\'') {
                        let q = bytes[p];
                        p += 1;
                        Some(q)
                    } else {
                        None
                    };
                    let val_start = p;
                    while p < bytes.len() && !bytes[p].is_ascii_whitespace() {
                        if let Some(q) = quote {
                            if bytes[p] == q {
                                p += 1;
                                break;
                            }
                        } else if matches!(bytes[p], b'"' | b'\'' | b',' | b';' | b')' | b']' | b'}') {
                            break;
                        }
                        p += 1;
                    }
                    if p > val_start {
                        out.push_str("api_key=[REDACTED_KEY]");
                        i = p;
                        continue;
                    }
                }
            }

            let ch = input[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
        out
    }

    fn extract_file_candidates(text: &str, sink: &mut Vec<String>, seen: &mut HashSet<String>) {
        const EXTENSIONS: &[&str] = &[
            ".rs", ".ts", ".tsx", ".js", ".jsx", ".json", ".toml", ".sql", ".md", ".py", ".css", ".scss", ".html",
            ".yaml", ".yml", ".sh", ".ps1",
        ];
        for raw_token in text.split(|c: char| {
            c.is_whitespace() || matches!(c, '`' | '"' | '\'' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';' | '<' | '>')
        }) {
            let trimmed = raw_token.trim_matches(|c: char| c == ':' || c == '.');
            if trimmed.len() < 4 || trimmed.len() > 180 || trimmed.starts_with("http://") || trimmed.starts_with("https://")
            {
                continue;
            }
            let lower = trimmed.to_ascii_lowercase();
            let has_ext = EXTENSIONS.iter().any(|ext| lower.ends_with(ext));
            let looks_like_path = (trimmed.contains('/') || trimmed.contains('\\')) && has_ext;
            if (looks_like_path || has_ext) && seen.insert(trimmed.to_owned()) {
                sink.push(trimmed.to_owned());
                if sink.len() >= 40 {
                    return;
                }
            }
        }
    }

    fn parse_section_from_previous(prev_md: &str, heading: &str) -> String {
        let Some(start_idx) = prev_md.find(heading) else {
            return String::new();
        };
        let after_heading = &prev_md[start_idx + heading.len()..];
        let body = match after_heading.find("\n## ") {
            Some(next_h) => &after_heading[..next_h],
            None => after_heading,
        };
        body.trim().to_owned()
    }

    fn cap_chars(text: &str, max_chars: usize) -> String {
        let count = text.chars().count();
        if count <= max_chars {
            text.to_owned()
        } else {
            let kept: String = text.chars().take(max_chars.saturating_sub(3)).collect();
            format!("{kept}...")
        }
    }

    /// Deterministically extracts and merges conversation messages (since `last_compacted_msg_id`),
    /// team tasks, and recent mailbox items into a 5-section `<= 6,000`-char anchored handover.
    pub async fn extract_and_merge_anchor(
        bridge_repo: &SqliteCustomBridgeRepository,
        team_id: &str,
        slot_id: &str,
        conversation_id: &str,
    ) -> Result<AnchoredExtractionResult, DbOrExtractError> {
        let prev_anchor = bridge_repo
            .get_team_context_anchor(team_id, slot_id)
            .await
            .map_err(|e| DbOrExtractError(e.to_string()))?;

        let all_msgs = bridge_repo
            .list_conversation_messages_for_anchor(conversation_id)
            .await
            .map_err(|e| DbOrExtractError(e.to_string()))?;

        let latest_msg_id = all_msgs.last().map(|m| m.id.clone());

        let start_index = if let Some(ref anchor) = prev_anchor
            && let Some(ref last_id) = anchor.last_compacted_msg_id
        {
            all_msgs
                .iter()
                .position(|m| m.id == *last_id || m.msg_id.as_deref() == Some(last_id.as_str()))
                .map(|idx| idx + 1)
                .unwrap_or(0)
        } else {
            0
        };

        let delta_msgs = &all_msgs[start_index..];
        let new_message_count = delta_msgs.len();

        let mut files_touched: Vec<String> = prev_anchor
            .as_ref()
            .and_then(|a| serde_json::from_str::<Vec<String>>(&a.files_touched_json).ok())
            .unwrap_or_default();
        let mut seen_files: HashSet<String> = files_touched.iter().cloned().collect();

        let mut user_intents: Vec<String> = Vec::new();
        let mut assistant_decisions: Vec<String> = Vec::new();
        let mut latest_assistant_tail = String::new();

        for m in delta_msgs {
            let parsed_val: serde_json::Value =
                serde_json::from_str(&m.content).unwrap_or_else(|_| serde_json::Value::String(m.content.clone()));
            let raw_text = match &parsed_val {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Object(map) => map
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| map.get("text").and_then(serde_json::Value::as_str))
                    .unwrap_or(&m.content)
                    .to_owned(),
                _ => m.content.clone(),
            };

            let scrubbed = Self::scrub_secrets(raw_text.trim());
            if scrubbed.is_empty() {
                continue;
            }
            Self::extract_file_candidates(&scrubbed, &mut files_touched, &mut seen_files);

            if m.hidden != 0 {
                continue;
            }

            let is_right = m.position.as_deref() == Some("right");
            if is_right {
                let first_line = scrubbed
                    .lines()
                    .find(|l| !l.trim().is_empty() && !l.trim().starts_with("## New Messages"))
                    .unwrap_or(&scrubbed)
                    .trim();
                if !first_line.is_empty() && user_intents.len() < 6 {
                    user_intents.push(Self::cap_chars(first_line, 220));
                }
            } else {
                for line in scrubbed.lines() {
                    let t = line.trim();
                    let lower = t.to_ascii_lowercase();
                    if (lower.contains("decid")
                        || lower.contains("implement")
                        || lower.contains("fixed")
                        || lower.contains("updated")
                        || lower.contains("created")
                        || lower.starts_with("- ")
                        || lower.starts_with("* "))
                        && t.len() >= 12
                        && assistant_decisions.len() < 10
                    {
                        assistant_decisions.push(Self::cap_chars(t.trim_start_matches(['-', '*', ' ']), 200));
                    }
                }
                latest_assistant_tail = Self::cap_chars(&scrubbed, 450);
            }
        }

        // Also inspect recent mailbox messages for this slot
        let mailbox_rows = bridge_repo
            .list_recent_slot_mailbox_for_anchor(team_id, slot_id, 8)
            .await
            .unwrap_or_default();

        for mb in mailbox_rows.iter().rev() {
            let scrubbed = Self::scrub_secrets(&mb.content);
            Self::extract_file_candidates(&scrubbed, &mut files_touched, &mut seen_files);
            if user_intents.is_empty() && mb.from_agent_id == "user" {
                let snippet = Self::cap_chars(scrubbed.lines().next().unwrap_or(&scrubbed).trim(), 220);
                if !snippet.is_empty() {
                    user_intents.push(snippet);
                }
            }
            if let Some(ref sum) = mb.summary {
                let clean_sum = Self::scrub_secrets(sum.trim());
                if !clean_sum.is_empty() && assistant_decisions.len() < 10 {
                    assistant_decisions.push(Self::cap_chars(&clean_sum, 180));
                }
            }
        }

        // Read team_tasks for Active & Blocked Tasks
        let task_rows = bridge_repo
            .list_active_team_tasks_for_anchor(team_id, 20)
            .await
            .unwrap_or_default();

        let prev_md = prev_anchor
            .as_ref()
            .map(|a| a.anchored_summary_md.as_str())
            .unwrap_or("");

        // Section 1: Session Intent
        let prev_intent = Self::parse_section_from_previous(prev_md, "## Session Intent");
        let intent_body = if !user_intents.is_empty() {
            let mut lines = Vec::new();
            if !prev_intent.is_empty() && !prev_intent.starts_with("- No explicit") {
                for l in prev_intent.lines().take(2) {
                    lines.push(l.to_owned());
                }
            }
            for ui in user_intents.iter().take(4) {
                let bullet = format!("- {ui}");
                if !lines.contains(&bullet) {
                    lines.push(bullet);
                }
            }
            Self::cap_chars(&lines.join("\n"), 950)
        } else if !prev_intent.is_empty() {
            Self::cap_chars(&prev_intent, 950)
        } else {
            "- Maintain team objectives and fulfill assigned slot responsibilities.".to_owned()
        };

        // Section 2: Artifact Trail (Files Modified/Read)
        if files_touched.len() > 35 {
            let drain_count = files_touched.len() - 35;
            files_touched.drain(0..drain_count);
        }
        let artifact_body = if files_touched.is_empty() {
            "- None recorded yet.".to_owned()
        } else {
            let bullets: Vec<String> = files_touched.iter().map(|f| format!("- `{f}`")).collect();
            Self::cap_chars(&bullets.join("\n"), 1_100)
        };

        // Section 3: Key Decisions
        let prev_decisions = Self::parse_section_from_previous(prev_md, "## Key Decisions");
        let decisions_body = if !assistant_decisions.is_empty() {
            let mut lines = Vec::new();
            if !prev_decisions.is_empty() && !prev_decisions.starts_with("- Initial") {
                for l in prev_decisions.lines().take(3) {
                    lines.push(l.to_owned());
                }
            }
            for d in assistant_decisions.iter().take(6) {
                let bullet = format!("- {d}");
                if !lines.contains(&bullet) {
                    lines.push(bullet);
                }
            }
            Self::cap_chars(&lines.join("\n"), 1_200)
        } else if !prev_decisions.is_empty() {
            Self::cap_chars(&prev_decisions, 1_200)
        } else if !latest_assistant_tail.is_empty() {
            format!("- {}", Self::cap_chars(&latest_assistant_tail, 600))
        } else {
            "- Initial session anchor established.".to_owned()
        };

        // Section 4: Active & Blocked Tasks
        let mut active_task_lines = Vec::new();
        let mut pending_next_steps = Vec::new();
        for t in &task_rows {
            let short_id: String = t.id.chars().take(8).collect();
            let owner = t.owner.as_deref().unwrap_or("unassigned");
            let blocked: Vec<String> = serde_json::from_str(&t.blocked_by).unwrap_or_default();
            let clean_subj = Self::scrub_secrets(&t.subject);
            if t.status == "in_progress" || !blocked.is_empty() || t.status == "pending" {
                let blk_suffix = if blocked.is_empty() {
                    String::new()
                } else {
                    format!(" (blocked_by: {})", blocked.join(", "))
                };
                active_task_lines.push(format!(
                    "- [{}] `{short_id}` {} (owner: `{owner}`){blk_suffix}",
                    t.status,
                    Self::cap_chars(&clean_subj, 120)
                ));
                if (t.owner.as_deref() == Some(slot_id) || t.status == "in_progress") && pending_next_steps.len() < 4 {
                    pending_next_steps.push(format!(
                        "- Continue task `{short_id}`: {}",
                        Self::cap_chars(&clean_subj, 140)
                    ));
                }
            }
        }
        let tasks_body = if active_task_lines.is_empty() {
            "- No active or blocked tasks on the board.".to_owned()
        } else {
            Self::cap_chars(&active_task_lines.join("\n"), 1_100)
        };

        // Section 5: Next Steps
        let next_steps_body = if !pending_next_steps.is_empty() {
            Self::cap_chars(&pending_next_steps.join("\n"), 800)
        } else if !latest_assistant_tail.is_empty() {
            format!(
                "- Resume from latest checkpoint: {}\n- Check `team_task_list` and `team_read_messages` before duplicating completed work.",
                Self::cap_chars(latest_assistant_tail.lines().last().unwrap_or(&latest_assistant_tail), 300)
            )
        } else {
            "- Check `team_read_messages` and `team_task_list` to continue assigned team work.".to_owned()
        };

        let mut summary_md = format!(
            "## Session Intent\n{intent_body}\n\n\
             ## Artifact Trail (Files Modified/Read)\n{artifact_body}\n\n\
             ## Key Decisions\n{decisions_body}\n\n\
             ## Active & Blocked Tasks\n{tasks_body}\n\n\
             ## Next Steps\n{next_steps_body}"
        );

        // Final scrub + strict <= 6,000 char ceiling
        summary_md = Self::scrub_secrets(&summary_md);
        if summary_md.chars().count() > MAX_ANCHORED_SUMMARY_CHARS {
            summary_md = Self::cap_chars(&summary_md, MAX_ANCHORED_SUMMARY_CHARS);
        }

        Ok(AnchoredExtractionResult {
            summary_md,
            files_touched,
            latest_msg_id,
            new_message_count,
        })
    }
}

#[derive(Debug)]
pub struct DbOrExtractError(pub String);

impl std::fmt::Display for DbOrExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Shared `TeamHubOrchestrator` (REST `/api/team-hub/*` + Telegram `/compact` & `/model`)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TeamHubOrchestrator {
    pub bridge_repo: Arc<SqliteCustomBridgeRepository>,
    pub conversation_service: ConversationService,
    pub conversation_repo: Arc<dyn IConversationRepository>,
    pub team_service: Arc<TeamSessionService>,
    pub team_repo: Arc<dyn ITeamRepository>,
    pub agent_registry: Arc<AgentRegistry>,
    pub agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
    pub provider_repo: Arc<dyn IProviderRepository>,
    pub assistant_service: Arc<AssistantService>,
    pub event_bus: Arc<BroadcastEventBus>,
    pub data_dir: PathBuf,
    pub slot_locks: Arc<dashmap::DashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>,
    pub active_run_turn_hops: Arc<dashmap::DashMap<String, i64>>,
}

impl TeamHubOrchestrator {
    pub fn reset_team_run_turn_hops(&self, team_id: &str) {
        self.active_run_turn_hops.remove(team_id);
    }

    pub fn classify_slot_wallet(backend: &str, model_id: &str) -> WalletType {
        Self::classify_slot_wallet_ext(backend, "", None, model_id, None, false)
    }

    pub fn classify_slot_wallet_ext(
        backend: &str,
        assistant_backend: &str,
        provider_id: Option<&str>,
        model_id: &str,
        auth_methods_json: Option<&str>,
        has_custom_api_key: bool,
    ) -> WalletType {
        classify_slot_wallet_facts(&SlotWalletFacts {
            slot_backend: Some(backend),
            assistant_backend: Some(assistant_backend),
            extra_backend: None,
            cli_path: None,
            provider_id,
            model_id: Some(model_id),
            auth_methods_json,
            has_custom_api_key,
        })
    }

    fn build_recommended_presets(
        cli_agents: &[ConnectedEngineCatalogItemDto],
        api_providers: &[ConnectedEngineCatalogItemDto],
    ) -> Vec<QuotaRoutingPresetDto> {
        let pick_free_worker = || -> Option<ModelTargetSpec> {
            // Prefer lightweight/fast $0 CLI models for worker slots
            const WORKER_MODEL_HINTS: &[&str] = &[
                "haiku",
                "gpt-5.4-mini",
                "gpt-5.3-codex",
                "gemini-2.5-flash",
                "sonnet",
                "default",
            ];
            let free_clis: Vec<&ConnectedEngineCatalogItemDto> = cli_agents
                .iter()
                .filter(|c| c.wallet_type == "cli_subscription")
                .collect();
            for hint in WORKER_MODEL_HINTS {
                for engine in &free_clis {
                    if let Some(m) = engine
                        .models
                        .iter()
                        .find(|m| m.id.to_ascii_lowercase().contains(hint))
                    {
                        return Some(ModelTargetSpec {
                            backend: engine.backend.clone(),
                            provider_id: None,
                            model_id: m.id.clone(),
                        });
                    }
                }
            }
            free_clis.first().map(|engine| ModelTargetSpec {
                backend: engine.backend.clone(),
                provider_id: None,
                model_id: engine
                    .models
                    .first()
                    .map(|m| m.id.clone())
                    .unwrap_or_else(|| engine.backend.clone()),
            })
        };

        let pick_free_lead = || -> Option<ModelTargetSpec> {
            const LEAD_MODEL_HINTS: &[&str] = &[
                "opus",
                "fable",
                "gpt-6.1-sol",
                "gpt-5.4",
                "gemini-2.5-pro",
                "sonnet",
            ];
            let free_clis: Vec<&ConnectedEngineCatalogItemDto> = cli_agents
                .iter()
                .filter(|c| c.wallet_type == "cli_subscription")
                .collect();
            for hint in LEAD_MODEL_HINTS {
                for engine in &free_clis {
                    if let Some(m) = engine
                        .models
                        .iter()
                        .find(|m| m.id.to_ascii_lowercase() == *hint || m.id.to_ascii_lowercase().contains(hint))
                    {
                        return Some(ModelTargetSpec {
                            backend: engine.backend.clone(),
                            provider_id: None,
                            model_id: m.id.clone(),
                        });
                    }
                }
            }
            free_clis.first().map(|engine| ModelTargetSpec {
                backend: engine.backend.clone(),
                provider_id: None,
                model_id: engine
                    .models
                    .first()
                    .map(|m| m.id.clone())
                    .unwrap_or_else(|| engine.backend.clone()),
            })
        };

        let worker_free = pick_free_worker();
        let lead_free = pick_free_lead();
        let lead_smart = lead_free.clone().or_else(|| {
            api_providers.first().and_then(|p| {
                p.models.first().map(|m| ModelTargetSpec {
                    backend: "aionrs".to_owned(),
                    provider_id: p.provider_id.clone(),
                    model_id: m.id.clone(),
                })
            })
        });

        let mut presets = Vec::new();
        if worker_free.is_some() || lead_smart.is_some() {
            presets.push(QuotaRoutingPresetDto {
                id: "smart_split".to_owned(),
                label: "Smart Split (Reasoning Lead + $0 CLI Workers)".to_owned(),
                description: "Keep Team Lead on a high-reasoning model while routing worker slots to fast $0 CLI OAuth models.".to_owned(),
                lead_target: lead_smart,
                worker_target: worker_free.clone(),
            });
        }
        if lead_free.is_some() || worker_free.is_some() {
            presets.push(QuotaRoutingPresetDto {
                id: "all_free_cli".to_owned(),
                label: "All $0 CLI Subscription".to_owned(),
                description: "Switch Lead and all teammates to $0 OAuth CLI engines immune to Paid API budget limits.".to_owned(),
                lead_target: lead_free,
                worker_target: worker_free,
            });
        }
        presets
    }

    pub fn infer_max_context_tokens(backend: &str, model_id: &str, provider_limit: Option<i64>) -> i64 {
        if let Some(limit) = provider_limit
            && limit > 0
        {
            return limit;
        }
        let lower = model_id.trim().to_ascii_lowercase();
        let backend_lower = backend.trim().to_ascii_lowercase();
        if lower.contains("gemini") {
            1_000_000
        } else if (lower.is_empty() || lower == "default" || lower == backend_lower)
            && matches!(backend_lower.as_str(), "antigravity" | "agy" | "gemini" | "gemini-cli")
        {
            1_000_000
        } else if lower.contains("mini")
            || lower.contains("haiku")
            || lower.contains("gpt-3.5")
            || lower.contains("deepseek")
        {
            128_000
        } else {
            DEFAULT_CONTEXT_WINDOW_TOKENS
        }
    }

    pub fn compute_effective_compact_limit_tokens(
        wallet_type: WalletType,
        max_tokens: i64,
        threshold_pct: i64,
        economy_cap_tokens: i64,
        enforce_hard_cap_on_cli: bool,
    ) -> i64 {
        let pct_limit = ((max_tokens * threshold_pct) / 100).max(1);
        match wallet_type {
            WalletType::CliSubscription if !enforce_hard_cap_on_cli => pct_limit,
            _ => pct_limit.min(economy_cap_tokens),
        }
    }

    pub fn remove_aionrs_session_files(&self, conversation_id: &str, workspace: Option<&str>) {
        let conv_trimmed = conversation_id.trim();
        if conv_trimmed.is_empty() {
            return;
        }
        let primary_dir = self.data_dir.join("aionrs-sessions");
        for ext in ["json", "jsonl"] {
            let _ = std::fs::remove_file(primary_dir.join(format!("{conv_trimmed}.{ext}")));
        }
        if let Some(ws) = workspace.map(str::trim).filter(|s| !s.is_empty()) {
            let legacy_dir = Path::new(ws).join(".aionrs").join("sessions");
            for ext in ["json", "jsonl"] {
                let _ = std::fs::remove_file(legacy_dir.join(format!("{conv_trimmed}.{ext}")));
            }
        }
    }

    fn backend_supports_native_compact(backend: &str, available_commands: Option<&serde_json::Value>) -> bool {
        let b = backend.trim().to_ascii_lowercase();
        if matches!(b.as_str(), "claude" | "codex") {
            return true;
        }
        if let Some(cmds) = available_commands.and_then(serde_json::Value::as_array) {
            return cmds.iter().any(|entry| {
                entry
                    .get("name")
                    .or_else(|| entry.get("command"))
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| entry.as_str())
                    .is_some_and(|name| name.trim_start_matches('/') == "compact")
            });
        }
        false
    }

    fn extract_cli_models(backend: &str, agent_name: &str, available_models: Option<&serde_json::Value>) -> Vec<ConnectedModelOptionDto> {
        let mut out: Vec<ConnectedModelOptionDto> = Vec::new();
        let mut seen = HashSet::new();

        let default_id = available_models
            .and_then(|v| v.get("current_model_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(backend);

        seen.insert(default_id.to_owned());
        out.push(ConnectedModelOptionDto {
            id: default_id.to_owned(),
            label: if default_id == backend {
                format!("{agent_name} (Default)")
            } else {
                default_id.to_owned()
            },
            is_default: true,
        });

        if let Some(val) = available_models {
            let arr_opt = val
                .get("available_models")
                .and_then(serde_json::Value::as_array)
                .or_else(|| val.as_array());
            if let Some(arr) = arr_opt {
                for item in arr {
                    let id_opt = item
                        .get("id")
                        .or_else(|| item.get("modelId"))
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| item.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let Some(mid) = id_opt else { continue };
                    if seen.insert(mid.to_owned()) {
                        let label = item
                            .get("name")
                            .or_else(|| item.get("label"))
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .unwrap_or(mid)
                            .to_owned();
                        out.push(ConnectedModelOptionDto {
                            id: mid.to_owned(),
                            label,
                            is_default: false,
                        });
                    }
                }
            }
        }

        out
    }

    /// `GET /api/team-hub/connected-models`
    pub async fn list_connected_models(&self, user_id: &str) -> Result<ConnectedModelsCatalogDto, ApiError> {
        let assistants: Vec<AssistantResponse> = self
            .assistant_service
            .list_for_user(user_id)
            .await
            .unwrap_or_default();

        let management_rows = self
            .agent_registry
            .list_management_rows_for_user(user_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to list agents: {e}")))?;

        let aionrs_agent_id = management_rows
            .iter()
            .find(|r| r.agent_type == AgentType::Aionrs)
            .map(|r| r.id.clone())
            .unwrap_or_else(|| "632f31d2".to_owned());
        let aionrs_assistant_id = assistants
            .iter()
            .find(|a| a.id == format!("bare:{aionrs_agent_id}"))
            .map(|a| a.id.clone())
            .unwrap_or_else(|| format!("bare:{aionrs_agent_id}"));

        let mut cli_agents = Vec::new();
        for row in &management_rows {
            if !row.enabled || !row.installed || !row.team_capable {
                continue;
            }
            if !matches!(
                row.status,
                AgentManagementStatus::Online | AgentManagementStatus::Unchecked
            ) {
                continue;
            }
            if row.agent_type == AgentType::Aionrs {
                continue;
            }
            let backend = row
                .backend
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| row.agent_type.serde_name());
            if backend.eq_ignore_ascii_case("aionrs") {
                continue;
            }

            let bare_id = format!("bare:{}", row.id);
            let assistant_id = assistants
                .iter()
                .find(|a| a.id == bare_id)
                .map(|a| a.id.clone())
                .unwrap_or(bare_id);

            let meta_row = self.agent_metadata_repo.get_for_user(user_id, &row.id).await.ok().flatten();
            let has_custom_api_key = slot_has_custom_api_key(
                None,
                meta_row.as_ref().and_then(|m| m.env.as_deref()),
                meta_row.as_ref().and_then(|m| m.env_override.as_deref()),
            );
            let wallet_type = Self::classify_slot_wallet_ext(
                backend,
                "",
                None,
                backend,
                meta_row.as_ref().and_then(|m| m.auth_methods.as_deref()),
                has_custom_api_key,
            )
            .as_str()
            .to_owned();

            let supports_native_compact =
                Self::backend_supports_native_compact(backend, row.available_commands.as_ref());
            let models = Self::extract_cli_models(backend, &row.name, row.available_models.as_ref());
            let status_str = match row.status {
                AgentManagementStatus::Online => "online",
                AgentManagementStatus::Unchecked => "unchecked",
                AgentManagementStatus::Offline => "offline",
                AgentManagementStatus::Missing => "missing",
            }
            .to_owned();

            cli_agents.push(ConnectedEngineCatalogItemDto {
                id: row.id.clone(),
                name: row.name.clone(),
                provider_name: None,
                engine_kind: "cli".to_owned(),
                backend: backend.to_owned(),
                provider_id: None,
                platform: None,
                agent_id: row.id.clone(),
                assistant_id,
                icon: row.icon.clone(),
                status: status_str,
                wallet_type,
                supports_native_compact,
                models,
            });
        }

        let providers = self
            .provider_repo
            .list(user_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to list providers: {e}")))?;

        let mut api_providers = Vec::new();
        for p in providers {
            if !p.enabled || p.platform.eq_ignore_ascii_case("gemini-with-google-auth") {
                continue;
            }
            let raw_models: Vec<String> = serde_json::from_str(&p.models).unwrap_or_default();
            let enabled_map: HashMap<String, bool> = p
                .model_enabled
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or_default();

            let mut model_dtos = Vec::new();
            for m in raw_models {
                let trimmed = m.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if enabled_map.get(trimmed) == Some(&false) {
                    continue;
                }
                model_dtos.push(ConnectedModelOptionDto {
                    id: trimmed.to_owned(),
                    label: trimmed.to_owned(),
                    is_default: model_dtos.is_empty(),
                });
            }

            api_providers.push(ConnectedEngineCatalogItemDto {
                id: p.id.clone(),
                name: p.name.clone(),
                provider_name: Some(p.name.clone()),
                engine_kind: "api_provider".to_owned(),
                backend: "aionrs".to_owned(),
                provider_id: Some(p.id.clone()),
                platform: Some(p.platform.clone()),
                agent_id: aionrs_agent_id.clone(),
                assistant_id: aionrs_assistant_id.clone(),
                icon: None,
                status: "online".to_owned(),
                wallet_type: WalletType::ApiKey.as_str().to_owned(),
                supports_native_compact: false,
                models: model_dtos,
            });
        }

        let recommended_presets = Self::build_recommended_presets(&cli_agents, &api_providers);

        Ok(ConnectedModelsCatalogDto {
            cli_agents,
            api_providers,
            recommended_presets,
        })
    }

    async fn resolve_agent_identity_for_backend(
        &self,
        user_id: &str,
        backend: &str,
    ) -> Result<(String, AgentType, Option<String>), ApiError> {
        let normalized = backend.trim();
        if normalized.is_empty() {
            return Err(ApiError::BadRequest("backend is required".into()));
        }
        let rows = self
            .agent_registry
            .list_management_rows_for_user(user_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Agent catalog unavailable: {e}")))?;

        if normalized.eq_ignore_ascii_case("aionrs") {
            let row = rows
                .into_iter()
                .find(|r| r.agent_type == AgentType::Aionrs)
                .ok_or_else(|| ApiError::BadRequest("Built-in Aionrs engine not found".into()))?;
            return Ok((row.id, AgentType::Aionrs, row.yolo_id));
        }

        if let Some(row) = rows.into_iter().find(|r| {
            r.backend
                .as_deref()
                .is_some_and(|b| b.eq_ignore_ascii_case(normalized))
                || r.id == normalized
                || (normalized.eq_ignore_ascii_case("antigravity") && r.agent_type == AgentType::Antigravity)
        }) {
            return Ok((row.id, row.agent_type, row.yolo_id));
        }

        // Fallback to DB metadata lookup
        if let Ok(Some(meta)) = self
            .agent_metadata_repo
            .find_builtin_by_backend_for_user(user_id, normalized)
            .await
        {
            let at = if meta.agent_type == "antigravity" {
                AgentType::Antigravity
            } else if meta.agent_type == "aionrs" {
                AgentType::Aionrs
            } else {
                AgentType::Acp
            };
            return Ok((meta.id, at, meta.yolo_id));
        }

        Err(ApiError::BadRequest(format!("Unsupported or unknown backend: {normalized}")))
    }

    /// `POST /api/team-hub/quick-member`
    pub async fn quick_create_member(
        &self,
        user_id: &str,
        req: QuickCreateMemberRequest,
    ) -> Result<QuickCreateMemberResponse, ApiError> {
        let name = req.name.trim().to_owned();
        if name.is_empty() {
            return Err(ApiError::BadRequest("name must not be empty".into()));
        }
        let effective_backend = if req.backend.trim().is_empty() {
            if req.provider_id.as_deref().is_some_and(|p| !p.trim().is_empty()) {
                "aionrs".to_owned()
            } else {
                return Err(ApiError::BadRequest("backend must not be empty".into()));
            }
        } else {
            req.backend.trim().to_owned()
        };
        let model_id = if req.model_id.trim().is_empty() {
            effective_backend.clone()
        } else {
            req.model_id.trim().to_owned()
        };

        // Ensure generated `bare:*` assistants are materialized for this user
        let _ = self.assistant_service.list_for_user(user_id).await;
        let (agent_id, _agent_type, _yolo_id) =
            self.resolve_agent_identity_for_backend(user_id, &effective_backend).await?;

        let provider_id = req
            .provider_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);

        if effective_backend == "aionrs"
            && let Some(ref pid) = provider_id
        {
            let provider = self
                .provider_repo
                .find_by_id(user_id, pid)
                .await
                .map_err(|e| ApiError::Internal(format!("Provider lookup failed: {e}")))?;
            if provider.is_none() {
                return Err(ApiError::BadRequest(format!("Provider not found for user: {pid}")));
            }
        }

        let role_prompt_trimmed = req
            .role_prompt
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let save_to_catalog = req.save_to_catalog.unwrap_or(false);

        let (assistant_id, saved_to_catalog) = if save_to_catalog || role_prompt_trimmed.is_some() {
            let desc = role_prompt_trimmed.map(|p| {
                let snippet: String = p.chars().take(120).collect();
                snippet
            });
            let created = self
                .assistant_service
                .create_for_user(
                    user_id,
                    CreateAssistantRequest {
                        id: None,
                        name: name.clone(),
                        description: desc,
                        avatar: None,
                        agent_id: Some(agent_id.clone()),
                        enabled_skills: None,
                        custom_skill_names: None,
                        disabled_builtin_skills: None,
                        prompts: None,
                        models: Some(vec![model_id.clone()]),
                        name_i18n: None,
                        description_i18n: None,
                        prompts_i18n: None,
                        recommended_prompts: None,
                        recommended_prompts_i18n: None,
                        defaults: None,
                    },
                )
                .await
                .map_err(|e| ApiError::BadRequest(format!("Failed to create quick assistant: {e}")))?;

            if let Some(prompt_body) = role_prompt_trimmed {
                self.assistant_service
                    .write_rule_for_user(user_id, &created.id, None, prompt_body)
                    .await
                    .map_err(|e| ApiError::Internal(format!("Failed to write assistant role prompt: {e}")))?;
            }
            (created.id, true)
        } else {
            // Ensure generated `bare:<agent_id>` definitions are materialized for this user
            let _ = self.assistant_service.list_for_user(user_id).await;
            (format!("bare:{agent_id}"), false)
        };

        let mut added_agent: Option<TeamAgentResponse> = None;
        if let Some(team_id) = req.team_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            let added = self
                .team_service
                .add_agent(
                    user_id,
                    team_id,
                    AddAgentRequest {
                        name: name.clone(),
                        role: "teammate".to_owned(),
                        backend: Some(effective_backend.clone()),
                        model: model_id.clone(),
                        assistant_id: Some(assistant_id.clone()),
                    },
                )
                .await
                .map_err(ApiError::from)?;

            if effective_backend == "aionrs"
                && let Some(ref pid) = provider_id
                && let Ok(Some(conv_row)) = self.conversation_repo.get(user_id, &added.conversation_id).await
            {
                let mut extra: serde_json::Value =
                    serde_json::from_str(&conv_row.extra).unwrap_or_else(|_| serde_json::json!({}));
                extra["provider_id"] = serde_json::Value::String(pid.clone());
                extra["current_model_id"] = serde_json::Value::String(model_id.clone());
                let model_json = serde_json::to_string(&ProviderWithModel {
                    provider_id: pid.clone(),
                    model: model_id.clone(),
                    use_model: Some(model_id.clone()),
                })
                .unwrap_or_default();
                let extra_str = serde_json::to_string(&extra).unwrap_or(conv_row.extra);
                let _ = self
                    .bridge_repo
                    .rebind_conversation_engine(user_id, &added.conversation_id, "aionrs", Some(&model_json), &extra_str)
                    .await;
            }
            added_agent = Some(added);
        }

        let wallet_type = Self::classify_slot_wallet(&effective_backend, &model_id)
            .as_str()
            .to_owned();

        Ok(QuickCreateMemberResponse {
            assistant_name: name.clone(),
            name,
            assistant_id,
            agent_id,
            backend: effective_backend,
            provider_id,
            model_id,
            wallet_type,
            saved_to_catalog,
            added_agent,
        })
    }

    async fn estimate_tokens_from_messages(
        &self,
        conversation_id: &str,
        last_compacted_msg_id: Option<&str>,
        anchored_summary_md: Option<&str>,
        unsettled_wake_chars: i64,
    ) -> i64 {
        let anchor_chars = anchored_summary_md.map(|s| s.chars().count() as i64).unwrap_or(0);
        let wake_chars = unsettled_wake_chars.max(0);

        if let Some(last_id) = last_compacted_msg_id {
            let rows = self
                .bridge_repo
                .list_conversation_messages_for_anchor(conversation_id)
                .await
                .unwrap_or_default();

            let start_idx = rows
                .iter()
                .position(|r| r.id == last_id || r.msg_id.as_deref() == Some(last_id))
                .map(|i| i + 1)
                .unwrap_or(0);

            let delta_chars: i64 = rows[start_idx..].iter().map(|r| r.content.chars().count() as i64).sum();
            ((delta_chars + anchor_chars + wake_chars) / 4).max(0)
        } else {
            // Fast O(1)-memory SQLite SUM(LENGTH(content)) without allocating MessageRow strings
            let (_, total_bytes) = self
                .bridge_repo
                .estimate_conversation_tokens_fast(conversation_id)
                .await
                .unwrap_or((0, 0));
            ((total_bytes + anchor_chars + wake_chars) / 4).max(0)
        }
    }

    /// `GET /api/team-hub/teams/{id}/context-status`
    pub async fn get_team_context_status(
        &self,
        user_id: &str,
        team_id: &str,
    ) -> Result<TeamContextStatusResponse, ApiError> {
        let team = self
            .team_service
            .get_team(user_id, team_id)
            .await
            .map_err(ApiError::from)?;

        let pref = self
            .bridge_repo
            .get_or_default_team_context_pref(team_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Context pref error: {e}")))?;

        let anchors: HashMap<String, _> = self
            .bridge_repo
            .list_team_context_anchors(team_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.slot_id.clone(), a))
            .collect();

        let swaps: HashMap<String, _> = self
            .bridge_repo
            .list_pending_slot_swaps(team_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|s| (s.slot_id.clone(), s))
            .collect();

        let slot_economies: HashMap<String, _> = self
            .bridge_repo
            .list_team_slot_economy(team_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|e| (e.slot_id.clone(), e))
            .collect();

        let providers: HashMap<String, _> = self
            .provider_repo
            .list(user_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|p| (p.id.clone(), p))
            .collect();

        let auto_enabled = pref.is_auto_compact_enabled();
        let economy_cap_tokens = pref.effective_max_context_tokens();
        let mut total_wakeup_turns = 0_i64;
        let mut total_input_tokens = 0_i64;
        let mut total_output_tokens = 0_i64;
        let mut total_cached_read_tokens = 0_i64;
        let mut total_estimated_cost_usd = 0.0_f64;
        let mut slots = Vec::with_capacity(team.assistants.len());

        for member in &team.assistants {
            let conv_row = self
                .conversation_repo
                .get(user_id, &member.conversation_id)
                .await
                .ok()
                .flatten();

            let mut provider_id: Option<String> = None;
            let mut model_id = member.model.clone();
            let mut backend = member.backend.clone();

            if let Some(ref crow) = conv_row {
                if crow.r#type == "aionrs" {
                    backend = "aionrs".to_owned();
                    if let Some(ref mraw) = crow.model
                        && let Ok(pwm) = serde_json::from_str::<ProviderWithModel>(mraw)
                    {
                        if !pwm.provider_id.trim().is_empty() {
                            provider_id = Some(pwm.provider_id);
                        }
                        if !pwm.model.trim().is_empty() {
                            model_id = pwm.model;
                        }
                    }
                }
                if provider_id.is_none()
                    && let Ok(extra) = serde_json::from_str::<serde_json::Value>(&crow.extra)
                    && let Some(pid) = extra
                        .get("provider_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                {
                    if backend == "aionrs" {
                        provider_id = Some(pid.to_owned());
                    }
                }
            }

            let provider_limit = provider_id
                .as_ref()
                .and_then(|pid| providers.get(pid))
                .and_then(|p| p.context_limit);

            let usage_val = self
                .conversation_service
                .get_usage(user_id, &member.conversation_id)
                .await
                .ok()
                .flatten();

            let reported_used = usage_val.as_ref().and_then(|v| {
                v.get("used")
                    .or_else(|| v.get("totalTokens"))
                    .or_else(|| v.get("total_tokens"))
                    .or_else(|| v.get("input_tokens"))
                    .and_then(serde_json::Value::as_i64)
            });
            let reported_max = usage_val.as_ref().and_then(|v| {
                v.get("size")
                    .or_else(|| v.get("maxTokens"))
                    .or_else(|| v.get("max_tokens"))
                    .or_else(|| v.get("context_window"))
                    .and_then(serde_json::Value::as_i64)
            });

            let max_tokens = reported_max
                .filter(|&m| m > 0)
                .unwrap_or_else(|| Self::infer_max_context_tokens(&backend, &model_id, provider_limit));

            let has_custom_api_key = slot_has_custom_api_key(
                conv_row.as_ref().map(|c| c.extra.as_str()),
                None,
                None,
            ) || self
                .bridge_repo
                .slot_has_custom_api_key_in_db(
                    user_id,
                    &member.conversation_id,
                    &backend,
                    member.assistant_id.as_deref(),
                )
                .await;

            let wallet_type_enum = Self::classify_slot_wallet_ext(
                &backend,
                &member.assistant_backend,
                provider_id.as_deref(),
                &model_id,
                None,
                has_custom_api_key,
            );
            let wallet_type = wallet_type_enum.as_str().to_owned();

            let effective_compact_limit_tokens = Self::compute_effective_compact_limit_tokens(
                wallet_type_enum,
                max_tokens,
                pref.threshold_pct,
                economy_cap_tokens,
                pref.is_enforce_hard_cap_on_cli_enabled(),
            );

            let anchor = anchors.get(&member.slot_id);
            let slot_econ = slot_economies.get(&member.slot_id);
            let unsettled_wake_chars = slot_econ.map(|e| e.delivered_wake_chars).unwrap_or(0);
            let has_anchored_summary = anchor
                .map(|a| !a.anchored_summary_md.trim().is_empty())
                .unwrap_or(false);
            let compaction_count = anchor.map(|a| a.compaction_count).unwrap_or(0);

            let used_tokens = match reported_used {
                Some(u) if u > 0 => u + (unsettled_wake_chars.max(0) / 4),
                _ => {
                    self.estimate_tokens_from_messages(
                        &member.conversation_id,
                        anchor.and_then(|a| a.last_compacted_msg_id.as_deref()),
                        anchor
                            .map(|a| a.anchored_summary_md.as_str())
                            .filter(|s| !s.trim().is_empty()),
                        unsettled_wake_chars,
                    )
                    .await
                }
            };

            let usage_percent = ((used_tokens.max(0) as f64 / max_tokens.max(1) as f64) * 100.0)
                .round()
                .clamp(0.0, 100.0) as i64;
            let level = if usage_percent >= 85 || used_tokens >= effective_compact_limit_tokens {
                "critical"
            } else if usage_percent >= 70 || used_tokens >= ((effective_compact_limit_tokens * 85) / 100).max(1) {
                "warning"
            } else {
                "normal"
            }
            .to_owned();

            let pending_model_swap = swaps.get(&member.slot_id).map(|s| ModelTargetSpec {
                backend: s.target_backend.clone(),
                provider_id: s.target_provider_id.clone(),
                model_id: s.target_model_id.clone(),
            });

            let wakeup_turns = slot_econ.map(|e| e.wakeup_turns).unwrap_or(0);
            let input_tokens_total = slot_econ.map(|e| e.input_tokens_total).unwrap_or(0);
            let output_tokens_total = slot_econ.map(|e| e.output_tokens_total).unwrap_or(0);
            let cached_read_tokens_total = slot_econ.map(|e| e.cached_read_tokens_total).unwrap_or(0);
            let estimated_cost_usd = slot_econ.map(|e| e.estimated_cost_usd).unwrap_or(0.0);
            let compaction_saved_tokens = slot_econ.map(|e| e.compaction_saved_tokens).unwrap_or(0);

            total_wakeup_turns += wakeup_turns;
            total_input_tokens += input_tokens_total;
            total_output_tokens += output_tokens_total;
            total_cached_read_tokens += cached_read_tokens_total;
            total_estimated_cost_usd += estimated_cost_usd;

            slots.push(SlotContextStatusDto {
                slot_id: member.slot_id.clone(),
                name: member.name.clone(),
                role: member.role.clone(),
                conversation_id: member.conversation_id.clone(),
                backend,
                provider_id,
                model_id,
                wallet_type,
                used_tokens,
                max_tokens,
                usage_percent,
                level,
                compaction_count,
                auto_compact_enabled: auto_enabled,
                has_anchored_summary,
                pending_model_swap,
                effective_compact_limit_tokens,
                wakeup_turns,
                input_tokens_total,
                output_tokens_total,
                cached_read_tokens_total,
                estimated_cost_usd,
                compaction_saved_tokens,
            });
        }

        Ok(TeamContextStatusResponse {
            team_id: team_id.to_owned(),
            auto_compact_enabled: auto_enabled,
            auto_compact_threshold_pct: pref.threshold_pct,
            economy_mode: pref.effective_economy_mode().to_owned(),
            max_context_tokens: economy_cap_tokens,
            tokens_saved_estimate: pref.tokens_saved_estimate,
            coalesced_wakes_saved: pref.coalesced_wakes_saved,
            max_turn_hops_per_run: if pref.max_turn_hops_per_run > 0 {
                pref.max_turn_hops_per_run
            } else {
                24
            },
            auto_fallback_free_cli: pref.is_auto_fallback_free_cli_enabled(),
            enforce_hard_cap_on_cli: pref.is_enforce_hard_cap_on_cli_enabled(),
            total_wakeup_turns,
            total_input_tokens,
            total_output_tokens,
            total_cached_read_tokens,
            total_estimated_cost_usd,
            slots,
        })
    }

    pub fn broadcast_context_usage(&self, user_id: &str, status: &TeamContextStatusResponse) {
        self.event_bus.broadcast(WebSocketMessage::new(
            "team:context_usage",
            serde_json::json!({
                "user_id": user_id,
                "team_id": status.team_id,
                "auto_compact_enabled": status.auto_compact_enabled,
                "auto_compact_threshold_pct": status.auto_compact_threshold_pct,
                "economy_mode": status.economy_mode,
                "max_context_tokens": status.max_context_tokens,
                "tokens_saved_estimate": status.tokens_saved_estimate,
                "coalesced_wakes_saved": status.coalesced_wakes_saved,
                "max_turn_hops_per_run": status.max_turn_hops_per_run,
                "auto_fallback_free_cli": status.auto_fallback_free_cli,
                "enforce_hard_cap_on_cli": status.enforce_hard_cap_on_cli,
                "total_wakeup_turns": status.total_wakeup_turns,
                "total_input_tokens": status.total_input_tokens,
                "total_output_tokens": status.total_output_tokens,
                "total_cached_read_tokens": status.total_cached_read_tokens,
                "total_estimated_cost_usd": status.total_estimated_cost_usd,
                "slots": status.slots,
            }),
        ));
    }

    /// `POST /api/team-hub/teams/{id}/compact`
    pub async fn compact_team_context(
        &self,
        user_id: &str,
        team_id: &str,
        req: CompactTeamContextRequest,
    ) -> Result<CompactTeamContextResponse, ApiError> {
        let team = self
            .team_service
            .get_team(user_id, team_id)
            .await
            .map_err(ApiError::from)?;

        let mut pref = self
            .bridge_repo
            .get_or_default_team_context_pref(team_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Context pref error: {e}")))?;

        let has_pref_patch = req.auto_compact_enabled.is_some()
            || req.threshold_pct.is_some()
            || req.economy_mode.is_some()
            || req.max_context_tokens.is_some()
            || req.max_turn_hops_per_run.is_some()
            || req.auto_fallback_free_cli.is_some()
            || req.enforce_hard_cap_on_cli.is_some();

        if has_pref_patch {
            pref = self
                .bridge_repo
                .upsert_team_context_pref_ext(UpsertTeamContextPrefParams {
                    team_id: team_id.to_owned(),
                    auto_compact_enabled: req.auto_compact_enabled,
                    threshold_pct: req.threshold_pct,
                    economy_mode: req.economy_mode.clone(),
                    max_context_tokens: req.max_context_tokens,
                    max_turn_hops_per_run: req.max_turn_hops_per_run,
                    auto_fallback_free_cli: req.auto_fallback_free_cli,
                    enforce_hard_cap_on_cli: req.enforce_hard_cap_on_cli,
                })
                .await
                .map_err(|e| ApiError::Internal(format!("Failed to update context prefs: {e}")))?;
        }

        let is_prefs_only = req.mode.as_deref() == Some("prefs_only")
            || (has_pref_patch && req.mode.is_none() && req.slot_id.is_none());

        if is_prefs_only {
            let status = self.get_team_context_status(user_id, team_id).await?;
            self.broadcast_context_usage(user_id, &status);
            return Ok(CompactTeamContextResponse {
                team_id: team_id.to_owned(),
                auto_compact_enabled: pref.is_auto_compact_enabled(),
                auto_compact_threshold_pct: pref.threshold_pct,
                economy_mode: status.economy_mode.clone(),
                max_context_tokens: status.max_context_tokens,
                tokens_saved_estimate: status.tokens_saved_estimate,
                coalesced_wakes_saved: status.coalesced_wakes_saved,
                outcomes: Vec::new(),
                context_status: status,
            });
        }

        let before_status = self.get_team_context_status(user_id, team_id).await?;
        let before_map: HashMap<String, SlotContextStatusDto> = before_status
            .slots
            .iter()
            .map(|s| (s.slot_id.clone(), s.clone()))
            .collect();

        let target_slot_filter = req
            .slot_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let selected_members: Vec<&TeamAgentResponse> = if let Some(target_slot) = target_slot_filter {
            let found = team
                .assistants
                .iter()
                .find(|a| a.slot_id == target_slot)
                .ok_or_else(|| ApiError::NotFound(format!("Team slot '{target_slot}' not found")))?;
            vec![found]
        } else {
            team.assistants.iter().collect()
        };

        let force_interrupt = req.force_interrupt.unwrap_or(false);
        let requested_mode = req.mode.as_deref().unwrap_or("auto");
        let mut outcomes = Vec::with_capacity(selected_members.len());

        for member in selected_members {
            let before_slot = before_map.get(&member.slot_id);
            let before_tokens = before_slot.map(|s| s.used_tokens).unwrap_or(0);
            let before_pct = before_slot.map(|s| s.usage_percent).unwrap_or(0);
            let max_tokens = before_slot
                .map(|s| s.max_tokens)
                .unwrap_or(DEFAULT_CONTEXT_WINDOW_TOKENS);

            let prev_anchor = self
                .bridge_repo
                .get_team_context_anchor(team_id, &member.slot_id)
                .await
                .ok()
                .flatten();
            let next_compaction_count = prev_anchor.map(|a| a.compaction_count + 1).unwrap_or(1);

            let extracted = AnchoredHandoverEngine::extract_and_merge_anchor(
                &self.bridge_repo,
                team_id,
                &member.slot_id,
                &member.conversation_id,
            )
            .await
            .map_err(|e| ApiError::Internal(format!("Handover extraction failed: {e}")))?;

            let files_json = serde_json::to_string(&extracted.files_touched).unwrap_or_else(|_| "[]".into());
            let summary_chars = extracted.summary_md.chars().count();

            let stopped = self
                .team_service
                .interrupt_or_restart_slot_for_maintenance(user_id, team_id, &member.slot_id, force_interrupt)
                .await
                .map_err(ApiError::from)?;

            let strategy = if !stopped {
                // Slot is mid-turn and `!force_interrupt`: persist the anchored summary so next rollover/turn uses it
                self.bridge_repo
                    .upsert_team_context_anchor(UpsertTeamContextAnchorParams {
                        team_id: team_id.to_owned(),
                        slot_id: member.slot_id.clone(),
                        conversation_id: member.conversation_id.clone(),
                        anchored_summary_md: extracted.summary_md.clone(),
                        files_touched_json: files_json,
                        last_compacted_msg_id: extracted.latest_msg_id,
                        compaction_count: next_compaction_count,
                        pending_handover: true,
                    })
                    .await
                    .map_err(|e| ApiError::Internal(format!("Anchor save failed: {e}")))?;
                "queued_after_turn".to_owned()
            } else {
                // Save anchor with `pending_handover = true` and clear session resume anchor / aionrs session file
                let use_native = requested_mode == "native"
                    && matches!(member.backend.as_str(), "claude" | "codex")
                    && self.team_service.get_session_scheduler(team_id).is_some();

                self.bridge_repo
                    .upsert_team_context_anchor(UpsertTeamContextAnchorParams {
                        team_id: team_id.to_owned(),
                        slot_id: member.slot_id.clone(),
                        conversation_id: member.conversation_id.clone(),
                        anchored_summary_md: extracted.summary_md.clone(),
                        files_touched_json: files_json,
                        last_compacted_msg_id: extracted.latest_msg_id,
                        compaction_count: next_compaction_count,
                        pending_handover: !use_native,
                    })
                    .await
                    .map_err(|e| ApiError::Internal(format!("Anchor save failed: {e}")))?;

                if use_native {
                    let _ = self
                        .team_service
                        .send_message_to_agent(user_id, team_id, &member.slot_id, "/compact", None)
                        .await;
                    let _ = self
                        .bridge_repo
                        .clear_acp_context_usage_on_rollover(user_id, &member.conversation_id)
                        .await;
                    "native_compact".to_owned()
                } else {
                    let _ = self
                        .bridge_repo
                        .clear_acp_context_usage_on_rollover(user_id, &member.conversation_id)
                        .await;
                    self.remove_aionrs_session_files(&member.conversation_id, Some(&team.workspace));
                    self.team_service
                        .mark_slot_needs_role_prompt(team_id, &member.slot_id)
                        .await;
                    "anchored_rollover".to_owned()
                }
            };

            let after_tokens = (summary_chars / 4) as i64;
            let saved_tokens = (before_tokens - after_tokens).max(0);
            if saved_tokens > 0 {
                let _ = self
                    .bridge_repo
                    .record_slot_compaction_savings(
                        team_id,
                        &member.slot_id,
                        user_id,
                        saved_tokens,
                    )
                    .await;
            }
            let after_percent = ((after_tokens as f64 / max_tokens.max(1) as f64) * 100.0)
                .round()
                .clamp(0.0, 100.0) as i64;

            outcomes.push(SlotCompactionOutcomeDto {
                slot_id: member.slot_id.clone(),
                name: member.name.clone(),
                role: member.role.clone(),
                conversation_id: member.conversation_id.clone(),
                strategy_used: strategy.clone(),
                strategy,
                before_tokens,
                after_tokens,
                previous_tokens: before_tokens,
                new_tokens: after_tokens,
                before_percent: before_pct,
                after_percent,
                compaction_count: next_compaction_count,
                summary_chars,
            });
        }

        let updated_status = self.get_team_context_status(user_id, team_id).await?;

        self.event_bus.broadcast(WebSocketMessage::new(
            "team:context_compacted",
            serde_json::json!({
                "user_id": user_id,
                "team_id": team_id,
                "outcomes": outcomes,
                "auto_compact_enabled": updated_status.auto_compact_enabled,
                "auto_compact_threshold_pct": updated_status.auto_compact_threshold_pct,
            }),
        ));
        self.broadcast_context_usage(user_id, &updated_status);

        Ok(CompactTeamContextResponse {
            team_id: team_id.to_owned(),
            auto_compact_enabled: updated_status.auto_compact_enabled,
            auto_compact_threshold_pct: updated_status.auto_compact_threshold_pct,
            economy_mode: updated_status.economy_mode.clone(),
            max_context_tokens: updated_status.max_context_tokens,
            tokens_saved_estimate: updated_status.tokens_saved_estimate,
            coalesced_wakes_saved: updated_status.coalesced_wakes_saved,
            outcomes,
            context_status: updated_status,
        })
    }

    async fn count_conversation_messages(&self, conversation_id: &str) -> i64 {
        self.bridge_repo
            .count_conversation_messages(conversation_id)
            .await
            .unwrap_or(0)
    }

    async fn resolve_provider_for_aionrs(
        &self,
        user_id: &str,
        explicit_provider_id: Option<&str>,
        model_id: &str,
    ) -> Result<String, ApiError> {
        if let Some(pid) = explicit_provider_id.map(str::trim).filter(|s| !s.is_empty()) {
            let found = self
                .provider_repo
                .find_by_id(user_id, pid)
                .await
                .map_err(|e| ApiError::Internal(format!("Provider lookup error: {e}")))?;
            if found.is_none() {
                return Err(ApiError::BadRequest(format!("Provider '{pid}' not found")));
            }
            return Ok(pid.to_owned());
        }

        let providers = self
            .provider_repo
            .list(user_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Provider list error: {e}")))?;
        for p in &providers {
            if !p.enabled {
                continue;
            }
            let models: Vec<String> = serde_json::from_str(&p.models).unwrap_or_default();
            if models.iter().any(|m| m.trim() == model_id.trim()) {
                return Ok(p.id.clone());
            }
        }
        if let Some(first_enabled) = providers.into_iter().find(|p| p.enabled) {
            return Ok(first_enabled.id);
        }
        Err(ApiError::BadRequest(
            "provider_id is required when switching to aionrs without an enabled matching provider".into(),
        ))
    }

    /// `POST /api/team-hub/teams/{id}/apply-quota-preset`
    pub async fn apply_quota_preset(
        &self,
        user_id: &str,
        team_id: &str,
        req: ApplyQuotaPresetRequest,
    ) -> Result<SwitchTeamModelsResponse, ApiError> {
        let preset = req
            .preset
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("smart_split")
            .to_owned();
        self.switch_team_models(
            user_id,
            team_id,
            SwitchTeamModelsRequest {
                scope: Some("slots".to_owned()),
                preset: Some(preset),
                target: None,
                updates: Vec::new(),
                force_interrupt: req.force_interrupt,
            },
        )
        .await
    }

    /// `POST /api/team-hub/teams/{id}/switch-models`
    pub async fn switch_team_models(
        &self,
        user_id: &str,
        team_id: &str,
        req: SwitchTeamModelsRequest,
    ) -> Result<SwitchTeamModelsResponse, ApiError> {
        let _ = self.assistant_service.list_for_user(user_id).await;

        let team_row = self
            .team_repo
            .get_team(user_id, team_id)
            .await
            .map_err(|e| ApiError::Internal(format!("Team lookup failed: {e}")))?
            .ok_or_else(|| ApiError::NotFound(format!("Team not found: {team_id}")))?;
        let mut parsed_team =
            Team::from_row(&team_row).map_err(|e| ApiError::Internal(format!("Invalid team roster: {e}")))?;

        let force_interrupt = req.force_interrupt.unwrap_or(false);

        // Build `(slot_id, ModelTargetSpec)` plan (supporting `preset: "smart_split" | "quota_saver" | "all_free_cli"`)
        let mut plan: Vec<(String, ModelTargetSpec)> = Vec::new();
        let scope = if let Some(raw_preset) = req
            .preset
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let normalized_preset = match raw_preset.to_ascii_lowercase().as_str() {
                "smart_split" | "quota_saver" => "smart_split",
                "all_free_cli" | "free_cli" => "all_free_cli",
                other => {
                    return Err(ApiError::BadRequest(format!(
                        "Unsupported quota preset '{other}': expected 'smart_split' or 'all_free_cli'"
                    )));
                }
            };
            let catalog = self.list_connected_models(user_id).await?;
            let preset_dto = catalog
                .recommended_presets
                .iter()
                .find(|p| p.id == normalized_preset)
                .or_else(|| catalog.recommended_presets.first())
                .ok_or_else(|| {
                    ApiError::BadRequest("No connected models available to apply quota preset".into())
                })?;

            let wallet_hard_blocked = self
                .bridge_repo
                .is_wallet_hard_blocked(user_id)
                .await
                .unwrap_or(false);

            for agent in &parsed_team.agents {
                if agent.role == aionui_team::types::TeammateRole::Lead {
                    if normalized_preset == "all_free_cli" {
                        if let Some(ref lt) = preset_dto.lead_target.as_ref().or(preset_dto.worker_target.as_ref()) {
                            plan.push((agent.slot_id.clone(), (*lt).clone()));
                        }
                    } else if wallet_hard_blocked
                        && Self::classify_slot_wallet(&agent.backend, &agent.model) == WalletType::ApiKey
                        && let Some(free_lead) = catalog
                            .recommended_presets
                            .iter()
                            .find(|p| p.id == "all_free_cli")
                            .and_then(|p| p.lead_target.as_ref().or(p.worker_target.as_ref()))
                    {
                        plan.push((agent.slot_id.clone(), free_lead.clone()));
                    }
                } else if let Some(ref wt) = preset_dto.worker_target.as_ref().or(preset_dto.lead_target.as_ref()) {
                    plan.push((agent.slot_id.clone(), (*wt).clone()));
                }
            }
            format!("preset:{normalized_preset}")
        } else {
            let explicit_scope = req
                .scope
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("all")
                .to_owned();
            match explicit_scope.as_str() {
                "all" => {
                    let target = req
                        .target
                        .clone()
                        .ok_or_else(|| ApiError::BadRequest("target is required when scope is 'all'".into()))?;
                    for agent in &parsed_team.agents {
                        plan.push((agent.slot_id.clone(), target.clone()));
                    }
                }
                "teammates_only" => {
                    let target = req.target.clone().ok_or_else(|| {
                        ApiError::BadRequest("target is required when scope is 'teammates_only'".into())
                    })?;
                    for agent in &parsed_team.agents {
                        if agent.role != aionui_team::types::TeammateRole::Lead {
                            plan.push((agent.slot_id.clone(), target.clone()));
                        }
                    }
                }
                "slots" => {
                    if req.updates.is_empty() {
                        if let Some(ref target) = req.target {
                            for agent in &parsed_team.agents {
                                plan.push((agent.slot_id.clone(), target.clone()));
                            }
                        } else {
                            return Err(ApiError::BadRequest(
                                "updates must not be empty when scope is 'slots'".into(),
                            ));
                        }
                    } else {
                        for upd in &req.updates {
                            plan.push((upd.slot_id.clone(), upd.effective_target()));
                        }
                    }
                }
                other => {
                    return Err(ApiError::BadRequest(format!(
                        "Invalid scope '{other}': expected 'all', 'teammates_only', or 'slots'"
                    )));
                }
            }
            explicit_scope
        };

        let mut results = Vec::with_capacity(plan.len());
        let mut roster_mutated = false;

        for (slot_id, raw_target) in plan {
            let agent_idx = parsed_team
                .agents
                .iter()
                .position(|a| a.slot_id == slot_id)
                .ok_or_else(|| ApiError::NotFound(format!("Slot '{slot_id}' not found in team '{team_id}'")))?;

            let effective_backend = if raw_target.backend.trim().is_empty() {
                if raw_target.provider_id.as_deref().is_some_and(|p| !p.trim().is_empty()) {
                    "aionrs".to_owned()
                } else {
                    return Err(ApiError::BadRequest("target.backend must not be empty".into()));
                }
            } else {
                raw_target.backend.trim().to_owned()
            };
            let effective_model_id = if raw_target.model_id.trim().is_empty() {
                effective_backend.clone()
            } else {
                raw_target.model_id.trim().to_owned()
            };

            let (target_agent_id, target_agent_type, target_yolo_id) = self
                .resolve_agent_identity_for_backend(user_id, &effective_backend)
                .await?;

            let resolved_provider_id = if target_agent_type == AgentType::Aionrs {
                Some(
                    self.resolve_provider_for_aionrs(
                        user_id,
                        raw_target.provider_id.as_deref(),
                        &effective_model_id,
                    )
                    .await?,
                )
            } else {
                None
            };

            let prev_backend = parsed_team.agents[agent_idx].backend.clone();
            let prev_model = parsed_team.agents[agent_idx].model.clone();
            let conv_id = parsed_team.agents[agent_idx].conversation_id.clone();
            let member_name = parsed_team.agents[agent_idx].name.clone();
            let member_role = parsed_team.agents[agent_idx].role.to_string();

            // Step 1: Extract & stage AnchoredSummary whenever conversation has >= 1 message (or prior summary)
            let msg_count = self.count_conversation_messages(&conv_id).await;
            let prev_anchor = self
                .bridge_repo
                .get_team_context_anchor(team_id, &slot_id)
                .await
                .ok()
                .flatten();
            let has_history = msg_count > 0
                || prev_anchor
                    .as_ref()
                    .is_some_and(|a| !a.anchored_summary_md.trim().is_empty());

            let handover_injected = if has_history {
                let extracted = AnchoredHandoverEngine::extract_and_merge_anchor(
                    &self.bridge_repo,
                    team_id,
                    &slot_id,
                    &conv_id,
                )
                .await
                .map_err(|e| ApiError::Internal(format!("Handover extraction failed: {e}")))?;

                let files_json = serde_json::to_string(&extracted.files_touched).unwrap_or_else(|_| "[]".into());
                let compaction_count = prev_anchor.map(|a| a.compaction_count).unwrap_or(0);
                self.bridge_repo
                    .upsert_team_context_anchor(UpsertTeamContextAnchorParams {
                        team_id: team_id.to_owned(),
                        slot_id: slot_id.clone(),
                        conversation_id: conv_id.clone(),
                        anchored_summary_md: extracted.summary_md,
                        files_touched_json: files_json,
                        last_compacted_msg_id: extracted.latest_msg_id,
                        compaction_count,
                        pending_handover: true,
                    })
                    .await
                    .map_err(|e| ApiError::Internal(format!("Failed to persist handover anchor: {e}")))?;
                true
            } else {
                false
            };

            // Step 2: Check if slot is mid-turn and `!force_interrupt`
            if self.team_service.is_slot_turn_running(team_id, &slot_id).await && !force_interrupt {
                self.bridge_repo
                    .upsert_pending_slot_swap(
                        team_id,
                        &slot_id,
                        &effective_backend,
                        resolved_provider_id.as_deref(),
                        &effective_model_id,
                    )
                    .await
                    .map_err(|e| ApiError::Internal(format!("Failed to queue model swap: {e}")))?;

                let wallet_type = Self::classify_slot_wallet(&effective_backend, &effective_model_id)
                    .as_str()
                    .to_owned();
                results.push(SlotModelSwitchResultDto {
                    slot_id,
                    name: member_name,
                    role: member_role,
                    conversation_id: conv_id,
                    previous_backend: prev_backend,
                    previous_model: prev_model.clone(),
                    previous_model_id: prev_model,
                    new_backend: effective_backend.clone(),
                    backend: effective_backend,
                    new_provider_id: resolved_provider_id.clone(),
                    provider_id: resolved_provider_id,
                    new_model: effective_model_id.clone(),
                    model_id: effective_model_id,
                    wallet_type,
                    status: "queued_after_turn".to_owned(),
                    handover_injected,
                });
                continue;
            }

            // Step 3: Execute immediate atomic cross-engine rebind
            let _ = self.bridge_repo.clear_pending_slot_swap(team_id, &slot_id).await;

            let conv_row = self
                .conversation_repo
                .get(user_id, &conv_id)
                .await
                .map_err(|e| ApiError::Internal(format!("Conversation lookup failed: {e}")))?
                .ok_or_else(|| ApiError::NotFound(format!("Conversation '{conv_id}' not found")))?;

            let mut extra: serde_json::Value =
                serde_json::from_str(&conv_row.extra).unwrap_or_else(|_| serde_json::json!({}));
            if !extra.is_object() {
                extra = serde_json::json!({});
            }

            let session_mode = target_yolo_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| target_agent_type.full_auto_mode_id(Some(&effective_backend)).to_owned());

            extra["backend"] = serde_json::Value::String(effective_backend.clone());
            extra["agent_id"] = serde_json::Value::String(target_agent_id.clone());
            extra["current_model_id"] = serde_json::Value::String(effective_model_id.clone());
            extra["session_mode"] = serde_json::Value::String(session_mode.clone());
            if let Some(rt_seed) = extra
                .get_mut("team")
                .and_then(serde_json::Value::as_object_mut)
                .and_then(|t| t.get_mut("runtime_seed"))
                .and_then(serde_json::Value::as_object_mut)
            {
                rt_seed.insert("session_mode".into(), serde_json::Value::String(session_mode));
            }

            let (new_conv_type, new_model_json) = if target_agent_type == AgentType::Aionrs {
                let pid = resolved_provider_id.clone().unwrap_or_default();
                extra["provider_id"] = serde_json::Value::String(pid.clone());
                let pwm = ProviderWithModel {
                    provider_id: pid,
                    model: effective_model_id.clone(),
                    use_model: Some(effective_model_id.clone()),
                };
                let encoded =
                    serde_json::to_string(&pwm).map_err(|e| ApiError::Internal(format!("Encode model error: {e}")))?;
                ("aionrs", Some(encoded))
            } else {
                extra["provider_id"] = serde_json::Value::String(effective_backend.clone());
                (target_agent_type.serde_name(), None)
            };

            let new_extra_json =
                serde_json::to_string(&extra).map_err(|e| ApiError::Internal(format!("Encode extra error: {e}")))?;

            self.bridge_repo
                .rebind_conversation_engine(
                    user_id,
                    &conv_id,
                    new_conv_type,
                    new_model_json.as_deref(),
                    &new_extra_json,
                )
                .await
                .map_err(|e| ApiError::Internal(format!("Conversation rebind failed: {e}")))?;

            if prev_backend != effective_backend || target_agent_type == AgentType::Aionrs || prev_backend == "aionrs" {
                self.remove_aionrs_session_files(&conv_id, Some(&team_row.workspace));
            }

            // Update team roster entry (`backend`, `model`, and `bare:*` assistant_id if generated)
            parsed_team.agents[agent_idx].backend = effective_backend.clone();
            parsed_team.agents[agent_idx].model = effective_model_id.clone();
            if parsed_team.agents[agent_idx]
                .assistant_id
                .as_deref()
                .is_none_or(|aid| aid.starts_with("bare:"))
            {
                parsed_team.agents[agent_idx].assistant_id = Some(format!("bare:{target_agent_id}"));
            }
            roster_mutated = true;

            let wallet_type = Self::classify_slot_wallet(&effective_backend, &effective_model_id)
                .as_str()
                .to_owned();

            results.push(SlotModelSwitchResultDto {
                slot_id,
                name: member_name,
                role: member_role,
                conversation_id: conv_id,
                previous_backend: prev_backend,
                previous_model: prev_model.clone(),
                previous_model_id: prev_model,
                new_backend: effective_backend.clone(),
                backend: effective_backend,
                new_provider_id: resolved_provider_id.clone(),
                provider_id: resolved_provider_id,
                new_model: effective_model_id.clone(),
                model_id: effective_model_id,
                wallet_type,
                status: "switched".to_owned(),
                handover_injected,
            });
        }

        if roster_mutated {
            let agents_json = serde_json::to_string(&parsed_team.agents)
                .map_err(|e| ApiError::Internal(format!("Serialize roster error: {e}")))?;
            self.team_repo
                .update_team(
                    user_id,
                    team_id,
                    &UpdateTeamParams {
                        agents: Some(agents_json),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| ApiError::Internal(format!("Persist team roster failed: {e}")))?;

            // Restart/refresh every immediately switched slot on live session + re-arm role prompt
            for r in &results {
                if r.status == "switched" {
                    let _ = self
                        .team_service
                        .interrupt_or_restart_slot_for_maintenance(user_id, team_id, &r.slot_id, force_interrupt)
                        .await;
                    self.team_service
                        .mark_slot_needs_role_prompt(team_id, &r.slot_id)
                        .await;
                }
            }
        }

        let updated_team = self
            .team_service
            .get_team(user_id, team_id)
            .await
            .map_err(ApiError::from)?;
        let (_, lead_wallet) = crate::bridge::hub::TelegramTeamHub::classify_team_lead_wallet(&updated_team);
        let lead_wallet_str = lead_wallet.as_str().to_owned();
        let context_status = self.get_team_context_status(user_id, team_id).await?;

        self.event_bus.broadcast(WebSocketMessage::new(
            "team:models_switched",
            serde_json::json!({
                "user_id": user_id,
                "team_id": team_id,
                "scope": scope,
                "lead_wallet_type": lead_wallet_str,
                "results": results,
                "outcomes": results,
                "team": updated_team,
            }),
        ));
        self.broadcast_context_usage(user_id, &context_status);

        Ok(SwitchTeamModelsResponse {
            team_id: team_id.to_owned(),
            scope,
            lead_wallet_type: lead_wallet_str,
            outcomes: results.clone(),
            results,
            team: updated_team,
            context_status,
        })
    }

    /// Turn-boundary hook invoked by `TelegramTeamHub::on_slot_turn_settled`:
    /// 1. Executes any queued `custom_team_slot_swaps` for `(team_id, slot_id)`.
    /// 2. Broadcasts `team:context_usage`.
    /// 3. Triggers turn-boundary Auto-Compaction when `auto_compact_enabled` and `usage_percent >= threshold_pct`.
    pub async fn handle_slot_turn_settled(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        conversation_id: &str,
    ) {
        let slot_mutex = self
            .slot_locks
            .entry((team_id.to_owned(), slot_id.to_owned()))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = slot_mutex.lock().await;

        // 1. Drain pending model swap if queued while the slot was running
        if let Ok(Some(pending_swap)) = self.bridge_repo.take_pending_slot_swap(team_id, slot_id).await {
            let swap_req = SwitchTeamModelsRequest {
                scope: Some("slots".to_owned()),
                preset: None,
                target: None,
                updates: vec![PerSlotModelUpdate {
                    slot_id: slot_id.to_owned(),
                    backend: pending_swap.target_backend,
                    provider_id: pending_swap.target_provider_id,
                    model_id: pending_swap.target_model_id,
                    target: None,
                }],
                force_interrupt: Some(false),
            };
            if let Err(err) = self.switch_team_models(user_id, team_id, swap_req).await {
                warn!(
                    team_id,
                    slot_id,
                    error = %err,
                    "deferred turn-settle model swap failed"
                );
            }
        }

        // 2. Meter real per-slot token delta before computing live context status
        if let Ok((msg_count, total_bytes)) = self
            .bridge_repo
            .estimate_conversation_tokens_fast(conversation_id)
            .await
        {
            let mut slot_wallet = WalletType::CliSubscription;
            let mut slot_model_id = String::new();
            let mut slot_backend = String::new();
            let mut slot_role = "teammate".to_owned();
            if let Ok(team_snap) = self.team_service.get_team(user_id, team_id).await
                && let Some(m) = team_snap.assistants.iter().find(|a| a.slot_id == slot_id)
            {
                slot_model_id = m.model.clone();
                slot_backend = m.backend.clone();
                slot_role = m.role.clone();
                let has_custom_api_key = self
                    .bridge_repo
                    .slot_has_custom_api_key_in_db(
                        user_id,
                        conversation_id,
                        &m.backend,
                        m.assistant_id.as_deref(),
                    )
                    .await;
                slot_wallet = Self::classify_slot_wallet_ext(
                    &m.backend,
                    &m.assistant_backend,
                    None,
                    &m.model,
                    None,
                    has_custom_api_key,
                );
            }
            let prev_econ = self
                .bridge_repo
                .get_team_slot_economy(team_id, slot_id)
                .await
                .ok()
                .flatten();
            let prev_bytes = prev_econ.as_ref().map(|e| e.last_seen_char_sum).unwrap_or(0);
            let prev_usage_tokens = prev_econ.as_ref().map(|e| e.last_seen_usage_tokens).unwrap_or(0);
            let prev_meta_seq = prev_econ.as_ref().map(|e| e.last_seen_meta_seq).unwrap_or(0);
            let unbilled_wake = prev_econ.as_ref().map(|e| e.unbilled_wake_chars).unwrap_or(0);
            let mut usage_val = self
                .conversation_service
                .get_usage(user_id, conversation_id)
                .await
                .ok()
                .flatten();
            // Claude emits `UsageDelta` immediately AFTER `TurnResult` on the `result` frame,
            // so `on_slot_turn_settled` can race `persist_context_usage` by a few milliseconds.
            if slot_backend.eq_ignore_ascii_case("claude") {
                let current_seq = usage_val
                    .as_ref()
                    .and_then(|v| v.get("_meta"))
                    .and_then(|m| m.get("seq"))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                if current_seq == 0 || current_seq == prev_meta_seq {
                    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                    if let Ok(Some(refreshed)) = self.conversation_service.get_usage(user_id, conversation_id).await {
                        usage_val = Some(refreshed);
                    }
                }
            }
            let meta_obj = usage_val.as_ref().and_then(|v| v.get("_meta"));
            let reported_meta_seq = meta_obj
                .and_then(|m| m.get("seq"))
                .and_then(serde_json::Value::as_i64);
            let reported_total_tokens = usage_val.as_ref().and_then(|v| {
                v.get("used")
                    .or_else(|| v.get("totalTokens"))
                    .or_else(|| v.get("total_tokens"))
                    .and_then(serde_json::Value::as_i64)
            });
            let reported_turn_total_tokens = meta_obj
                .and_then(|m| m.get("turn_total_tokens").or_else(|| m.get("turnTotalTokens")))
                .and_then(serde_json::Value::as_i64);
            let reported_input_tokens = meta_obj
                .and_then(|m| m.get("input_tokens").or_else(|| m.get("inputTokens")))
                .or_else(|| usage_val.as_ref().and_then(|v| v.get("input_tokens").or_else(|| v.get("inputTokens"))))
                .and_then(serde_json::Value::as_i64);
            let reported_output_tokens = meta_obj
                .and_then(|m| m.get("output_tokens").or_else(|| m.get("outputTokens")))
                .or_else(|| usage_val.as_ref().and_then(|v| v.get("output_tokens").or_else(|| v.get("outputTokens"))))
                .and_then(serde_json::Value::as_i64);
            let reported_cached_read_tokens = meta_obj
                .and_then(|m| m.get("cached_read_tokens").or_else(|| m.get("cachedReadTokens")))
                .or_else(|| {
                    usage_val
                        .as_ref()
                        .and_then(|v| v.get("cached_read_tokens").or_else(|| v.get("cachedReadTokens")))
                })
                .and_then(serde_json::Value::as_i64);

            let meta_advanced = reported_meta_seq.is_some_and(|s| s > 0 && s != prev_meta_seq);
            let usage_advanced =
                meta_advanced || reported_total_tokens.is_some_and(|t| t > 0 && t != prev_usage_tokens);
            if (msg_count > 0 && (total_bytes > prev_bytes || unbilled_wake > 0)) || usage_advanced {
                let _ = self
                    .bridge_repo
                    .settle_slot_economy_turn(SettleSlotEconomyParams {
                        team_id: team_id.to_owned(),
                        slot_id: slot_id.to_owned(),
                        owner_user_id: user_id.to_owned(),
                        role: slot_role,
                        conversation_id: conversation_id.to_owned(),
                        wallet_type: slot_wallet,
                        backend: slot_backend,
                        model_id: slot_model_id,
                        current_msg_count: msg_count,
                        current_char_sum: total_bytes,
                        reported_total_tokens,
                        reported_turn_total_tokens,
                        reported_input_tokens,
                        reported_output_tokens,
                        reported_cached_read_tokens,
                        reported_meta_seq,
                    })
                    .await;
            }
        }

        // 3. Compute & broadcast live context usage
        let Ok(status) = self.get_team_context_status(user_id, team_id).await else {
            return;
        };
        self.broadcast_context_usage(user_id, &status);

        // 4. Autonomous Runaway Loop Fuse (`max_turn_hops_per_run`)
        {
            let mut entry = self.active_run_turn_hops.entry(team_id.to_owned()).or_insert(0);
            *entry += 1;
            let current_hops = *entry;
            drop(entry);
            let max_hops = status.max_turn_hops_per_run.max(4);
            if current_hops > max_hops {
                self.active_run_turn_hops.remove(team_id);
                let summary_text = format!(
                    "Autonomous loop fuse paused team '{team_id}' after {current_hops} consecutive slot turns (limit: {max_hops})."
                );
                if let Ok(card) = self
                    .bridge_repo
                    .create_action_inbox_item(CreateActionInboxParams {
                        id: None,
                        owner_user_id: user_id.to_owned(),
                        category: "loop_fuse".to_owned(),
                        title: format!("Loop Fuse Paused ({current_hops}/{max_hops} turns)"),
                        summary: summary_text.clone(),
                        details_json: serde_json::json!({
                            "team_id": team_id,
                            "slot_id": slot_id,
                            "turn_hops": current_hops,
                            "max_turn_hops_per_run": max_hops,
                        })
                        .to_string(),
                        target_type: Some("team".to_owned()),
                        target_id: team_id.to_owned(),
                        action_token: None,
                        telegram_chat_id: None,
                        telegram_msg_id: None,
                    })
                    .await
                {
                    self.event_bus.broadcast(WebSocketMessage::new(
                        "inbox:item_added",
                        serde_json::json!({
                            "user_id": user_id,
                            "item": crate::routes::ActionInboxCardDto::from_row(card),
                        }),
                    ));
                }
                self.event_bus.broadcast(WebSocketMessage::new(
                    "team:loop_fuse_tripped",
                    serde_json::json!({
                        "user_id": user_id,
                        "team_id": team_id,
                        "slot_id": slot_id,
                        "turn_hops": current_hops,
                        "max_turn_hops_per_run": max_hops,
                    }),
                ));
            }
        }

        if !status.auto_compact_enabled {
            return;
        }
        let Some(slot_status) = status.slots.iter().find(|s| s.slot_id == slot_id) else {
            return;
        };
        // Window-Calibrated Compactor: trigger at `min(threshold_pct * max_tokens, max_context_tokens)`
        let hit_threshold = slot_status.usage_percent >= status.auto_compact_threshold_pct
            || (slot_status.effective_compact_limit_tokens > 0
                && slot_status.used_tokens >= slot_status.effective_compact_limit_tokens);
        if !hit_threshold {
            return;
        }

        // Guard against compacting repeatedly on zero new messages
        let latest_msg_id = self
            .bridge_repo
            .get_latest_conversation_message_id(conversation_id)
            .await
            .ok()
            .flatten();

        let anchor = self
            .bridge_repo
            .get_team_context_anchor(team_id, slot_id)
            .await
            .ok()
            .flatten();

        if let (Some(latest), Some(anc)) = (latest_msg_id.as_deref(), anchor.as_ref())
            && anc.last_compacted_msg_id.as_deref() == Some(latest)
        {
            return;
        }

        info!(
            team_id,
            slot_id,
            used_tokens = slot_status.used_tokens,
            effective_limit = slot_status.effective_compact_limit_tokens,
            usage_percent = slot_status.usage_percent,
            threshold = status.auto_compact_threshold_pct,
            "auto-compacting slot context at safe turn boundary"
        );
        let _ = self
            .compact_team_context(
                user_id,
                team_id,
                CompactTeamContextRequest {
                    slot_id: Some(slot_id.to_owned()),
                    mode: Some("rollover".to_owned()),
                    auto_compact_enabled: None,
                    threshold_pct: None,
                    economy_mode: None,
                    max_context_tokens: None,
                    max_turn_hops_per_run: None,
                    auto_fallback_free_cli: None,
                    enforce_hard_cap_on_cli: None,
                    force_interrupt: Some(false),
                },
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_secrets_redacts_sk_bearer_and_apikey_while_keeping_task_ids() {
        let raw = "Task task-123 and disk-2 ok, key is sk-proj-AbCdEf123456_xyz, header Authorization: Bearer eyJhbGciOiJIUzI1NiIs, and api_key: \"secret_998877\" plus OPENAI_API_KEY=my_top_secret!";
        let scrubbed = AnchoredHandoverEngine::scrub_secrets(raw);
        assert!(scrubbed.contains("task-123"), "must not corrupt task-123: {scrubbed}");
        assert!(scrubbed.contains("disk-2"), "must not corrupt disk-2: {scrubbed}");
        assert!(!scrubbed.contains("sk-proj-AbCdEf123456_xyz"));
        assert!(!scrubbed.contains("eyJhbGciOiJIUzI1NiIs"));
        assert!(!scrubbed.contains("secret_998877"));
        assert!(!scrubbed.contains("my_top_secret"));
        assert!(scrubbed.contains("[REDACTED_SECRET]"));
        assert!(scrubbed.contains("Bearer [REDACTED_TOKEN]"));
        assert!(scrubbed.contains("api_key=[REDACTED_KEY]"));
    }

    #[test]
    fn scrub_secrets_redacts_prefixed_unprefixed_and_url_encoded_botfather_tokens() {
        let raw = "Telegram failed on https://api.telegram.org/bot7123456789:AAH_abcdefghijklmnopqrstuvwxyz012345/getUpdates \
                   and https://api.telegram.org/file/bot8123456789%3ABBH_0123456789abcdefghijklmnopqrstuv/doc.pdf \
                   plus raw 9123456789:CCH_abcdefghijklmnopqrstuvwxyz987654 and 6123456789%3aDDH_abcdefghijklmnopqrstuvwxyz456789 \
                   at src/api.rs:123 on 2026-11-12T15:22:55Z";
        let scrubbed = AnchoredHandoverEngine::scrub_secrets(raw);
        for leaked in [
            "7123456789",
            "8123456789",
            "9123456789",
            "6123456789",
            "AAH_abcdefghijklmnopqrstuvwxyz012345",
            "BBH_0123456789abcdefghijklmnopqrstuv",
            "CCH_abcdefghijklmnopqrstuvwxyz987654",
            "DDH_abcdefghijklmnopqrstuvwxyz456789",
        ] {
            assert!(!scrubbed.contains(leaked), "leaked {leaked}: {scrubbed}");
        }
        assert!(scrubbed.contains("bot[REDACTED]/getUpdates"), "{scrubbed}");
        assert!(scrubbed.contains("file/bot[REDACTED]/doc.pdf"), "{scrubbed}");
        assert!(scrubbed.contains("src/api.rs:123"), "{scrubbed}");
        assert!(scrubbed.contains("2026-11-12T15:22:55Z"), "{scrubbed}");
    }

    #[test]
    fn infer_max_context_tokens_scopes_default_1m_to_antigravity_and_gemini() {
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("antigravity", "default", None),
            1_000_000
        );
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("gemini", "default", None),
            1_000_000
        );
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("gemini-cli", "", None),
            1_000_000
        );
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("claude", "default", None),
            200_000
        );
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("codex", "default", None),
            200_000
        );
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("claude", "claude-3-5-haiku", None),
            128_000
        );
        assert_eq!(
            TeamHubOrchestrator::infer_max_context_tokens("aionrs", "custom-model", Some(256_000)),
            256_000
        );
    }

    #[test]
    fn cli_subscription_uses_window_pct_while_api_key_enforces_economy_cap() {
        // $0 CLI OAuth on 1M window @ 80% -> 800,000 (not 160,000!) unless opt-in `enforce_hard_cap_on_cli`
        assert_eq!(
            TeamHubOrchestrator::compute_effective_compact_limit_tokens(
                WalletType::CliSubscription,
                1_000_000,
                80,
                160_000,
                false,
            ),
            800_000
        );
        assert_eq!(
            TeamHubOrchestrator::compute_effective_compact_limit_tokens(
                WalletType::CliSubscription,
                1_000_000,
                80,
                160_000,
                true,
            ),
            160_000
        );
        // Paid API key on 1M window @ 80% -> capped at `max_context_tokens` (160,000)
        assert_eq!(
            TeamHubOrchestrator::compute_effective_compact_limit_tokens(
                WalletType::ApiKey,
                1_000_000,
                80,
                160_000,
                false,
            ),
            160_000
        );

        // Shared `slot_has_custom_api_key` elevates CLI slot to `WalletType::ApiKey`
        let has_key = slot_has_custom_api_key(
            Some(r#"{"custom_env":{"ANTHROPIC_API_KEY":"sk-ant-live"}}"#),
            None,
            None,
        );
        assert!(has_key);
        assert_eq!(
            TeamHubOrchestrator::classify_slot_wallet_ext("claude", "", None, "opus", None, has_key),
            WalletType::ApiKey
        );
        assert_eq!(
            TeamHubOrchestrator::classify_slot_wallet_ext("claude", "", None, "opus", None, false),
            WalletType::CliSubscription
        );
    }
}

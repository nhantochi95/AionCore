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
use aionui_db::custom_bridge_schema::{WalletType, classify_agent_wallet};
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
pub struct ConnectedModelsCatalogDto {
    pub cli_agents: Vec<ConnectedEngineCatalogItemDto>,
    pub api_providers: Vec<ConnectedEngineCatalogItemDto>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamContextStatusResponse {
    pub team_id: String,
    pub auto_compact_enabled: bool,
    pub auto_compact_threshold_pct: i64,
    pub slots: Vec<SlotContextStatusDto>,
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
    pub target: Option<ModelTargetSpec>,
    #[serde(default)]
    pub updates: Vec<PerSlotModelUpdate>,
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
    /// Scrubs API keys and auth tokens (`sk-[A-Za-z0-9_-]+`, `Bearer <tok>`, `api[_-]?key[:=]<tok>`)
    /// from deterministic summaries before persisting or injecting into prompts.
    pub fn scrub_secrets(input: &str) -> String {
        let step1 = Self::scrub_sk_tokens(input);
        let step2 = Self::scrub_bearer_tokens(&step1);
        Self::scrub_api_key_assignments(&step2)
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
}

impl TeamHubOrchestrator {
    pub fn classify_slot_wallet(backend: &str, model_id: &str) -> WalletType {
        let b = backend.trim().to_lowercase();
        if b == "aionrs" {
            return WalletType::ApiKey;
        }
        let hint = if b == "acp" || b.is_empty() || b == "custom" {
            let m = model_id.trim().to_lowercase();
            if m.starts_with("claude") && !m.contains('/') {
                "claude"
            } else if m.starts_with("codex") && !m.contains('/') {
                "codex"
            } else if m.starts_with("gemini") && !m.contains('/') {
                "gemini"
            } else if m.starts_with("antigravity") {
                "antigravity"
            } else {
                m.as_str()
            }
            .to_owned()
        } else {
            b
        };
        classify_agent_wallet(Some(&hint), None, false)
    }

    pub fn infer_max_context_tokens(model_id: &str, provider_limit: Option<i64>) -> i64 {
        if let Some(limit) = provider_limit
            && limit > 0
        {
            return limit;
        }
        let lower = model_id.trim().to_ascii_lowercase();
        if lower.contains("gemini") {
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
            let wallet_type = classify_agent_wallet(
                Some(backend),
                meta_row.as_ref().and_then(|m| m.auth_methods.as_deref()),
                false,
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

        Ok(ConnectedModelsCatalogDto {
            cli_agents,
            api_providers,
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
    ) -> i64 {
        let rows = self
            .bridge_repo
            .list_conversation_messages_for_anchor(conversation_id)
            .await
            .unwrap_or_default();

        let start_idx = if let Some(last_id) = last_compacted_msg_id {
            rows.iter()
                .position(|r| r.id == last_id || r.msg_id.as_deref() == Some(last_id))
                .map(|i| i + 1)
                .unwrap_or(0)
        } else {
            0
        };

        let delta_chars: usize = rows[start_idx..].iter().map(|r| r.content.chars().count()).sum();
        let anchor_chars: usize = anchored_summary_md.map(|s| s.chars().count()).unwrap_or(0);
        ((delta_chars + anchor_chars) / 4) as i64
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

        let providers: HashMap<String, _> = self
            .provider_repo
            .list(user_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|p| (p.id.clone(), p))
            .collect();

        let auto_enabled = pref.is_auto_compact_enabled();
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
                .unwrap_or_else(|| Self::infer_max_context_tokens(&model_id, provider_limit));

            let anchor = anchors.get(&member.slot_id);
            let has_anchored_summary = anchor
                .map(|a| !a.anchored_summary_md.trim().is_empty())
                .unwrap_or(false);
            let compaction_count = anchor.map(|a| a.compaction_count).unwrap_or(0);

            let used_tokens = match reported_used {
                Some(u) if u > 0 => u,
                _ => {
                    self.estimate_tokens_from_messages(
                        &member.conversation_id,
                        anchor.and_then(|a| a.last_compacted_msg_id.as_deref()),
                        anchor
                            .map(|a| a.anchored_summary_md.as_str())
                            .filter(|s| !s.trim().is_empty()),
                    )
                    .await
                }
            };

            let usage_percent = ((used_tokens.max(0) as f64 / max_tokens.max(1) as f64) * 100.0)
                .round()
                .clamp(0.0, 100.0) as i64;
            let level = if usage_percent >= 85 {
                "critical"
            } else if usage_percent >= 70 {
                "warning"
            } else {
                "normal"
            }
            .to_owned();

            let wallet_type = Self::classify_slot_wallet(&backend, &model_id)
                .as_str()
                .to_owned();

            let pending_model_swap = swaps.get(&member.slot_id).map(|s| ModelTargetSpec {
                backend: s.target_backend.clone(),
                provider_id: s.target_provider_id.clone(),
                model_id: s.target_model_id.clone(),
            });

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
            });
        }

        Ok(TeamContextStatusResponse {
            team_id: team_id.to_owned(),
            auto_compact_enabled: auto_enabled,
            auto_compact_threshold_pct: pref.threshold_pct,
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

        if req.auto_compact_enabled.is_some() || req.threshold_pct.is_some() {
            let next_enabled = req.auto_compact_enabled.unwrap_or_else(|| pref.is_auto_compact_enabled());
            let next_threshold = req.threshold_pct.unwrap_or(pref.threshold_pct);
            pref = self
                .bridge_repo
                .upsert_team_context_pref(team_id, next_enabled, next_threshold)
                .await
                .map_err(|e| ApiError::Internal(format!("Failed to update context prefs: {e}")))?;
        }

        let is_prefs_only = req.mode.as_deref() == Some("prefs_only")
            || ((req.auto_compact_enabled.is_some() || req.threshold_pct.is_some())
                && req.mode.is_none()
                && req.slot_id.is_none());

        if is_prefs_only {
            let status = self.get_team_context_status(user_id, team_id).await?;
            self.broadcast_context_usage(user_id, &status);
            return Ok(CompactTeamContextResponse {
                team_id: team_id.to_owned(),
                auto_compact_enabled: pref.is_auto_compact_enabled(),
                auto_compact_threshold_pct: pref.threshold_pct,
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

        let scope = req
            .scope
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("all")
            .to_owned();
        let force_interrupt = req.force_interrupt.unwrap_or(false);

        // Build `(slot_id, ModelTargetSpec)` plan
        let mut plan: Vec<(String, ModelTargetSpec)> = Vec::new();
        match scope.as_str() {
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
                let target = req
                    .target
                    .clone()
                    .ok_or_else(|| ApiError::BadRequest("target is required when scope is 'teammates_only'".into()))?;
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

        // 2. Compute & broadcast live context usage
        let Ok(status) = self.get_team_context_status(user_id, team_id).await else {
            return;
        };
        self.broadcast_context_usage(user_id, &status);

        if !status.auto_compact_enabled {
            return;
        }
        let Some(slot_status) = status.slots.iter().find(|s| s.slot_id == slot_id) else {
            return;
        };
        if slot_status.usage_percent < status.auto_compact_threshold_pct {
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
}

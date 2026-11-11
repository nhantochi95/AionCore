//! 3-Tier Adaptive Router for Multi-Team Shared Rooms (Rakazo pattern + zero-config fallback).
//!
//! Tier 1 (0ms, 0 tokens): Explicit `@TeamName` / `@all`, Reply-to-Leader, or active Cross-Team Task assignee.
//! Tier 2 (<4s timeout): Fast HTTP API Engagement Judge when an enabled HTTP Provider is configured.
//! Tier 3 (Zero-config CLI-only fallback): Route untagged human messages to the room's Host/Chair Leader.

use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;

use aionui_common::decrypt_string;
use aionui_db::CustomSharedRoomTaskRow;
use aionui_db::models::Provider;
use serde::{Deserialize, Serialize};

fn shared_default_judge_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

#[derive(Debug, Clone)]
pub struct ParticipatingTeam {
    pub team_id: String,
    pub name: String,
    pub role_in_room: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutedTarget {
    pub team_id: String,
    pub reason: String,
    pub tier: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JudgeUsageMetrics {
    pub provider_id: String,
    pub model: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
}

#[derive(Debug, Clone, Default)]
pub struct RoomRoutingHeuristics<'a> {
    /// Optional `(team_id, role_tags)` pairs for domain-tag matching before Tier 2 HTTP Judge.
    pub member_role_tags: &'a [(String, Vec<String>)],
    /// Most recent Leader speaker `(team_id, created_at_ms)` in this room for sticky short replies.
    pub last_leader_speaker: Option<(&'a str, i64)>,
    /// Current epoch timestamp in ms (used to evaluate 3-minute sticky speaker window).
    pub now_ms: i64,
    /// When true, Paid API 100% Hard Fuse is active so Tier 2 HTTP Judge is skipped ($0 fallback).
    pub api_wallet_hard_blocked: bool,
}

#[derive(Debug, Clone)]
pub struct RoomRoutingInput<'a> {
    pub sender_type: &'a str, // "user" | "leader"
    pub sender_team_id: Option<&'a str>,
    pub content: &'a str,
    pub explicit_mentions: &'a [String],
    pub reply_to_sender_team_id: Option<&'a str>,
    pub host_team_id: Option<&'a str>,
    pub members: &'a [ParticipatingTeam],
    pub room_tasks: &'a [CustomSharedRoomTaskRow],
}

/// Normalize a team name into a compact `@mention` handle (lowercase alphanumeric + `-`/`_`),
/// collapsing consecutive whitespace into a single `-` to match Frontend `mentionSlug` 100%.
pub fn mention_slug(name: &str) -> String {
    let mut out = String::new();
    let mut prev_ws = false;
    for c in name.trim().chars() {
        if c.is_whitespace() {
            if !prev_ws {
                out.push('-');
                prev_ws = true;
            }
        } else {
            prev_ws = false;
            if c.is_alphanumeric() || c == '-' || c == '_' {
                for lc in c.to_lowercase() {
                    out.push(lc);
                }
            } else {
                out.push('_');
            }
        }
    }
    out.trim_matches(['-', '_']).to_owned()
}

/// Returns true if a specific task still has at least one `blocked_by` dependency that is NOT
/// yet `"in_review"`, `"completed"`, or `"done"`.
pub fn is_task_blocked(task: &CustomSharedRoomTaskRow, room_tasks: &[CustomSharedRoomTaskRow]) -> bool {
    if matches!(task.status.as_str(), "in_review" | "completed" | "done" | "cancelled") {
        return false;
    }
    let deps: Vec<String> = serde_json::from_str(&task.blocked_by_json).unwrap_or_default();
    if deps.is_empty() {
        return false;
    }
    let satisfied_ids: HashSet<&str> = room_tasks
        .iter()
        .filter(|t| matches!(t.status.as_str(), "in_review" | "completed" | "done"))
        .map(|t| t.id.as_str())
        .collect();
    deps.iter().any(|dep_id| !satisfied_ids.contains(dep_id.as_str()))
}

/// Validates `blocked_by` task IDs before creating or updating a Cross-Team Task:
/// - Every referenced ID must exist in `room_tasks` (in the same Shared Room).
/// - No self-reference (`new_task_id` in `blocked_by`) or cycle (`A -> B -> A`).
pub fn validate_task_dependencies(
    new_task_id: &str,
    blocked_by: &[String],
    room_tasks: &[CustomSharedRoomTaskRow],
) -> Result<(), String> {
    if blocked_by.is_empty() {
        return Ok(());
    }

    let mut dep_graph: std::collections::HashMap<&str, Vec<String>> = std::collections::HashMap::new();
    for t in room_tasks {
        let deps: Vec<String> = serde_json::from_str(&t.blocked_by_json).unwrap_or_default();
        dep_graph.insert(t.id.as_str(), deps);
    }

    for dep in blocked_by {
        let trimmed = dep.trim();
        if trimmed.is_empty() {
            return Err("blocked_by task ID must not be empty".to_owned());
        }
        if trimmed == new_task_id {
            return Err(format!("Task '{new_task_id}' cannot depend on itself"));
        }
        if !dep_graph.contains_key(trimmed) {
            return Err(format!(
                "Dependency task '{trimmed}' does not exist in this Shared Room"
            ));
        }
    }

    // Check cycle reachability: if `new_task_id` is reachable from any `dep`, adding `new_task_id -> dep` creates a cycle.
    for dep in blocked_by {
        let mut stack = vec![dep.as_str()];
        let mut visited = HashSet::new();
        while let Some(curr) = stack.pop() {
            if curr == new_task_id {
                return Err(format!(
                    "Circular blocked_by dependency detected involving task '{new_task_id}' and '{dep}'"
                ));
            }
            if !visited.insert(curr) {
                continue;
            }
            if let Some(next_deps) = dep_graph.get(curr) {
                for nd in next_deps {
                    stack.push(nd.as_str());
                }
            }
        }
    }

    Ok(())
}

/// Returns true if `team_id` currently has an unfinished task in this room whose
/// `blocked_by` task IDs are NOT yet `completed`.
pub fn is_team_blocked_by_dependencies(team_id: &str, room_tasks: &[CustomSharedRoomTaskRow]) -> bool {
    room_tasks
        .iter()
        .any(|task| task.to_team_id == team_id && is_task_blocked(task, room_tasks))
}

/// Deterministic Tier-1 routing check. Returns `Some(targets)` if Tier 1 matched
/// (even if blocked_by filtered out a target, so we don't mistakenly fall through to random teams).
pub fn evaluate_tier1_deterministic(input: &RoomRoutingInput<'_>) -> Option<Vec<RoutedTarget>> {
    let lower_content = input.content.to_lowercase();

    // 1. `@all` broadcast
    if has_at_all_token(&lower_content) {
        let targets = input
            .members
            .iter()
            .filter(|m| Some(m.team_id.as_str()) != input.sender_team_id)
            .filter(|m| !is_team_blocked_by_dependencies(&m.team_id, input.room_tasks))
            .map(|m| RoutedTarget {
                team_id: m.team_id.clone(),
                reason: "broadcast:@all".to_owned(),
                tier: 1,
            })
            .collect();
        return Some(targets);
    }

    // 2. Explicit `mentions` array or `@TeamName` / `@team-slug` in message text
    let mut matched_ids = Vec::new();
    let mut seen = HashSet::new();

    for raw_mention in input.explicit_mentions {
        let needle = raw_mention.trim().trim_start_matches('@').to_lowercase();
        if needle.is_empty() {
            continue;
        }
        for m in input.members {
            if (m.team_id.eq_ignore_ascii_case(&needle)
                || m.name.to_lowercase() == needle
                || mention_slug(&m.name) == mention_slug(&needle))
                && Some(m.team_id.as_str()) != input.sender_team_id
                && seen.insert(m.team_id.clone())
            {
                matched_ids.push(m.team_id.clone());
            }
        }
    }

    // Check `@Name` or `@slug` inside content (sort longest names first so `@NhanTown Pro` matches before `@NhanTown`)
    let mut sorted_members: Vec<&ParticipatingTeam> = input.members.iter().collect();
    sorted_members.sort_by_key(|b| std::cmp::Reverse(b.name.len()));
    for m in sorted_members {
        if Some(m.team_id.as_str()) == input.sender_team_id {
            continue;
        }
        let exact_tag = format!("@{}", m.name.to_lowercase());
        let slug = mention_slug(&m.name);
        let slug_tag = format!("@{slug}");
        if contains_mention_tag(&lower_content, &exact_tag)
            || (!slug.is_empty() && contains_mention_tag(&lower_content, &slug_tag))
        {
            if seen.insert(m.team_id.clone()) {
                matched_ids.push(m.team_id.clone());
            }
        }
    }

    if !matched_ids.is_empty() {
        let targets = matched_ids
            .into_iter()
            .filter(|tid| !is_team_blocked_by_dependencies(tid, input.room_tasks))
            .map(|team_id| RoutedTarget {
                team_id,
                reason: "explicit:@mention".to_owned(),
                tier: 1,
            })
            .collect();
        return Some(targets);
    }

    // 3. Suppress low-signal agent acknowledgement chatter ("Got it, working on it!") before
    //    waking a replied-to Leader so teams don't burn quota on polite ping-pong acks.
    if input.sender_type != "user" && is_low_signal_agent_ack(input.content) {
        return Some(Vec::new());
    }

    // 4. Direct reply to a Leader's message
    if let Some(reply_team_id) = input.reply_to_sender_team_id
        && Some(reply_team_id) != input.sender_team_id
        && input.members.iter().any(|m| m.team_id == reply_team_id)
    {
        if is_team_blocked_by_dependencies(reply_team_id, input.room_tasks) {
            return Some(Vec::new());
        }
        return Some(vec![RoutedTarget {
            team_id: reply_team_id.to_owned(),
            reason: "reply_to_leader".to_owned(),
            tier: 1,
        }]);
    }

    // 5. Single-team room short-circuit (0 LLM Judge calls needed when only 1 team participates)
    if input.sender_type == "user" && input.members.len() == 1 {
        let only = &input.members[0];
        if is_team_blocked_by_dependencies(&only.team_id, input.room_tasks) {
            return Some(Vec::new());
        }
        return Some(vec![RoutedTarget {
            team_id: only.team_id.clone(),
            reason: "single_room_member".to_owned(),
            tier: 1,
        }]);
    }

    // 6. If a human sends an untagged follow-up and exactly one team has an `in_progress` room task
    if input.sender_type == "user" {
        let in_progress_assignees: HashSet<&str> = input
            .room_tasks
            .iter()
            .filter(|t| t.status == "in_progress")
            .map(|t| t.to_team_id.as_str())
            .filter(|tid| input.members.iter().any(|m| m.team_id == *tid))
            .collect();
        if in_progress_assignees.len() == 1
            && let Some(active_tid) = in_progress_assignees.into_iter().next()
            && !is_team_blocked_by_dependencies(active_tid, input.room_tasks)
        {
            return Some(vec![RoutedTarget {
                team_id: active_tid.to_owned(),
                reason: "active_room_task_owner".to_owned(),
                tier: 1,
            }]);
        }
    }

    None
}

/// Returns true when a non-human message is a brief acknowledgement without questions,
/// explicit mentions, or artifact references (e.g. "Got it, working on this now.").
pub fn is_low_signal_agent_ack(content: &str) -> bool {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return true;
    }
    if trimmed.chars().count() > 160 || trimmed.contains('?') || trimmed.contains('@') {
        return false;
    }
    let lower = trimmed.to_lowercase();
    if lower.contains(".md")
        || lower.contains(".json")
        || lower.contains(".yaml")
        || lower.contains(".yml")
        || lower.contains(".rs")
        || lower.contains(".ts")
        || lower.contains(".py")
        || lower.contains(".sql")
        || lower.contains("```")
    {
        return false;
    }
    const ACK_PREFIXES: &[&str] = &[
        "got it",
        "acknowledged",
        "ack.",
        "ack!",
        "ack,",
        "on it",
        "working on it",
        "working on this",
        "understood",
        "noted",
        "received",
        "thanks",
        "thank you",
        "will do",
        "starting now",
        "đã nhận",
        "đang xử lý",
        "ok đã nhận",
        "cảm ơn",
    ];
    lower == "ack" || ACK_PREFIXES.iter().any(|p| lower.starts_with(p))
}

/// Deterministic Tier-1.5 heuristics evaluated before calling any paid Tier-2 HTTP LLM Judge.
pub fn evaluate_tier1_5_heuristics(
    input: &RoomRoutingInput<'_>,
    heuristics: &RoomRoutingHeuristics<'_>,
) -> Option<Vec<RoutedTarget>> {
    if input.sender_type != "user" {
        return None;
    }

    // 1. Single unblocked open task assignee ("pending" | "in_progress" | "in_review")
    let open_assignees: HashSet<&str> = input
        .room_tasks
        .iter()
        .filter(|t| matches!(t.status.as_str(), "pending" | "in_progress" | "in_review"))
        .filter(|t| !is_task_blocked(t, input.room_tasks))
        .map(|t| t.to_team_id.as_str())
        .filter(|tid| input.members.iter().any(|m| m.team_id == *tid))
        .collect();
    if open_assignees.len() == 1
        && let Some(owner_tid) = open_assignees.into_iter().next()
        && !is_team_blocked_by_dependencies(owner_tid, input.room_tasks)
    {
        return Some(vec![RoutedTarget {
            team_id: owner_tid.to_owned(),
            reason: "open_room_task_owner".to_owned(),
            tier: 1,
        }]);
    }

    // 2. Unique member role_tags keyword match in human message
    if !heuristics.member_role_tags.is_empty() {
        let lower_msg = input.content.to_lowercase();
        let mut matched_teams = Vec::new();
        for (team_id, tags) in heuristics.member_role_tags {
            if !input.members.iter().any(|m| m.team_id == *team_id)
                || is_team_blocked_by_dependencies(team_id, input.room_tasks)
            {
                continue;
            }
            let hit = tags.iter().any(|tag| {
                let clean = tag.trim().to_lowercase();
                clean.chars().count() >= 3 && contains_word_token(&lower_msg, &clean)
            });
            if hit {
                matched_teams.push(team_id.clone());
            }
        }
        if matched_teams.len() == 1 {
            return Some(vec![RoutedTarget {
                team_id: matched_teams.remove(0),
                reason: "role_tag_keyword_match".to_owned(),
                tier: 1,
            }]);
        }
    }

    // 3. Sticky recent speaker within 3 minutes (180_000 ms) for short follow-up replies
    if let Some((last_tid, last_ts)) = heuristics.last_leader_speaker
        && heuristics.now_ms > 0
        && last_ts > 0
        && heuristics.now_ms.saturating_sub(last_ts) <= 180_000
        && input.content.chars().count() <= 120
        && input.members.iter().any(|m| m.team_id == last_tid)
        && !is_team_blocked_by_dependencies(last_tid, input.room_tasks)
    {
        return Some(vec![RoutedTarget {
            team_id: last_tid.to_owned(),
            reason: "sticky_recent_leader".to_owned(),
            tier: 1,
        }]);
    }

    None
}

fn contains_word_token(lower_text: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let mut from = 0;
    while let Some(rel) = lower_text[from..].find(needle) {
        let idx = from + rel;
        let before_ok = idx == 0
            || lower_text[..idx]
                .chars()
                .next_back()
                .is_none_or(|c| !c.is_alphanumeric());
        let after = idx + needle.len();
        let after_ok = after >= lower_text.len()
            || lower_text[after..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_alphanumeric());
        if before_ok && after_ok {
            return true;
        }
        from = idx + needle.chars().next().map_or(1, char::len_utf8);
    }
    false
}

fn has_at_all_token(lower_text: &str) -> bool {
    contains_mention_tag(lower_text, "@all")
}

fn contains_mention_tag(lower_text: &str, tag: &str) -> bool {
    if tag.len() <= 1 {
        return false;
    }
    let mut search_from = 0;
    while let Some(rel_idx) = lower_text[search_from..].find(tag) {
        let idx = search_from + rel_idx;
        let before_ok = idx == 0
            || lower_text[..idx]
                .chars()
                .next_back()
                .is_none_or(|c| c.is_whitespace() || matches!(c, '(' | '[' | ',' | ';' | ':'));
        let after_idx = idx + tag.len();
        let after_ok = after_idx >= lower_text.len()
            || lower_text[after_idx..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-');
        if before_ok && after_ok {
            return true;
        }
        search_from = idx + tag.chars().next().map_or(1, char::len_utf8);
    }
    false
}

/// Evaluate all 3 tiers cleanly:
/// - Leader messages NEVER fall back to Tier 2/3 if untagged (prevents runaway chatter when a Leader posts a closing report without mentioning another Team!).
/// - Human messages use Tier 1 -> Tier 2 (Fast HTTP Judge if available) -> Tier 3 (Host/Chair Leader).
pub async fn resolve_room_targets(
    input: &RoomRoutingInput<'_>,
    providers: &[Provider],
    encryption_key: &[u8; 32],
) -> Vec<RoutedTarget> {
    let heuristics = RoomRoutingHeuristics::default();
    let (targets, _usage) = resolve_room_targets_smart(
        input,
        &heuristics,
        providers,
        encryption_key,
        shared_default_judge_client(),
    )
    .await;
    targets
}

/// Smart 3-Tier room router with Tier-1.5 zero-token heuristics, shared HTTP client,
/// Paid API Hard Fuse protection, reasoning-model exclusion, and Judge token usage reporting.
pub async fn resolve_room_targets_smart(
    input: &RoomRoutingInput<'_>,
    heuristics: &RoomRoutingHeuristics<'_>,
    providers: &[Provider],
    encryption_key: &[u8; 32],
    http_client: &reqwest::Client,
) -> (Vec<RoutedTarget>, Option<JudgeUsageMetrics>) {
    if input.members.is_empty() {
        return (Vec::new(), None);
    }

    if let Some(tier1) = evaluate_tier1_deterministic(input) {
        return (tier1, None);
    }

    // Crucial Rakazo anti-chatter invariant: when a Team Leader posts a message to the room
    // without mentioning `@OtherTeam`, `@all`, or replying to another Leader, it is a status/closing
    // report to the room — do NOT randomly wake other Leaders!
    if input.sender_type != "user" {
        return (Vec::new(), None);
    }

    if let Some(tier1_5) = evaluate_tier1_5_heuristics(input, heuristics) {
        return (tier1_5, None);
    }

    // Tier 2: Fast HTTP API Engagement Judge (skipped when Paid API 100% Hard Fuse is active)
    if !heuristics.api_wallet_hard_blocked
        && let Some((tier2, usage)) =
            try_tier2_http_judge(input, providers, encryption_key, http_client).await
        && !tier2.is_empty()
    {
        return (tier2, Some(usage));
    }

    // Tier 3: Zero-config fallback -> Host/Chair Leader (or first member Team in room)
    let fallback_team = input
        .host_team_id
        .and_then(|hid| input.members.iter().find(|m| m.team_id == hid))
        .or_else(|| input.members.first());

    let fallback = fallback_team
        .into_iter()
        .map(|m| RoutedTarget {
            team_id: m.team_id.clone(),
            reason: "host_leader_fallback".to_owned(),
            tier: 3,
        })
        .collect();
    (fallback, None)
}

#[derive(Deserialize)]
struct JudgeJsonOutput {
    #[serde(default)]
    targets: Vec<JudgeTargetItem>,
}

#[derive(Deserialize)]
struct JudgeTargetItem {
    team_id: String,
    #[serde(default)]
    reason: String,
}

/// Returns true for expensive/slow reasoning-only models that should never be selected as a fast router judge.
pub fn is_reasoning_only_model(model_id: &str) -> bool {
    let l = model_id.trim().to_ascii_lowercase();
    l == "o1"
        || l.starts_with("o1-")
        || l == "o3"
        || l.starts_with("o3-")
        || l == "o4-mini"
        || l.starts_with("o4-")
        || l.contains("reasoner")
        || l.contains("deepseek-r1")
        || l.ends_with("-r1")
        || l.contains("qwq")
        || l.contains("thinking")
}

pub fn select_fast_judge_model(models: &[String]) -> Option<String> {
    let eligible: Vec<&String> = models
        .iter()
        .filter(|m| !m.trim().is_empty() && !is_reasoning_only_model(m))
        .collect();
    if eligible.is_empty() {
        return None;
    }
    eligible
        .iter()
        .find(|m| {
            let l = m.to_ascii_lowercase();
            l.contains("mini") || l.contains("flash") || l.contains("haiku") || l.contains("fast") || l.contains("nano")
        })
        .or_else(|| eligible.first())
        .map(|m| (*m).clone())
}

async fn try_tier2_http_judge(
    input: &RoomRoutingInput<'_>,
    providers: &[Provider],
    encryption_key: &[u8; 32],
    http_client: &reqwest::Client,
) -> Option<(Vec<RoutedTarget>, JudgeUsageMetrics)> {
    // Select first enabled OpenAI-compatible HTTP provider with a valid API key and non-reasoning fast model.
    let (candidate, model) = providers.iter().find_map(|p| {
        if !p.enabled
            || p.api_key_encrypted.trim().is_empty()
            || p.base_url.trim().is_empty()
            || p.platform.eq_ignore_ascii_case("anthropic")
            || p.platform.eq_ignore_ascii_case("bedrock")
        {
            return None;
        }
        let models: Vec<String> = serde_json::from_str(&p.models).unwrap_or_default();
        let chosen = select_fast_judge_model(&models)?;
        Some((p, chosen))
    })?;

    let api_key = decrypt_string(&candidate.api_key_encrypted, encryption_key).ok()?;
    if api_key.trim().is_empty() {
        return None;
    }

    let endpoint = if candidate.is_full_url {
        candidate.base_url.trim().to_owned()
    } else {
        format!("{}/chat/completions", candidate.base_url.trim_end_matches('/'))
    };

    let roster_lines: Vec<String> = input
        .members
        .iter()
        .map(|m| format!("- {}: {} ({})", m.team_id, m.name, m.role_in_room))
        .collect();
    let clipped_msg: String = input.content.chars().take(600).collect();

    let system_prompt = "Fast room router. Return ONLY JSON: {\"targets\":[{\"team_id\":\"...\",\"reason\":\"short\"}]}. Pick 1-3 relevant teams or chair.";
    let user_prompt = format!(
        "Teams:\n{}\nMsg:\n{}",
        roster_lines.join("\n"),
        clipped_msg
    );

    let resp = http_client
        .post(&endpoint)
        .timeout(Duration::from_secs(4))
        .bearer_auth(api_key.trim())
        .json(&serde_json::json!({
            "model": model,
            "temperature": 0.0,
            "max_tokens": 120,
            "response_format": { "type": "json_object" },
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user", "content": user_prompt }
            ]
        }))
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    let raw_text = body["choices"][0]["message"]["content"].as_str()?;
    let est_in = ((system_prompt.len() + user_prompt.len()) as i64 / 4).max(1);
    let est_out = (raw_text.len() as i64 / 4).max(1);
    let prompt_tokens = body["usage"]["prompt_tokens"]
        .as_i64()
        .filter(|v| *v > 0)
        .unwrap_or(est_in);
    let completion_tokens = body["usage"]["completion_tokens"]
        .as_i64()
        .filter(|v| *v > 0)
        .unwrap_or(est_out);

    let cleaned = raw_text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    let parsed: JudgeJsonOutput = serde_json::from_str(cleaned).ok()?;

    let valid_ids: HashSet<&str> = input.members.iter().map(|m| m.team_id.as_str()).collect();
    let routed: Vec<RoutedTarget> = parsed
        .targets
        .into_iter()
        .filter(|item| valid_ids.contains(item.team_id.as_str()))
        .filter(|item| !is_team_blocked_by_dependencies(&item.team_id, input.room_tasks))
        .map(|item| RoutedTarget {
            team_id: item.team_id,
            reason: if item.reason.is_empty() {
                "http_judge".to_owned()
            } else {
                item.reason
            },
            tier: 2,
        })
        .collect();

    Some((
        routed,
        JudgeUsageMetrics {
            provider_id: candidate.id.clone(),
            model,
            prompt_tokens,
            completion_tokens,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_members() -> Vec<ParticipatingTeam> {
        vec![
            ParticipatingTeam {
                team_id: "t-nhantown".into(),
                name: "NhanTown".into(),
                role_in_room: "chair".into(),
            },
            ParticipatingTeam {
                team_id: "t-vibeclip".into(),
                name: "VibeClip".into(),
                role_in_room: "member".into(),
            },
        ]
    }

    #[tokio::test]
    async fn tier1_routes_explicit_mention_and_skips_blocked_team() {
        let members = sample_members();
        let tasks = vec![CustomSharedRoomTaskRow {
            id: "rt-2".into(),
            room_id: "room-1".into(),
            title: "Render video".into(),
            description: String::new(),
            from_team_id: Some("t-nhantown".into()),
            to_team_id: "t-vibeclip".into(),
            status: "pending".into(),
            blocked_by_json: r#"["rt-1"]"#.into(),
            artifact_paths_json: "[]".into(),
            result_summary: None,
            created_at: 1,
            updated_at: 1,
        }];

        let input = RoomRoutingInput {
            sender_type: "user",
            sender_team_id: None,
            content: "Nhờ @NhanTown chốt spec rồi @VibeClip dựng nhé",
            explicit_mentions: &[],
            reply_to_sender_team_id: None,
            host_team_id: Some("t-nhantown"),
            members: &members,
            room_tasks: &tasks,
        };

        let routed = resolve_room_targets(&input, &[], &[0u8; 32]).await;
        // VibeClip is gated by unfinished rt-1, so only NhanTown wakes now!
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].team_id, "t-nhantown");
        assert_eq!(routed[0].tier, 1);
    }

    #[tokio::test]
    async fn untagged_user_message_falls_back_to_host_leader_while_leader_message_stops() {
        let members = sample_members();
        let user_input = RoomRoutingInput {
            sender_type: "user",
            sender_team_id: None,
            content: "Anh cần làm landing page kèm video demo cho dự án mới",
            explicit_mentions: &[],
            reply_to_sender_team_id: None,
            host_team_id: Some("t-nhantown"),
            members: &members,
            room_tasks: &[],
        };
        let routed = resolve_room_targets(&user_input, &[], &[0u8; 32]).await;
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].team_id, "t-nhantown");
        assert_eq!(routed[0].tier, 3);

        let leader_closing = RoomRoutingInput {
            sender_type: "leader",
            sender_team_id: Some("t-nhantown"),
            content: "Team NhanTown đã hoàn thành landing page nhé anh!",
            explicit_mentions: &[],
            reply_to_sender_team_id: None,
            host_team_id: Some("t-nhantown"),
            members: &members,
            room_tasks: &[],
        };
        let routed_after_leader = resolve_room_targets(&leader_closing, &[], &[0u8; 32]).await;
        assert!(routed_after_leader.is_empty());
    }

    #[test]
    fn dependency_validator_rejects_self_unknown_and_cycles() {
        let tasks = vec![
            CustomSharedRoomTaskRow {
                id: "rt-1".into(),
                room_id: "r1".into(),
                from_team_id: None,
                to_team_id: "t-a".into(),
                title: "Step 1".into(),
                description: String::new(),
                status: "pending".into(),
                blocked_by_json: r#"["rt-2"]"#.into(),
                artifact_paths_json: "[]".into(),
                result_summary: None,
                created_at: 1,
                updated_at: 1,
            },
            CustomSharedRoomTaskRow {
                id: "rt-2".into(),
                room_id: "r1".into(),
                from_team_id: None,
                to_team_id: "t-b".into(),
                title: "Step 2".into(),
                description: String::new(),
                status: "pending".into(),
                blocked_by_json: "[]".into(),
                artifact_paths_json: "[]".into(),
                result_summary: None,
                created_at: 2,
                updated_at: 2,
            },
        ];

        assert!(validate_task_dependencies("rt-3", &["rt-3".into()], &tasks).is_err());
        assert!(validate_task_dependencies("rt-3", &["rt-missing".into()], &tasks).is_err());
        // Adding `rt-2 -> rt-1` when `rt-1 -> rt-2` already exists creates a cycle!
        assert!(validate_task_dependencies("rt-2", &["rt-1".into()], &tasks).is_err());
        // Valid acyclic chain `rt-3 -> rt-1` succeeds!
        assert!(validate_task_dependencies("rt-3", &["rt-1".into()], &tasks).is_ok());
    }

    #[tokio::test]
    async fn f19_in_review_satisfies_blocked_by_and_low_signal_ack_is_suppressed() {
        let members = sample_members();
        let tasks = vec![
            CustomSharedRoomTaskRow {
                id: "rt-1".into(),
                room_id: "room-1".into(),
                title: "Build API".into(),
                description: String::new(),
                from_team_id: Some("t-nhantown".into()),
                to_team_id: "t-nhantown".into(),
                status: "in_review".into(),
                blocked_by_json: "[]".into(),
                artifact_paths_json: "[]".into(),
                result_summary: Some("Ready for sign-off".into()),
                created_at: 1,
                updated_at: 2,
            },
            CustomSharedRoomTaskRow {
                id: "rt-2".into(),
                room_id: "room-1".into(),
                title: "Render video".into(),
                description: String::new(),
                from_team_id: Some("t-nhantown".into()),
                to_team_id: "t-vibeclip".into(),
                status: "pending".into(),
                blocked_by_json: r#"["rt-1"]"#.into(),
                artifact_paths_json: "[]".into(),
                result_summary: None,
                created_at: 1,
                updated_at: 1,
            },
        ];
        assert!(
            !is_task_blocked(&tasks[1], &tasks),
            "in_review dependency must unblock downstream task"
        );

        let ack_input = RoomRoutingInput {
            sender_type: "leader",
            sender_team_id: Some("t-vibeclip"),
            content: "Got it, working on it right now!",
            explicit_mentions: &[],
            reply_to_sender_team_id: Some("t-nhantown"),
            host_team_id: Some("t-nhantown"),
            members: &members,
            room_tasks: &tasks,
        };
        let ack_routed = resolve_room_targets(&ack_input, &[], &[0u8; 32]).await;
        assert!(
            ack_routed.is_empty(),
            "polite low-signal agent ack must not wake replied-to leader"
        );

        let single_room = vec![members[0].clone()];
        let single_input = RoomRoutingInput {
            sender_type: "user",
            sender_team_id: None,
            content: "Status update?",
            explicit_mentions: &[],
            reply_to_sender_team_id: None,
            host_team_id: None,
            members: &single_room,
            room_tasks: &[],
        };
        let single_routed = resolve_room_targets(&single_input, &[], &[0u8; 32]).await;
        assert_eq!(single_routed.len(), 1);
        assert_eq!(single_routed[0].tier, 1);
        assert_eq!(single_routed[0].reason, "single_room_member");

        assert!(is_reasoning_only_model("o3-mini"));
        assert!(is_reasoning_only_model("deepseek-r1"));
        assert_eq!(
            select_fast_judge_model(&["o3-mini".into(), "gpt-4o".into(), "gpt-4o-mini".into()]),
            Some("gpt-4o-mini".into())
        );
        assert_eq!(
            select_fast_judge_model(&["o1-preview".into(), "deepseek-r1".into()]),
            None
        );
    }
}

//! 3-Tier Adaptive Router for Multi-Team Shared Rooms (Rakazo pattern + zero-config fallback).
//!
//! Tier 1 (0ms, 0 tokens): Explicit `@TeamName` / `@all`, Reply-to-Leader, or active Cross-Team Task assignee.
//! Tier 2 (<4s timeout): Fast HTTP API Engagement Judge when an enabled HTTP Provider is configured.
//! Tier 3 (Zero-config CLI-only fallback): Route untagged human messages to the room's Host/Chair Leader.

use std::collections::HashSet;
use std::time::Duration;

use aionui_common::decrypt_string;
use aionui_db::CustomSharedRoomTaskRow;
use aionui_db::models::Provider;
use serde::{Deserialize, Serialize};

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

/// Normalize a team name into a compact `@mention` handle (lowercase alphanumeric + `-`/`_`).
pub fn mention_slug(name: &str) -> String {
    name.trim()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c.to_lowercase().next().unwrap_or(c)
            } else if c.is_whitespace() {
                '-'
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches(['-', '_'])
        .to_owned()
}

/// Returns true if `team_id` currently has an unfinished task in this room whose
/// `blocked_by` task IDs are NOT yet `completed`.
pub fn is_team_blocked_by_dependencies(team_id: &str, room_tasks: &[CustomSharedRoomTaskRow]) -> bool {
    let completed_ids: HashSet<&str> = room_tasks
        .iter()
        .filter(|t| t.status == "completed")
        .map(|t| t.id.as_str())
        .collect();

    for task in room_tasks {
        if task.to_team_id != team_id || matches!(task.status.as_str(), "completed" | "cancelled") {
            continue;
        }
        let deps: Vec<String> = serde_json::from_str(&task.blocked_by_json).unwrap_or_default();
        if deps.iter().any(|dep_id| !completed_ids.contains(dep_id.as_str())) {
            return true;
        }
    }
    false
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

    // 3. Direct reply to a Leader's message
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

    // 4. If a human sends an untagged follow-up and exactly one team has an `in_progress` room task
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
    if input.members.is_empty() {
        return Vec::new();
    }

    if let Some(tier1) = evaluate_tier1_deterministic(input) {
        return tier1;
    }

    // Crucial Rakazo anti-chatter invariant: when a Team Leader posts a message to the room
    // without mentioning `@OtherTeam`, `@all`, or replying to another Leader, it is a status/closing
    // report to the room — do NOT randomly wake other Leaders!
    if input.sender_type != "user" {
        return Vec::new();
    }

    // Tier 2: Fast HTTP API Engagement Judge (if an OpenAI-compatible HTTP provider is enabled)
    if let Some(tier2) = try_tier2_http_judge(input, providers, encryption_key).await
        && !tier2.is_empty()
    {
        return tier2;
    }

    // Tier 3: Zero-config fallback -> Host/Chair Leader (or first member Team in room)
    let fallback_team = input
        .host_team_id
        .and_then(|hid| input.members.iter().find(|m| m.team_id == hid))
        .or_else(|| input.members.first());

    fallback_team
        .into_iter()
        .map(|m| RoutedTarget {
            team_id: m.team_id.clone(),
            reason: "host_leader_fallback".to_owned(),
            tier: 3,
        })
        .collect()
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

async fn try_tier2_http_judge(
    input: &RoomRoutingInput<'_>,
    providers: &[Provider],
    encryption_key: &[u8; 32],
) -> Option<Vec<RoutedTarget>> {
    // Select first enabled OpenAI-compatible HTTP provider with a valid API key and model.
    let candidate = providers.iter().find(|p| {
        p.enabled
            && !p.api_key_encrypted.trim().is_empty()
            && !p.base_url.trim().is_empty()
            && !p.platform.eq_ignore_ascii_case("anthropic")
            && !p.platform.eq_ignore_ascii_case("bedrock")
    })?;

    let api_key = decrypt_string(&candidate.api_key_encrypted, encryption_key).ok()?;
    if api_key.trim().is_empty() {
        return None;
    }

    let models: Vec<String> = serde_json::from_str(&candidate.models).unwrap_or_default();
    let model = models
        .iter()
        .find(|m| {
            let l = m.to_ascii_lowercase();
            l.contains("mini") || l.contains("flash") || l.contains("haiku") || l.contains("fast")
        })
        .or_else(|| models.first())?
        .clone();

    let endpoint = if candidate.is_full_url {
        candidate.base_url.trim().to_owned()
    } else {
        format!("{}/chat/completions", candidate.base_url.trim_end_matches('/'))
    };

    let roster_lines: Vec<String> = input
        .members
        .iter()
        .map(|m| format!("- team_id=\"{}\", name=\"{}\", role=\"{}\"", m.team_id, m.name, m.role_in_room))
        .collect();

    let system_prompt = "You are a fast multi-team room router. Given a list of participating teams and a user message, return ONLY valid JSON: {\"targets\":[{\"team_id\":\"...\",\"reason\":\"short reason\"}]}. Pick 1 to 3 relevant teams, or the chair/host team if general.";
    let user_prompt = format!(
        "Teams:\n{}\n\nUser message:\n{}",
        roster_lines.join("\n"),
        input.content
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .ok()?;

    let resp = client
        .post(&endpoint)
        .bearer_auth(api_key.trim())
        .json(&serde_json::json!({
            "model": model,
            "temperature": 0.0,
            "max_tokens": 200,
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

    Some(routed)
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
}

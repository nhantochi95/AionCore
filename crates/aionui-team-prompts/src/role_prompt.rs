use crate::governance::with_team_governance;
use crate::team_tool_usage::build_team_tool_usage;
use aionui_api_types::TeamToolTransport;
use serde::Serialize;
use std::collections::HashMap;

pub const LEAD_PROMPT_TEMPLATE: &str = r#"# You are the Team Leader

## Your Identity
Name: {{AGENT_NAME}}
Slot ID: {{AGENT_SLOT_ID}}
Role: lead

## Your Role
You coordinate a team of AI agents. By default you do NOT do implementation
work yourself — you break down tasks, assign them to teammates, and synthesize
results. If the user explicitly asks you to implement, fix, or edit code
yourself, you may do that directly.${workspaceSection}

## Conversation Style
- If the user greets you, starts a new chat, or asks what you can do without giving a concrete task yet, reply warmly, introduce yourself briefly as the team leader, and invite them to share their goal
- Do NOT mention teammate proposals, recommended assistants, or confirmation workflow until there is a concrete task that needs delegation

## Team Coordination Tools
{{TEAM_TOOL_USAGE}}

On your first team turn, inspect the `## Team Roster` snapshot in your wake prompt if present; call `team_members` only when no roster snapshot is shown or membership may have changed. Use display names in user-facing text and `slot_id` values for all tool arguments. Task IDs in the wake summary (8-char prefixes or full UUIDs) work directly in `team_task_update`; call `team_task_list` only when you need hidden rows or full descriptions.
All unread messages queued before this turn are already delivered in `## New Messages`. Do NOT call `team_read_messages` routinely at the start or end of a turn; call it only after a long multi-minute execution sequence or when paging `has_more: true` via `since_message_id` = `next_since_message_id`. When a long report is saved to `.aionui/handoffs/<id>.md`, read that file directly; do not act on a raw `content_truncated: true` preview without reading its handoff file.

## Workflow
1. Receive user request
   - Exception: if the user explicitly asked YOU to implement, fix, or edit something
     yourself, skip the rest of this workflow — do the work with your own tools and
     report back.
2. Decide whether the current team roster is sufficient
3. If additional teammates would help: check the roster (`## Team Roster` or `team_members`), call `team_list_assistants` (and `team_describe_assistant` only to disambiguate), then propose a staffing table with teammate name, responsibility, and recommended assistant.${presetFormattingStepRule}
4. Open the proposal with one short sentence on why teammates help, ask for explicit confirmation (or adjustments), remind the user they can later replace/adjust/shut down teammates, and end your turn — do NOT call `team_spawn_agent` in the proposal turn unless the user already explicitly instructed immediate creation or a `[SYSTEM NOTE]` confirms prior approval.
5. After confirmation, spawn approved teammates via `team_spawn_agent` (`assistant_id` from `team_list_assistants`; never pass a model).
6. Break the work into tasks with `team_task_create` (when dispatching 2+ tasks via CLI, prefer atomic `team task create-batch` with a JSON array on stdin instead of separate calls or shell `;` chains) — assigning a task to a teammate (via `owner`) automatically notifies and wakes them with the task details, so do NOT send a duplicate `team_send_message` just to hand off work or wake them.
7. Use `team_send_message` only for follow-up clarifications beyond the task description; never send low-signal "acknowledged/thanks" ping-pongs.
8. When teammates report back (via concise 4-state digest), review results, reassign if failed/blocked, and synthesize the final response to the user.

## Bug Fix Priority & Idle Teammates
- Bug fixes: **locate the problem → fix the problem → types/code style last**.
- Teammates go idle after every turn (normal waiting state, not an error). Idle signals are settled automatically by the runtime; assign `owner` on tasks or call `team_send_message` whenever you need to wake an idle teammate.

## Sequencing Dependent Work (CRITICAL — avoid teammate timeouts)
Never dispatch a dependent task telling teammate B to "stand by until A finishes" in an open stream (causes ~300s provider timeouts). For any dependency chain:
1. Either create B's task with `blocked_by: [task_A_id]` (the runtime automatically wakes B when A completes), OR dispatch A's task first, wait for A to finish, and then dispatch B's task.

## Lifecycle & Important Rules
- Use Team tools for coordination, not plain text.${presetFormattingImportantRule}
- Dismiss/remove ("下线/解雇/开除"): call `team_shutdown_agent` (never `team_send_message`), wait for confirmation, then report.
- Rename ("改名"): call `team_rename_agent`.
- Do NOT duplicate work teammates are actively executing.

## Proactive User Reporting & Multi-Team Shared Rooms
- Use `team_notify_user` (`approval_needed`, `milestone`, `blocker`, `completed` + optional `files`) for meaningful Telegram/external updates; never on trivial greetings or quick 1-turn answers.
- When representing your team in a Multi-Team Shared Room (`[PENDING ROOM HANDOFF]` or `[SHARED ROOM ...]`):
  - Use `team_room_read` (`since_msg_id` delta) when extra cross-team context is needed.
  - Close cross-team work with `team_room_task_update` (`status="in_review"` when ready for human Done approval, or `"blocked"` — never `"completed"`, which is human-only) and `team_room_post` (optional `mentions` / `files`)."#;

const PLACEHOLDER_WORKSPACE_SECTION: &str = "${workspaceSection}";
const PLACEHOLDER_PRESET_FORMATTING_STEP_RULE: &str = "${presetFormattingStepRule}";
const PLACEHOLDER_PRESET_FORMATTING_IMPORTANT_RULE: &str = "${presetFormattingImportantRule}";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamPromptRole {
    Lead,
    Teammate,
}

impl std::fmt::Display for TeamPromptRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TeamPromptRole::Lead => f.write_str("lead"),
            TeamPromptRole::Teammate => f.write_str("teammate"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TeamPromptAgent {
    pub slot_id: String,
    pub name: String,
    pub role: TeamPromptRole,
    pub backend: String,
    pub model: String,
    pub status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AvailableAgentType {
    pub agent_type: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AvailableAssistant {
    pub assistant_id: String,
    pub name: String,
    pub backend: String,
    pub description: String,
    pub skills: Vec<String>,
}

pub struct LeadPromptParams<'a> {
    pub agent: &'a TeamPromptAgent,
    pub team_name: &'a str,
    pub teammates: &'a [TeamPromptAgent],
    pub available_agent_types: &'a [AvailableAgentType],
    pub available_assistants: &'a [AvailableAssistant],
    pub renamed_agents: &'a HashMap<String, String>,
    pub team_workspace: Option<&'a str>,
    pub tool_transport: TeamToolTransport,
}

pub struct TeammatePromptParams<'a> {
    pub agent: &'a TeamPromptAgent,
    pub team_name: &'a str,
    pub leader: &'a TeamPromptAgent,
    pub teammates: &'a [TeamPromptAgent],
    pub renamed_agents: &'a HashMap<String, String>,
    pub team_workspace: Option<&'a str>,
    pub tool_transport: TeamToolTransport,
}

pub fn build_lead_prompt(params: &LeadPromptParams<'_>) -> String {
    let role_prompt = build_lead_role_prompt(params);
    with_team_governance(&role_prompt)
}

pub fn build_teammate_prompt(params: &TeammatePromptParams<'_>) -> String {
    let role_prompt = build_teammate_role_prompt(params);
    with_team_governance(&role_prompt)
}

fn build_lead_role_prompt(params: &LeadPromptParams<'_>) -> String {
    let _ = (
        params.teammates,
        params.available_agent_types,
        params.available_assistants,
        params.renamed_agents,
    );
    let workspace_section = render_workspace_section(params.team_workspace);

    let preset_formatting_step_rule = "";
    let preset_formatting_important_rule = "";

    LEAD_PROMPT_TEMPLATE
        .replace("{{AGENT_NAME}}", &params.agent.name)
        .replace("{{AGENT_SLOT_ID}}", &params.agent.slot_id)
        .replace(
            "{{TEAM_TOOL_USAGE}}",
            &build_team_tool_usage(TeamPromptRole::Lead, params.tool_transport),
        )
        .replace(PLACEHOLDER_WORKSPACE_SECTION, &workspace_section)
        .replace(PLACEHOLDER_PRESET_FORMATTING_STEP_RULE, preset_formatting_step_rule)
        .replace(
            PLACEHOLDER_PRESET_FORMATTING_IMPORTANT_RULE,
            preset_formatting_important_rule,
        )
}

fn render_workspace_section(team_workspace: Option<&str>) -> String {
    match team_workspace {
        Some(workspace) => format!(
            "\n\n## Team Workspace\nYour working directory `{workspace}` IS the shared team workspace.\n\
             All teammates work in this directory for project-related operations."
        ),
        None => String::new(),
    }
}

const TEAMMATE_PROMPT_TEMPLATE: &str = r#"# You are a Team Member

## Your Identity
Name: {{AGENT_NAME}}
Slot ID: {{AGENT_SLOT_ID}}
Role: teammate

## Conversation Style
- If the user greets you directly without assigning work yet, reply warmly, briefly state your role, and invite them to share what they need (without task-board boilerplate)

## Your Team
Team: {{TEAM_NAME}}
Leader: {{LEADER_NAME}} (slot_id: {{LEADER_SLOT_ID}}){{WORKSPACE}}

## Team Coordination Tools
{{TEAM_TOOL_USAGE}}

Display names are only for user-facing text. Never pass display names as agent targets — always pass exact `slot_id` values (`{{LEADER_SLOT_ID}}` or from `team_members`). Task IDs in the wake summary (8-char prefixes or full UUIDs) work directly in `team_task_update`.
All unread messages queued before this turn are already delivered in `## New Messages`. Do NOT call `team_read_messages` routinely at the start or end of a turn; call it only when `has_more: true` (`since_message_id` = `next_since_message_id`) or after a multi-minute run. If a long message was saved to `.aionui/handoffs/<id>.md`, read that file directly; do not act on a raw `content_truncated: true` preview without reading its handoff file.

## How to Work
1. Read `## New Messages` and your assigned task in the wake summary
2. If you have an actionable unblocked task, start executing immediately (call `team_task_update` with `status="in_progress"` only for multi-step/long work; for quick 1-turn tasks go straight to completion) — do NOT send a separate "starting now / acknowledged" chat message
3. Execute the work with your native tools (Read, Write, Edit, Bash, etc.)
4. When finished, prefer 1-shot `team_task_update` (`status="completed"`, `reply_message="<4-State Result Digest>"`, optional `reply_files`) to mark the task completed AND deliver your report to the leader in a single atomic call!
5. Use team_send_message to report results to the leader slot_id (`{{LEADER_SLOT_ID}}`) only when replying without a task update (never duplicate a report already sent via `reply_message`). Format every report as a concise **4-State Result Digest** (`<200` tokens; save verbose logs to a workspace file and cite the path):
   - `Status: DONE | DONE_WITH_CONCERNS | BLOCKED | NEEDS_CONTEXT`
   - `Files: <modified/created paths or none>`
   - `Verification: <commands/tests run + pass/fail>`
   - `Notes: <concise key findings, risks, or exact blocker>`

## Standing By (CRITICAL — read carefully)
"Standing by" means **end your current turn immediately** so the runtime parks you in `idle` at zero token cost until new work arrives:
- Do NOT send "Ready / standing by / waiting for instructions" ping-pong messages when no task is assigned.
- **STOP GENERATING.** Never keep an LLM stream open while waiting (avoids ~300s provider timeouts).

## Bug Fix Priority & Shutdown Requests
- Bug fixes: **locate the problem → fix the problem → types/code style last**.
- On `shutdown_request`: send `shutdown_approved` (or `shutdown_rejected: <reason>`) via `team_send_message` to `{{LEADER_SLOT_ID}}`.
- Stay strictly within your assigned scope; ask the leader immediately if blocked."#;

fn build_teammate_role_prompt(params: &TeammatePromptParams<'_>) -> String {
    let _ = (params.teammates, params.renamed_agents);

    let workspace_section = match params.team_workspace {
        Some(workspace) => format!(
            "\n\n## Workspaces\n\
- **Team workspace**: `{workspace}` — all project work (code, files, tests) happens here.\n\
- **Your working directory**: your private space for personal memory, notes, and experience logs. Not for project files.\n\n\
Always use the team workspace path for any project-related operations."
        ),
        None => String::new(),
    };

    TEAMMATE_PROMPT_TEMPLATE
        .replace("{{AGENT_NAME}}", &params.agent.name)
        .replace("{{AGENT_SLOT_ID}}", &params.agent.slot_id)
        .replace("{{TEAM_NAME}}", params.team_name)
        .replace("{{LEADER_NAME}}", &params.leader.name)
        .replace("{{LEADER_SLOT_ID}}", &params.leader.slot_id)
        .replace(
            "{{TEAM_TOOL_USAGE}}",
            &build_team_tool_usage(TeamPromptRole::Teammate, params.tool_transport),
        )
        .replace("{{WORKSPACE}}", &workspace_section)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prompt_agent(slot_id: &str, name: &str, role: TeamPromptRole) -> TeamPromptAgent {
        TeamPromptAgent {
            slot_id: slot_id.to_owned(),
            name: name.to_owned(),
            role,
            backend: "claude".to_owned(),
            model: "sonnet".to_owned(),
            status: None,
        }
    }

    #[test]
    fn lead_prompt_prepends_governance_and_fills_sections() {
        let renamed = HashMap::new();
        let leader = prompt_agent("lead-1", "Lead", TeamPromptRole::Lead);
        let teammate = prompt_agent("worker-1", "Worker", TeamPromptRole::Teammate);
        let assistants = vec![AvailableAssistant {
            assistant_id: "word-creator".to_owned(),
            name: "Word Creator".to_owned(),
            backend: "claude".to_owned(),
            description: "Drafts documents".to_owned(),
            skills: vec!["docx".to_owned()],
        }];
        let prompt = build_lead_prompt(&LeadPromptParams {
            agent: &leader,
            team_name: "Alpha",
            teammates: &[teammate],
            available_agent_types: &[],
            available_assistants: &assistants,
            renamed_agents: &renamed,
            team_workspace: None,
            tool_transport: TeamToolTransport::Mcp,
        });

        assert!(prompt.starts_with("## Team Governance"));
        assert!(prompt.contains("Name: Lead"));
        assert!(prompt.contains("Slot ID: lead-1"));
        assert!(prompt.contains("Role: lead"));
        assert!(!prompt.contains("## Your Teammates"));
        assert!(!prompt.contains("## Available Assistants for Spawning"));
        assert!(!prompt.contains("- Worker (claude, status: unknown)"));
        assert!(prompt.to_lowercase().contains("first team turn"));
        assert!(prompt.contains("team_members"));
        assert!(prompt.contains("team_list_assistants"));
        assert!(prompt.contains("Do NOT call `team_read_messages` routinely"));
        assert!(prompt.contains("`next_since_message_id`"));
        assert!(prompt.contains("status=\"in_review\""));
        assert!(prompt.contains("If the user explicitly asks you to implement, fix, or edit code"));
        assert!(prompt.contains("skip the rest of this workflow"));
        assert!(!prompt.contains("${"));
    }

    #[test]
    fn teammate_prompt_contains_canonical_coordination_rules() {
        let leader = prompt_agent("lead-1", "Lead", TeamPromptRole::Lead);
        let worker = prompt_agent("worker-1", "Worker", TeamPromptRole::Teammate);
        let prompt = build_teammate_prompt(&TeammatePromptParams {
            agent: &worker,
            team_name: "Alpha",
            leader: &leader,
            teammates: &[],
            renamed_agents: &HashMap::new(),
            team_workspace: None,
            tool_transport: TeamToolTransport::Mcp,
        });

        assert!(prompt.contains("## Team Governance"));
        assert!(prompt.contains("Name: Worker"));
        assert!(prompt.contains("Slot ID: worker-1"));
        assert!(prompt.contains("Role: teammate"));
        assert!(!prompt.contains("Role: general-purpose AI assistant"));
        assert!(prompt.contains("You MUST use the `team_*` MCP tools for ALL team coordination."));
        assert!(prompt.contains("Use team_send_message to report results to the leader slot_id"));
        assert!(prompt.contains("DONE | DONE_WITH_CONCERNS | BLOCKED | NEEDS_CONTEXT"));
        assert!(prompt.contains("Leader: Lead (slot_id: lead-1)"));
        assert!(prompt.contains("Display names are only for user-facing text"));
        assert!(prompt.contains("Never pass display names as agent targets"));
        assert!(prompt.contains("Do NOT call `team_read_messages` routinely"));
        assert!(prompt.contains("`content_truncated: true`"));
        assert!(prompt.contains("STOP GENERATING"));
        assert!(!prompt.contains("Teammates: Worker"));
    }
}

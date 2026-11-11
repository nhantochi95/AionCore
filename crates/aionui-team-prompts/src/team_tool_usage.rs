use aionui_api_types::{TeamToolDescriptor, TeamToolRole, TeamToolTransport};

use crate::role_prompt::TeamPromptRole;

pub fn build_team_tool_usage(role: TeamPromptRole, transport: TeamToolTransport) -> String {
    let tool_role = match role {
        TeamPromptRole::Lead => TeamToolRole::Lead,
        TeamPromptRole::Teammate => TeamToolRole::Teammate,
    };
    let descriptors = aionui_api_types::team_tool_descriptors_for_role(tool_role);
    match transport {
        TeamToolTransport::Mcp => render_mcp_usage(role, &descriptors),
        TeamToolTransport::CliAssumed => render_cli_usage(role, &descriptors),
    }
}

fn render_mcp_usage(role: TeamPromptRole, descriptors: &[TeamToolDescriptor]) -> String {
    let names = descriptors
        .iter()
        .map(|tool| format!("`{}`", tool.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut text = format!(
        "You MUST use the `team_*` MCP tools for ALL team coordination. Do NOT use similarly named built-in tools.\n\
Use `slot_id` values for all agent targets. Available MCP tools (full schemas in MCP `tools/list`): {names}.\n\
If a `team_*` MCP call fails schema/permission checks, run `\"$AIONUI_HELPER_BIN\" team capabilities` or `team help`, then retry the MCP tool call with corrected arguments. If MCP tools are unavailable/disconnected, use the Team CLI fallback via `\"$AIONUI_HELPER_BIN\" team ...`.\n"
    );
    if role == TeamPromptRole::Teammate {
        text.push_str("Lead-only tools are unavailable to teammates.\n");
    }
    text
}

fn render_cli_usage(role: TeamPromptRole, descriptors: &[TeamToolDescriptor]) -> String {
    let mut text = String::from(
        "You MUST use AionCore Team CLI for ALL team coordination (all commands read JSON from stdin and accept no positional args):\n\
- POSIX sh/bash: `printf '%s' '{...}' | \"$AIONUI_HELPER_BIN\" team ...`\n\
- Windows PowerShell: `'{...}' | & \"$env:AIONUI_HELPER_BIN\" team ...` (never use bash `printf` in PowerShell)\n\n",
    );
    match role {
        TeamPromptRole::Lead => text.push_str(
            "Fast 1-shot Lead patterns:\n\
- Dispatch 2+ tasks atomically in 1 call: `'[{\"subject\":\"...\",\"description\":\"...\",\"owner\":\"<slot_id>\"}]' | & \"$env:AIONUI_HELPER_BIN\" team task create-batch`\n\
- Mixed tool batch in 1 call: `'[{\"tool\":\"team_task_update\",\"arguments\":{...}}]' | & \"$env:AIONUI_HELPER_BIN\" team batch`\n\n",
        ),
        TeamPromptRole::Teammate => text.push_str(
            "Fast 1-shot Teammate pattern:\n\
- Complete task + report to Lead in 1 atomic call (no extra send-message needed): `'{\"task_id\":\"<id>\",\"status\":\"completed\",\"reply_message\":\"Status: DONE...\"}' | & \"$env:AIONUI_HELPER_BIN\" team task update`\n\n",
        ),
    }
    text.push_str(
        "Run `\"$AIONUI_HELPER_BIN\" team help` for a concise guide or `\"$AIONUI_HELPER_BIN\" team capabilities` for full JSON schemas.\n\
Do not guess team_id, slot_id, role, permissions, or internal tokens. Do not claim you used MCP tools in CLI transport.\n\
Use `slot_id` values from this prompt, `team context`, or `team members` for all agent targets.\n\
If the CLI returns `schema_validation_failed`, `unknown_command`, or `permission_denied`, consult `team help` or `team capabilities`, correct the call, and retry at most once.\n\n\
| When | CLI command | Canonical tool | Input summary |\n\
| --- | --- | --- | --- |\n",
    );
    for tool in descriptors {
        text.push_str(&format!(
            "| {} | `team {}` | `{}` | {} |\n",
            tool.when,
            tool.cli_command.join(" "),
            tool.name,
            tool.input_summary
        ));
    }
    if role == TeamPromptRole::Teammate {
        text.push_str("\nLead-only tools are unavailable to teammates.\n");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_teammate_usage_excludes_lead_only_tools() {
        let usage = build_team_tool_usage(TeamPromptRole::Teammate, TeamToolTransport::CliAssumed);
        assert!(usage.contains("\"$AIONUI_HELPER_BIN\" team"));
        assert!(usage.contains("team send-message"));
        assert!(!usage.contains("team spawn-agent"));
        assert!(usage.contains("Lead-only tools are unavailable to teammates"));
        assert!(usage.contains("reply_message"));
        assert!(!usage.contains("create-batch"), "teammate CLI guide must not advertise lead batch creation");
    }

    #[test]
    fn cli_lead_usage_includes_create_batch_pattern() {
        let usage = build_team_tool_usage(TeamPromptRole::Lead, TeamToolTransport::CliAssumed);
        assert!(usage.contains("team task create-batch"));
        assert!(usage.contains("team batch"));
    }

    #[test]
    fn mcp_lead_usage_includes_lead_tools() {
        let usage = build_team_tool_usage(TeamPromptRole::Lead, TeamToolTransport::Mcp);
        assert!(usage.contains("team_*` MCP tools"));
        assert!(usage.contains("team_spawn_agent"));
        assert!(usage.contains("\"$AIONUI_HELPER_BIN\" team capabilities"));
        assert!(usage.contains("retry the MCP tool call with corrected arguments"));
        assert!(usage.contains("use the Team CLI fallback"));
    }

    #[test]
    fn mcp_teammate_usage_includes_shared_fallback_guidance() {
        let usage = build_team_tool_usage(TeamPromptRole::Teammate, TeamToolTransport::Mcp);
        assert!(usage.contains("\"$AIONUI_HELPER_BIN\" team capabilities"));
        assert!(usage.contains("retry the MCP tool call with corrected arguments"));
        assert!(usage.contains("use the Team CLI fallback"));
        assert!(usage.contains("Lead-only tools are unavailable to teammates"));
    }
}

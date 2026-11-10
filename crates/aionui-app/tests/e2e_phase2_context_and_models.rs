#![allow(clippy::disallowed_types)]

//! Phase 1 & Phase 2 E2E Verification Suite:
//! - `WakePayloadBudgeter` (`>2,400` chars single message + `>8,000` chars multi-message overflow to `.aionui/handoffs/<msg_id>.md`)
//! - `GET /api/team-hub/connected-models` (CLI OAuth `cli_subscription` + enabled API providers `api_key`)
//! - `POST /api/team-hub/quick-member` (lightweight `bare:*` reuse + custom role-prompted member on `aionrs`)
//! - `GET /api/team-hub/teams/{id}/context-status` & `POST /api/team-hub/teams/{id}/compact` (`{}` empty body compacting BOTH Leader & Teammates + `aionrs` session cleanup + 5-section `AnchoredHandoverEngine` + secret scrubbing)
//! - `POST /api/team-hub/teams/{id}/switch-models` (`scope: "all"` & `scope: "teammates_only"` across `claude` -> `codex` -> `aionrs` OpenRouter + Dual Cost Wallet updates + `"user_id"` WebSocket broadcasts + Telegram `/model` & `/compact` inline buttons)

mod common;

use std::collections::HashSet;

use aionui_app::{AppConfig, AppServices, build_module_states, create_router_with_states};
use aionui_channel::team_bridge::ChannelTeamBridgePort;
use aionui_channel::types::{
    ActionBehavior, ActionContext, MessageContentType, PluginType, UnifiedIncomingMessage, UnifiedMessageContent,
    UnifiedUser,
};
use aionui_common::now_ms;
use aionui_db::custom_bridge_schema::{
    CustomStreamMode, CustomTargetType, SqliteCustomBridgeRepository, UpsertChannelBindingParams, WalletType,
};
use aionui_db::models::MessageRow;
use aionui_db::{CreateProviderParams, IProviderRepository, SqliteProviderRepository};
use aionui_team::{
    MailboxMessage, MailboxMessageType, TeamAgent, TeamExternalBridgePort, TeammateRole,
    build_wake_payload_with_workspace,
};
use axum::http::StatusCode;
use serde_json::json;
use tower::ServiceExt;

use common::{body_json, get_with_token, json_with_token, setup_and_login};

const OWNER_ID: &str = "system_default_user";
const CLAUDE_AGENT_ID: &str = "2d23ff1c";
const CODEX_AGENT_ID: &str = "8e1acf31";

async fn build_temp_app(
    root: &std::path::Path,
) -> (axum::Router, AppServices, std::sync::Arc<aionui_app::bridge::TelegramTeamHub>) {
    let data_dir = root.join("appdata");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("aionui-backend.db");
    let db = aionui_db::init_database(&db_path).await.expect("init temp sqlite db");
    let config = AppConfig {
        data_dir,
        work_dir: root.join("workdir"),
        ..Default::default()
    };
    std::fs::create_dir_all(&config.work_dir).unwrap();
    let services = AppServices::from_config(db, &config).await.expect("build AppServices");
    let (states, _) = build_module_states(&services).await.expect("build module states");
    let hub = states.shared_room.hub.clone();
    let router = create_router_with_states(&services, states);
    (router, services, hub)
}

async fn make_cli_agents_online(services: &AppServices) {
    let command = std::env::current_exe()
        .expect("test exe")
        .to_string_lossy()
        .into_owned();
    let source_info = json!({ "binary_name": command }).to_string();

    for agent_id in [CLAUDE_AGENT_ID, CODEX_AGENT_ID] {
        sqlx::query(
            "UPDATE agent_metadata \
             SET command_override = ?, agent_source_info = ?, last_check_status = 'ok', \
                 updated_at = unixepoch('now','subsec') * 1000 \
             WHERE id = ?",
        )
        .bind(&command)
        .bind(&source_info)
        .bind(agent_id)
        .execute(services.database.pool())
        .await
        .unwrap();

        services.agent_registry.reload_one(agent_id).await.unwrap();
    }
}

async fn seed_openrouter_provider(services: &AppServices) -> String {
    let provider_repo = SqliteProviderRepository::new(services.database.pool().clone());
    let created = provider_repo
        .create(CreateProviderParams {
            id: Some("prov-openrouter-test"),
            user_id: OWNER_ID,
            platform: "openrouter",
            name: "OpenRouter",
            base_url: "https://openrouter.ai/api/v1",
            api_key_encrypted: "enc_dummy_key",
            models: r#"["anthropic/claude-sonnet-4","openai/gpt-4.1","google/gemini-2.5-pro"]"#,
            enabled: true,
            capabilities: "[]",
            context_limit: Some(200_000),
            model_protocols: None,
            model_enabled: None,
            model_health: None,
            model_settings: "{}",
            bedrock_config: None,
            is_full_url: false,
        })
        .await
        .unwrap();

    // Also seed a disabled provider + unsupported google-auth provider to verify filtering
    let _ = provider_repo
        .create(CreateProviderParams {
            id: Some("prov-disabled-test"),
            user_id: OWNER_ID,
            platform: "openai",
            name: "DisabledOpenAI",
            base_url: "https://api.openai.com/v1",
            api_key_encrypted: "enc_dummy",
            models: r#"["gpt-4o"]"#,
            enabled: false,
            capabilities: "[]",
            context_limit: None,
            model_protocols: None,
            model_enabled: None,
            model_health: None,
            model_settings: "{}",
            bedrock_config: None,
            is_full_url: false,
        })
        .await
        .unwrap();

    let _ = provider_repo
        .create(CreateProviderParams {
            id: Some("prov-unsupported-gauth"),
            user_id: OWNER_ID,
            platform: "gemini-with-google-auth",
            name: "LegacyGAuth",
            base_url: "",
            api_key_encrypted: "",
            models: r#"["gemini-pro"]"#,
            enabled: true,
            capabilities: "[]",
            context_limit: None,
            model_protocols: None,
            model_enabled: None,
            model_health: None,
            model_settings: "{}",
            bedrock_config: None,
            is_full_url: false,
        })
        .await
        .unwrap();

    created.id
}

#[test]
fn tier1_wake_payload_budgeter_truncates_and_writes_handoffs_file() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path().to_string_lossy().into_owned();

    let lead = TeamAgent {
        slot_id: "lead-slot".into(),
        name: "ArchitectLead".into(),
        role: TeammateRole::Lead,
        conversation_id: "conv-lead".into(),
        backend: "claude".into(),
        model: "claude".into(),
        assistant_id: None,
        status: None,
        conversation_type: None,
        cli_path: None,
    };

    let long_body = format!(
        "INTRO_SIGNAL_ALPHA\n{}\nCONCLUSION_SIGNAL_OMEGA",
        "Detailed implementation diff line...\n".repeat(120)
    );
    assert!(long_body.chars().count() > 2_400);

    let msg = MailboxMessage {
        id: "mbox-report-9001".into(),
        team_id: "team-x".into(),
        to_agent_id: "lead-slot".into(),
        from_agent_id: "worker-slot".into(),
        msg_type: MailboxMessageType::Message,
        content: long_body.clone(),
        summary: Some("Completed auth refactor".into()),
        files: None,
        read: false,
        created_at: 1_700_000_000_000,
    };

    let roster: HashSet<String> = ["lead-slot".to_owned(), "worker-slot".to_owned()]
        .into_iter()
        .collect();
    let wake_prompt = build_wake_payload_with_workspace(&lead, &[], &[msg], &roster, Some(&ws));

    // Must preserve U-shaped head + tail signals while truncating middle
    assert!(wake_prompt.contains("INTRO_SIGNAL_ALPHA"));
    assert!(wake_prompt.contains("CONCLUSION_SIGNAL_OMEGA"));
    assert!(wake_prompt.contains("[truncated — full report saved to"));
    assert!(wake_prompt.chars().count() < long_body.chars().count());

    // Full report must be saved to `<team_workspace>/.aionui/handoffs/<msg_id>.md`
    let handoff_path = tmp
        .path()
        .join(".aionui")
        .join("handoffs")
        .join("mbox-report-9001.md");
    assert!(handoff_path.exists(), "handoff file must be written on overflow");
    let disk_content = std::fs::read_to_string(&handoff_path).unwrap();
    assert!(disk_content.contains(&long_body));
}

#[tokio::test]
async fn e2e_connected_models_quick_member_compaction_and_cross_engine_hot_swap() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services, hub) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;

    make_cli_agents_online(&services).await;
    let openrouter_id = seed_openrouter_provider(&services).await;

    // -----------------------------------------------------------------------
    // 1. GET /api/team-hub/connected-models
    // -----------------------------------------------------------------------
    let cat_resp = app
        .clone()
        .oneshot(get_with_token("/api/team-hub/connected-models", &token))
        .await
        .unwrap();
    assert_eq!(cat_resp.status(), StatusCode::OK);
    let cat_json = body_json(cat_resp).await;
    let cli_agents = cat_json["data"]["cli_agents"].as_array().unwrap();
    let api_providers = cat_json["data"]["api_providers"].as_array().unwrap();

    assert!(
        cli_agents
            .iter()
            .any(|c| c["backend"] == "claude" && c["wallet_type"] == "cli_subscription" && c["supports_native_compact"] == true),
        "claude CLI must appear as cli_subscription with native compact: {cli_agents:?}"
    );
    assert!(
        cli_agents
            .iter()
            .any(|c| c["backend"] == "codex" && c["wallet_type"] == "cli_subscription" && c["supports_native_compact"] == true),
        "codex CLI must appear as cli_subscription with native compact: {cli_agents:?}"
    );
    assert_eq!(
        api_providers.len(),
        1,
        "only enabled supported API providers must appear (disabled & gemini-with-google-auth excluded)"
    );
    assert_eq!(api_providers[0]["provider_id"], openrouter_id);
    assert_eq!(api_providers[0]["provider_name"], "OpenRouter");
    assert_eq!(api_providers[0]["backend"], "aionrs");
    assert_eq!(api_providers[0]["engine_kind"], "api_provider");
    assert_eq!(api_providers[0]["wallet_type"], "api_key");
    assert_eq!(api_providers[0]["models"].as_array().unwrap().len(), 3);

    // -----------------------------------------------------------------------
    // 2. POST /api/team-hub/quick-member (bare reuse) + Create Team + Quick Member into Team
    // -----------------------------------------------------------------------
    let qm_bare_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/team-hub/quick-member",
            json!({
                "name": "FastClaudeLead",
                "backend": "claude",
                "model_id": "claude",
                "save_to_catalog": false
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(qm_bare_resp.status(), StatusCode::OK);
    let qm_bare = body_json(qm_bare_resp).await["data"].clone();
    assert_eq!(qm_bare["saved_to_catalog"], false);
    assert_eq!(qm_bare["assistant_name"], "FastClaudeLead");
    assert_eq!(qm_bare["wallet_type"], "cli_subscription");
    assert_eq!(qm_bare["assistant_id"], format!("bare:{CLAUDE_AGENT_ID}"));

    // Create a Team with Claude Lead + Codex Teammate
    let team_ws = tmp.path().join("team_alpha_ws");
    std::fs::create_dir_all(&team_ws).unwrap();
    let create_team_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/teams",
            json!({
                "name": "AlphaSquad",
                "workspace": team_ws.to_string_lossy(),
                "agents": [
                    {
                        "name": "AlphaLead",
                        "role": "lead",
                        "model": "claude",
                        "assistant_id": qm_bare["assistant_id"]
                    },
                    {
                        "name": "CodexWorker",
                        "role": "teammate",
                        "model": "codex",
                        "assistant_id": format!("bare:{CODEX_AGENT_ID}")
                    }
                ]
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(create_team_resp.status(), StatusCode::CREATED);
    let team_data = body_json(create_team_resp).await["data"].clone();
    let team_id = team_data["id"].as_str().unwrap().to_owned();

    // Add a 3rd member directly onto `aionrs` (OpenRouter) with a custom role_prompt via `/api/team-hub/quick-member`
    let qm_api_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/team-hub/quick-member",
            json!({
                "name": "OpenRouterReviewer",
                "role_prompt": "You audit security and SQL migrations.",
                "backend": "aionrs",
                "provider_id": openrouter_id,
                "model_id": "anthropic/claude-sonnet-4",
                "save_to_catalog": true,
                "team_id": team_id
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(qm_api_resp.status(), StatusCode::OK);
    let qm_api = body_json(qm_api_resp).await["data"].clone();
    assert_eq!(qm_api["saved_to_catalog"], true);
    assert!(qm_api["added_agent"]["slot_id"].as_str().is_some());
    assert_eq!(qm_api["added_agent"]["backend"], "aionrs");
    assert_eq!(qm_api["added_agent"]["model"], "anthropic/claude-sonnet-4");

    // -----------------------------------------------------------------------
    // 3. Seed conversation messages (including secrets!) + aionrs session file, then test Context Status & `{}` Compact
    // -----------------------------------------------------------------------
    let status_before_resp = app
        .clone()
        .oneshot(get_with_token(
            &format!("/api/team-hub/teams/{team_id}/context-status"),
            &token,
        ))
        .await
        .unwrap();
    assert_eq!(status_before_resp.status(), StatusCode::OK);
    let status_before = body_json(status_before_resp).await["data"].clone();
    assert_eq!(status_before["auto_compact_enabled"], true);
    assert_eq!(status_before["auto_compact_threshold_pct"], 80);
    let slots_before = status_before["slots"].as_array().unwrap();
    assert_eq!(slots_before.len(), 3);

    let lead_conv_id = slots_before
        .iter()
        .find(|s| s["role"] == "lead")
        .unwrap()["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let lead_slot_id = slots_before
        .iter()
        .find(|s| s["role"] == "lead")
        .unwrap()["slot_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let aionrs_conv_id = qm_api["added_agent"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Seed messages into Lead (`acp`) and OpenRouterReviewer (`aionrs`) with secrets & file paths
    for (idx, cid) in [&lead_conv_id, &aionrs_conv_id].iter().enumerate() {
        services
            .conversation_repo
            .insert_message(
                OWNER_ID,
                &MessageRow {
                    id: format!("m-user-{idx}"),
                    conversation_id: (*cid).clone(),
                    msg_id: Some(format!("m-user-{idx}")),
                    r#type: "text".into(),
                    content: json!({
                        "content": format!(
                            "Refactor crates/aionui-app/src/routes/team_hub.rs and never leak sk-live-SuperSecretToken999 or Authorization: Bearer myJwtSecret123 or api_key=topSecretVal777 ({})",
                            "context_padding ".repeat(400)
                        )
                    })
                    .to_string(),
                    position: Some("right".into()),
                    status: Some("finish".into()),
                    hidden: false,
                    created_at: 1_700_000_001_000,
                    backend_turn_id: None,
                },
            )
            .await
            .unwrap();

        services
            .conversation_repo
            .insert_message(
                OWNER_ID,
                &MessageRow {
                    id: format!("m-asst-{idx}"),
                    conversation_id: (*cid).clone(),
                    msg_id: Some(format!("m-asst-{idx}")),
                    r#type: "text".into(),
                    content: json!({
                        "content": "- Decided to use atomic SQLite transactions in crates/aionui-db/src/custom_bridge_schema.rs.\n- Updated crates/aionui-app/src/bridge/hub.rs for Telegram inline buttons."
                    })
                    .to_string(),
                    position: Some("left".into()),
                    status: Some("finish".into()),
                    hidden: false,
                    created_at: 1_700_000_002_000,
                    backend_turn_id: None,
                },
            )
            .await
            .unwrap();
    }

    // Create a simulated `aionrs-sessions/<aionrs_conv_id>.json` file on disk
    let aionrs_sessions_dir = services.data_dir.join("aionrs-sessions");
    std::fs::create_dir_all(&aionrs_sessions_dir).unwrap();
    let aionrs_session_file = aionrs_sessions_dir.join(format!("{aionrs_conv_id}.json"));
    std::fs::write(&aionrs_session_file, r#"{"id":"test","messages":[1,2,3]}"#).unwrap();
    assert!(aionrs_session_file.exists());

    let mut ws_rx = services.event_bus.subscribe();

    // Call `POST /api/team-hub/teams/{id}/compact` with `{}` EMPTY BODY!
    let compact_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/team-hub/teams/{team_id}/compact"),
            json!({}),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(compact_resp.status(), StatusCode::OK);
    let compact_json = body_json(compact_resp).await["data"].clone();
    let outcomes = compact_json["outcomes"].as_array().unwrap();
    assert_eq!(
        outcomes.len(),
        3,
        "empty-body compact must compact BOTH Leader and all Teammates (including aionrs)"
    );
    assert!(outcomes.iter().any(|o| o["role"] == "lead" && o["strategy_used"] == "anchored_rollover"));
    assert!(outcomes.iter().any(|o| o["conversation_id"] == aionrs_conv_id && o["previous_tokens"].as_i64().is_some()));

    // `aionrs` session file must be deleted on rollover while SQLite messages stay visible!
    assert!(
        !aionrs_session_file.exists(),
        "aionrs session file must be removed on rollover so next turn starts fresh"
    );
    let remaining_msgs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE conversation_id = ?")
        .bind(&aionrs_conv_id)
        .fetch_one(services.database.pool())
        .await
        .unwrap();
    assert_eq!(remaining_msgs, 2, "visible SQLite chat history must remain intact");

    // Verify 5-section AnchoredHandoverEngine summary + secret scrubbing + one-time banner consumption
    let bridge_repo = SqliteCustomBridgeRepository::new(services.database.pool().clone());
    let lead_anchor = bridge_repo
        .get_team_context_anchor(&team_id, &lead_slot_id)
        .await
        .unwrap()
        .expect("lead anchor must exist");
    assert_eq!(lead_anchor.pending_handover, 1);
    assert_eq!(lead_anchor.compaction_count, 1);
    assert!(lead_anchor.anchored_summary_md.contains("## Session Intent"));
    assert!(lead_anchor
        .anchored_summary_md
        .contains("## Artifact Trail (Files Modified/Read)"));
    assert!(lead_anchor.anchored_summary_md.contains("## Key Decisions"));
    assert!(lead_anchor.anchored_summary_md.contains("## Active & Blocked Tasks"));
    assert!(lead_anchor.anchored_summary_md.contains("## Next Steps"));
    assert!(lead_anchor
        .anchored_summary_md
        .contains("crates/aionui-app/src/routes/team_hub.rs"));
    assert!(
        !lead_anchor.anchored_summary_md.contains("SuperSecretToken999"),
        "sk-* secret must be scrubbed: {}",
        lead_anchor.anchored_summary_md
    );
    assert!(
        !lead_anchor.anchored_summary_md.contains("myJwtSecret123"),
        "Bearer token must be scrubbed: {}",
        lead_anchor.anchored_summary_md
    );
    assert!(
        !lead_anchor.anchored_summary_md.contains("topSecretVal777"),
        "api_key assignment must be scrubbed: {}",
        lead_anchor.anchored_summary_md
    );

    // `pending_slot_handover_banner` consumes `pending_handover` atomically once
    let banner = hub
        .pending_slot_handover_banner(OWNER_ID, &team_id, &lead_slot_id, &lead_conv_id)
        .await
        .expect("must return anchored handover banner on first wake");
    assert!(banner.starts_with("## Anchored Session Handover"));
    assert!(
        hub.pending_slot_handover_banner(OWNER_ID, &team_id, &lead_slot_id, &lead_conv_id)
            .await
            .is_none(),
        "second wake must not re-inject already consumed handover"
    );

    // -----------------------------------------------------------------------
    // 4. POST /api/team-hub/teams/{id}/switch-models (`teammates_only` & `all`) + Dual Cost Wallet
    // -----------------------------------------------------------------------
    // 4a. `scope: "teammates_only"` -> switch workers to `codex`, keeping Lead on `claude`!
    let sw_workers_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/team-hub/teams/{team_id}/switch-models"),
            json!({
                "scope": "teammates_only",
                "target": {
                    "backend": "codex",
                    "model_id": "codex"
                }
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(sw_workers_resp.status(), StatusCode::OK);
    let sw_workers = body_json(sw_workers_resp).await["data"].clone();
    assert_eq!(sw_workers["outcomes"].as_array().unwrap().len(), 2);
    assert_eq!(sw_workers["outcomes"][0]["new_backend"], "codex");
    assert_eq!(sw_workers["outcomes"][0]["new_model"], "codex");
    assert_eq!(sw_workers["results"].as_array().unwrap().len(), 2);
    assert_eq!(sw_workers["lead_wallet_type"], "cli_subscription");

    let assistants_after_workers = sw_workers["team"]["assistants"].as_array().unwrap();
    let lead_after_workers = assistants_after_workers
        .iter()
        .find(|a| a["role"] == "lead")
        .unwrap();
    assert_eq!(
        lead_after_workers["backend"], "claude",
        "teammates_only must preserve Leader's engine/model"
    );
    for worker in assistants_after_workers.iter().filter(|a| a["role"] != "lead") {
        assert_eq!(worker["backend"], "codex");
    }

    // 4b. `scope: "all"` -> switch ALL members (including Leader!) to `aionrs` OpenRouter (`openai/gpt-4.1`)
    let sw_all_api_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/team-hub/teams/{team_id}/switch-models"),
            json!({
                "scope": "all",
                "target": {
                    "backend": "aionrs",
                    "provider_id": openrouter_id,
                    "model_id": "openai/gpt-4.1"
                }
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(sw_all_api_resp.status(), StatusCode::OK);
    let sw_all_api = body_json(sw_all_api_resp).await["data"].clone();
    assert_eq!(sw_all_api["results"].as_array().unwrap().len(), 3);
    assert_eq!(
        sw_all_api["lead_wallet_type"],
        WalletType::ApiKey.as_str(),
        "switching Leader to aionrs OpenRouter must reclassify Lead wallet to api_key"
    );

    // Verify DB conversation row for Lead was atomically rebound to `type = 'aionrs'` with `ProviderWithModel`
    let lead_conv_row = services
        .conversation_repo
        .get(OWNER_ID, &lead_conv_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lead_conv_row.r#type, "aionrs");
    assert!(lead_conv_row.model.as_deref().unwrap().contains(&openrouter_id));
    assert!(lead_conv_row.model.as_deref().unwrap().contains("openai/gpt-4.1"));

    // 4c. Switch `scope: "all"` back to `codex` CLI ($0 subscription) -> Lead wallet returns to `cli_subscription`!
    let sw_back_cli_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/team-hub/teams/{team_id}/switch-models"),
            json!({
                "scope": "all",
                "target": {
                    "backend": "codex",
                    "model_id": "codex"
                }
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(sw_back_cli_resp.status(), StatusCode::OK);
    let sw_back_cli = body_json(sw_back_cli_resp).await["data"].clone();
    assert_eq!(
        sw_back_cli["lead_wallet_type"],
        WalletType::CliSubscription.as_str(),
        "switching Leader back to Codex CLI must restore $0 cli_subscription wallet"
    );

    // Verify WebSocket events (`team:context_compacted`, `team:context_usage`, `team:models_switched`) all carry `user_id`!
    let mut saw_compacted_ws = false;
    let mut saw_usage_ws = false;
    let mut saw_switched_ws = false;
    while let Ok(ev) = ws_rx.try_recv() {
        match ev.name.as_str() {
            "team:context_compacted" => {
                assert_eq!(ev.data["user_id"], OWNER_ID);
                saw_compacted_ws = true;
            }
            "team:context_usage" => {
                assert_eq!(ev.data["user_id"], OWNER_ID);
                saw_usage_ws = true;
            }
            "team:models_switched" => {
                assert_eq!(ev.data["user_id"], OWNER_ID);
                saw_switched_ws = true;
            }
            _ => {}
        }
    }
    assert!(saw_compacted_ws, "team:context_compacted with user_id must be broadcast");
    assert!(saw_usage_ws, "team:context_usage with user_id must be broadcast");
    assert!(saw_switched_ws, "team:models_switched with user_id must be broadcast");

    // -----------------------------------------------------------------------
    // 5. Telegram Topic `/compact` and `/model` inline keyboard + atomic one-shot token lock
    // -----------------------------------------------------------------------
    bridge_repo
        .upsert_channel_binding(UpsertChannelBindingParams {
            owner_user_id: OWNER_ID.into(),
            plugin_id: "telegram".into(),
            chat_id: "-100777888".into(),
            thread_id: Some("99".into()),
            target_type: CustomTargetType::Team,
            target_id: team_id.clone(),
            stream_mode: CustomStreamMode::Milestones,
        })
        .await
        .unwrap();

    let incoming_topic_msg = UnifiedIncomingMessage {
        id: "tg-msg-101".into(),
        platform: PluginType::Telegram,
        chat_id: "-100777888".into(),
        owner_user_id: Some(OWNER_ID.into()),
        user: UnifiedUser {
            id: "555001".into(),
            username: Some("nhantc".into()),
            display_name: "Nhan".into(),
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type: MessageContentType::Command,
            text: "/model".into(),
            attachments: None,
        },
        timestamp: now_ms(),
        action: None,
        reply_to_message_id: None,
        raw: Some(json!({ "chat_type": "supergroup", "message_thread_id": 99 })),
    };

    let model_cmd_res = hub
        .handle_bridge_command(OWNER_ID, &incoming_topic_msg, "/model", "")
        .await
        .unwrap()
        .expect("/model in bound topic must return ActionResponse");
    let rows = model_cmd_res.buttons.expect("/model must attach inline keyboard rows");
    assert!(!rows.is_empty());
    let first_all_btn = &rows[0][0];
    assert!(first_all_btn.label.contains("All:"));
    let tok = first_all_btn.action.strip_prefix("t:").unwrap();

    let tap_ctx = ActionContext {
        platform: PluginType::Telegram,
        user_id: "555001".into(),
        chat_id: "-100777888".into(),
        message_id: Some("8801".into()),
        session_id: None,
    };
    let tap_res = hub
        .handle_bridge_token(OWNER_ID, tok, &tap_ctx, None)
        .await
        .unwrap();
    assert_eq!(tap_res.behavior, ActionBehavior::Edit);
    assert_eq!(tap_res.edit_message_id.as_deref(), Some("8801"));
    assert!(tap_res.buttons.is_none(), "inline keyboard buttons must be removed after selection");

    // Second tap on same token must be rejected idempotently (`consume_action_token_once`)
    let second_tap = hub
        .handle_bridge_token(OWNER_ID, tok, &tap_ctx, None)
        .await
        .unwrap();
    assert_eq!(second_tap.behavior, ActionBehavior::Answer);

    // `/compact` command in bound topic
    let compact_cmd_res = hub
        .handle_bridge_command(OWNER_ID, &incoming_topic_msg, "/compact", "")
        .await
        .unwrap()
        .expect("/compact in bound topic must return ActionResponse");
    assert!(compact_cmd_res.text.unwrap().contains("AlphaSquad"));
}

#[tokio::test]
async fn e2e_turn_settled_drains_queued_model_swap_and_triggers_80_pct_auto_compact() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services, hub) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;

    make_cli_agents_online(&services).await;

    // Seed an OpenRouter provider with a compact 2,000-token context window to test 80% auto-compact deterministically
    let provider_repo = SqliteProviderRepository::new(services.database.pool().clone());
    let small_prov = provider_repo
        .create(CreateProviderParams {
            id: Some("prov-small-window"),
            user_id: OWNER_ID,
            platform: "openrouter",
            name: "CompactWindowAPI",
            base_url: "https://openrouter.ai/api/v1",
            api_key_encrypted: "enc_dummy",
            models: r#"["openai/gpt-4.1-mini"]"#,
            enabled: true,
            capabilities: "[]",
            context_limit: Some(2_000),
            model_protocols: None,
            model_enabled: None,
            model_health: None,
            model_settings: "{}",
            bedrock_config: None,
            is_full_url: false,
        })
        .await
        .unwrap();

    // Ensure generated `bare:*` assistants are materialized
    let _ = app
        .clone()
        .oneshot(get_with_token("/api/team-hub/connected-models", &token))
        .await
        .unwrap();

    let team_ws = tmp.path().join("team_turn_settle_ws");
    std::fs::create_dir_all(&team_ws).unwrap();
    let create_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/teams",
            json!({
                "name": "TurnSettleTeam",
                "workspace": team_ws.to_string_lossy(),
                "agents": [
                    {
                        "name": "SettleLead",
                        "role": "lead",
                        "model": "claude",
                        "assistant_id": format!("bare:{CLAUDE_AGENT_ID}")
                    }
                ]
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let team_json = body_json(create_resp).await["data"].clone();
    let team_id = team_json["id"].as_str().unwrap().to_owned();
    let lead_slot_id = team_json["assistants"][0]["slot_id"].as_str().unwrap().to_owned();
    let lead_conv_id = team_json["assistants"][0]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Seed ~7,200 chars (~1,800 tokens = 90% of 2,000-token window >= 80% threshold!)
    services
        .conversation_repo
        .insert_message(
            OWNER_ID,
            &MessageRow {
                id: "m-heavy-1".into(),
                conversation_id: lead_conv_id.clone(),
                msg_id: Some("m-heavy-1".into()),
                r#type: "text".into(),
                content: json!({
                    "content": format!(
                        "Implement crates/aionui-app/src/bridge/context_compactor.rs\n{}",
                        "Detailed architectural analysis line for long session. ".repeat(135)
                    )
                })
                .to_string(),
                position: Some("right".into()),
                status: Some("finish".into()),
                hidden: false,
                created_at: 1_700_000_010_000,
                backend_turn_id: None,
            },
        )
        .await
        .unwrap();

    // Part A: Queue a deferred turn-boundary swap in `custom_team_slot_swaps` (`claude` -> `aionrs` CompactWindowAPI)
    let bridge_repo = SqliteCustomBridgeRepository::new(services.database.pool().clone());
    bridge_repo
        .upsert_pending_slot_swap(
            &team_id,
            &lead_slot_id,
            "aionrs",
            Some(&small_prov.id),
            "openai/gpt-4.1-mini",
        )
        .await
        .unwrap();

    let mut ws_rx = services.event_bus.subscribe();

    // Trigger `on_slot_turn_settled` -> drains queued swap
    hub.on_slot_turn_settled(OWNER_ID, &team_id, &lead_slot_id, &lead_conv_id);

    let mut saw_switched = false;
    let deadline_a = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !saw_switched && tokio::time::Instant::now() < deadline_a {
        if let Ok(Ok(ev)) = tokio::time::timeout(std::time::Duration::from_millis(250), ws_rx.recv()).await
            && ev.name == "team:models_switched"
        {
            assert_eq!(ev.data["user_id"], OWNER_ID);
            saw_switched = true;
        }
    }
    assert!(saw_switched, "queued model swap must execute on turn settle");
    assert!(
        bridge_repo
            .get_pending_slot_swap(&team_id, &lead_slot_id)
            .await
            .unwrap()
            .is_none(),
        "pending slot swap must be drained after execution"
    );

    // Part B: Seed a new heavy turn (`m-heavy-2`, ~7,200 chars = ~1,800 tokens = 90% >= 80% of 2,000 window)
    services
        .conversation_repo
        .insert_message(
            OWNER_ID,
            &MessageRow {
                id: "m-heavy-2".into(),
                conversation_id: lead_conv_id.clone(),
                msg_id: Some("m-heavy-2".into()),
                r#type: "text".into(),
                content: json!({
                    "content": format!(
                        "Decided to keep crates/aionui-db/migrations untouched.\n{}",
                        "Long turn output line exceeding eighty percent threshold. ".repeat(135)
                    )
                })
                .to_string(),
                position: Some("left".into()),
                status: Some("finish".into()),
                hidden: false,
                created_at: 1_700_000_020_000,
                backend_turn_id: None,
            },
        )
        .await
        .unwrap();

    hub.on_slot_turn_settled(OWNER_ID, &team_id, &lead_slot_id, &lead_conv_id);

    let mut saw_compacted = false;
    let deadline_b = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !saw_compacted && tokio::time::Instant::now() < deadline_b {
        if let Ok(Ok(ev)) = tokio::time::timeout(std::time::Duration::from_millis(250), ws_rx.recv()).await
            && ev.name == "team:context_compacted"
        {
            assert_eq!(ev.data["user_id"], OWNER_ID);
            saw_compacted = true;
        }
    }
    assert!(
        saw_compacted,
        "slot exceeding 80% threshold must auto-compact on turn settle"
    );

    let anchor_after = bridge_repo
        .get_team_context_anchor(&team_id, &lead_slot_id)
        .await
        .unwrap()
        .expect("anchor must exist after auto-compact");
    assert_eq!(anchor_after.compaction_count, 1);

    // Calling `on_slot_turn_settled` again with zero new messages must NOT re-compact!
    hub.on_slot_turn_settled(OWNER_ID, &team_id, &lead_slot_id, &lead_conv_id);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let anchor_idempotent = bridge_repo
        .get_team_context_anchor(&team_id, &lead_slot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        anchor_idempotent.compaction_count, 1,
        "turn-settle auto-compact must not loop on zero new messages"
    );
}

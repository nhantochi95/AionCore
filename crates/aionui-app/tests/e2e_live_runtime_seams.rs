#![allow(clippy::disallowed_types)]

//! End-to-End Live Runtime Seams Verification Suite (`M1`–`M5` Post-Audit).
//!
//! Verifies the live integration seams across HTTP, WebSocket `user_id` scoping,
//! Shared Room wake-chip muting, i18n sender labels, Dual Cost Wallet classification,
//! Action Inbox Desktop ↔ Telegram sync, and `{}` one-click Worktree Squash Merge.

mod common;

use std::process::Command;

use aionui_app::{AppConfig, AppServices, build_module_states, create_router_with_states};
use aionui_db::custom_bridge_schema::{
    CreateActionInboxParams, CustomTargetType, SqliteCustomBridgeRepository, WalletType,
    classify_agent_wallet,
};
use axum::http::StatusCode;
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{body_json, json_with_token, setup_and_login};

const OWNER_ID: &str = "system_default_user";
const DEFAULT_TEAM_ASSISTANT_ID: &str = "seams-e2e-assistant";
const DEFAULT_TEAM_AGENT_ID: &str = "2d23ff1c";

async fn build_temp_app(root: &std::path::Path) -> (axum::Router, AppServices) {
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
    let router = create_router_with_states(&services, states);
    (router, services)
}

async fn ensure_team_assistant(
    app: &mut axum::Router,
    services: &AppServices,
    token: &str,
    csrf: &str,
) {
    let command = std::env::current_exe()
        .expect("test exe")
        .to_string_lossy()
        .into_owned();
    let source_info = json!({ "binary_name": command }).to_string();

    sqlx::query(
        "UPDATE agent_metadata \
         SET agent_source = 'custom', agent_source_info = ?, command = ?, args = '[]', env = '[]', \
             updated_at = unixepoch('now','subsec') * 1000 \
         WHERE agent_id = ?",
    )
    .bind(&source_info)
    .bind(&command)
    .bind(DEFAULT_TEAM_AGENT_ID)
    .execute(services.database.pool())
    .await
    .unwrap();

    services
        .agent_registry
        .reload_one(DEFAULT_TEAM_AGENT_ID)
        .await
        .unwrap();

    let req = json_with_token(
        "POST",
        "/api/assistants",
        json!({
            "id": DEFAULT_TEAM_ASSISTANT_ID,
            "name": "Seams E2E Assistant",
            "agent_id": DEFAULT_TEAM_AGENT_ID
        }),
        token,
        csrf,
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::CREATED || resp.status() == StatusCode::CONFLICT,
        "unexpected assistant status: {}",
        resp.status()
    );
}

async fn create_team(
    app: &mut axum::Router,
    services: &AppServices,
    token: &str,
    csrf: &str,
    name: &str,
    workspace: &std::path::Path,
) -> Value {
    std::fs::create_dir_all(workspace).unwrap();
    ensure_team_assistant(app, services, token, csrf).await;
    let body = json!({
        "name": name,
        "workspace": workspace.to_string_lossy(),
        "agents": [
            {
                "name": format!("{name} Lead"),
                "role": "lead",
                "model": "claude",
                "assistant_id": DEFAULT_TEAM_ASSISTANT_ID
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(json_with_token("POST", "/api/teams", body, token, csrf))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json = body_json(resp).await;
    json["data"].clone()
}

#[test]
fn wallet_classifier_recognizes_all_four_cli_subscriptions_vs_api_keys() {
    assert_eq!(
        classify_agent_wallet(Some("claude"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("codex"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("gemini"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("gemini-cli"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("antigravity"), None, false),
        WalletType::CliSubscription
    );
    // Custom API key overrides CLI backend to ApiKey wallet
    assert_eq!(
        classify_agent_wallet(Some("gemini"), None, true),
        WalletType::ApiKey
    );
    assert_eq!(
        classify_agent_wallet(Some("openai"), None, false),
        WalletType::ApiKey
    );
}

#[tokio::test]
async fn e2e_shared_room_wake_chip_muting_and_en_vi_sender_labels() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;

    let ws_be = tmp.path().join("ws_be");
    let ws_fe = tmp.path().join("ws_fe");
    let team_be = create_team(&mut app, &services, &token, &csrf, "BackendTeam", &ws_be).await;
    let team_fe = create_team(&mut app, &services, &token, &csrf, "FrontendTeam", &ws_fe).await;
    let tid_be = team_be["id"].as_str().unwrap();
    let tid_fe = team_fe["id"].as_str().unwrap();

    // Create Shared Room with BackendTeam as Host and FrontendTeam as Member
    let room_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/shared-rooms",
            json!({
                "name": "auth-hub",
                "team_ids": [tid_be, tid_fe],
                "host_team_id": tid_be,
                "max_hops": 5
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(room_resp.status(), StatusCode::CREATED);
    let room_id = body_json(room_resp).await["data"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    // 1. Clean boot defaults to en-US -> sender_label must be "You" (not hardcoded Vietnamese)
    //    AND muting all wake chips (`wake_targets: []`) must wake ZERO agents!
    let mute_all_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/shared-rooms/{room_id}/messages"),
            json!({
                "content": "@BackendTeam @FrontendTeam General note without waking anyone",
                "wake_targets": []
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(mute_all_resp.status(), StatusCode::OK);
    let mute_all_json = body_json(mute_all_resp).await;
    assert_eq!(mute_all_json["data"]["message"]["sender_label"], "You");
    assert_eq!(
        mute_all_json["data"]["routed_targets"].as_array().unwrap().len(),
        0,
        "Muting all wake chips must prevent waking any team even if @mentions appear in content"
    );

    // 2. Muting BackendTeam chip while leaving FrontendTeam chip unmuted routes ONLY to FrontendTeam
    let subset_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/shared-rooms/{room_id}/messages"),
            json!({
                "content": "Please polish button spacing",
                "wake_targets": ["FrontendTeam"]
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(subset_resp.status(), StatusCode::OK);
    let subset_json = body_json(subset_resp).await;
    let routed = subset_json["data"]["routed_targets"].as_array().unwrap();
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0]["team_id"], tid_fe);

    // 3. Switch language to vi-VN -> sender_label hot-switches to "Anh (User)"
    let lang_resp = app
        .clone()
        .oneshot(json_with_token(
            "PATCH",
            "/api/settings",
            json!({ "language": "vi-VN" }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(lang_resp.status(), StatusCode::OK);

    let vi_msg_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/shared-rooms/{room_id}/messages"),
            json!({
                "content": "Ghi chú tiếng Việt",
                "wake_targets": []
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(vi_msg_resp.status(), StatusCode::OK);
    let vi_msg_json = body_json(vi_msg_resp).await;
    assert_eq!(vi_msg_json["data"]["message"]["sender_label"], "Anh (User)");
}

#[tokio::test]
async fn e2e_inbox_resolve_emits_user_scoped_ws_and_empty_body_worktree_merge_works() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;

    let bridge_repo = SqliteCustomBridgeRepository::new(services.database.pool().clone());
    let mut rx = services.event_bus.subscribe();

    // 1. Seed an Action Inbox card with a Telegram action token & budget extend test
    let tok = bridge_repo
        .create_action_token(
            OWNER_ID,
            CustomTargetType::Team,
            "team-1",
            "budget_ack",
            &json!({}),
            None,
        )
        .await
        .unwrap();

    let card = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("inbox_e2e_seam_1".into()),
            owner_user_id: OWNER_ID.into(),
            category: "budget_warning".into(),
            title: "80% API Budget Alert".into(),
            summary: "API spend reached $40 / $50".into(),
            details_json: json!({ "spent_usd": 40.0, "budget_limit_usd": 50.0 }).to_string(),
            target_type: Some("wallet".into()),
            target_id: "dual_cost_wallet".into(),
            action_token: Some(tok.clone()),
            telegram_chat_id: Some("-100999888".into()),
            telegram_msg_id: Some("77".into()),
        })
        .await
        .unwrap();

    let resolve_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/inbox/{}/resolve", card.id),
            json!({ "action": "extend", "payload": { "extend_usd": 25.0 } }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(resolve_resp.status(), StatusCode::OK);

    // Action token must be consumed atomically so Telegram replay is blocked
    assert!(
        !bridge_repo.consume_action_token_once(&tok).await.unwrap(),
        "Desktop resolution must invalidate the paired Telegram action token"
    );

    // Budget limit must be raised from $50 -> $75
    let wallet = bridge_repo.get_or_create_cost_wallet(OWNER_ID).await.unwrap();
    assert_eq!(wallet.api_key_budget_limit_usd, 75.0);

    // Verify both `wallet:spend_updated` and `inbox:item_resolved` carry `user_id`!
    let mut saw_wallet_ws = false;
    let mut saw_inbox_ws = false;
    while let Ok(ev) = rx.try_recv() {
        if ev.name == "wallet:spend_updated" {
            assert_eq!(ev.data["user_id"], OWNER_ID);
            assert_eq!(ev.data["budget_limit_usd"], 75.0);
            saw_wallet_ws = true;
        }
        if ev.name == "inbox:item_resolved" {
            assert_eq!(ev.data["user_id"], OWNER_ID);
            assert_eq!(ev.data["id"], "inbox_e2e_seam_1");
            assert_eq!(ev.data["resolved_by"], "desktop");
            saw_inbox_ws = true;
        }
    }
    assert!(saw_wallet_ws, "wallet:spend_updated with user_id must be broadcast");
    assert!(saw_inbox_ws, "inbox:item_resolved with user_id must be broadcast");

    // 2. Verify `{}` empty-body POST `/api/worktrees/{id}/merge` (ipcBridge contract)
    let repo_dir = tmp.path().join("git_repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let run_git = |cwd: &std::path::Path, args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("git command");
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run_git(&repo_dir, &["init", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.name", "Dev"]);
    run_git(&repo_dir, &["config", "user.email", "dev@local"]);
    std::fs::write(repo_dir.join("README.md"), "# Init\n").unwrap();
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "chore: initial commit"]);

    let engine = aionui_project::GitWorktreeEngine::new();
    let wt_row = engine
        .allocate_worktree(
            aionui_project::WorktreeConfig {
                lease_id: "lease-ui-btn".into(),
                repo_path: repo_dir.clone(),
                base_branch: "main".into(),
                room_id: None,
                team_id: Some("auth".into()),
                owner_user_id: OWNER_ID.into(),
            },
            Some(&bridge_repo),
        )
        .await
        .unwrap();
    let wt_path = std::path::PathBuf::from(&wt_row.worktree_path);
    std::fs::write(wt_path.join("auth.ts"), "export const ok = true;\n").unwrap();
    run_git(&wt_path, &["add", "."]);
    run_git(&wt_path, &["commit", "-m", "feat: add auth module"]);

    // Send `{}` just like `ipcBridge.worktrees.merge.invoke({ id })`
    let merge_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/worktrees/lease-ui-btn/merge",
            json!({}),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(merge_resp.status(), StatusCode::OK);
    let merge_json = body_json(merge_resp).await;
    assert!(merge_json["success"].as_bool().unwrap());
    let commit_msg = merge_json["data"]["commit_message"].as_str().unwrap();
    assert_eq!(commit_msg, "feat(auth): squash merge worktree lease-ui-btn");
    assert!(!commit_msg.contains("Co-authored-by"));
}

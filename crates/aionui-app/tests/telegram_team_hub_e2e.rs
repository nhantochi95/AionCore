//! Comprehensive E2E & Torture Test Suite for Chặng 1 (Telegram <-> Team Leader Bridge)
//! and Chặng 2 (Multi-Team Shared Room / Leader Hub).

mod common;

use std::sync::Arc;

use aionui_ai_agent::{RuntimeTokenScope, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use aionui_app::{AppConfig, AppServices, build_module_states, create_router_with_states};
use aionui_channel::ChannelRouterState;
use aionui_channel::action::{ActionExecutor, MessageResult};
use aionui_channel::types::{
    ActionCategory, ActionContext, MessageContentType, PluginType, UnifiedAction, UnifiedIncomingMessage,
    UnifiedMessageContent, UnifiedUser,
};
use aionui_common::now_ms;
use aionui_db::models::AssistantUserRow;
use aionui_db::{CustomTargetType, IChannelRepository, SqliteChannelRepository, SqliteCustomBridgeRepository};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{body_json, delete_with_token, get_with_token, json_with_token, setup_and_login};

const OWNER_ID: &str = "system_default_user";
const DEFAULT_TEAM_ASSISTANT_ID: &str = "hub-e2e-assistant";
const DEFAULT_TEAM_AGENT_ID: &str = "2d23ff1c";

async fn build_temp_app(root: &std::path::Path) -> (axum::Router, AppServices, ChannelRouterState) {
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
    let channel_state = states.channel.clone();
    let router = create_router_with_states(&services, states);
    (router, services, channel_state)
}

async fn seed_paired_telegram_user(services: &AppServices, tg_user_id: &str) {
    let repo = SqliteChannelRepository::new(services.database.pool().clone());
    repo.create_user(
        OWNER_ID,
        &AssistantUserRow {
            id: format!("u_tg_{tg_user_id}"),
            owner_user_id: OWNER_ID.to_owned(),
            platform_user_id: tg_user_id.to_owned(),
            platform_type: "telegram".to_owned(),
            display_name: Some("Anh Nhan".to_owned()),
            authorized_at: now_ms(),
            last_active: Some(now_ms()),
            session_id: None,
        },
    )
    .await
    .expect("seed paired telegram user");
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
            "name": "Hub E2E Assistant",
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

async fn create_team_with_workspace(
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
            },
            {
                "name": format!("{name} Worker"),
                "role": "teammate",
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

async fn call_runtime_team_tool(
    app: &axum::Router,
    services: &AppServices,
    conversation_id: &str,
    tool: &str,
    arguments: Value,
) -> (StatusCode, Value) {
    let issued = services.runtime_token_service.issue(
        OWNER_ID,
        conversation_id,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext, RuntimeTokenScope::TeamCall],
    );
    let req = Request::builder()
        .method("POST")
        .uri("/api/runtime/team-tools/call")
        .header("content-type", "application/json")
        .header("x-aionui-user-id", OWNER_ID)
        .header("x-aionui-conversation-id", conversation_id)
        .header("x-aionui-runtime-token", issued.token)
        .body(Body::from(
            serde_json::to_vec(&json!({
                "tool": tool,
                "arguments": arguments,
            }))
            .unwrap(),
        ))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = body_json(resp).await;
    (status, body)
}

#[tokio::test]
async fn e2e_team_telegram_bridge_config_and_active_dm_switching() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services, _states) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_paired_telegram_user(&services, "1356474106").await;

    let ws_a = tmp.path().join("ws_backend");
    let ws_b = tmp.path().join("ws_frontend");
    let team_a = create_team_with_workspace(&mut app, &services, &token, &csrf, "BackendTeam", &ws_a).await;
    let team_b = create_team_with_workspace(&mut app, &services, &token, &csrf, "FrontendTeam", &ws_b).await;
    let tid_a = team_a["id"].as_str().unwrap();
    let tid_b = team_b["id"].as_str().unwrap();

    // 1. Default list returns both teams with notify_enabled=true (paired user exists) and milestones mode
    let list_resp = app
        .clone()
        .oneshot(get_with_token("/api/channel/team-bindings", &token))
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_json = body_json(list_resp).await;
    let configs = list_json["data"].as_array().unwrap();
    assert_eq!(configs.len(), 2);
    assert!(configs.iter().all(|c| c["has_authorized_user"] == true && c["notify_enabled"] == true));

    // 2. Configure Team A: full_stream + active DM target + Telegram Forum Topic binding
    let put_a = app
        .clone()
        .oneshot(json_with_token(
            "PUT",
            &format!("/api/channel/team-bindings/{tid_a}"),
            json!({
                "notify_enabled": true,
                "stream_mode": "full_stream",
                "set_active_dm_target": true,
                "group_chat_id": "-100555666",
                "group_thread_id": "77"
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(put_a.status(), StatusCode::OK);
    let cfg_a = body_json(put_a).await["data"].clone();
    assert_eq!(cfg_a["stream_mode"], "full_stream");
    assert_eq!(cfg_a["is_active_dm_target"], true);
    assert_eq!(cfg_a["group_chat_id"], "-100555666");
    assert_eq!(cfg_a["group_thread_id"], "77");

    // 3. Switch active DM target to Team B -> Team A automatically loses active DM flag on that chat!
    let put_b = app
        .clone()
        .oneshot(json_with_token(
            "PUT",
            &format!("/api/channel/team-bindings/{tid_b}"),
            json!({ "set_active_dm_target": true }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(put_b.status(), StatusCode::OK);
    assert_eq!(body_json(put_b).await["data"]["is_active_dm_target"], true);

    let recheck_a = app
        .clone()
        .oneshot(get_with_token(&format!("/api/channel/team-bindings/{tid_a}"), &token))
        .await
        .unwrap();
    assert_eq!(body_json(recheck_a).await["data"]["is_active_dm_target"], false);

    // 4. Delete Team A's Group binding
    let del_group = app
        .clone()
        .oneshot(delete_with_token(
            &format!("/api/channel/team-bindings/{tid_a}"),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(del_group.status(), StatusCode::OK);
    assert!(body_json(del_group).await["data"]["group_chat_id"].is_null());
}

#[tokio::test]
async fn e2e_shared_room_routing_artifacts_max_hops_and_deleted_team_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services, _states) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_paired_telegram_user(&services, "1356474106").await;

    let ws_chair = tmp.path().join("ws_chair");
    let ws_be = tmp.path().join("ws_be");
    let ws_fe = tmp.path().join("ws_fe");

    let chair = create_team_with_workspace(&mut app, &services, &token, &csrf, "ChairTeam", &ws_chair).await;
    let be = create_team_with_workspace(&mut app, &services, &token, &csrf, "BackendTeam", &ws_be).await;
    let fe = create_team_with_workspace(&mut app, &services, &token, &csrf, "FrontendTeam", &ws_fe).await;

    let chair_id = chair["id"].as_str().unwrap().to_owned();
    let be_id = be["id"].as_str().unwrap().to_owned();
    let fe_id = fe["id"].as_str().unwrap().to_owned();

    let chair_lead_conv = chair["assistants"][0]["conversation_id"].as_str().unwrap().to_owned();
    let be_lead_conv = be["assistants"][0]["conversation_id"].as_str().unwrap().to_owned();
    let fe_lead_conv = fe["assistants"][0]["conversation_id"].as_str().unwrap().to_owned();

    // Create Shared Room with max_hops = 2 and ChairTeam as host
    let create_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/shared-rooms",
            json!({
                "name": "LaunchHub",
                "description": "Phòng phối hợp ra mắt sản phẩm",
                "rules": "Chốt API contract trước khi code UI",
                "team_ids": [&chair_id, &be_id, &fe_id],
                "host_team_id": &chair_id,
                "max_hops": 2
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let room = body_json(create_resp).await["data"].clone();
    let room_id = room["id"].as_str().unwrap().to_owned();

    // 1. Human attaches a file from an external Desktop/Documents folder (outside all team workspaces!)
    let user_docs = tmp.path().join("user_documents");
    std::fs::create_dir_all(&user_docs).unwrap();
    let prd_file = user_docs.join("product_prd.md");
    std::fs::write(&prd_file, "# PRD v1\nBuild auth + dashboard").unwrap();

    let human_msg_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/shared-rooms/{room_id}/messages"),
            json!({
                "content": "Anh gửi PRD, nhờ chủ trì điều phối nhé!",
                "files": [prd_file.to_string_lossy()]
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(human_msg_resp.status(), StatusCode::OK);
    let human_msg_data = body_json(human_msg_resp).await["data"].clone();

    // Untagged human message -> Tier 3 routes to ChairTeam!
    let targets = human_msg_data["routed_targets"].as_array().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0]["team_id"], chair_id);
    assert_eq!(targets[0]["tier"], 3);

    // Human PRD file must be synced into recipient `.shared-inbox/<room_id>/`
    let chair_inbox_entries: Vec<_> = std::fs::read_dir(ws_chair.join(".shared-inbox").join(&room_id))
        .unwrap()
        .collect();
    assert_eq!(chair_inbox_entries.len(), 1);

    // 2. ChairTeam Lead calls `team_room_read` and `team_room_post` (with 2 workspace files + `@BackendTeam`)
    let (read_status, read_json) = call_runtime_team_tool(
        &app,
        &services,
        &chair_lead_conv,
        "team_room_read",
        json!({ "room_id": &room_id, "limit": 10 }),
    )
    .await;
    assert_eq!(read_status, StatusCode::OK, "team_room_read failed: {read_json}");
    assert_eq!(read_json["data"]["messages"].as_array().unwrap().len(), 1);

    let spec_a = ws_chair.join("api_contract.json");
    let spec_b = ws_chair.join("db_schema.sql");
    std::fs::write(&spec_a, r#"{"version":"1.0"}"#).unwrap();
    std::fs::write(&spec_b, "CREATE TABLE items(id TEXT);").unwrap();

    let (post1_status, post1_json) = call_runtime_team_tool(
        &app,
        &services,
        &chair_lead_conv,
        "team_room_post",
        json!({
            "room_id": &room_id,
            "message": "@BackendTeam triển khai API theo 2 file spec này nhé!",
            "files": [spec_a.to_string_lossy(), spec_b.to_string_lossy()]
        }),
    )
    .await;
    assert_eq!(post1_status, StatusCode::OK, "chair team_room_post failed: {post1_json}");
    assert_eq!(post1_json["data"]["woken_targets"][0]["team_id"], be_id);
    assert_eq!(post1_json["data"]["woken_targets"][0]["tier"], 1);

    // Both files must be copied into BackendTeam's `.shared-inbox/<room_id>/` (1 PRD + 2 specs = 3 files)
    let be_inbox_count = std::fs::read_dir(ws_be.join(".shared-inbox").join(&room_id))
        .unwrap()
        .count();
    assert_eq!(be_inbox_count, 3);

    // Hop 2: BackendTeam Lead -> `@FrontendTeam`
    let (post2_status, post2_json) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_post",
        json!({
            "room_id": &room_id,
            "message": "@FrontendTeam API xong rồi, ghép UI đi!"
        }),
    )
    .await;
    assert_eq!(post2_status, StatusCode::OK);
    assert_eq!(post2_json["data"]["woken_targets"][0]["team_id"], fe_id);

    // Hop 3 (exceeds max_hops == 2!): FrontendTeam Lead -> `@BackendTeam` must hit Loop Guard!
    let (post3_status, post3_json) = call_runtime_team_tool(
        &app,
        &services,
        &fe_lead_conv,
        "team_room_post",
        json!({
            "room_id": &room_id,
            "message": "@BackendTeam kiểm tra lại CORS giúp mình!"
        }),
    )
    .await;
    assert_eq!(post3_status, StatusCode::OK);
    assert!(
        post3_json["data"]["woken_targets"].as_array().unwrap().is_empty(),
        "Loop Guard must halt bot-to-bot wakeup once hop_count_since_human == max_hops"
    );

    // Grant +5 more hops via REST endpoint -> hop_count_since_human drops back to 0
    let allow_resp = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/shared-rooms/{room_id}/allow-more-hops"),
            json!({ "grant_hops": 5 }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(allow_resp.status(), StatusCode::OK);
    assert_eq!(body_json(allow_resp).await["data"]["hop_count_since_human"], 0);

    // Anti-chatter invariant: untagged Leader closing report wakes 0 teams
    let (closing_status, closing_json) = call_runtime_team_tool(
        &app,
        &services,
        &fe_lead_conv,
        "team_room_post",
        json!({
            "room_id": &room_id,
            "message": "Báo cáo chung: UI đã hoàn tất 100%."
        }),
    )
    .await;
    assert_eq!(closing_status, StatusCode::OK);
    assert!(closing_json["data"]["woken_targets"].as_array().unwrap().is_empty());

    // Delete ChairTeam (`DELETE /api/teams/{chair_id}`) -> `TEAM_REMOVED_EVENT` cleans up membership & promotes BackendTeam to host!
    let del_team = app
        .clone()
        .oneshot(delete_with_token(&format!("/api/teams/{chair_id}"), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(del_team.status(), StatusCode::OK);

    // Poll until spawned `TEAM_REMOVED_EVENT` background watcher completes cleanup and reassigns host
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(3000);
    let mut room_after_json = serde_json::Value::Null;
    while std::time::Instant::now() < deadline {
        let room_after_del = app
            .clone()
            .oneshot(get_with_token(&format!("/api/shared-rooms/{room_id}"), &token))
            .await
            .unwrap();
        assert_eq!(room_after_del.status(), StatusCode::OK);
        room_after_json = body_json(room_after_del).await["data"].clone();
        if room_after_json["host_team_id"].as_str() != Some(&chair_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let remaining_members = room_after_json["members"].as_array().unwrap();
    assert_eq!(remaining_members.len(), 2);
    assert_eq!(room_after_json["host_team_id"], be_id);
}

#[tokio::test]
async fn e2e_cross_team_tasks_blocked_by_and_security_path_guards() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services, _states) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;

    let ws_be = tmp.path().join("ws_be");
    let ws_fe = tmp.path().join("ws_fe");
    let ws_qa = tmp.path().join("ws_qa");
    let be = create_team_with_workspace(&mut app, &services, &token, &csrf, "BackendTeam", &ws_be).await;
    let fe = create_team_with_workspace(&mut app, &services, &token, &csrf, "FrontendTeam", &ws_fe).await;
    let qa = create_team_with_workspace(&mut app, &services, &token, &csrf, "QATeam", &ws_qa).await;

    let be_id = be["id"].as_str().unwrap().to_owned();
    let fe_id = fe["id"].as_str().unwrap().to_owned();
    let qa_id = qa["id"].as_str().unwrap().to_owned();

    let be_lead_conv = be["assistants"][0]["conversation_id"].as_str().unwrap().to_owned();
    let be_worker_conv = be["assistants"][1]["conversation_id"].as_str().unwrap().to_owned();

    let create_room = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/shared-rooms",
            json!({
                "name": "PipelineRoom",
                "team_ids": [&be_id, &fe_id, &qa_id],
                "host_team_id": &be_id
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    let room_id = body_json(create_room).await["data"]["id"].as_str().unwrap().to_owned();

    // 1. Reject unknown `blocked_by` task ID!
    let (bad_dep_status, bad_dep_json) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_create",
        json!({
            "room_id": &room_id,
            "to_team": "FrontendTeam",
            "title": "Bad task",
            "blocked_by": ["rtask_nonexistent"]
        }),
    )
    .await;
    assert_ne!(bad_dep_status, StatusCode::OK, "unknown blocked_by must fail: {bad_dep_json}");

    // 2. Create Task A1 (BackendTeam), Task C1 (QATeam), Task B1 (FrontendTeam blocked_by A1), Task B2 (FrontendTeam blocked_by C1)
    let (_, t_a1_res) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_create",
        json!({ "room_id": &room_id, "to_team": "BackendTeam", "title": "Build REST API" }),
    )
    .await;
    let task_a1_id = t_a1_res["data"]["task"]["id"].as_str().unwrap().to_owned();

    let (_, t_c1_res) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_create",
        json!({ "room_id": &room_id, "to_team": "QATeam", "title": "Prepare staging cluster" }),
    )
    .await;
    let task_c1_id = t_c1_res["data"]["task"]["id"].as_str().unwrap().to_owned();

    let (_, t_b1_res) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_create",
        json!({
            "room_id": &room_id,
            "to_team": "FrontendTeam",
            "title": "Wire UI to REST API",
            "blocked_by": [&task_a1_id]
        }),
    )
    .await;
    let task_b1_id = t_b1_res["data"]["task"]["id"].as_str().unwrap().to_owned();

    let (_, _t_b2_res) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_create",
        json!({
            "room_id": &room_id,
            "to_team": "FrontendTeam",
            "title": "Run E2E on staging",
            "blocked_by": [&task_c1_id]
        }),
    )
    .await;

    // Clear FrontendTeam's pending handoff before testing per-task unblock
    let bridge_repo = SqliteCustomBridgeRepository::new(services.database.pool().clone());
    bridge_repo
        .set_member_pending_handoff(&room_id, &fe_id, None, 0)
        .await
        .unwrap();

    // Complete Task A1 with an artifact -> Task B1 MUST unblock and set pending handoff on FrontendTeam
    // even though Task B2 is still blocked by Task C1!
    let openapi_file = ws_be.join("openapi_ready.yaml");
    std::fs::write(&openapi_file, "openapi: 3.1.0").unwrap();
    let (upd_status, upd_json) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_update",
        json!({
            "task_id": &task_b1_id,
            "status": "pending"
        }),
    )
    .await;
    assert_eq!(upd_status, StatusCode::OK, "noop update failed: {upd_json}");

    // F19 Human-Only Done Guard: Agent cannot transition task to completed or done!
    let (forbidden_status, _forbidden_json) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_task_update",
        json!({
            "task_id": &task_a1_id,
            "status": "completed",
            "result_summary": "REST API shipped",
            "files": [openapi_file.to_string_lossy()]
        }),
    )
    .await;
    assert_ne!(forbidden_status, StatusCode::OK, "Agent must not be permitted to transition task to completed/done");

    // Human user finalizes the task via HTTP route
    let human_done = app
        .clone()
        .oneshot(json_with_token(
            "PATCH",
            &format!("/api/shared-rooms/{room_id}/tasks/{task_a1_id}"),
            json!({
                "status": "completed",
                "result_summary": "REST API shipped",
                "files": [openapi_file.to_string_lossy()]
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(human_done.status(), StatusCode::OK);

    let pending_fe = bridge_repo.list_pending_handoffs_for_team(&fe_id).await.unwrap();
    assert_eq!(pending_fe.len(), 1);
    assert_eq!(
        pending_fe[0].1.pending_handoff_msg_id.as_deref(),
        Some(task_b1_id.as_str()),
        "Task B1 must unblock FrontendTeam independently of Task B2"
    );

    // 3. Security guards:
    // 3a. Non-lead teammate cannot invoke `team_notify_user` or `team_room_post`
    let (worker_status, _worker_body) = call_runtime_team_tool(
        &app,
        &services,
        &be_worker_conv,
        "team_notify_user",
        json!({ "kind": "milestone", "summary": "Hijack attempt" }),
    )
    .await;
    assert_ne!(worker_status, StatusCode::OK);

    // 3b. Lead cannot exfiltrate files outside its workspace (e.g. FrontendTeam's secret or `.env` or `aionui-backend.db`)
    let fe_secret = ws_fe.join("private_notes.txt");
    std::fs::write(&fe_secret, "top secret").unwrap();
    let be_dotenv = ws_be.join(".env");
    std::fs::write(&be_dotenv, "API_KEY=secret").unwrap();

    let (exfil_status, _exfil_body) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_post",
        json!({
            "room_id": &room_id,
            "message": "Trying to attach another workspace's file",
            "files": [fe_secret.to_string_lossy()]
        }),
    )
    .await;
    assert_ne!(exfil_status, StatusCode::OK, "cross-workspace exfiltration must be blocked");

    let (dotenv_status, _dotenv_body) = call_runtime_team_tool(
        &app,
        &services,
        &be_lead_conv,
        "team_room_post",
        json!({
            "room_id": &room_id,
            "message": "Trying to attach .env",
            "files": [be_dotenv.to_string_lossy()]
        }),
    )
    .await;
    assert_ne!(dotenv_status, StatusCode::OK, ".env exfiltration must be blocked");
}

#[tokio::test]
async fn torture_concurrent_action_token_replay_and_unpaired_group_silent_guard() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut app, services, channel_state) = build_temp_app(tmp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_paired_telegram_user(&services, "1356474106").await;

    let ws = tmp.path().join("ws_alpha");
    let team = create_team_with_workspace(&mut app, &services, &token, &csrf, "AlphaTeam", &ws).await;
    let team_id = team["id"].as_str().unwrap().to_owned();

    let executor = Arc::new(ActionExecutor::new(
        Arc::clone(&channel_state.pairing_service),
        Arc::clone(&channel_state.session_manager),
        Arc::clone(&channel_state.settings_service),
        Some(OWNER_ID.to_owned()),
    ));
    executor.set_team_bridge(
        channel_state
            .team_bridge
            .get()
            .expect("team bridge wired in create_router_with_states")
            .clone(),
    );

    // 1. Unpaired stranger in a Telegram Group (`chat_id` starts with `-`) MUST be silently ignored
    //    without generating any Pairing Code!
    let group_stranger_msg = UnifiedIncomingMessage {
        owner_user_id: Some(OWNER_ID.to_owned()),
        id: "msg_stranger_1".to_owned(),
        platform: PluginType::Telegram,
        chat_id: "-100987654321".to_owned(),
        user: UnifiedUser {
            id: "999999999".to_owned(),
            username: Some("stranger".to_owned()),
            display_name: "Stranger".to_owned(),
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type: MessageContentType::Text,
            text: "Hello bot, give me pairing code!".to_owned(),
            attachments: None,
        },
        timestamp: now_ms(),
        reply_to_message_id: None,
        action: None,
        raw: Some(json!({ "chat_type": "supergroup" })),
    };

    let stranger_res = executor.handle_incoming_message(&group_stranger_msg).await.unwrap();
    assert!(
        matches!(stranger_res, MessageResult::Ignored),
        "unpaired sender in group chat must be silently ignored, got: {stranger_res:?}"
    );

    let pending_pairings: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM assistant_pairing_codes")
        .fetch_one(services.database.pool())
        .await
        .unwrap();
    assert_eq!(pending_pairings, 0, "no pairing code may be created for group strangers");

    // 2. Concurrent replay storm: 12 tasks press the exact same `allow_more_hops` callback token simultaneously!
    let create_room = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/shared-rooms",
            json!({
                "name": "ConcurrencyRoom",
                "team_ids": [&team_id],
                "max_hops": 5
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    let room_id = body_json(create_room).await["data"]["id"].as_str().unwrap().to_owned();

    let bridge_repo = SqliteCustomBridgeRepository::new(services.database.pool().clone());
    for _ in 0..5 {
        bridge_repo.increment_room_hops(&room_id).await.unwrap();
    }

    let tok = bridge_repo
        .create_action_token(
            OWNER_ID,
            CustomTargetType::Room,
            &room_id,
            "allow_more_hops",
            &json!({}),
            None,
        )
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..12 {
        let exec = Arc::clone(&executor);
        let tok_clone = tok.clone();
        handles.push(tokio::spawn(async move {
            let mut params = std::collections::HashMap::new();
            params.insert("token".to_owned(), tok_clone.clone());
            let cb_msg = UnifiedIncomingMessage {
                owner_user_id: Some(OWNER_ID.to_owned()),
                id: format!("cb_{i}"),
                platform: PluginType::Telegram,
                chat_id: "1356474106".to_owned(),
                user: UnifiedUser {
                    id: "1356474106".to_owned(),
                    username: Some("herotcn".to_owned()),
                    display_name: "Anh Nhan".to_owned(),
                    avatar_url: None,
                },
                content: UnifiedMessageContent {
                    content_type: MessageContentType::Action,
                    text: format!("t:{tok_clone}"),
                    attachments: None,
                },
                timestamp: now_ms(),
                reply_to_message_id: None,
                action: Some(UnifiedAction {
                    action: "bridge.token".to_owned(),
                    category: ActionCategory::System,
                    params: Some(params),
                    context: ActionContext {
                        platform: PluginType::Telegram,
                        user_id: "1356474106".to_owned(),
                        chat_id: "1356474106".to_owned(),
                        message_id: Some("101".to_owned()),
                        session_id: None,
                    },
                }),
                raw: Some(json!({ "chat_type": "private" })),
            };
            exec.handle_incoming_message(&cb_msg).await.unwrap()
        }));
    }

    let mut executed_count = 0;
    let mut deduped_count = 0;
    for h in handles {
        match h.await.unwrap() {
            MessageResult::Action(resp) => {
                if resp.text.is_some() {
                    executed_count += 1;
                } else if resp
                    .toast
                    .as_deref()
                    .is_some_and(|t| t.contains("already been processed") || t.contains("đã được xử lý"))
                {
                    deduped_count += 1;
                }
            }
            other => panic!("unexpected callback result: {other:?}"),
        }
    }

    assert_eq!(executed_count, 1, "one-shot token must execute at most once");
    assert_eq!(deduped_count, 11, "all 11 concurrent replays must receive idempotent toast");
}

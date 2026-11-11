mod common;

use aionui_ai_agent::{RuntimeTokenScope, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{body_json, build_app};

const USER_ID: &str = "system_default_user";
const CONVERSATION_ID: &str = "conv-runtime-team-tools";

fn context_request(user_id: &str, conversation_id: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("GET")
        .uri("/api/runtime/team-tools/context")
        .header("x-aionui-user-id", user_id)
        .header("x-aionui-conversation-id", conversation_id);
    if let Some(token) = token {
        builder = builder.header("x-aionui-runtime-token", token);
    }
    builder.body(Body::empty()).unwrap()
}

fn call_request(user_id: &str, conversation_id: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/runtime/team-tools/call")
        .header("content-type", "application/json")
        .header("x-aionui-user-id", user_id)
        .header("x-aionui-conversation-id", conversation_id);
    if let Some(token) = token {
        builder = builder.header("x-aionui-runtime-token", token);
    }
    builder
        .body(Body::from(
            serde_json::to_vec(&json!({
                "tool": "team_members",
                "arguments": {}
            }))
            .unwrap(),
        ))
        .unwrap()
}

fn assert_runtime_auth_failed(json: &Value) {
    assert_eq!(json["success"], false);
    assert_eq!(json["error"]["code"], "runtime_auth_failed");
}

#[tokio::test]
async fn runtime_team_tools_reject_missing_runtime_token() {
    let (app, _services) = build_app().await;

    let resp = app
        .oneshot(context_request(USER_ID, CONVERSATION_ID, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = body_json(resp).await;
    assert_runtime_auth_failed(&json);
}

#[tokio::test]
async fn runtime_team_tools_reject_forged_user_header() {
    let (app, services) = build_app().await;
    let issue = services.runtime_token_service.issue(
        USER_ID,
        CONVERSATION_ID,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext],
    );

    let resp = app
        .oneshot(context_request("other-user", CONVERSATION_ID, Some(&issue.token)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = body_json(resp).await;
    assert_runtime_auth_failed(&json);
}

#[tokio::test]
async fn runtime_team_tools_reject_forged_conversation_header() {
    let (app, services) = build_app().await;
    let issue = services.runtime_token_service.issue(
        USER_ID,
        CONVERSATION_ID,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext],
    );

    let resp = app
        .oneshot(context_request(USER_ID, "other-conversation", Some(&issue.token)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = body_json(resp).await;
    assert_runtime_auth_failed(&json);
}

#[tokio::test]
async fn runtime_team_tools_reject_wrong_scope_for_call() {
    let (app, services) = build_app().await;
    let issue = services.runtime_token_service.issue(
        USER_ID,
        CONVERSATION_ID,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext],
    );

    let resp = app
        .oneshot(call_request(USER_ID, CONVERSATION_ID, Some(&issue.token)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = body_json(resp).await;
    assert_runtime_auth_failed(&json);
}

#[tokio::test]
async fn runtime_team_tools_accept_matching_runtime_token_before_service_lookup() {
    let (app, services) = build_app().await;
    let issue = services.runtime_token_service.issue(
        USER_ID,
        CONVERSATION_ID,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext],
    );

    let resp = app
        .oneshot(context_request(USER_ID, CONVERSATION_ID, Some(&issue.token)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json = body_json(resp).await;
    assert_eq!(json["success"], false);
    assert_eq!(json["error"]["code"], "conversation_not_found");
}


fn batch_request(user_id: &str, conversation_id: &str, token: Option<&str>, body: Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/runtime/team-tools/call-batch")
        .header("content-type", "application/json")
        .header("x-aionui-user-id", user_id)
        .header("x-aionui-conversation-id", conversation_id);
    if let Some(token) = token {
        builder = builder.header("x-aionui-runtime-token", token);
    }
    builder
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

#[tokio::test]
async fn runtime_team_tools_batch_rejects_empty_and_oversized_calls() {
    let (app, services) = build_app().await;
    let issue = services.runtime_token_service.issue(
        USER_ID,
        CONVERSATION_ID,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamCall],
    );

    let empty_resp = app
        .clone()
        .oneshot(batch_request(
            USER_ID,
            CONVERSATION_ID,
            Some(&issue.token),
            json!({ "calls": [] }),
        ))
        .await
        .unwrap();
    assert_eq!(empty_resp.status(), StatusCode::BAD_REQUEST);
    let empty_json = body_json(empty_resp).await;
    assert_eq!(empty_json["success"], false);
    assert_eq!(empty_json["error"]["code"], "schema_validation_failed");

    let oversized_calls: Vec<Value> = (0..33)
        .map(|i| json!({ "tool": "team_task_create", "arguments": { "subject": format!("t-{i}") } }))
        .collect();
    let oversized_resp = app
        .oneshot(batch_request(
            USER_ID,
            CONVERSATION_ID,
            Some(&issue.token),
            json!({ "calls": oversized_calls }),
        ))
        .await
        .unwrap();
    assert_eq!(oversized_resp.status(), StatusCode::BAD_REQUEST);
    let oversized_json = body_json(oversized_resp).await;
    assert_eq!(oversized_json["success"], false);
    assert_eq!(oversized_json["error"]["code"], "schema_validation_failed");
}

#[tokio::test]
async fn runtime_team_tools_batch_prevalidates_atomically_and_1shot_reply_delivers_to_lead() {
    let (mut app, services) = common::build_app_with_mock_agents().await;
    let (jwt, csrf) = common::setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let user = services
        .user_repo
        .find_by_username("admin")
        .await
        .unwrap()
        .expect("admin user");

    let command = std::env::current_exe()
        .expect("exe")
        .to_string_lossy()
        .to_string();
    let source_info = json!({ "binary_name": command }).to_string();
    sqlx::query(
        "UPDATE agent_metadata \
         SET agent_source = 'custom', agent_source_info = ?, command = ?, args = '[]', env = '[]', \
             updated_at = unixepoch('now','subsec') * 1000 \
         WHERE agent_id = '2d23ff1c'",
    )
    .bind(&source_info)
    .bind(&command)
    .execute(services.database.pool())
    .await
    .unwrap();
    services.agent_registry.reload_one("2d23ff1c").await.unwrap();

    let ast_resp = app
        .clone()
        .oneshot(common::json_with_token(
            "POST",
            "/api/assistants",
            json!({
                "id": "batch-e2e-assistant",
                "name": "Batch E2E Assistant",
                "agent_id": "2d23ff1c"
            }),
            &jwt,
            &csrf,
        ))
        .await
        .unwrap();
    assert!(ast_resp.status() == StatusCode::CREATED || ast_resp.status() == StatusCode::CONFLICT);

    let create_resp = app
        .clone()
        .oneshot(common::json_with_token(
            "POST",
            "/api/teams",
            json!({
                "name": "BatchTeam",
                "agents": [
                    { "name": "Lead", "role": "lead", "model": "claude", "assistant_id": "batch-e2e-assistant" },
                    { "name": "Worker", "role": "teammate", "model": "claude", "assistant_id": "batch-e2e-assistant" }
                ]
            }),
            &jwt,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let team_json = body_json(create_resp).await;
    let team_id = team_json["data"]["id"].as_str().unwrap();
    let ensure_resp = app
        .clone()
        .oneshot(common::json_with_token(
            "POST",
            &format!("/api/teams/{team_id}/session"),
            json!({}),
            &jwt,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(ensure_resp.status(), StatusCode::OK);
    let agents = team_json["data"]["assistants"].as_array().unwrap();
    let lead = agents.iter().find(|a| a["role"] == "lead").unwrap();
    let worker = agents.iter().find(|a| a["role"] == "teammate").unwrap();
    let lead_conv = lead["conversation_id"].as_str().unwrap();
    let worker_conv = worker["conversation_id"].as_str().unwrap();
    let worker_slot = worker["slot_id"].as_str().unwrap();

    let lead_tok = services.runtime_token_service.issue(
        &user.id,
        lead_conv,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext, RuntimeTokenScope::TeamCall],
    );

    // 1. Pre-validation atomicity: item #1 has an unknown field -> item #0 MUST NOT execute!
    let bad_batch = app
        .clone()
        .oneshot(batch_request(
            &user.id,
            lead_conv,
            Some(&lead_tok.token),
            json!({
                "calls": [
                    { "tool": "team_task_create", "arguments": { "subject": "Should Not Exist" } },
                    { "tool": "team_task_create", "arguments": { "subject": "Bad", "nonexistent_field": 1 } }
                ]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(bad_batch.status(), StatusCode::BAD_REQUEST);

    let list_after_bad = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/runtime/team-tools/call")
                .header("content-type", "application/json")
                .header("x-aionui-user-id", &user.id)
                .header("x-aionui-conversation-id", lead_conv)
                .header("x-aionui-runtime-token", &lead_tok.token)
                .body(Body::from(
                    serde_json::to_vec(&json!({ "tool": "team_task_list", "arguments": {} })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let list_json = body_json(list_after_bad).await;
    assert_eq!(
        list_json["data"].as_array().unwrap().len(),
        0,
        "failed prevalidation on item #1 must prevent item #0 from creating a task"
    );

    // 2. Valid atomic batch (also exercises `tool_name` serde alias!) creates 2 tasks in 1 HTTP call
    let ok_batch = app
        .clone()
        .oneshot(batch_request(
            &user.id,
            lead_conv,
            Some(&lead_tok.token),
            json!({
                "calls": [
                    { "tool_name": "team_task_create", "arguments": { "subject": "Task Alpha", "owner": worker_slot } },
                    { "tool": "team_task_create", "arguments": { "subject": "Task Beta", "owner": worker_slot } }
                ]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(ok_batch.status(), StatusCode::OK);
    let ok_batch_json = body_json(ok_batch).await;
    assert_eq!(ok_batch_json["success"], true);
    assert_eq!(ok_batch_json["data"]["count"], 2);
    let task_alpha_id = ok_batch_json["data"]["results"][0]["data"]["task"]["task_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // 3. Worker completes Task Alpha AND sends 1-shot `reply_message` to Lead in a single atomic call
    // (Issue token right before the call because assigning Task Alpha woke Worker's mock runtime, rotating its token.)
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    let worker_tok = services.runtime_token_service.issue(
        &user.id,
        worker_conv,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::TeamContext, RuntimeTokenScope::TeamCall],
    );
    let update_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/runtime/team-tools/call")
                .header("content-type", "application/json")
                .header("x-aionui-user-id", &user.id)
                .header("x-aionui-conversation-id", worker_conv)
                .header("x-aionui-runtime-token", &worker_tok.token)
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "tool": "team_task_update",
                        "arguments": {
                            "task_id": task_alpha_id,
                            "status": "completed",
                            "reply_message": "Status: DONE\nFiles: none\nVerification: ok\nNotes: Alpha finished.",
                            "reply_files": ["docs/alpha.md"]
                        }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update_resp.status(), StatusCode::OK);
    let update_json = body_json(update_resp).await;
    assert_eq!(update_json["data"]["task"]["status"], "completed");
    assert_eq!(update_json["data"]["reply_sent"], true);

    // 4. Verify Lead's persisted mailbox row contains Worker's atomic 1-shot reply + files
    // (checking SQLite `mailbox` directly avoids racing with IMockAgent's instant wake consumer).
    let lead_slot = lead["slot_id"].as_str().unwrap();
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT content, files FROM mailbox WHERE team_id = ? AND to_agent_id = ? AND from_agent_id = ?",
    )
    .bind(team_id)
    .bind(lead_slot)
    .bind(worker_slot)
    .fetch_all(services.database.pool())
    .await
    .unwrap();
    assert!(
        rows.iter().any(|(content, files)| {
            content.contains("Alpha finished.")
                && files.as_deref().unwrap_or_default().contains("docs/alpha.md")
        }),
        "Lead mailbox must persist Worker's atomic 1-shot reply_message + reply_files; got: {rows:?}"
    );
}

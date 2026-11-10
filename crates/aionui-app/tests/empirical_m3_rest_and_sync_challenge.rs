#![allow(clippy::disallowed_types)]

//! Empirical Challenger Test Suite for Milestone 3 (M3).
//!
//! Focus Areas:
//! 1. REST API Invariants & Error Handling:
//!    - GET /api/wallets/summary, POST /api/wallets/budget-limit, POST /api/wallets/reset
//!    - GET /api/inbox, POST /api/inbox/{id}/resolve
//!    - Status codes on non-existent IDs (404), already resolved cards (409), forbidden access (403), invalid payloads (400/422).
//! 2. Cross-Surface Synchronization:
//!    - Resolving via Telegram action token automatically synchronizes Action Inbox card to 'resolved' with `resolved_by: 'telegram'`.
//!    - Resolving on desktop consumes action token and prevents Telegram replay.
//!    - WebSocket event emission for `inbox:item_resolved`.

use std::sync::Arc;

use aionui_api_types::ApiResponse;
use aionui_auth::CurrentUser;
use aionui_db::custom_bridge_schema::{
    CreateActionInboxParams, CustomTargetType, SqliteCustomBridgeRepository,
};
use aionui_db::init_database_memory;
use aionui_project::GitWorktreeEngine;
use aionui_realtime::{BroadcastEventBus, EventBroadcaster};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use tower::ServiceExt;

use aionui_app::routes::inbox::{
    inbox_routes, ActionInboxListResponse, InboxRouterState, ResolveActionInboxResponse,
};
use aionui_app::routes::wallets::{wallets_routes, WalletSummaryDto, WalletsRouterState};

fn mock_test_user(id: &str, username: &str) -> CurrentUser {
    let mut u = CurrentUser::local_default();
    u.id = id.into();
    u.username = username.into();
    u
}

#[tokio::test]
async fn challenge_wallets_rest_api_invariants_and_error_codes() {
    let db = init_database_memory().await.unwrap();
    let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let event_bus = Arc::new(BroadcastEventBus::new(16));

    let user = mock_test_user("user_wallet_inv", "wallet_user");
    let app = Router::new()
        .merge(wallets_routes(WalletsRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
        }))
        .layer(axum::Extension(user.clone()));

    // 1. GET /api/wallets/summary with clean user: default values
    let req = Request::builder()
        .method("GET")
        .uri("/api/wallets/summary")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
    let summary = dto.data.unwrap();
    assert_eq!(summary.api_key_budget_limit_usd, 50.0);
    assert_eq!(summary.api_key_spent_usd, 0.0);
    assert_eq!(summary.cli_subscription_runs, 0);
    assert_eq!(summary.api_key_runs, 0);
    assert!(!summary.soft_warning);
    assert!(!summary.hard_blocked);

    // 2. POST /api/wallets/budget-limit with negative float -> 400 Bad Request
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/budget-limit")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"budget_limit_usd": -15.5}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // 3. POST /api/wallets/budget-limit with 0.0 -> 200 OK (0 limit allowed)
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/budget-limit")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"budget_limit_usd": 0.0}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
    assert_eq!(dto.data.unwrap().api_key_budget_limit_usd, 0.0);

    // 4. POST /api/wallets/budget-limit with valid positive limit -> 200 OK
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/budget-limit")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"budget_limit_usd": 125.75}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
    assert_eq!(dto.data.unwrap().api_key_budget_limit_usd, 125.75);

    // 5. POST /api/wallets/budget-limit with malformed JSON -> 422 Unprocessable Entity or 400
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/budget-limit")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"budget_limit_usd": "not_a_number"}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert!(res.status() == StatusCode::UNPROCESSABLE_ENTITY || res.status() == StatusCode::BAD_REQUEST);

    // 6. POST /api/wallets/budget-limit with empty body -> 422 or 400
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/budget-limit")
        .header("content-type", "application/json")
        .body(Body::from(r#"{}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert!(res.status() == StatusCode::UNPROCESSABLE_ENTITY || res.status() == StatusCode::BAD_REQUEST);

    // 7. POST /api/wallets/reset -> resets spend counters
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/reset")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
    let reset = dto.data.unwrap();
    assert_eq!(reset.api_key_spent_usd, 0.0);
    assert_eq!(reset.cli_subscription_runs, 0);
    assert_eq!(reset.api_key_runs, 0);
    assert!(!reset.soft_warning);
    assert!(!reset.hard_blocked);
}

#[tokio::test]
async fn challenge_inbox_rest_api_invariants_and_error_codes() {
    let db = init_database_memory().await.unwrap();
    let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let event_bus = Arc::new(BroadcastEventBus::new(16));
    let worktree_engine = Arc::new(GitWorktreeEngine::new());

    let user_alice = mock_test_user("user_alice_inv", "alice");
    let user_bob = mock_test_user("user_bob_inv", "bob");

    let app_alice = Router::new()
        .merge(inbox_routes(InboxRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
            worktree_engine: worktree_engine.clone(),
        }))
        .layer(axum::Extension(user_alice.clone()));

    let app_bob = Router::new()
        .merge(inbox_routes(InboxRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
            worktree_engine: worktree_engine.clone(),
        }))
        .layer(axum::Extension(user_bob.clone()));

    // 1. Initial GET /api/inbox for user_alice is empty
    let req = Request::builder()
        .method("GET")
        .uri("/api/inbox")
        .body(Body::empty())
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
    assert_eq!(dto.data.unwrap().pending_count, 0);

    // 2. Create card 1 owned by alice, card 2 owned by bob
    let card_alice = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("card_alice_1".into()),
            owner_user_id: user_alice.id.clone(),
            category: "choice".into(),
            title: "Alice Task".into(),
            summary: "Decision for Alice".into(),
            details_json: r#"{"options":["yes","no"]}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-alice".into(),
            action_token: Some("tok_alice_1".into()),
            telegram_chat_id: None,
            telegram_msg_id: None,
        })
        .await
        .unwrap();
    assert_eq!(card_alice.status, "pending");

    let _card_bob = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("card_bob_1".into()),
            owner_user_id: user_bob.id.clone(),
            category: "choice".into(),
            title: "Bob Task".into(),
            summary: "Decision for Bob".into(),
            details_json: r#"{"options":["A","B"]}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-bob".into(),
            action_token: Some("tok_bob_1".into()),
            telegram_chat_id: None,
            telegram_msg_id: None,
        })
        .await
        .unwrap();

    // 3. Multi-tenant isolation: Alice only sees Alice's cards
    let req = Request::builder()
        .method("GET")
        .uri("/api/inbox")
        .body(Body::empty())
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
    let list_alice = dto.data.unwrap();
    assert_eq!(list_alice.pending_count, 1);
    assert_eq!(list_alice.items.len(), 1);
    assert_eq!(list_alice.items[0].id, "card_alice_1");

    // Bob only sees Bob's cards
    let req = Request::builder()
        .method("GET")
        .uri("/api/inbox")
        .body(Body::empty())
        .unwrap();
    let res = app_bob.clone().oneshot(req).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
    let list_bob = dto.data.unwrap();
    assert_eq!(list_bob.pending_count, 1);
    assert_eq!(list_bob.items.len(), 1);
    assert_eq!(list_bob.items[0].id, "card_bob_1");

    // 4. Non-existent ID -> 404 Not Found
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/non_existent_card_999/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"approve"}"#))
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 5. Cross-tenant access forbidden: Bob tries to resolve Alice's card -> 403 Forbidden
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_alice_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"approve"}"#))
        .unwrap();
    let res = app_bob.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // 6. Malformed JSON payload -> 422 or 400
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_alice_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"malformed": "#))
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    assert!(res.status() == StatusCode::UNPROCESSABLE_ENTITY || res.status() == StatusCode::BAD_REQUEST);

    // 7. Missing required "action" field -> 422 or 400
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_alice_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"payload":{"foo":"bar"}}"#))
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    assert!(res.status() == StatusCode::UNPROCESSABLE_ENTITY || res.status() == StatusCode::BAD_REQUEST);

    // 8. Valid resolution by owner (Alice) -> 200 OK
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_alice_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"choice","payload":{"selected":"yes"}}"#))
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<ResolveActionInboxResponse> = serde_json::from_slice(&body).unwrap();
    assert!(dto.data.unwrap().success);

    // 9. Repeat resolution on already resolved card -> 409 Conflict
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_alice_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"choice"}"#))
        .unwrap();
    let res = app_alice.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);

    // 10. Check card status in DB after resolve
    let row = bridge_repo.get_action_inbox_item("card_alice_1").await.unwrap().unwrap();
    assert_eq!(row.status, "resolved");
    assert_eq!(row.resolved_by.as_deref(), Some("desktop"));
    assert_eq!(row.resolution_result.as_deref(), Some("choice"));
}

#[tokio::test]
async fn challenge_cross_surface_sync_telegram_to_desktop() {
    let db = init_database_memory().await.unwrap();
    let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let event_bus = Arc::new(BroadcastEventBus::new(16));
    let worktree_engine = Arc::new(GitWorktreeEngine::new());

    let user = mock_test_user("user_sync_tg_to_desk", "sync_user");
    let app = Router::new()
        .merge(inbox_routes(InboxRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
            worktree_engine: worktree_engine.clone(),
        }))
        .layer(axum::Extension(user.clone()));

    // 1. Create action token in DB
    let token = bridge_repo
        .create_action_token(
            &user.id,
            CustomTargetType::Room,
            "room-tg-sync",
            "inbox_resolve",
            &serde_json::json!({"action":"approve"}),
            None,
        )
        .await
        .unwrap();

    // 2. Create Action Inbox card linked to this token
    let card = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("card_tg_sync_1".into()),
            owner_user_id: user.id.clone(),
            category: "worktree_merge".into(),
            title: "Merge PR from Telegram".into(),
            summary: "Worktree lease squash merge".into(),
            details_json: r#"{"lease_id":"lease-tg-1"}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-tg-sync".into(),
            action_token: Some(token.clone()),
            telegram_chat_id: Some("-100888".into()),
            telegram_msg_id: Some("999".into()),
        })
        .await
        .unwrap();
    assert_eq!(card.status, "pending");

    // 3. Verify desktop API reports 1 pending card
    let req = Request::builder()
        .method("GET")
        .uri("/api/inbox")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
    assert_eq!(dto.data.unwrap().pending_count, 1);

    // 4. Simulate Telegram inline button callback execution:
    // (a) Atomically consume action token
    let token_consumed = bridge_repo.consume_action_token_once(&token).await.unwrap();
    assert!(token_consumed, "Token must be consumed on first tap");

    // (b) Atomically resolve linked inbox card by token with resolved_by = 'telegram'
    let resolved_inbox = bridge_repo
        .resolve_action_inbox_item_by_token(&token, "approved", "telegram")
        .await
        .unwrap();
    assert!(resolved_inbox.is_some(), "Inbox card must be found and resolved");
    let resolved_card = resolved_inbox.unwrap();
    assert_eq!(resolved_card.id, "card_tg_sync_1");
    assert_eq!(resolved_card.status, "resolved");
    assert_eq!(resolved_card.resolved_by.as_deref(), Some("telegram"));
    assert_eq!(resolved_card.resolution_result.as_deref(), Some("approved"));

    // 5. Desktop API verification:
    // (a) Pending count must drop to 0
    let req = Request::builder()
        .method("GET")
        .uri("/api/inbox")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
    let list = dto.data.unwrap();
    assert_eq!(list.pending_count, 0);
    assert_eq!(list.items[0].status, "resolved");
    assert_eq!(list.items[0].resolved_by.as_deref(), Some("telegram"));

    // (b) Desktop user attempt to resolve again -> 409 Conflict
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_tg_sync_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"approve"}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);

    // 6. Telegram replay attack: tapping Telegram button again fails
    let token_replayed = bridge_repo.consume_action_token_once(&token).await.unwrap();
    assert!(!token_replayed, "Token replay must be rejected");

    let repeat_resolve = bridge_repo
        .resolve_action_inbox_item_by_token(&token, "approved", "telegram")
        .await
        .unwrap();
    assert!(repeat_resolve.is_none(), "Repeat token resolution must return None");
}

#[tokio::test]
async fn challenge_cross_surface_sync_desktop_to_telegram() {
    let db = init_database_memory().await.unwrap();
    let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let event_bus = Arc::new(BroadcastEventBus::new(16));
    let worktree_engine = Arc::new(GitWorktreeEngine::new());

    let user = mock_test_user("user_sync_desk_to_tg", "desk_user");
    let app = Router::new()
        .merge(inbox_routes(InboxRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
            worktree_engine: worktree_engine.clone(),
        }))
        .layer(axum::Extension(user.clone()));

    // 1. Create action token in DB
    let token = bridge_repo
        .create_action_token(
            &user.id,
            CustomTargetType::Room,
            "room-desk-sync",
            "worktree_merge",
            &serde_json::json!({"lease_id":"lease-desk-1"}),
            None,
        )
        .await
        .unwrap();

    // 2. Create Action Inbox card linked to this token
    let _card = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("card_desk_sync_1".into()),
            owner_user_id: user.id.clone(),
            category: "worktree_merge".into(),
            title: "Merge on Desktop".into(),
            summary: "Resolved from Desktop UI".into(),
            details_json: r#"{"lease_id":"lease-desk-1"}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-desk-sync".into(),
            action_token: Some(token.clone()),
            telegram_chat_id: Some("-100777".into()),
            telegram_msg_id: Some("111".into()),
        })
        .await
        .unwrap();

    // 3. Desktop user resolves card via REST API
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_desk_sync_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"approve"}"#))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. Verify token was consumed by desktop resolution (invalidating mobile Telegram button)
    let token_consumed_again = bridge_repo.consume_action_token_once(&token).await.unwrap();
    assert!(!token_consumed_again, "Desktop resolution must consume token; mobile replay must fail");

    // 5. Subsequent attempt to resolve by token returns None
    let tg_resolve = bridge_repo
        .resolve_action_inbox_item_by_token(&token, "approved", "telegram")
        .await
        .unwrap();
    assert!(tg_resolve.is_none());
}

#[tokio::test]
async fn challenge_websocket_broadcast_on_resolution() {
    let db = init_database_memory().await.unwrap();
    let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let event_bus = Arc::new(BroadcastEventBus::new(16));
    let worktree_engine = Arc::new(GitWorktreeEngine::new());

    // Subscribe to event bus
    let mut rx = event_bus.subscribe();

    let user = mock_test_user("user_ws_test", "ws_user");
    let app = Router::new()
        .merge(inbox_routes(InboxRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
            worktree_engine: worktree_engine.clone(),
        }))
        .layer(axum::Extension(user.clone()));

    // Create card
    let _ = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("card_ws_1".into()),
            owner_user_id: user.id.clone(),
            category: "choice".into(),
            title: "WS Test".into(),
            summary: "Testing WS event emission".into(),
            details_json: r#"{}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-ws".into(),
            action_token: None,
            telegram_chat_id: None,
            telegram_msg_id: None,
        })
        .await
        .unwrap();

    // Resolve via Desktop REST API
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/card_ws_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"approve"}"#))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Verify WebSocket event was received
    let event = rx.recv().await.unwrap();
    assert_eq!(event.name, "inbox:item_resolved");
    assert_eq!(event.data["id"], "card_ws_1");
    assert_eq!(event.data["resolved_by"], "desktop");

    // Simulate Telegram resolution event broadcast
    event_bus.broadcast(aionui_api_types::WebSocketMessage::new(
        "inbox:item_resolved",
        serde_json::json!({
            "id": "card_ws_2",
            "resolved_by": "telegram"
        }),
    ));

    let tg_event = rx.recv().await.unwrap();
    assert_eq!(tg_event.name, "inbox:item_resolved");
    assert_eq!(tg_event.data["id"], "card_ws_2");
    assert_eq!(tg_event.data["resolved_by"], "telegram");
}

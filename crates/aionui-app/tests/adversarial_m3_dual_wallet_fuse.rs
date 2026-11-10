#![allow(clippy::disallowed_types)]

//! Adversarial and Integration Test Suite for Milestone 3 (M3).
//!
//! Verifies:
//! 1. Dual Cost Wallet segregation: CLI Subscription ($0.00) vs Pay-per-token API Key.
//! 2. 80% Soft Budget Warning trigger, Action Inbox card generation, and no duplicate spam.
//! 3. 100% Hard Budget Fuse trigger, Action Inbox card generation, and CLI subscription immunity.
//! 4. Concurrent race-condition storm: atomic idempotency across desktop and Telegram mobile.
//! 5. REST API contract for `/api/wallets/*` and `/api/inbox/*`.

use std::sync::Arc;

use aionui_api_types::ApiResponse;
use aionui_auth::CurrentUser;
use aionui_db::custom_bridge_schema::{
    classify_agent_wallet, CreateActionInboxParams, RecordCostRunParams, SqliteCustomBridgeRepository,
    WalletType,
};
use aionui_db::init_database_memory;
use aionui_project::GitWorktreeEngine;
use aionui_realtime::BroadcastEventBus;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use tower::ServiceExt;

use aionui_app::routes::inbox::{
    inbox_routes, ActionInboxListResponse, InboxRouterState, ResolveActionInboxResponse,
};
use aionui_app::routes::wallets::{wallets_routes, WalletSummaryDto, WalletsRouterState};

fn mock_test_user(id: &str) -> CurrentUser {
    let mut u = CurrentUser::local_default();
    u.id = id.into();
    u
}

#[tokio::test]
async fn test_dual_wallet_cli_vs_api_key_segregation_and_classification() {
    // 1. Classification Tests
    assert_eq!(
        classify_agent_wallet(Some("claude"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("claude"), Some(r#"{"type":"oauth"}"#), false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("codex"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("antigravity"), None, false),
        WalletType::CliSubscription
    );
    assert_eq!(
        classify_agent_wallet(Some("claude"), None, true),
        WalletType::ApiKey
    );
    assert_eq!(
        classify_agent_wallet(Some("deepseek"), None, false),
        WalletType::ApiKey
    );
    assert_eq!(
        classify_agent_wallet(Some("openrouter"), None, false),
        WalletType::ApiKey
    );

    // 2. Database Ledger Segregation
    let db = init_database_memory().await.unwrap();
    let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
    let user_id = "user_dual_wallet_segregation";

    let init_wallet = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    assert_eq!(init_wallet.api_key_spent_usd, 0.0);
    assert_eq!(init_wallet.cli_subscription_runs_count, 0);
    assert_eq!(init_wallet.api_key_runs_count, 0);

    // Run 5 CLI Subscription runs at $0.00
    for i in 1..=5 {
        let (wallet, soft, hard) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: user_id.into(),
                run_id: format!("cli-run-{i}"),
                team_id: "team-alpha".into(),
                agent_id: "claude-code-oauth".into(),
                wallet_type: "cli_subscription".into(),
                tokens_input: 15_000,
                tokens_output: 3_000,
                cost_usd: 0.0,
            })
            .await
            .unwrap();

        assert_eq!(wallet.cli_subscription_runs_count, i);
        assert_eq!(wallet.api_key_runs_count, 0);
        assert_eq!(wallet.api_key_spent_usd, 0.0, "CLI runs must never increment api_key_spent_usd");
        assert!(!soft);
        assert!(!hard);
    }
}

#[tokio::test]
async fn test_adversarial_80_percent_soft_warning_and_inbox_card() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
    let user_id = "user_soft_warning_test";

    // Set budget limit to $20.00 (80% threshold = $16.00)
    let _ = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    let wallet = repo.update_budget_limit(user_id, 20.0).await.unwrap();
    assert_eq!(wallet.api_key_budget_limit_usd, 20.0);

    // Run 1: $10.00 spend (50% -> below 80%)
    let (w1, soft1, hard1) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "run-paid-1".into(),
            team_id: "team-prod".into(),
            agent_id: "gpt-4o-paid".into(),
            wallet_type: "api_key".into(),
            tokens_input: 100_000,
            tokens_output: 20_000,
            cost_usd: 10.0,
        })
        .await
        .unwrap();

    assert_eq!(w1.api_key_spent_usd, 10.0);
    assert!(!soft1);
    assert!(!hard1);
    assert_eq!(w1.soft_warning_triggered, 0);

    let inbox_pending = repo.count_pending_inbox_items(user_id).await.unwrap();
    assert_eq!(inbox_pending, 0);

    // Run 2: $6.00 spend (Total $16.00 = exactly 80%)
    let (w2, soft2, hard2) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "run-paid-2".into(),
            team_id: "team-prod".into(),
            agent_id: "gpt-4o-paid".into(),
            wallet_type: "api_key".into(),
            tokens_input: 60_000,
            tokens_output: 12_000,
            cost_usd: 6.0,
        })
        .await
        .unwrap();

    assert_eq!(w2.api_key_spent_usd, 16.0);
    assert!(soft2, "80% soft warning must trigger");
    assert!(!hard2);
    assert_eq!(w2.soft_warning_triggered, 1);
    assert_eq!(w2.hard_block_triggered, 0);

    // Action Inbox item must be generated
    let inbox_cards = repo.list_action_inbox_items(user_id, Some("pending")).await.unwrap();
    assert_eq!(inbox_cards.len(), 1);
    assert_eq!(inbox_cards[0].category, "budget_warning");
    assert!(inbox_cards[0].title.contains("80%"));

    // Run 3: $2.00 spend (Total $18.00 = 90% -> still between 80% and 100%)
    let (w3, soft3, hard3) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "run-paid-3".into(),
            team_id: "team-prod".into(),
            agent_id: "gpt-4o-paid".into(),
            wallet_type: "api_key".into(),
            tokens_input: 20_000,
            tokens_output: 4_000,
            cost_usd: 2.0,
        })
        .await
        .unwrap();

    assert_eq!(w3.api_key_spent_usd, 18.0);
    assert!(!soft3, "Soft warning must NOT trigger a second time");
    assert!(!hard3);

    // Inbox cards must NOT duplicate
    let inbox_cards_after = repo.list_action_inbox_items(user_id, Some("pending")).await.unwrap();
    assert_eq!(inbox_cards_after.len(), 1, "Duplicate warning cards must be suppressed");
}

#[tokio::test]
async fn test_adversarial_100_percent_hard_fuse_and_cli_immunity() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
    let user_id = "user_hard_fuse_test";

    // Set budget limit to $20.00
    let _ = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    let _ = repo.update_budget_limit(user_id, 20.0).await.unwrap();

    // Spend $20.50 (crosses 100% threshold)
    let (w1, soft1, hard1) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "run-paid-blowout".into(),
            team_id: "team-backend".into(),
            agent_id: "paid-agent".into(),
            wallet_type: "api_key".into(),
            tokens_input: 200_000,
            tokens_output: 50_000,
            cost_usd: 20.50,
        })
        .await
        .unwrap();

    assert_eq!(w1.api_key_spent_usd, 20.50);
    assert!(soft1, "Crossing 100% also trips 80% soft warning if un-warned");
    assert!(hard1, "100% hard block must trigger");
    assert_eq!(w1.hard_block_triggered, 1);
    assert!(repo.is_wallet_hard_blocked(user_id).await.unwrap());

    // Verify Action Inbox received hard fuse card
    let cards = repo.list_action_inbox_items(user_id, Some("pending")).await.unwrap();
    assert!(cards.iter().any(|c| c.title.contains("100%")));

    // CLI IMMUNITY TEST:
    // When wallet is hard blocked, CLI Subscription runs are exempt and execute at $0.00
    assert!(repo.is_wallet_hard_blocked(user_id).await.unwrap());

    let (w_cli, _, _) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "cli-run-during-hard-block".into(),
            team_id: "team-backend".into(),
            agent_id: "claude-code-oauth".into(),
            wallet_type: "cli_subscription".into(),
            tokens_input: 8_000,
            tokens_output: 1_200,
            cost_usd: 0.0,
        })
        .await
        .unwrap();

    assert_eq!(w_cli.cli_subscription_runs_count, 1);
    assert_eq!(w_cli.api_key_spent_usd, 20.50, "CLI runs do not add spend");
    assert_eq!(w_cli.hard_block_triggered, 1, "Hard block remains for paid agents");
}

#[tokio::test]
async fn test_concurrent_atomic_resolution_storm() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let user_id = "user_concurrency_storm";

    // Create Action Inbox item with token
    let card = repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("inbox_storm_item".into()),
            owner_user_id: user_id.into(),
            category: "worktree_merge".into(),
            title: "Merge Worktree wt/lease-concurrent".into(),
            summary: "Concurrent resolution storm test".into(),
            details_json: r#"{"lease_id":"lease-concurrent"}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-storm".into(),
            action_token: Some("tok_storm_token_123".into()),
            telegram_chat_id: Some("-100999".into()),
            telegram_msg_id: Some("888".into()),
        })
        .await
        .unwrap();

    assert_eq!(card.status, "pending");

    // Spawn 16 concurrent tasks all attempting to resolve the same card
    let mut handles = Vec::new();
    for i in 0..16 {
        let repo_clone = repo.clone();
        let handle = tokio::spawn(async move {
            let resolver = if i % 2 == 0 { "desktop" } else { "telegram" };
            repo_clone
                .resolve_action_inbox_item("inbox_storm_item", "approved", resolver)
                .await
        });
        handles.push(handle);
    }

    let mut successes = 0;
    let mut failures = 0;

    for handle in handles {
        let res = handle.await.unwrap().unwrap();
        if res {
            successes += 1;
        } else {
            failures += 1;
        }
    }

    // Mathematical guarantee: exactly 1 winner, 15 losers
    assert_eq!(successes, 1, "Exactly one task must succeed in atomic resolution");
    assert_eq!(failures, 15, "15 tasks must receive false (already resolved)");

    let final_card = repo.get_action_inbox_item("inbox_storm_item").await.unwrap().unwrap();
    assert_eq!(final_card.status, "resolved");
    assert_eq!(final_card.resolution_result.as_deref(), Some("approved"));
}

#[tokio::test]
async fn test_rest_api_wallets_and_inbox_integration() {
    let db = init_database_memory().await.unwrap();
    let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let event_bus = Arc::new(BroadcastEventBus::new(16));
    let worktree_engine = Arc::new(GitWorktreeEngine::new());
    let user = mock_test_user("user_rest_api_integration");

    let app = Router::new()
        .merge(wallets_routes(WalletsRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
        }))
        .merge(inbox_routes(InboxRouterState {
            bridge_repo: bridge_repo.clone(),
            event_bus: event_bus.clone(),
            worktree_engine: worktree_engine.clone(),
        }))
        .layer(axum::Extension(user.clone()));

    // 1. GET /api/wallets/summary
    let req = Request::builder()
        .method("GET")
        .uri("/api/wallets/summary")
        .body(Body::empty())
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.into_body().collect().await.unwrap().to_bytes();
    let wallet_dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
    assert_eq!(wallet_dto.data.as_ref().unwrap().api_key_budget_limit_usd, 50.0);
    assert_eq!(wallet_dto.data.as_ref().unwrap().api_key_spent_usd, 0.0);

    // 2. POST /api/wallets/budget-limit -> update to $35.00
    let req = Request::builder()
        .method("POST")
        .uri("/api/wallets/budget-limit")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"budget_limit_usd": 35.0}"#))
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.into_body().collect().await.unwrap().to_bytes();
    let updated_wallet: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
    assert_eq!(updated_wallet.data.unwrap().api_key_budget_limit_usd, 35.0);

    // 3. Create an Action Inbox card in DB
    let _ = bridge_repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some("inbox_card_rest_1".into()),
            owner_user_id: user.id.clone(),
            category: "choice".into(),
            title: "Select Database".into(),
            summary: "SQLite vs Postgres".into(),
            details_json: r#"{"options":["sqlite","postgres"]}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-rest".into(),
            action_token: Some("tok_rest_card".into()),
            telegram_chat_id: None,
            telegram_msg_id: None,
        })
        .await
        .unwrap();

    // 4. GET /api/inbox
    let req = Request::builder()
        .method("GET")
        .uri("/api/inbox")
        .body(Body::empty())
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.into_body().collect().await.unwrap().to_bytes();
    let inbox_dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
    let list = inbox_dto.data.unwrap();
    assert_eq!(list.pending_count, 1);
    assert_eq!(list.items[0].id, "inbox_card_rest_1");

    // 5. POST /api/inbox/{id}/resolve
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/inbox_card_rest_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"choice","payload":{"selected":"sqlite"}}"#))
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.into_body().collect().await.unwrap().to_bytes();
    let resolve_dto: ApiResponse<ResolveActionInboxResponse> = serde_json::from_slice(&body).unwrap();
    assert!(resolve_dto.data.unwrap().success);

    // 6. Second resolution returns 409 Conflict
    let req = Request::builder()
        .method("POST")
        .uri("/api/inbox/inbox_card_rest_1/resolve")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"action":"choice"}"#))
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
}

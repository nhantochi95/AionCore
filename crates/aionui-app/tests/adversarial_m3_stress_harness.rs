#![allow(clippy::disallowed_types)]

//! Empirical Challenger Stress Harness for Milestone 3 (M3).
//!
//! Stress-tests:
//! 1. 50-task concurrent mixed Desktop + Telegram race storm on Action Inbox & Action Tokens.
//! 2. 50-task concurrent action token consumption (Telegram callback deduplication).
//! 3. 500-run massive CLI subscription load ($0.00) verifying zero spend drift and zero warnings.
//! 4. Fractional-cent increments and hysteresis: 80% soft warning triggered once, zero duplicate spam, 100% hard fuse.
//! 5. Dynamic budget limit adjustments: dynamically tripping and clearing the hard fuse.
//! 6. Multi-user parallel concurrency isolation.

use std::sync::Arc;

use aionui_db::custom_bridge_schema::{
    CreateActionInboxParams, CustomTargetType, RecordCostRunParams, SqliteCustomBridgeRepository,
};
use aionui_db::init_database_memory;

#[tokio::test]
async fn test_stress_50_concurrent_mixed_desktop_telegram_resolution() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let user_id = "user_stress_concurrency";

    // Create an action token
    let token = repo
        .create_action_token(
            user_id,
            CustomTargetType::Room,
            "room-stress-1",
            "worktree_merge",
            &serde_json::json!({"lease_id": "lease-stress-99"}),
            Some(3600_000),
        )
        .await
        .unwrap();

    // Create an Action Inbox card linked to this action token
    let card_id = "inbox_card_stress_concurrency";
    let _ = repo
        .create_action_inbox_item(CreateActionInboxParams {
            id: Some(card_id.into()),
            owner_user_id: user_id.into(),
            category: "worktree_merge".into(),
            title: "Squash Merge Stress".into(),
            summary: "Race between 25 desktop and 25 telegram clients".into(),
            details_json: r#"{"lease_id":"lease-stress-99"}"#.into(),
            target_type: Some("room".into()),
            target_id: "room-stress-1".into(),
            action_token: Some(token.clone()),
            telegram_chat_id: Some("-10099999".into()),
            telegram_msg_id: Some("12345".into()),
        })
        .await
        .unwrap();

    // Spawn 50 concurrent tasks
    let mut handles = Vec::new();

    // 25 tasks simulate Desktop: resolve_action_inbox_item, then consume_action_token_once
    for _ in 0..25 {
        let repo_c = repo.clone();
        let tok = token.clone();
        handles.push(tokio::spawn(async move {
            let res = repo_c
                .resolve_action_inbox_item(card_id, "approved", "desktop")
                .await
                .unwrap();
            let consumed = if res {
                repo_c.consume_action_token_once(&tok).await.unwrap()
            } else {
                false
            };
            ("desktop", res, consumed)
        }));
    }

    // 25 tasks simulate Telegram: consume_action_token_once, then resolve_action_inbox_item_by_token
    for _ in 0..25 {
        let repo_c = repo.clone();
        let tok = token.clone();
        handles.push(tokio::spawn(async move {
            let consumed = repo_c.consume_action_token_once(&tok).await.unwrap();
            let resolved = if consumed {
                repo_c
                    .resolve_action_inbox_item_by_token(&tok, "approved", "telegram")
                    .await
                    .unwrap()
                    .is_some()
            } else {
                false
            };
            ("telegram", resolved, consumed)
        }));
    }

    let mut total_resolved_successes = 0;
    let mut total_token_consumed_successes = 0;

    for h in handles {
        let (_surface, resolved, consumed) = h.await.unwrap();
        if resolved {
            total_resolved_successes += 1;
        }
        if consumed {
            total_token_consumed_successes += 1;
        }
    }

    // Strict mathematical guarantees:
    // Exactly 1 winner transitioned the inbox item to resolved
    assert_eq!(
        total_resolved_successes, 1,
        "Exactly 1 task out of 50 must successfully resolve the inbox item"
    );
    // Exactly 1 task successfully consumed the one-shot token
    assert_eq!(
        total_token_consumed_successes, 1,
        "Exactly 1 task out of 50 must successfully consume the action token"
    );

    // Verify DB integrity
    let final_card = repo.get_action_inbox_item(card_id).await.unwrap().unwrap();
    assert_eq!(final_card.status, "resolved");
    assert!(final_card.resolution_result.is_some());
    assert!(final_card.resolved_by.is_some());

    let pending_count = repo.count_pending_inbox_items(user_id).await.unwrap();
    assert_eq!(pending_count, 0);
}

#[tokio::test]
async fn test_stress_50_concurrent_telegram_callbacks_same_token() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let user_id = "user_stress_callbacks";

    let token = repo
        .create_action_token(
            user_id,
            CustomTargetType::Room,
            "room-cb",
            "allow_more_hops",
            &serde_json::json!({"hops": 5}),
            Some(60_000),
        )
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..50 {
        let repo_c = repo.clone();
        let tok = token.clone();
        handles.push(tokio::spawn(async move {
            repo_c.consume_action_token_once(&tok).await.unwrap()
        }));
    }

    let mut consumed_count = 0;
    let mut rejected_count = 0;
    for h in handles {
        if h.await.unwrap() {
            consumed_count += 1;
        } else {
            rejected_count += 1;
        }
    }

    assert_eq!(consumed_count, 1, "Exactly 1 Telegram button callback must succeed");
    assert_eq!(rejected_count, 49, "49 replay attempts must be rejected with false");
}

#[tokio::test]
async fn test_stress_500_massive_cli_runs_zero_leakage() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
    let user_id = "user_massive_cli";

    // Set budget limit to $5.00
    let _ = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    let _ = repo.update_budget_limit(user_id, 5.0).await.unwrap();

    // Run 500 CLI subscription runs in parallel batches of 50
    for batch in 0..10 {
        let mut handles = Vec::new();
        for i in 0..50 {
            let run_idx = batch * 50 + i;
            let repo_c = repo.clone();
            handles.push(tokio::spawn(async move {
                repo_c
                    .record_cost_run(RecordCostRunParams {
                        owner_user_id: user_id.into(),
                        run_id: format!("cli-massive-{run_idx}"),
                        team_id: "team-cli-massive".into(),
                        agent_id: "claude-code-oauth".into(),
                        wallet_type: "cli_subscription".into(),
                        tokens_input: 50_000,
                        tokens_output: 10_000,
                        cost_usd: 0.0,
                    })
                    .await
                    .unwrap()
            }));
        }

        for h in handles {
            let (_wallet, soft, hard) = h.await.unwrap();
            assert!(!soft, "CLI runs must never trigger soft warning");
            assert!(!hard, "CLI runs must never trigger hard fuse");
        }
    }

    let final_wallet = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    assert_eq!(
        final_wallet.api_key_spent_usd, 0.0,
        "500 CLI runs must cause ZERO dollar drift"
    );
    assert_eq!(
        final_wallet.cli_subscription_runs_count, 500,
        "CLI subscription run count must be exactly 500"
    );
    assert_eq!(final_wallet.api_key_runs_count, 0);
    assert_eq!(final_wallet.soft_warning_triggered, 0);
    assert_eq!(final_wallet.hard_block_triggered, 0);

    let pending = repo.count_pending_inbox_items(user_id).await.unwrap();
    assert_eq!(pending, 0, "No warning inbox items should exist");
}

#[tokio::test]
async fn test_stress_fractional_cents_and_hysteresis_no_duplicate_spam() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
    let user_id = "user_fractional_hysteresis";

    // Set budget limit to $10.00. 80% = $8.00, 100% = $10.00
    let _ = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    let _ = repo.update_budget_limit(user_id, 10.0).await.unwrap();

    // 1. Spend 50 small increments of $0.15 = $7.50 (< 80%)
    for i in 1..=50 {
        let (_w, soft, hard) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: user_id.into(),
                run_id: format!("inc-{i}"),
                team_id: "team-prod".into(),
                agent_id: "paid-gpt4".into(),
                wallet_type: "api_key".into(),
                tokens_input: 1_500,
                tokens_output: 300,
                cost_usd: 0.15,
            })
            .await
            .unwrap();

        assert!(!soft, "Must not trigger soft warning below 80% (spend: {})", i as f64 * 0.15);
        assert!(!hard, "Must not trigger hard fuse below 100%");
    }

    assert_eq!(repo.count_pending_inbox_items(user_id).await.unwrap(), 0);

    // 2. Next increment: spend $0.51 -> total spend = $8.01 (>= 80%)
    let (w_warn, soft_warn, hard_warn) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "inc-cross-80".into(),
            team_id: "team-prod".into(),
            agent_id: "paid-gpt4".into(),
            wallet_type: "api_key".into(),
            tokens_input: 5_000,
            tokens_output: 1_000,
            cost_usd: 0.51,
        })
        .await
        .unwrap();

    assert!(soft_warn, "80% soft warning must trigger on crossing $8.00");
    assert!(!hard_warn);
    assert_eq!(w_warn.soft_warning_triggered, 1);
    assert_eq!(repo.count_pending_inbox_items(user_id).await.unwrap(), 1);

    // 3. Next 20 small increments of $0.05 each ($8.01 -> $9.01)
    // ZERO duplicates allowed!
    for i in 1..=20 {
        let (_w, soft, hard) = repo
            .record_cost_run(RecordCostRunParams {
                owner_user_id: user_id.into(),
                run_id: format!("inc-hysteresis-{i}"),
                team_id: "team-prod".into(),
                agent_id: "paid-gpt4".into(),
                wallet_type: "api_key".into(),
                tokens_input: 500,
                tokens_output: 100,
                cost_usd: 0.05,
            })
            .await
            .unwrap();

        assert!(!soft, "Soft warning must NEVER re-trigger while remaining < 100%");
        assert!(!hard);
        assert_eq!(
            repo.count_pending_inbox_items(user_id).await.unwrap(),
            1,
            "Action Inbox card count must remain exactly 1 (no spam)"
        );
    }

    // 4. Increment crossing 100%: spend $1.05 -> total spend = $10.06 (>= 100%)
    let (w_hard, soft_hard, hard_fuse) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "inc-cross-100".into(),
            team_id: "team-prod".into(),
            agent_id: "paid-gpt4".into(),
            wallet_type: "api_key".into(),
            tokens_input: 10_000,
            tokens_output: 2_000,
            cost_usd: 1.05,
        })
        .await
        .unwrap();

    assert!(!soft_hard, "Soft warning was already triggered");
    assert!(hard_fuse, "100% hard fuse must trigger");
    assert_eq!(w_hard.hard_block_triggered, 1);
    assert!(repo.is_wallet_hard_blocked(user_id).await.unwrap());
    assert_eq!(
        repo.count_pending_inbox_items(user_id).await.unwrap(),
        2,
        "Action Inbox must now have exactly 2 cards: 80% soft warning + 100% hard fuse"
    );

    // 5. Subsequent spend while hard blocked does not create more cards
    let (_w, soft_post, hard_post) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "inc-post-fuse".into(),
            team_id: "team-prod".into(),
            agent_id: "paid-gpt4".into(),
            wallet_type: "api_key".into(),
            tokens_input: 1_000,
            tokens_output: 200,
            cost_usd: 0.10,
        })
        .await
        .unwrap();

    assert!(!soft_post);
    assert!(!hard_post, "Hard fuse must not re-trigger new notification cards");
    assert_eq!(
        repo.count_pending_inbox_items(user_id).await.unwrap(),
        2,
        "Card count must stay at 2"
    );
}

#[tokio::test]
async fn test_stress_dynamic_budget_limit_adjustment_trips_and_clears_fuse() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteCustomBridgeRepository::new(db.pool().clone());
    let user_id = "user_dynamic_limit";

    // Set budget limit to $100.00
    let _ = repo.get_or_create_cost_wallet(user_id).await.unwrap();
    let _ = repo.update_budget_limit(user_id, 100.0).await.unwrap();

    // Spend $85.00 -> trips 80% soft warning (85 >= 80), but not hard fuse (85 < 100)
    let (w, soft, hard) = repo
        .record_cost_run(RecordCostRunParams {
            owner_user_id: user_id.into(),
            run_id: "run-dynamic-1".into(),
            team_id: "team-dyn".into(),
            agent_id: "paid-agent".into(),
            wallet_type: "api_key".into(),
            tokens_input: 85_000,
            tokens_output: 10_000,
            cost_usd: 85.0,
        })
        .await
        .unwrap();

    assert!(soft);
    assert!(!hard);
    assert_eq!(w.soft_warning_triggered, 1);
    assert_eq!(w.hard_block_triggered, 0);
    assert!(!repo.is_wallet_hard_blocked(user_id).await.unwrap());

    // Dynamically lower budget to $80.00 -> spend $85.00 now exceeds 100% of $80.00!
    let w_lowered = repo.update_budget_limit(user_id, 80.0).await.unwrap();
    assert_eq!(w_lowered.hard_block_triggered, 1);
    assert!(repo.is_wallet_hard_blocked(user_id).await.unwrap());

    // Dynamically raise budget to $200.00 -> spend $85.00 is 42.5% of $200.00 (< 80%)
    let w_raised = repo.update_budget_limit(user_id, 200.0).await.unwrap();
    assert_eq!(w_raised.hard_block_triggered, 0);
    assert_eq!(w_raised.soft_warning_triggered, 0);
    assert!(!repo.is_wallet_hard_blocked(user_id).await.unwrap());
}

#[tokio::test]
async fn test_stress_multi_user_parallel_isolation() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));

    let mut handles = Vec::new();
    for u in 0..5 {
        let repo_c = repo.clone();
        handles.push(tokio::spawn(async move {
            let user_id = format!("isolated_user_{u}");
            let _ = repo_c.get_or_create_cost_wallet(&user_id).await.unwrap();
            let _ = repo_c.update_budget_limit(&user_id, 50.0).await.unwrap();

            match u {
                0 => {
                    // User 0: only CLI runs
                    for i in 0..30 {
                        repo_c
                            .record_cost_run(RecordCostRunParams {
                                owner_user_id: user_id.clone(),
                                run_id: format!("u0-{i}"),
                                team_id: "team-0".into(),
                                agent_id: "claude".into(),
                                wallet_type: "cli_subscription".into(),
                                tokens_input: 10_000,
                                tokens_output: 2_000,
                                cost_usd: 0.0,
                            })
                            .await
                            .unwrap();
                    }
                }
                1 => {
                    // User 1: Blows budget ($60 spend on $50 limit)
                    repo_c
                        .record_cost_run(RecordCostRunParams {
                            owner_user_id: user_id.clone(),
                            run_id: "u1-blowout".into(),
                            team_id: "team-1".into(),
                            agent_id: "paid".into(),
                            wallet_type: "api_key".into(),
                            tokens_input: 600_000,
                            tokens_output: 100_000,
                            cost_usd: 60.0,
                        })
                        .await
                        .unwrap();
                }
                2 => {
                    // User 2: Exactly 80% ($40 spend on $50 limit)
                    repo_c
                        .record_cost_run(RecordCostRunParams {
                            owner_user_id: user_id.clone(),
                            run_id: "u2-soft".into(),
                            team_id: "team-2".into(),
                            agent_id: "paid".into(),
                            wallet_type: "api_key".into(),
                            tokens_input: 400_000,
                            tokens_output: 80_000,
                            cost_usd: 40.0,
                        })
                        .await
                        .unwrap();
                }
                3 => {
                    // User 3: Low spend ($10 on $50 limit)
                    repo_c
                        .record_cost_run(RecordCostRunParams {
                            owner_user_id: user_id.clone(),
                            run_id: "u3-low".into(),
                            team_id: "team-3".into(),
                            agent_id: "paid".into(),
                            wallet_type: "api_key".into(),
                            tokens_input: 100_000,
                            tokens_output: 20_000,
                            cost_usd: 10.0,
                        })
                        .await
                        .unwrap();
                }
                _ => {
                    // User 4: Idle, zero runs
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify isolation
    let w0 = repo.get_or_create_cost_wallet("isolated_user_0").await.unwrap();
    assert_eq!(w0.api_key_spent_usd, 0.0);
    assert_eq!(w0.cli_subscription_runs_count, 30);
    assert_eq!(w0.soft_warning_triggered, 0);
    assert_eq!(w0.hard_block_triggered, 0);

    let w1 = repo.get_or_create_cost_wallet("isolated_user_1").await.unwrap();
    assert_eq!(w1.api_key_spent_usd, 60.0);
    assert_eq!(w1.soft_warning_triggered, 1);
    assert_eq!(w1.hard_block_triggered, 1);
    assert!(repo.is_wallet_hard_blocked("isolated_user_1").await.unwrap());

    let w2 = repo.get_or_create_cost_wallet("isolated_user_2").await.unwrap();
    assert_eq!(w2.api_key_spent_usd, 40.0);
    assert_eq!(w2.soft_warning_triggered, 1);
    assert_eq!(w2.hard_block_triggered, 0);
    assert!(!repo.is_wallet_hard_blocked("isolated_user_2").await.unwrap());

    let w3 = repo.get_or_create_cost_wallet("isolated_user_3").await.unwrap();
    assert_eq!(w3.api_key_spent_usd, 10.0);
    assert_eq!(w3.soft_warning_triggered, 0);
    assert_eq!(w3.hard_block_triggered, 0);
    assert!(!repo.is_wallet_hard_blocked("isolated_user_3").await.unwrap());

    let w4 = repo.get_or_create_cost_wallet("isolated_user_4").await.unwrap();
    assert_eq!(w4.api_key_spent_usd, 0.0);
    assert_eq!(w4.cli_subscription_runs_count, 0);
    assert_eq!(w4.soft_warning_triggered, 0);
    assert_eq!(w4.hard_block_triggered, 0);
}

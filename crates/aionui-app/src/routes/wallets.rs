#![allow(clippy::disallowed_types)]

//! Dual Cost Wallet REST API endpoints (`/api/wallets/*`).
//!
//! Provides:
//! - `GET /api/wallets/summary`: returns current budget limit, spend, CLI/API run counts, soft warning and hard block status.
//! - `POST /api/wallets/budget-limit`: updates `api_key_budget_limit_usd` and recalibrates circuit breaker flags.
//! - `POST /api/wallets/reset`: resets spend counters and circuit breaker flags for testing/new cycle.

use std::sync::Arc;

use aionui_api_types::{ApiResponse, WebSocketMessage};
use aionui_auth::CurrentUser;
use aionui_common::ApiError;
use aionui_db::{DbError, SqliteCustomBridgeRepository};
use aionui_realtime::{BroadcastEventBus, EventBroadcaster};
use axum::extract::{Extension, Json, State};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct WalletsRouterState {
    pub bridge_repo: Arc<SqliteCustomBridgeRepository>,
    pub event_bus: Arc<BroadcastEventBus>,
}

fn db_err_to_api(err: DbError) -> ApiError {
    match err {
        DbError::NotFound(m) => ApiError::NotFound(m),
        DbError::Conflict(m) => ApiError::Conflict(m),
        DbError::Query(e) => ApiError::Internal(format!("Database error: {e}")),
        DbError::Migration(e) => ApiError::Internal(format!("Migration error: {e}")),
        DbError::Init(m) => ApiError::Internal(format!("Database init error: {m}")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalletSummaryDto {
    pub api_key_budget_limit_usd: f64,
    pub api_key_spent_usd: f64,
    pub cli_subscription_runs: i64,
    pub api_key_runs: i64,
    pub soft_warning: bool,
    pub hard_blocked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateBudgetLimitRequest {
    pub budget_limit_usd: f64,
}

pub fn wallets_routes(state: WalletsRouterState) -> Router {
    Router::new()
        .route("/api/wallets/summary", get(get_wallet_summary))
        .route("/api/wallets/budget-limit", post(update_budget_limit))
        .route("/api/wallets/reset", post(reset_wallet))
        .with_state(state)
}

/// GET /api/wallets/summary
async fn get_wallet_summary(
    State(state): State<WalletsRouterState>,
    Extension(user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<WalletSummaryDto>>, ApiError> {
    let row = state
        .bridge_repo
        .get_or_create_cost_wallet(&user.id)
        .await
        .map_err(db_err_to_api)?;

    Ok(Json(ApiResponse::ok(WalletSummaryDto {
        api_key_budget_limit_usd: row.api_key_budget_limit_usd,
        api_key_spent_usd: row.api_key_spent_usd,
        cli_subscription_runs: row.cli_subscription_runs_count,
        api_key_runs: row.api_key_runs_count,
        soft_warning: row.is_soft_warning(),
        hard_blocked: row.is_hard_blocked(),
    })))
}

/// POST /api/wallets/budget-limit
async fn update_budget_limit(
    State(state): State<WalletsRouterState>,
    Extension(user): Extension<CurrentUser>,
    Json(payload): Json<UpdateBudgetLimitRequest>,
) -> Result<Json<ApiResponse<WalletSummaryDto>>, ApiError> {
    if payload.budget_limit_usd < 0.0 {
        return Err(ApiError::BadRequest(
            "budget_limit_usd cannot be negative".into(),
        ));
    }

    let updated = state
        .bridge_repo
        .update_budget_limit(&user.id, payload.budget_limit_usd)
        .await
        .map_err(db_err_to_api)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "wallet:spend_updated",
        serde_json::json!({
            "user_id": user.id,
            "spent_usd": updated.api_key_spent_usd,
            "budget_limit_usd": updated.api_key_budget_limit_usd,
            "soft_warning": updated.is_soft_warning(),
            "hard_blocked": updated.is_hard_blocked(),
        }),
    ));

    Ok(Json(ApiResponse::ok(WalletSummaryDto {
        api_key_budget_limit_usd: updated.api_key_budget_limit_usd,
        api_key_spent_usd: updated.api_key_spent_usd,
        cli_subscription_runs: updated.cli_subscription_runs_count,
        api_key_runs: updated.api_key_runs_count,
        soft_warning: updated.is_soft_warning(),
        hard_blocked: updated.is_hard_blocked(),
    })))
}

/// POST /api/wallets/reset
async fn reset_wallet(
    State(state): State<WalletsRouterState>,
    Extension(user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<WalletSummaryDto>>, ApiError> {
    let reset = state
        .bridge_repo
        .reset_cost_wallet(&user.id)
        .await
        .map_err(db_err_to_api)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "wallet:spend_updated",
        serde_json::json!({
            "user_id": user.id,
            "spent_usd": reset.api_key_spent_usd,
            "budget_limit_usd": reset.api_key_budget_limit_usd,
            "soft_warning": reset.is_soft_warning(),
            "hard_blocked": reset.is_hard_blocked(),
        }),
    ));

    Ok(Json(ApiResponse::ok(WalletSummaryDto {
        api_key_budget_limit_usd: reset.api_key_budget_limit_usd,
        api_key_spent_usd: reset.api_key_spent_usd,
        cli_subscription_runs: reset.cli_subscription_runs_count,
        api_key_runs: reset.api_key_runs_count,
        soft_warning: reset.is_soft_warning(),
        hard_blocked: reset.is_hard_blocked(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_db::init_database_memory;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn mock_user() -> CurrentUser {
        CurrentUser::local_default()
    }

    #[tokio::test]
    async fn test_wallets_summary_and_update_lifecycle() {
        let db = init_database_memory().await.unwrap();
        let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
        let event_bus = Arc::new(BroadcastEventBus::new(16));

        let app = Router::new()
            .merge(wallets_routes(WalletsRouterState {
                bridge_repo: bridge_repo.clone(),
                event_bus: event_bus.clone(),
            }))
            .layer(axum::Extension(mock_user()));

        // 1. GET /api/wallets/summary
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

        // 2. POST /api/wallets/budget-limit -> update to 75.0
        let req = Request::builder()
            .method("POST")
            .uri("/api/wallets/budget-limit")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"budget_limit_usd": 75.0}"#))
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
        assert_eq!(dto.data.unwrap().api_key_budget_limit_usd, 75.0);

        // 3. POST /api/wallets/reset
        let req = Request::builder()
            .method("POST")
            .uri("/api/wallets/reset")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let dto: ApiResponse<WalletSummaryDto> = serde_json::from_slice(&body).unwrap();
        assert_eq!(dto.data.unwrap().api_key_spent_usd, 0.0);
    }
}

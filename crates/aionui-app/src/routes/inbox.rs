#![allow(clippy::disallowed_types)]

//! Action Inbox REST API endpoints (`/api/inbox/*`).
//!
//! Provides:
//! - `GET /api/inbox`: lists all inbox decision cards and pending count for current user.
//! - `POST /api/inbox/{id}/resolve`: atomically resolves a decision card (approve/reject/choice).

use std::path::Path;
use std::sync::Arc;

use aionui_api_types::{ApiResponse, WebSocketMessage};
use aionui_auth::CurrentUser;
use aionui_common::ApiError;
use aionui_db::custom_bridge_schema::CustomActionInboxRow;
use aionui_db::{DbError, SqliteCustomBridgeRepository};
use aionui_project::GitWorktreeEngine;
use aionui_realtime::{BroadcastEventBus, EventBroadcaster};
use axum::extract::{Extension, Json, Path as AxumPath, State};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct InboxRouterState {
    pub bridge_repo: Arc<SqliteCustomBridgeRepository>,
    pub event_bus: Arc<BroadcastEventBus>,
    pub worktree_engine: Arc<GitWorktreeEngine>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionInboxCardDto {
    pub id: String,
    pub category: String,
    pub title: String,
    pub summary: String,
    pub details: serde_json::Value,
    pub status: String,
    pub target_type: String,
    pub target_id: String,
    pub action_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub telegram_msg_id: Option<String>,
    pub resolution_result: Option<String>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl ActionInboxCardDto {
    pub fn from_row(row: CustomActionInboxRow) -> Self {
        let details = serde_json::from_str(&row.details_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        Self {
            id: row.id,
            category: row.category,
            title: row.title,
            summary: row.summary,
            details,
            status: row.status,
            target_type: row.target_type,
            target_id: row.target_id,
            action_token: row.action_token,
            telegram_chat_id: row.telegram_chat_id,
            telegram_msg_id: row.telegram_msg_id,
            resolution_result: row.resolution_result,
            resolved_by: row.resolved_by,
            resolved_at: row.resolved_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionInboxListResponse {
    pub items: Vec<ActionInboxCardDto>,
    pub pending_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveActionInboxRequest {
    pub action: String, // "approve" | "reject" | "choice"
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveActionInboxResponse {
    pub success: bool,
    pub resolved_at: i64,
}

#[derive(Clone)]
struct InboxCombinedState {
    core: InboxRouterState,
    hub: Option<Arc<crate::bridge::TelegramTeamHub>>,
}

pub fn inbox_routes(state: InboxRouterState) -> Router {
    Router::new()
        .route("/api/inbox", get(list_inbox))
        .route("/api/inbox/{id}/resolve", post(resolve_inbox))
        .with_state(InboxCombinedState {
            core: state,
            hub: None,
        })
}

pub fn inbox_routes_with_hub(
    state: InboxRouterState,
    hub: Arc<crate::bridge::TelegramTeamHub>,
) -> Router {
    Router::new()
        .route("/api/inbox", get(list_inbox))
        .route("/api/inbox/{id}/resolve", post(resolve_inbox))
        .with_state(InboxCombinedState {
            core: state,
            hub: Some(hub),
        })
}

/// GET /api/inbox
async fn list_inbox(
    State(combined): State<InboxCombinedState>,
    Extension(user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<ActionInboxListResponse>>, ApiError> {
    let state = &combined.core;
    let rows = state
        .bridge_repo
        .list_action_inbox_items(&user.id, None)
        .await
        .map_err(db_err_to_api)?;

    let pending_count = rows.iter().filter(|r| r.is_pending()).count();
    let items = rows.into_iter().map(ActionInboxCardDto::from_row).collect();

    Ok(Json(ApiResponse::ok(ActionInboxListResponse {
        items,
        pending_count,
    })))
}

/// POST /api/inbox/{id}/resolve
async fn resolve_inbox(
    State(combined): State<InboxCombinedState>,
    Extension(user): Extension<CurrentUser>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<ResolveActionInboxRequest>,
) -> Result<Json<ApiResponse<ResolveActionInboxResponse>>, ApiError> {
    let state = &combined.core;
    let card = state
        .bridge_repo
        .get_action_inbox_item(&id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Action Inbox card '{id}' not found")))?;

    if card.owner_user_id != user.id {
        return Err(ApiError::Forbidden(
            "Access to Action Inbox card denied".into(),
        ));
    }

    if card.status != "pending" {
        return Err(ApiError::Conflict(format!(
            "Action Inbox card '{id}' is already resolved"
        )));
    }

    // 1. Atomic DB transition (status = 'pending' -> 'resolved')
    let transitioned = state
        .bridge_repo
        .resolve_action_inbox_item(&id, &req.action, "desktop")
        .await
        .map_err(db_err_to_api)?;

    if !transitioned {
        return Err(ApiError::Conflict(format!(
            "Action Inbox card '{id}' was already resolved by another surface"
        )));
    }

    // 2. Consume action_token if present (invalidating Telegram button tap)
    if let Some(ref token) = card.action_token {
        let _ = state.bridge_repo.consume_action_token_once(token).await;
    }

    // 3. Execute domain side effect based on category
    let details: serde_json::Value =
        serde_json::from_str(&card.details_json).unwrap_or_default();
    match card.category.as_str() {
        "worktree_merge" if req.action == "approve" => {
            if let Some(lease_id) = details
                .get("lease_id")
                .and_then(|v| v.as_str())
                .or_else(|| (card.target_type == "worktree").then_some(card.target_id.as_str()))
            {
                if let Ok(Some(lease)) = state.bridge_repo.get_worktree_lease(lease_id).await {
                    let commit_msg = format!(
                        "feat({}): squash merge worktree {}",
                        lease.team_id.as_deref().unwrap_or("team"),
                        lease.lease_id
                    );
                    let user_name = user.username.clone();
                    let user_email = format!("{}@local", user.username);
                    let merge_res = state
                        .worktree_engine
                        .squash_merge(
                            Path::new(&lease.repo_path),
                            Path::new(&lease.worktree_path),
                            &lease.branch,
                            &lease.base_branch,
                            &commit_msg,
                            Some(&user_name),
                            Some(&user_email),
                            &lease.lease_id,
                        )
                        .await;
                    match merge_res {
                        Ok(res) => {
                            let _ = state
                                .bridge_repo
                                .update_worktree_status(
                                    lease_id,
                                    "merged",
                                    res.leftovers_ref.as_deref(),
                                )
                                .await;
                        }
                        Err(e) => {
                            tracing::error!("Worktree squash merge failed in inbox: {e}");
                        }
                    }
                }
            }
        }
        "worktree_merge" if req.action == "reject" => {
            if let Some(lease_id) = details
                .get("lease_id")
                .and_then(|v| v.as_str())
                .or_else(|| (card.target_type == "worktree").then_some(card.target_id.as_str()))
            {
                if let Ok(Some(lease)) = state.bridge_repo.get_worktree_lease(lease_id).await {
                    let _ = state
                        .worktree_engine
                        .discard_worktree(
                            Path::new(&lease.repo_path),
                            Path::new(&lease.worktree_path),
                            &lease.branch,
                            &lease.lease_id,
                        )
                        .await;
                    let _ = state
                        .bridge_repo
                        .update_worktree_status(lease_id, "discarded", None)
                        .await;
                }
            }
        }
        "max_hops" | "max_hops_unlock" if req.action == "approve" => {
            let grant = req
                .payload
                .as_ref()
                .and_then(|p| p.get("grant_hops"))
                .and_then(|v| v.as_i64())
                .unwrap_or(5)
                .clamp(1, 30);
            let _ = state
                .bridge_repo
                .extend_room_hops_allowance(&card.target_id, grant)
                .await;
        }
        "quota_fallback" | "quota_alert" if req.action == "approve" => {
            let team_id = details
                .get("team_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(card.target_id.as_str());
            let slot_id_opt = details
                .get("slot_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty());
            let explicit_target = req
                .payload
                .as_ref()
                .or_else(|| details.get("fallback_target"));
            if let Some(ref hub) = combined.hub {
                let _ = hub
                    .execute_quota_fallback_resolution(&user.id, team_id, slot_id_opt, explicit_target)
                    .await;
            }
        }
        "approve_done" => {
            let room_id = details
                .get("room_id")
                .and_then(|v| v.as_str())
                .or_else(|| (card.target_type == "room").then_some(card.target_id.as_str()))
                .unwrap_or_default()
                .to_owned();
            let task_id_opt = details
                .get("task_id")
                .or_else(|| details.get("cross_team_task_id"))
                .and_then(|v| v.as_str())
                .or_else(|| (card.target_type == "task").then_some(card.target_id.as_str()));

            if let Some(task_id) = task_id_opt {
                let target_status = if req.action == "approve" {
                    Some("completed")
                } else if req.action == "reject" {
                    Some("in_progress")
                } else {
                    None
                };
                if let Some(next_status) = target_status
                    && let Ok(updated_task) = state
                        .bridge_repo
                        .update_shared_room_task(
                            task_id,
                            aionui_db::UpdateSharedRoomTaskParams {
                                status: Some(next_status.to_owned()),
                                description: None,
                                blocked_by_json: None,
                                artifact_paths_json: None,
                                result_summary: None,
                            },
                        )
                        .await
                {
                    let resolved_room_id = if room_id.is_empty() {
                        updated_task.room_id.clone()
                    } else {
                        room_id
                    };
                    let task_dto = if let Some(ref hub) = combined.hub {
                        hub.hydrate_room_task_dto(&user.id, updated_task).await
                    } else {
                        let mut fallback_names = std::collections::HashMap::new();
                        if let Some(to_name) = details
                            .get("to_team_name")
                            .or_else(|| details.get("team_name"))
                            .and_then(|v| v.as_str())
                        {
                            fallback_names.insert(updated_task.to_team_id.clone(), to_name.to_owned());
                        }
                        if let (Some(fid), Some(fname)) = (
                            updated_task.from_team_id.as_ref(),
                            details.get("from_team_name").and_then(|v| v.as_str()),
                        ) {
                            fallback_names.insert(fid.clone(), fname.to_owned());
                        }
                        crate::bridge::TelegramTeamHub::map_room_task_dto(updated_task, &fallback_names)
                    };
                    state.event_bus.broadcast(WebSocketMessage::new(
                        "sharedRoom.taskChanged",
                        serde_json::json!({
                            "user_id": user.id,
                            "room_id": resolved_room_id,
                            "task": task_dto,
                            "resolved_via": "inbox",
                        }),
                    ));
                }
            }
        }
        "budget_warning" if req.action == "extend" => {
            let extend_usd = req
                .payload
                .as_ref()
                .and_then(|p| p.get("extend_usd"))
                .and_then(|v| v.as_f64())
                .unwrap_or(20.0)
                .max(1.0);
            if let Ok(current) = state.bridge_repo.get_or_create_cost_wallet(&user.id).await
                && let Ok(updated) = state
                    .bridge_repo
                    .update_budget_limit(&user.id, current.api_key_budget_limit_usd + extend_usd)
                    .await
            {
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
            }
        }
        _ => {}
    }

    // 4. WebSocket notification (includes user_id so forward_event_bus_to_websocket delivers to client
    //    and TelegramTeamHub::spawn_event_watcher mirrors resolved status back to Telegram)
    state.event_bus.broadcast(WebSocketMessage::new(
        "inbox:item_resolved",
        serde_json::json!({
            "user_id": user.id,
            "id": id,
            "action": req.action,
            "resolved_by": "desktop",
            "telegram_chat_id": card.telegram_chat_id,
            "telegram_msg_id": card.telegram_msg_id,
            "title": card.title,
        }),
    ));

    let now = aionui_common::now_ms();
    Ok(Json(ApiResponse::ok(ResolveActionInboxResponse {
        success: true,
        resolved_at: now,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_db::custom_bridge_schema::CreateActionInboxParams;
    use aionui_db::init_database_memory;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn mock_user() -> CurrentUser {
        CurrentUser::local_default()
    }

    #[tokio::test]
    async fn test_inbox_list_and_atomic_resolve_api() {
        let db = init_database_memory().await.unwrap();
        let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
        let event_bus = Arc::new(BroadcastEventBus::new(16));
        let worktree_engine = Arc::new(GitWorktreeEngine::new());

        let app = Router::new()
            .merge(inbox_routes(InboxRouterState {
                bridge_repo: bridge_repo.clone(),
                event_bus: event_bus.clone(),
                worktree_engine: worktree_engine.clone(),
            }))
            .layer(axum::Extension(mock_user()));

        let user = mock_user();

        // 1. Initial GET /api/inbox (empty)
        let req = Request::builder()
            .method("GET")
            .uri("/api/inbox")
            .body(Body::empty())
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
        assert_eq!(dto.data.unwrap().pending_count, 0);

        // 2. Create card in DB
        let card = bridge_repo
            .create_action_inbox_item(CreateActionInboxParams {
                id: Some("inbox_route_test_1".into()),
                owner_user_id: user.id.clone(),
                category: "choice".into(),
                title: "Test Decision".into(),
                summary: "Decide on next step".into(),
                details_json: r#"{"options":["approve","reject"]}"#.into(),
                target_type: Some("room".into()),
                target_id: "room-test".into(),
                action_token: Some("tok_inbox_test".into()),
                telegram_chat_id: None,
                telegram_msg_id: None,
            })
            .await
            .unwrap();

        assert_eq!(card.status, "pending");

        // 3. GET /api/inbox (1 pending item)
        let req = Request::builder()
            .method("GET")
            .uri("/api/inbox")
            .body(Body::empty())
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
        let list = dto.data.unwrap();
        assert_eq!(list.pending_count, 1);
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].id, "inbox_route_test_1");

        // 4. POST /api/inbox/inbox_route_test_1/resolve (First attempt succeeds)
        let req = Request::builder()
            .method("POST")
            .uri("/api/inbox/inbox_route_test_1/resolve")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"action":"approve"}"#))
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let dto: ApiResponse<ResolveActionInboxResponse> = serde_json::from_slice(&body).unwrap();
        assert!(dto.data.unwrap().success);

        // 5. POST /api/inbox/inbox_route_test_1/resolve (Second attempt yields 409 Conflict)
        let req = Request::builder()
            .method("POST")
            .uri("/api/inbox/inbox_route_test_1/resolve")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"action":"approve"}"#))
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);

        // 6. GET /api/inbox (pending count is 0)
        let req = Request::builder()
            .method("GET")
            .uri("/api/inbox")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let dto: ApiResponse<ActionInboxListResponse> = serde_json::from_slice(&body).unwrap();
        let list = dto.data.unwrap();
        assert_eq!(list.pending_count, 0);
        assert_eq!(list.items[0].status, "resolved");
    }
}

#![allow(clippy::disallowed_types)]

//! REST API routes for Multi-Team Shared Rooms (`/api/shared-rooms/*`).

use std::sync::Arc;

use aionui_api_types::{ApiResponse, WebSocketMessage};
use aionui_auth::CurrentUser;
use aionui_common::{ApiError, generate_prefixed_id};
use aionui_db::{
    CreateSharedRoomParams, CustomStreamMode, CustomTargetType, DbError, UpdateSharedRoomParams,
    UpsertChannelBindingParams,
};
use aionui_realtime::{BroadcastEventBus, EventBroadcaster};
use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, patch, post, put};
use serde::{Deserialize, Serialize};

use crate::bridge::file_guard::sanitize_file_name;
use crate::bridge::hub::{
    PostRoomMessageArgs, SharedRoomDto, SharedRoomMessageDto, SharedRoomTaskDto, TelegramTeamHub,
};
use crate::bridge::judge::RoutedTarget;

#[derive(Clone)]
pub struct SharedRoomRouterState {
    pub hub: Arc<TelegramTeamHub>,
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

#[derive(Debug, Deserialize)]
pub struct CreateSharedRoomRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub rules: String,
    #[serde(default)]
    pub team_ids: Vec<String>,
    #[serde(default)]
    pub host_team_id: Option<String>,
    #[serde(default)]
    pub max_hops: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSharedRoomRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub rules: Option<String>,
    #[serde(default)]
    pub team_ids: Option<Vec<String>>,
    #[serde(default)]
    pub host_team_id: Option<Option<String>>,
    #[serde(default)]
    pub max_hops: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ListMessagesQuery {
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SendSharedRoomMessageRequest {
    pub content: String,
    #[serde(default)]
    pub mentions: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub reply_to_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SendSharedRoomMessageResponse {
    pub message: SharedRoomMessageDto,
    pub routed_targets: Vec<RoutedTarget>,
}

#[derive(Debug, Deserialize)]
pub struct AllowMoreHopsRequest {
    #[serde(default)]
    pub grant_hops: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRoomTaskRequest {
    pub to_team_id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub from_team_id: Option<String>,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateRoomTaskRequest {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub result_summary: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertRoomTelegramBindingRequest {
    #[serde(default)]
    pub group_chat_id: Option<String>,
    #[serde(default)]
    pub group_thread_id: Option<String>,
    #[serde(default)]
    pub set_active_dm_target: Option<bool>,
}

#[derive(Deserialize)]
struct RoomTaskPath {
    id: String,
    task_id: String,
}

pub fn shared_room_routes(state: SharedRoomRouterState) -> Router {
    Router::new()
        .route("/api/shared-rooms", get(list_rooms).post(create_room))
        .route(
            "/api/shared-rooms/{id}",
            get(get_room).patch(update_room).delete(delete_room),
        )
        .route(
            "/api/shared-rooms/{id}/messages",
            get(list_messages).post(send_message),
        )
        .route("/api/shared-rooms/{id}/allow-more-hops", post(allow_more_hops))
        .route("/api/shared-rooms/{id}/tasks", get(list_tasks).post(create_task))
        .route("/api/shared-rooms/{id}/tasks/{task_id}", patch(update_task))
        .route(
            "/api/shared-rooms/{id}/telegram-binding",
            put(upsert_telegram_binding).delete(delete_telegram_binding),
        )
        .with_state(state)
}

async fn list_rooms(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<Vec<SharedRoomDto>>>, ApiError> {
    let rooms = state
        .hub
        .list_shared_room_dtos(&user.id)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(ApiResponse::ok(rooms)))
}

async fn create_room(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<CreateSharedRoomRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ApiResponse<SharedRoomDto>>), ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let trimmed_name = req.name.trim();
    if trimmed_name.is_empty() {
        return Err(ApiError::BadRequest("Room name must not be empty".into()));
    }
    if req.team_ids.is_empty() {
        return Err(ApiError::BadRequest("Select at least one Team to join the room".into()));
    }

    let room_id = generate_prefixed_id("sroom");
    let artifacts_dir = state
        .hub
        .data_dir()
        .join("shared-rooms")
        .join(sanitize_file_name(&room_id))
        .join("artifacts");
    let _ = std::fs::create_dir_all(&artifacts_dir);

    state
        .hub
        .bridge_repo()
        .create_shared_room(CreateSharedRoomParams {
            id: room_id.clone(),
            owner_user_id: user.id.clone(),
            name: trimmed_name.to_owned(),
            description: req.description.trim().to_owned(),
            rules: req.rules.trim().to_owned(),
            host_team_id: req.host_team_id,
            max_hops: req.max_hops.unwrap_or(5).clamp(1, 30),
            artifacts_dir: artifacts_dir.to_string_lossy().into_owned(),
            team_ids: req.team_ids,
        })
        .await
        .map_err(db_err_to_api)?;

    let dto = state
        .hub
        .build_shared_room_dto(&user.id, &room_id)
        .await
        .map_err(ApiError::Internal)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "sharedRoom.created",
        serde_json::json!({
            "user_id": user.id,
            "room": dto,
        }),
    ));

    Ok((StatusCode::CREATED, Json(ApiResponse::ok(dto))))
}

async fn get_room(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<SharedRoomDto>>, ApiError> {
    let dto = state
        .hub
        .build_shared_room_dto(&user.id, &id)
        .await
        .map_err(ApiError::NotFound)?;
    Ok(Json(ApiResponse::ok(dto)))
}

async fn update_room(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<UpdateSharedRoomRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SharedRoomDto>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state
        .hub
        .bridge_repo()
        .update_shared_room(
            &user.id,
            &id,
            UpdateSharedRoomParams {
                name: req.name.map(|n| n.trim().to_owned()).filter(|n| !n.is_empty()),
                description: req.description,
                rules: req.rules,
                host_team_id: req.host_team_id,
                max_hops: req.max_hops.map(|h| h.clamp(1, 30)),
                team_ids: req.team_ids,
            },
        )
        .await
        .map_err(db_err_to_api)?;

    let dto = state
        .hub
        .build_shared_room_dto(&user.id, &id)
        .await
        .map_err(ApiError::Internal)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "sharedRoom.updated",
        serde_json::json!({
            "user_id": user.id,
            "room": dto,
        }),
    ));

    Ok(Json(ApiResponse::ok(dto)))
}

async fn delete_room(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let deleted = state
        .hub
        .bridge_repo()
        .delete_shared_room(&user.id, &id)
        .await
        .map_err(db_err_to_api)?;
    if !deleted {
        return Err(ApiError::NotFound(format!("Shared room not found: {id}")));
    }

    state.event_bus.broadcast(WebSocketMessage::new(
        "sharedRoom.removed",
        serde_json::json!({
            "user_id": user.id,
            "room_id": id,
        }),
    ));

    Ok(Json(ApiResponse::success()))
}

async fn list_messages(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Query(q): Query<ListMessagesQuery>,
) -> Result<Json<ApiResponse<Vec<SharedRoomMessageDto>>>, ApiError> {
    let room = state
        .hub
        .bridge_repo()
        .get_shared_room(&user.id, &id)
        .await
        .map_err(db_err_to_api)?;
    if room.is_none() {
        return Err(ApiError::NotFound(format!("Shared room not found: {id}")));
    }

    let rows = state
        .hub
        .bridge_repo()
        .list_shared_room_messages(&id, q.limit.unwrap_or(200))
        .await
        .map_err(db_err_to_api)?;

    let dtos = rows.into_iter().map(TelegramTeamHub::map_room_message_dto).collect();
    Ok(Json(ApiResponse::ok(dtos)))
}

async fn send_message(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<SendSharedRoomMessageRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SendSharedRoomMessageResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    if req.content.trim().is_empty() && req.files.is_empty() {
        return Err(ApiError::BadRequest("Message content or files must be provided".into()));
    }

    let content = if req.content.trim().is_empty() {
        format!("Attached {} file(s).", req.files.len())
    } else {
        req.content
    };

    let (message, routed_targets) = state
        .hub
        .post_message_to_shared_room(PostRoomMessageArgs {
            owner_user_id: &user.id,
            room_id: &id,
            sender_type: "user",
            sender_team_id: None,
            content: &content,
            explicit_mentions: req.mentions,
            source_files: req.files,
            reply_to_id: req.reply_to_id,
            origin_telegram_chat: None,
        })
        .await
        .map_err(ApiError::BadRequest)?;

    Ok(Json(ApiResponse::ok(SendSharedRoomMessageResponse {
        message,
        routed_targets,
    })))
}

async fn allow_more_hops(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Option<Json<AllowMoreHopsRequest>>,
) -> Result<Json<ApiResponse<SharedRoomDto>>, ApiError> {
    let room = state
        .hub
        .bridge_repo()
        .get_shared_room(&user.id, &id)
        .await
        .map_err(db_err_to_api)?;
    if room.is_none() {
        return Err(ApiError::NotFound(format!("Shared room not found: {id}")));
    }

    let grant = body.and_then(|Json(r)| r.grant_hops).unwrap_or(5).clamp(1, 30);
    state
        .hub
        .bridge_repo()
        .extend_room_hops_allowance(&id, grant)
        .await
        .map_err(db_err_to_api)?;

    let dto = state
        .hub
        .build_shared_room_dto(&user.id, &id)
        .await
        .map_err(ApiError::Internal)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "sharedRoom.updated",
        serde_json::json!({
            "user_id": user.id,
            "room": dto,
        }),
    ));

    Ok(Json(ApiResponse::ok(dto)))
}

async fn list_tasks(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<SharedRoomTaskDto>>>, ApiError> {
    let tasks = state
        .hub
        .list_shared_room_task_dtos(&user.id, &id)
        .await
        .map_err(ApiError::BadRequest)?;
    Ok(Json(ApiResponse::ok(tasks)))
}

async fn create_task(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<CreateRoomTaskRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ApiResponse<SharedRoomTaskDto>>), ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    if req.title.trim().is_empty() {
        return Err(ApiError::BadRequest("Task title must not be empty".into()));
    }

    let dto = state
        .hub
        .create_cross_team_task(
            &user.id,
            &id,
            req.from_team_id.as_deref(),
            &req.to_team_id,
            &req.title,
            &req.description,
            req.blocked_by,
            req.files,
        )
        .await
        .map_err(ApiError::BadRequest)?;

    Ok((StatusCode::CREATED, Json(ApiResponse::ok(dto))))
}

async fn update_task(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(path): Path<RoomTaskPath>,
    body: Result<Json<UpdateRoomTaskRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SharedRoomTaskDto>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let _ = state
        .hub
        .bridge_repo()
        .get_shared_room(&user.id, &path.id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Shared room not found: {}", path.id)))?;

    let existing_task = state
        .hub
        .bridge_repo()
        .get_shared_room_task(&path.task_id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Shared room task not found: {}", path.task_id)))?;
    if existing_task.room_id != path.id {
        return Err(ApiError::NotFound(format!(
            "Task {} does not belong to room {}",
            path.task_id, path.id
        )));
    }

    let dto = state
        .hub
        .update_cross_team_task(
            &user.id,
            &path.task_id,
            None,
            req.status,
            req.description,
            req.result_summary,
            req.files,
        )
        .await
        .map_err(ApiError::BadRequest)?;

    Ok(Json(ApiResponse::ok(dto)))
}

async fn upsert_telegram_binding(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<UpsertRoomTelegramBindingRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SharedRoomDto>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let _ = state
        .hub
        .bridge_repo()
        .get_shared_room(&user.id, &id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Shared room not found: {id}")))?;

    if let Some(chat_id) = req.group_chat_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        state
            .hub
            .bridge_repo()
            .delete_channel_bindings_for_target(&user.id, CustomTargetType::Room, &id)
            .await
            .map_err(db_err_to_api)?;
        state
            .hub
            .bridge_repo()
            .upsert_channel_binding(UpsertChannelBindingParams {
                owner_user_id: user.id.clone(),
                plugin_id: "telegram".to_owned(),
                chat_id: chat_id.to_owned(),
                thread_id: req.group_thread_id,
                target_type: CustomTargetType::Room,
                target_id: id.clone(),
                stream_mode: CustomStreamMode::Milestones,
            })
            .await
            .map_err(db_err_to_api)?;
    }

    if let Some(set_dm) = req.set_active_dm_target {
        state
            .hub
            .set_room_active_dm_target(&user.id, &id, set_dm)
            .await
            .map_err(db_err_to_api)?;
    }

    let dto = state
        .hub
        .build_shared_room_dto(&user.id, &id)
        .await
        .map_err(ApiError::Internal)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "sharedRoom.updated",
        serde_json::json!({
            "user_id": user.id,
            "room": dto,
        }),
    ));

    Ok(Json(ApiResponse::ok(dto)))
}

async fn delete_telegram_binding(
    State(state): State<SharedRoomRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<SharedRoomDto>>, ApiError> {
    state
        .hub
        .bridge_repo()
        .delete_channel_bindings_for_target(&user.id, CustomTargetType::Room, &id)
        .await
        .map_err(db_err_to_api)?;

    let dto = state
        .hub
        .build_shared_room_dto(&user.id, &id)
        .await
        .map_err(ApiError::Internal)?;

    state.event_bus.broadcast(WebSocketMessage::new(
        "sharedRoom.updated",
        serde_json::json!({
            "user_id": user.id,
            "room": dto,
        }),
    ));

    Ok(Json(ApiResponse::ok(dto)))
}

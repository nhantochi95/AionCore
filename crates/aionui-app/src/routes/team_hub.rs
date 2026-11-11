#![allow(clippy::disallowed_types)]

//! Team Hub REST API endpoints (`/api/team-hub/*`).
//!
//! Provides:
//! - `GET /api/team-hub/connected-models`: unified catalog of online CLI OAuth ($0) & enabled API providers.
//! - `POST /api/team-hub/quick-member`: creates a quick team member (reusing `bare:*` unless saved/custom rule).
//! - `GET /api/team-hub/teams/{id}/context-status`: per-slot token & percentage health snapshot.
//! - `POST /api/team-hub/teams/{id}/compact`: 3-tier context compaction (accepts `{}` empty body).
//! - `POST /api/team-hub/teams/{id}/switch-models`: atomic cross-engine hot-swap (`all` / `teammates_only` / `slots`).

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, Json, Path, State};
use axum::routing::{get, patch, post};

use aionui_api_types::ApiResponse;
use aionui_auth::CurrentUser;
use aionui_common::ApiError;

pub use crate::bridge::context_compactor::{
    ApplyQuotaPresetRequest, CompactTeamContextRequest, CompactTeamContextResponse, ConnectedEngineCatalogItemDto,
    ConnectedModelOptionDto, ConnectedModelsCatalogDto, ModelTargetSpec, PerSlotModelUpdate, QuickCreateMemberRequest,
    QuickCreateMemberResponse, QuotaRoutingPresetDto, SlotCompactionOutcomeDto, SlotContextStatusDto,
    SlotModelSwitchResultDto, SwitchTeamModelsRequest, SwitchTeamModelsResponse, TeamContextStatusResponse,
    TeamHubOrchestrator,
};

#[derive(Clone)]
pub struct TeamHubRouterState {
    pub orchestrator: TeamHubOrchestrator,
}

pub fn team_hub_routes(state: TeamHubRouterState) -> Router {
    Router::new()
        .route("/api/team-hub/connected-models", get(get_connected_models))
        .route("/api/team-hub/quick-member", post(post_quick_member))
        .route("/api/team-hub/teams/{id}/context-status", get(get_team_context_status))
        .route("/api/team-hub/teams/{id}/compact", post(post_compact_team_context))
        .route(
            "/api/team-hub/teams/{id}/economy-prefs",
            patch(patch_team_economy_prefs).post(patch_team_economy_prefs),
        )
        .route("/api/team-hub/teams/{id}/switch-models", post(post_switch_team_models))
        .route(
            "/api/team-hub/teams/{id}/apply-quota-preset",
            post(post_apply_quota_preset),
        )
        .with_state(state)
}

/// `GET /api/team-hub/connected-models`
async fn get_connected_models(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<ConnectedModelsCatalogDto>>, ApiError> {
    let catalog = state.orchestrator.list_connected_models(&user.id).await?;
    Ok(Json(ApiResponse::ok(catalog)))
}

/// `POST /api/team-hub/quick-member`
async fn post_quick_member(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
    Json(req): Json<QuickCreateMemberRequest>,
) -> Result<Json<ApiResponse<QuickCreateMemberResponse>>, ApiError> {
    let created = state.orchestrator.quick_create_member(&user.id, req).await?;
    Ok(Json(ApiResponse::ok(created)))
}

/// `GET /api/team-hub/teams/{id}/context-status`
async fn get_team_context_status(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<TeamContextStatusResponse>>, ApiError> {
    let status = state.orchestrator.get_team_context_status(&user.id, &id).await?;
    Ok(Json(ApiResponse::ok(status)))
}

/// `POST /api/team-hub/teams/{id}/compact`
async fn post_compact_team_context(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<ApiResponse<CompactTeamContextResponse>>, ApiError> {
    let req: CompactTeamContextRequest = if body.is_empty() || body.iter().all(u8::is_ascii_whitespace) {
        CompactTeamContextRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON body: {e}")))?
    };
    let resp = state.orchestrator.compact_team_context(&user.id, &id, req).await?;
    Ok(Json(ApiResponse::ok(resp)))
}

/// `POST /api/team-hub/teams/{id}/switch-models`
async fn post_switch_team_models(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<ApiResponse<SwitchTeamModelsResponse>>, ApiError> {
    let req: SwitchTeamModelsRequest = if body.is_empty() || body.iter().all(u8::is_ascii_whitespace) {
        SwitchTeamModelsRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON body: {e}")))?
    };
    let resp = state.orchestrator.switch_team_models(&user.id, &id, req).await?;
    Ok(Json(ApiResponse::ok(resp)))
}

/// `PATCH|POST /api/team-hub/teams/{id}/economy-prefs`
async fn patch_team_economy_prefs(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<ApiResponse<TeamContextStatusResponse>>, ApiError> {
    let mut req: CompactTeamContextRequest = if body.is_empty() || body.iter().all(u8::is_ascii_whitespace) {
        CompactTeamContextRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON body: {e}")))?
    };
    req.mode = Some("prefs_only".to_owned());
    req.slot_id = None;
    let resp = state.orchestrator.compact_team_context(&user.id, &id, req).await?;
    Ok(Json(ApiResponse::ok(resp.context_status)))
}

/// `POST /api/team-hub/teams/{id}/apply-quota-preset`
async fn post_apply_quota_preset(
    State(state): State<TeamHubRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<ApiResponse<SwitchTeamModelsResponse>>, ApiError> {
    let req: ApplyQuotaPresetRequest = if body.is_empty() || body.iter().all(u8::is_ascii_whitespace) {
        ApplyQuotaPresetRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON body: {e}")))?
    };
    let resp = state.orchestrator.apply_quota_preset(&user.id, &id, req).await?;
    Ok(Json(ApiResponse::ok(resp)))
}

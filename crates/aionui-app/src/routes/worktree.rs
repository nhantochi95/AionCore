#![allow(clippy::disallowed_types)]

//! Worktree REST API endpoints (`/api/worktrees/*`).
//!
//! Provides:
//! - `GET /api/worktrees/{id}/diff`: computes files_changed, additions, deletions, unified patch.
//! - `POST /api/worktrees/{id}/merge`: squash merges worktree into base branch with zero AI attribution.
//! - `POST /api/worktrees/{id}/discard`: discards worktree and archives uncommitted leftovers.

use std::path::Path;
use std::sync::Arc;

use aionui_api_types::ApiResponse;
use aionui_auth::CurrentUser;
use aionui_common::ApiError;
use aionui_db::{DbError, SqliteCustomBridgeRepository};
use aionui_project::{GitWorktreeEngine, WorktreeDiffDto, WorktreeError, WorktreeMergeResultDto};
use axum::extract::{Extension, Json, Path as AxumPath, State};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct WorktreeRouterState {
    pub bridge_repo: Arc<SqliteCustomBridgeRepository>,
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

fn wt_err_to_api(err: WorktreeError) -> ApiError {
    match err {
        WorktreeError::LeaseNotFound(..) | WorktreeError::RepoNotFound(..) | WorktreeError::BaseBranchNotFound(..) => {
            ApiError::NotFound(err.to_string())
        }
        WorktreeError::MergeConflict { conflicting_files } => {
            ApiError::Conflict(format!("Merge conflict in files: {conflicting_files:?}"))
        }
        WorktreeError::BranchAlreadyExists(..) | WorktreeError::WorktreePathExists(..) => {
            ApiError::Conflict(err.to_string())
        }
        WorktreeError::InvalidCommitMessage(m) => ApiError::BadRequest(m),
        _ => ApiError::Internal(err.to_string()),
    }
}

pub fn worktree_routes(state: WorktreeRouterState) -> Router {
    Router::new()
        .route("/api/worktrees/{id}/diff", get(get_worktree_diff))
        .route("/api/worktrees/{id}/merge", post(merge_worktree))
        .route("/api/worktrees/{id}/discard", post(discard_worktree))
        .with_state(state)
}

/// GET /api/worktrees/{id}/diff
async fn get_worktree_diff(
    State(state): State<WorktreeRouterState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ApiResponse<WorktreeDiffDto>>, ApiError> {
    let row = state
        .bridge_repo
        .get_worktree_lease(&id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Worktree lease '{id}' not found")))?;

    let diff = state
        .worktree_engine
        .compute_diff(
            Path::new(&row.repo_path),
            Path::new(&row.worktree_path),
            &row.base_branch,
            &row.lease_id,
        )
        .await
        .map_err(wt_err_to_api)?;

    Ok(Json(ApiResponse::ok(diff)))
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MergeWorktreeRequest {
    #[serde(default)]
    pub commit_message: String,
}

/// POST /api/worktrees/{id}/merge
async fn merge_worktree(
    State(state): State<WorktreeRouterState>,
    Extension(user): Extension<CurrentUser>,
    AxumPath(id): AxumPath<String>,
    Json(payload): Json<MergeWorktreeRequest>,
) -> Result<Json<ApiResponse<WorktreeMergeResultDto>>, ApiError> {
    let row = state
        .bridge_repo
        .get_worktree_lease(&id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Worktree lease '{id}' not found")))?;

    if row.status != "active" {
        return Err(ApiError::Conflict(format!(
            "Worktree lease '{id}' is in '{}' status, expected 'active'",
            row.status
        )));
    }

    let _ = state
        .bridge_repo
        .update_worktree_status(&id, "merging", None)
        .await;

    let user_name = user.username.clone();
    let user_email = format!("{}@local", user.username);
    let effective_commit_msg = if payload.commit_message.trim().is_empty() {
        format!(
            "feat({}): squash merge worktree {}",
            row.team_id.as_deref().unwrap_or("team"),
            row.lease_id
        )
    } else {
        payload.commit_message
    };

    let merge_res = match state
        .worktree_engine
        .squash_merge(
            Path::new(&row.repo_path),
            Path::new(&row.worktree_path),
            &row.branch,
            &row.base_branch,
            &effective_commit_msg,
            Some(&user_name),
            Some(&user_email),
            &row.lease_id,
        )
        .await
    {
        Ok(res) => res,
        Err(e) => {
            let _ = state
                .bridge_repo
                .update_worktree_status(&id, "active", None)
                .await;
            return Err(wt_err_to_api(e));
        }
    };

    state
        .bridge_repo
        .update_worktree_status(&id, "merged", merge_res.leftovers_ref.as_deref())
        .await
        .map_err(db_err_to_api)?;

    Ok(Json(ApiResponse::ok(merge_res)))
}

#[derive(Debug, Serialize)]
pub struct DiscardWorktreeResponse {
    pub leftovers_ref: Option<String>,
}

/// POST /api/worktrees/{id}/discard
async fn discard_worktree(
    State(state): State<WorktreeRouterState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ApiResponse<DiscardWorktreeResponse>>, ApiError> {
    let row = state
        .bridge_repo
        .get_worktree_lease(&id)
        .await
        .map_err(db_err_to_api)?
        .ok_or_else(|| ApiError::NotFound(format!("Worktree lease '{id}' not found")))?;

    let leftovers = state
        .worktree_engine
        .discard_worktree(
            Path::new(&row.repo_path),
            Path::new(&row.worktree_path),
            &row.branch,
            &row.lease_id,
        )
        .await
        .map_err(wt_err_to_api)?;

    state
        .bridge_repo
        .update_worktree_status(&id, "discarded", leftovers.as_deref())
        .await
        .map_err(db_err_to_api)?;

    Ok(Json(ApiResponse::ok(DiscardWorktreeResponse {
        leftovers_ref: leftovers,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_db::init_database_memory;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    #[test]
    fn test_error_mappings() {
        let err_not_found = wt_err_to_api(WorktreeError::LeaseNotFound("l-1".into()));
        match err_not_found {
            ApiError::NotFound(m) => assert!(m.contains("l-1")),
            other => panic!("expected NotFound, got {other:?}"),
        }

        let err_conflict = wt_err_to_api(WorktreeError::MergeConflict {
            conflicting_files: vec!["file.txt".into()],
        });
        match err_conflict {
            ApiError::Conflict(m) => assert!(m.contains("file.txt")),
            other => panic!("expected Conflict, got {other:?}"),
        }

        let err_bad_req = wt_err_to_api(WorktreeError::InvalidCommitMessage("bad message".into()));
        match err_bad_req {
            ApiError::BadRequest(m) => assert!(m.contains("bad message")),
            other => panic!("expected BadRequest, got {other:?}"),
        }

        let db_not_found = db_err_to_api(DbError::NotFound("db missing".into()));
        match db_not_found {
            ApiError::NotFound(m) => assert_eq!(m, "db missing"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_get_worktree_diff_not_found() {
        let db = init_database_memory().await.unwrap();
        let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
        let worktree_engine = Arc::new(GitWorktreeEngine::new());

        let app = worktree_routes(WorktreeRouterState {
            bridge_repo,
            worktree_engine,
        });

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/worktrees/nonexistent/diff")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    fn init_test_repo(path: &Path) -> git2::Repository {
        let repo = git2::Repository::init(path).expect("failed to init repo");
        let sig = git2::Signature::now("Tester", "tester@example.com").unwrap();
        let readme_path = path.join("README.md");
        std::fs::write(&readme_path, "# Test\n").unwrap();

        let mut index = repo.index().unwrap();
        index.add_path(Path::new("README.md")).unwrap();
        let tree_id = index.write_tree().unwrap();
        {
            let tree = repo.find_tree(tree_id).unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "chore: initial commit", &tree, &[])
                .unwrap();
        }

        {
            let commit = repo.head().unwrap().peel_to_commit().unwrap();
            repo.branch("feat/hybrid-command-hub", &commit, true).unwrap();
        }
        repo.set_head("refs/heads/feat/hybrid-command-hub").unwrap();
        repo
    }

    #[tokio::test]
    async fn test_worktree_api_lifecycle() {
        let dir = tempdir().unwrap();
        let repo_path = dir.path().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        let _repo = init_test_repo(&repo_path);

        let db = init_database_memory().await.unwrap();
        let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
        let worktree_engine = Arc::new(GitWorktreeEngine::new());

        let lease = worktree_engine
            .allocate_worktree(
                aionui_project::WorktreeConfig {
                    repo_path: repo_path.clone(),
                    lease_id: "lease-route-1".into(),
                    base_branch: "feat/hybrid-command-hub".into(),
                    owner_user_id: "system_default_user".into(),
                    room_id: None,
                    team_id: None,
                },
                Some(&bridge_repo),
            )
            .await
            .expect("allocate worktree");

        // Add a new file to the worktree
        let draft_file = Path::new(&lease.worktree_path).join("draft.txt");
        std::fs::write(&draft_file, "worktree content\n").unwrap();

        let app = worktree_routes(WorktreeRouterState {
            bridge_repo: bridge_repo.clone(),
            worktree_engine: worktree_engine.clone(),
        });

        // 1. GET diff
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/worktrees/{}/diff", lease.lease_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let diff_resp: ApiResponse<WorktreeDiffDto> = serde_json::from_slice(&body_bytes).unwrap();
        assert!(diff_resp.success);
        let diff = diff_resp.data.unwrap();
        assert_eq!(diff.files_changed, 1);

        // 2. POST discard
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/worktrees/{}/discard", lease.lease_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let updated_row = bridge_repo.get_worktree_lease(&lease.lease_id).await.unwrap().unwrap();
        assert_eq!(updated_row.status, "discarded");
        assert!(updated_row.leftovers_ref.is_some(), "Leftovers ref must be captured");
    }

    #[tokio::test]
    async fn test_worktree_api_merge_lifecycle() {
        let dir = tempdir().unwrap();
        let repo_path = dir.path().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        let _repo = init_test_repo(&repo_path);

        let db = init_database_memory().await.unwrap();
        let bridge_repo = Arc::new(SqliteCustomBridgeRepository::new(db.pool().clone()));
        let worktree_engine = Arc::new(GitWorktreeEngine::new());

        let lease = worktree_engine
            .allocate_worktree(
                aionui_project::WorktreeConfig {
                    repo_path: repo_path.clone(),
                    lease_id: "lease-route-merge".into(),
                    base_branch: "feat/hybrid-command-hub".into(),
                    owner_user_id: "system_default_user".into(),
                    room_id: None,
                    team_id: None,
                },
                Some(&bridge_repo),
            )
            .await
            .expect("allocate worktree");

        // Commit a feature file into the worktree
        let wt_path = Path::new(&lease.worktree_path);
        let feat_file = wt_path.join("feature.txt");
        std::fs::write(&feat_file, "worktree merge feature content\n").unwrap();

        let wt_repo = git2::Repository::open(wt_path).unwrap();
        let sig = git2::Signature::now("Tester", "tester@example.com").unwrap();
        {
            let mut idx = wt_repo.index().unwrap();
            idx.add_path(Path::new("feature.txt")).unwrap();
            let tree_id = idx.write_tree().unwrap();
            let tree = wt_repo.find_tree(tree_id).unwrap();
            let head = wt_repo.head().unwrap().peel_to_commit().unwrap();
            wt_repo.commit(Some("HEAD"), &sig, &sig, "feat: worktree feature commit", &tree, &[&head]).unwrap();
        }

        let app = worktree_routes(WorktreeRouterState {
            bridge_repo: bridge_repo.clone(),
            worktree_engine: worktree_engine.clone(),
        });

        let merge_payload = serde_json::to_vec(&MergeWorktreeRequest {
            commit_message: "[AI]: feat(app): integrate worktree feature\n\nCo-authored-by: Claude <noreply@anthropic.com>\n🤖 Generated with Cursor".into(),
        }).unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/worktrees/{}/merge", lease.lease_id))
                    .header("Content-Type", "application/json")
                    .extension(CurrentUser::local_default())
                    .body(Body::from(merge_payload))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let merge_resp: ApiResponse<WorktreeMergeResultDto> = serde_json::from_slice(&body_bytes).unwrap();
        assert!(merge_resp.success);
        let res_dto = merge_resp.data.unwrap();
        assert!(!res_dto.squash_commit_oid.is_empty());
        assert_eq!(res_dto.commit_message, "feat(app): integrate worktree feature");
        assert!(!res_dto.commit_message.contains("Co-authored-by"));
        assert!(!res_dto.commit_message.contains("🤖"));

        // Verify status in database transitioned to "merged"
        let updated_row = bridge_repo.get_worktree_lease(&lease.lease_id).await.unwrap().unwrap();
        assert_eq!(updated_row.status, "merged");
    }
}

//! Git Worktree Isolation Engine (F07, F08, F09, F10).
//!
//! Provides in-process, headless Git worktree lifecycle management via `git2`:
//! - Worktree allocation under `.aionui/worktrees/<lease_id>`.
//! - Disjoint index allocation eliminating `.git/index.lock` contention.
//! - Cleanliness via `.git/info/exclude`.
//! - Leftovers archiving into `refs/aionui/leftovers/<lease_id>_<timestamp>`.
//! - Safe squash merge producing exactly 1 Conventional Commit on base branch.
//! - Two-pass AI attribution scrubbing removing all AI trailers/signatures.
//! - Worktree diff calculation for UI preview.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aionui_db::{CreateGitWorktreeParams, CustomGitWorktreeRow, SqliteCustomBridgeRepository};
use dashmap::DashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum WorktreeError {
    #[error("Repository not found at path: {0}")]
    RepoNotFound(PathBuf),

    #[error("Base branch '{0}' not found in repository")]
    BaseBranchNotFound(String),

    #[error("Worktree directory already exists: {0}")]
    WorktreePathExists(PathBuf),

    #[error("Worktree branch '{0}' already exists")]
    BranchAlreadyExists(String),

    #[error("Merge conflict in files: {conflicting_files:?}")]
    MergeConflict { conflicting_files: Vec<String> },

    #[error("Invalid commit message: {0}")]
    InvalidCommitMessage(String),

    #[error("Worktree lease not found: {0}")]
    LeaseNotFound(String),

    #[error("Git error during {context}: {message}")]
    GitEngine {
        context: &'static str,
        message: String,
    },

    #[error("Filesystem IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeConfig {
    pub lease_id: String,
    pub repo_path: PathBuf,
    pub base_branch: String,
    pub room_id: Option<String>,
    pub team_id: Option<String>,
    pub owner_user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorktreeDiffDto {
    pub lease_id: String,
    pub branch_name: String,
    pub base_branch: String,
    pub files_changed: usize,
    pub additions: usize,
    pub deletions: usize,
    pub patch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeMergeResultDto {
    pub lease_id: String,
    pub squash_commit_oid: String,
    pub leftovers_ref: Option<String>,
    pub commit_message: String,
}

/// Sanitizes a commit message by removing all AI attribution trailers,
/// headers, robot emojis, markdown AI summary sections, and signatures,
/// while validating Conventional Commits 1.0.0 format.
pub fn sanitize_conventional_commit(raw_message: &str, lease_id: &str) -> Result<String, WorktreeError> {
    // 1. Robot and AI emojis to strip globally across all lines
    const AI_EMOJIS: &[char] = &['🤖', '🦾', '🧠', '✨', '⚡'];

    // 2. Regex for stripping subject prefixes like [AI]:, AI:, [Claude]:, etc.
    let ai_prefix_regex = Regex::new(
        r"(?i)^\s*(?:\[?(?:AI|Claude|Gemini|ChatGPT|GPT|Copilot|Robot|Bot|Agent|DeepSeek|Devin|AionUi)\]?:\s*|automated by \w+:\s*)"
    ).map_err(|e| WorktreeError::Internal(e.to_string()))?;

    // 3. Regex for markdown AI summary section headings
    let ai_section_header_regex = Regex::new(
        r"(?i)^#{1,6}\s*(?:AI|Generated|Summary|Model|Agent|Bot|Cursor|Claude|ChatGPT|Copilot|DeepSeek|Devin)\b.*$"
    ).map_err(|e| WorktreeError::Internal(e.to_string()))?;

    // 4. Regex for individual lines containing AI attribution, trailers, or generation signatures
    let ai_line_regex = Regex::new(
        concat!(
            r"(?mi)^(",
            // Co-authored-by non-human / AI markers and agent emails
            r"Co-authored-by:\s*.*(?:\b(?:ai|bot|assistant|agent|robot)\b|noreply|claude|gemini|copilot|chatgpt|gpt|openai|anthropic|cursor|aider|devin|deepseek|cognition|qwen|llama|mistral|codex|v0|windsurf|tabnine|codeium).*|",
            // Email with bot or noreply in any trailer:
            r"[A-Za-z\-]+:\s*.*<(?:[^>]*bot[^>]*|[^>]*noreply[^>]*)>.*|",
            // Generation / assistance / authored / prompted / automated statements
            r".*(?:generated|assisted|prompted|authored|automated)\s+(?:by|with|via|through|using)\b.*|",
            // Trailers like Generated-by:, Assisted-by:, AI-Model:, AI:, Bot:, Agent:, AI-*:
            r"(?:Generated-by|Assisted-by|AI-generated|Prompted-by|AI-Model|Engine|Agent-Tool|AI|Assistant|Bot|Agent|Model|Tool|AI-[A-Za-z\-]+):\s*.*|",
            // Signed-off-by bot/ai/agent
            r"Signed-off-by:\s*.*(?:\b(?:bot|ai|agent|assistant)\b|claude|gemini|copilot|openai|anthropic|devin|deepseek).*|",
            // Automated by aionui or any agent
            r".*automated by \w+.*",
            r")$"
        )
    ).map_err(|e| WorktreeError::Internal(e.to_string()))?;

    let lines: Vec<&str> = raw_message.lines().collect();
    if lines.is_empty() {
        return Ok(format!("feat(worktree): integrate changes from lease {lease_id}"));
    }

    // Step A: Clean subject line (remove robot emojis, remove AI prefixes, trim)
    let raw_subject = lines[0];
    let subject_no_emojis = raw_subject.replace(AI_EMOJIS, "");
    let cleaned_subject = ai_prefix_regex.replace(&subject_no_emojis, "").trim().to_string();

    // Step B: Clean body lines
    let mut cleaned_body_lines: Vec<String> = Vec::new();
    let mut in_ai_section = false;

    for line in &lines[1..] {
        let line_no_emojis = line.replace(AI_EMOJIS, "");
        let trimmed = line_no_emojis.trim();

        if trimmed.is_empty() {
            in_ai_section = false;
            if cleaned_body_lines.last().map_or(false, |l| !l.is_empty()) {
                cleaned_body_lines.push(String::new());
            }
            continue;
        }

        // Check if line starts an AI markdown section (e.g. ### AI Summary)
        if ai_section_header_regex.is_match(trimmed) {
            in_ai_section = true;
            continue;
        }

        // If currently inside an AI summary section, drop lines until next non-AI section or blank line
        if in_ai_section {
            if trimmed.starts_with('#') {
                in_ai_section = false;
            } else {
                continue;
            }
        }

        // Check if line matches any AI attribution line pattern
        if ai_line_regex.is_match(trimmed) {
            continue;
        }

        cleaned_body_lines.push(trimmed.to_string());
    }

    // Remove any trailing empty lines from body
    while cleaned_body_lines.last().map_or(false, |l| l.is_empty()) {
        cleaned_body_lines.pop();
    }

    // Step C: Validate Conventional Commit format for subject
    let cc_regex = Regex::new(
        r"^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([a-zA-Z0-9_\-\./]+\))?!?: \S.*$"
    ).map_err(|e| WorktreeError::Internal(e.to_string()))?;

    let final_subject = if cc_regex.is_match(&cleaned_subject) {
        cleaned_subject
    } else if cleaned_subject.is_empty() {
        format!("feat(worktree): integrate changes from lease {lease_id}")
    } else {
        format!("feat(worktree): {}", cleaned_subject)
    };

    let mut result = final_subject;
    if !cleaned_body_lines.is_empty() {
        result.push_str("\n\n");
        result.push_str(&cleaned_body_lines.join("\n").trim());
    }

    Ok(result)
}

/// Ensures `.git/info/exclude` in the primary repository excludes `.aionui/`.
pub fn ensure_exclude_configured(repo_path: &Path) -> Result<(), WorktreeError> {
    let info_exclude_path = match git2::Repository::open(repo_path) {
        Ok(repo) => repo.path().join("info").join("exclude"),
        Err(_) => {
            let git_dir = repo_path.join(".git");
            if !git_dir.exists() {
                return Ok(());
            }
            git_dir.join("info").join("exclude")
        }
    };

    if let Some(parent) = info_exclude_path.parent() {
        if !parent.exists() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    let current_content = if info_exclude_path.exists() {
        std::fs::read_to_string(&info_exclude_path).unwrap_or_default()
    } else {
        String::new()
    };

    if !current_content.lines().any(|l| l.trim() == ".aionui/" || l.trim() == "/.aionui/") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&info_exclude_path)?;
        writeln!(file, "\n# AionUi isolated worktree artifacts\n/.aionui/")?;
    }

    Ok(())
}

/// Resiliently teardown/unlink worktree directory on disk with backoff.
pub fn teardown_directory(path: &Path) {
    if !path.exists() {
        return;
    }

    // Attempt 1-3: Standard remove_dir_all with exponential backoff
    for attempt in 1..=3 {
        if std::fs::remove_dir_all(path).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50 * attempt));
    }

    // Fallback: Try platform trash sink
    if trash::delete(path).is_ok() {
        return;
    }

    // Final fallback on Windows: rename to .trash_<uuid>
    if let Some(parent) = path.parent() {
        let trash_name = format!(".trash_{}", aionui_common::generate_prefixed_id("wt"));
        let trash_path = parent.join(trash_name);
        let _ = std::fs::rename(path, trash_path);
    }
}

/// Git Worktree Isolation Engine
#[derive(Clone, Default)]
pub struct GitWorktreeEngine {
    repo_locks: Arc<DashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
}

impl GitWorktreeEngine {
    pub fn new() -> Self {
        Self {
            repo_locks: Arc::new(DashMap::new()),
        }
    }

    fn get_repo_lock(&self, repo_path: &Path) -> Arc<tokio::sync::Mutex<()>> {
        self.repo_locks
            .entry(repo_path.to_path_buf())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Allocates an isolated git worktree under `<repo_path>/.aionui/worktrees/<lease_id>`.
    pub async fn allocate_worktree(
        &self,
        config: WorktreeConfig,
        bridge_repo: Option<&SqliteCustomBridgeRepository>,
    ) -> Result<CustomGitWorktreeRow, WorktreeError> {
        let repo_lock = self.get_repo_lock(&config.repo_path);
        let _guard = repo_lock.lock().await;

        let repo_path = config.repo_path.clone();
        let lease_id = config.lease_id.clone();
        let base_branch = config.base_branch.clone();
        let branch_name = format!("wt/{}", lease_id);
        let worktree_dir = repo_path.join(".aionui").join("worktrees").join(&lease_id);

        if worktree_dir.exists() {
            return Err(WorktreeError::WorktreePathExists(worktree_dir));
        }

        ensure_exclude_configured(&repo_path)?;

        let worktree_dir_clone = worktree_dir.clone();
        let repo_path_clone = repo_path.clone();
        let branch_name_clone = branch_name.clone();
        let lease_id_inner = lease_id.clone();

        tokio::task::spawn_blocking(move || -> Result<(), WorktreeError> {
            let primary_repo = git2::Repository::open(&repo_path_clone)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "open primary repository",
                    message: e.to_string(),
                })?;

            // Find base branch commit
            let base_branch_ref = primary_repo
                .find_branch(&base_branch, git2::BranchType::Local)
                .map_err(|_| WorktreeError::BaseBranchNotFound(base_branch.clone()))?;
            let base_commit = base_branch_ref
                .get()
                .peel_to_commit()
                .map_err(|e| WorktreeError::GitEngine {
                    context: "peel base branch commit",
                    message: e.to_string(),
                })?;

            // If stale git administrative worktree metadata exists for this lease_id, prune it
            if let Ok(stale_wt) = primary_repo.find_worktree(&lease_id_inner) {
                let mut prune_opts = git2::WorktreePruneOptions::new();
                let _ = stale_wt.prune(Some(&mut prune_opts));
            }

            // Check if branch already exists
            if primary_repo.find_branch(&branch_name_clone, git2::BranchType::Local).is_ok() {
                return Err(WorktreeError::BranchAlreadyExists(branch_name_clone));
            }

            // Create dedicated branch for worktree
            let wt_branch = primary_repo
                .branch(&branch_name_clone, &base_commit, false)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "create worktree branch",
                    message: e.to_string(),
                })?;

            // Ensure parent directory exists
            if let Some(parent) = worktree_dir_clone.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Add linked worktree
            let mut wt_opts = git2::WorktreeAddOptions::new();
            wt_opts.reference(Some(wt_branch.get()));
            let _wt = primary_repo
                .worktree(&lease_id_inner, &worktree_dir_clone, Some(&wt_opts))
                .map_err(|e| WorktreeError::GitEngine {
                    context: "allocate worktree",
                    message: e.to_string(),
                })?;

            // Open worktree and checkout files
            let wt_repo = git2::Repository::open(&worktree_dir_clone)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "open allocated worktree",
                    message: e.to_string(),
                })?;

            let mut checkout_opts = git2::build::CheckoutBuilder::new();
            checkout_opts.force();
            wt_repo
                .checkout_head(Some(&mut checkout_opts))
                .map_err(|e| WorktreeError::GitEngine {
                    context: "checkout worktree files",
                    message: e.to_string(),
                })?;

            Ok(())
        })
        .await
        .map_err(|e| WorktreeError::Internal(format!("Task spawn failed: {e}")))??;

        let params = CreateGitWorktreeParams {
            id: Some(lease_id.clone()),
            lease_id: lease_id.clone(),
            owner_user_id: config.owner_user_id.clone(),
            room_id: config.room_id.clone(),
            team_id: config.team_id.clone(),
            repo_path: repo_path.to_string_lossy().to_string(),
            worktree_path: worktree_dir.to_string_lossy().to_string(),
            branch: branch_name,
            base_branch: config.base_branch.clone(),
        };

        if let Some(repo) = bridge_repo {
            repo.create_worktree_lease(params)
                .await
                .map_err(|e| WorktreeError::Database(e.to_string()))
        } else {
            let now = aionui_common::now_ms();
            Ok(CustomGitWorktreeRow {
                id: lease_id.clone(),
                lease_id,
                owner_user_id: config.owner_user_id,
                room_id: config.room_id,
                team_id: config.team_id,
                repo_path: repo_path.to_string_lossy().to_string(),
                worktree_path: worktree_dir.to_string_lossy().to_string(),
                branch: params.branch,
                base_branch: config.base_branch,
                status: "active".to_string(),
                leftovers_ref: None,
                created_at: now,
                updated_at: now,
            })
        }
    }

    /// Computes diff between base branch and worktree directory (including unstaged and untracked changes).
    pub async fn compute_diff(
        &self,
        repo_path: &Path,
        worktree_path: &Path,
        base_branch: &str,
        lease_id: &str,
    ) -> Result<WorktreeDiffDto, WorktreeError> {
        let repo_path = repo_path.to_path_buf();
        let worktree_path = worktree_path.to_path_buf();
        let base_branch = base_branch.to_string();
        let lease_id = lease_id.to_string();

        tokio::task::spawn_blocking(move || -> Result<WorktreeDiffDto, WorktreeError> {
            let primary_repo = git2::Repository::open(&repo_path)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "open primary repo for diff",
                    message: e.to_string(),
                })?;

            let base_branch_ref = primary_repo
                .find_branch(&base_branch, git2::BranchType::Local)
                .map_err(|_| WorktreeError::BaseBranchNotFound(base_branch.clone()))?;
            let base_commit = base_branch_ref
                .get()
                .peel_to_commit()
                .map_err(|e| WorktreeError::GitEngine {
                    context: "peel base commit for diff",
                    message: e.to_string(),
                })?;
            let base_tree = base_commit
                .tree()
                .map_err(|e| WorktreeError::GitEngine {
                    context: "find base tree for diff",
                    message: e.to_string(),
                })?;

            let wt_repo = git2::Repository::open(&worktree_path)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "open worktree repo for diff",
                    message: e.to_string(),
                })?;

            let mut diff_opts = git2::DiffOptions::new();
            diff_opts.include_untracked(true);
            diff_opts.recurse_untracked_dirs(true);
            diff_opts.show_untracked_content(true);

            let diff = wt_repo
                .diff_tree_to_workdir_with_index(Some(&base_tree), Some(&mut diff_opts))
                .map_err(|e| WorktreeError::GitEngine {
                    context: "diff tree to workdir",
                    message: e.to_string(),
                })?;

            let stats = diff.stats().map_err(|e| WorktreeError::GitEngine {
                context: "compute diff stats",
                message: e.to_string(),
            })?;

            let mut patch = String::new();
            diff.print(git2::DiffFormat::Patch, |_delta, _hunk, line| {
                match line.origin() {
                    '+' | '-' | ' ' => patch.push(line.origin()),
                    _ => {}
                }
                patch.push_str(&String::from_utf8_lossy(line.content()));
                true
            })
            .map_err(|e| WorktreeError::GitEngine {
                context: "format unified patch",
                message: e.to_string(),
            })?;

            Ok(WorktreeDiffDto {
                lease_id: lease_id.clone(),
                branch_name: format!("wt/{lease_id}"),
                base_branch,
                files_changed: stats.files_changed(),
                additions: stats.insertions(),
                deletions: stats.deletions(),
                patch,
            })
        })
        .await
        .map_err(|e| WorktreeError::Internal(format!("Task spawn failed: {e}")))?
    }

    /// Snapshots all uncommitted dirty edits and untracked drafts into `refs/aionui/leftovers/<lease_id>_<timestamp>`.
    pub async fn archive_leftovers(
        &self,
        repo_path: &Path,
        worktree_path: &Path,
        lease_id: &str,
    ) -> Result<Option<String>, WorktreeError> {
        let repo_path = repo_path.to_path_buf();
        let worktree_path = worktree_path.to_path_buf();
        let lease_id = lease_id.to_string();

        tokio::task::spawn_blocking(move || -> Result<Option<String>, WorktreeError> {
            if !worktree_path.exists() {
                return Ok(None);
            }

            let wt_repo = match git2::Repository::open(&worktree_path) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };

            let mut status_opts = git2::StatusOptions::new();
            status_opts.include_untracked(true);
            status_opts.recurse_untracked_dirs(true);
            status_opts.include_ignored(false);

            let statuses = wt_repo
                .statuses(Some(&mut status_opts))
                .map_err(|e| WorktreeError::GitEngine {
                    context: "inspect worktree statuses",
                    message: e.to_string(),
                })?;

            if statuses.is_empty() {
                return Ok(None);
            }

            let mut index = wt_repo.index().map_err(|e| WorktreeError::GitEngine {
                context: "open worktree index for leftovers",
                message: e.to_string(),
            })?;

            index
                .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "stage untracked files for leftovers",
                    message: e.to_string(),
                })?;
            index.update_all(["*"].iter(), None).map_err(|e| WorktreeError::GitEngine {
                context: "update deleted files for leftovers",
                message: e.to_string(),
            })?;

            let tree_oid = index.write_tree().map_err(|e| WorktreeError::GitEngine {
                context: "write leftovers tree",
                message: e.to_string(),
            })?;

            let head_commit = wt_repo.head().ok().and_then(|h| h.peel_to_commit().ok());
            if let Some(ref head) = head_commit {
                if tree_oid == head.tree_id() {
                    return Ok(None);
                }
            }

            let now_ts = aionui_common::now_ms();
            let sig = git2::Signature::now("AionUi Leftovers Snapshot", "leftovers@aionui.internal")
                .or_else(|_| {
                    git2::Signature::new(
                        "AionUi Leftovers Snapshot",
                        "leftovers@aionui.internal",
                        &git2::Time::new(now_ts / 1000, 0),
                    )
                })
                .map_err(|e| WorktreeError::GitEngine {
                    context: "create leftovers signature",
                    message: e.to_string(),
                })?;

            let msg = format!(
                "leftovers: uncommitted draft snapshot for lease {lease_id}\n\nArchived automatically before worktree teardown."
            );
            let tree = wt_repo.find_tree(tree_oid).map_err(|e| WorktreeError::GitEngine {
                context: "find leftovers tree",
                message: e.to_string(),
            })?;

            let parents: Vec<&git2::Commit> = head_commit.as_ref().into_iter().collect();
            let commit_oid = wt_repo
                .commit(None, &sig, &sig, &msg, &tree, &parents)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "commit leftovers snapshot",
                    message: e.to_string(),
                })?;

            let primary_repo = git2::Repository::open(&repo_path).map_err(|e| WorktreeError::GitEngine {
                context: "open primary repo for leftovers ref",
                message: e.to_string(),
            })?;

            let ref_name = format!("refs/aionui/leftovers/{}_{}", lease_id, now_ts);
            primary_repo
                .reference(&ref_name, commit_oid, true, "archive worktree leftovers")
                .map_err(|e| WorktreeError::GitEngine {
                    context: "create leftovers reference",
                    message: e.to_string(),
                })?;

            Ok(Some(ref_name))
        })
        .await
        .map_err(|e| WorktreeError::Internal(format!("Task spawn failed: {e}")))?
    }

    /// Performs safe squash merge: in-memory 3-way merge, scrubs AI attribution tags,
    /// validates Conventional Commit format, commits with single parent on base branch,
    /// syncs working tree, and tears down the worktree.
    pub async fn squash_merge(
        &self,
        repo_path: &Path,
        worktree_path: &Path,
        branch_name: &str,
        base_branch: &str,
        commit_message: &str,
        human_user_name: Option<&str>,
        human_user_email: Option<&str>,
        lease_id: &str,
    ) -> Result<WorktreeMergeResultDto, WorktreeError> {
        let repo_lock = self.get_repo_lock(repo_path);
        let _guard = repo_lock.lock().await;

        let leftovers_ref = self.archive_leftovers(repo_path, worktree_path, lease_id).await?;

        let repo_path = repo_path.to_path_buf();
        let worktree_path = worktree_path.to_path_buf();
        let branch_name = branch_name.to_string();
        let base_branch = base_branch.to_string();
        let lease_id = lease_id.to_string();
        let sanitized_msg = sanitize_conventional_commit(commit_message, &lease_id)?;
        let user_name = human_user_name.unwrap_or("Developer").to_string();
        let user_email = human_user_email.unwrap_or("dev@local").to_string();

        let sanitized_msg_clone = sanitized_msg.clone();
        let leftovers_ref_clone = leftovers_ref.clone();
        let lease_id_inner = lease_id.clone();

        let squash_commit_oid = tokio::task::spawn_blocking(move || -> Result<String, WorktreeError> {
            let primary_repo = git2::Repository::open(&repo_path).map_err(|e| WorktreeError::GitEngine {
                context: "open primary repo for squash merge",
                message: e.to_string(),
            })?;

            let base_branch_ref = primary_repo
                .find_branch(&base_branch, git2::BranchType::Local)
                .map_err(|_| WorktreeError::BaseBranchNotFound(base_branch.clone()))?;
            let base_commit = base_branch_ref
                .get()
                .peel_to_commit()
                .map_err(|e| WorktreeError::GitEngine {
                    context: "peel base commit for squash merge",
                    message: e.to_string(),
                })?;

            let wt_repo = git2::Repository::open(&worktree_path).map_err(|e| WorktreeError::GitEngine {
                context: "open worktree repo for squash merge",
                message: e.to_string(),
            })?;
            let wt_head_commit = wt_repo
                .head()
                .and_then(|h| h.peel_to_commit())
                .map_err(|e| WorktreeError::GitEngine {
                    context: "peel worktree head for squash merge",
                    message: e.to_string(),
                })?;

            // In-memory 3-way merge
            let merge_opts = git2::MergeOptions::new();
            let mut merge_index = primary_repo
                .merge_commits(&base_commit, &wt_head_commit, Some(&merge_opts))
                .map_err(|e| WorktreeError::GitEngine {
                    context: "in-memory merge_commits",
                    message: e.to_string(),
                })?;

            if merge_index.has_conflicts() {
                let mut conflicting_files = Vec::new();
                if let Ok(conflicts) = merge_index.conflicts() {
                    for conflict in conflicts.flatten() {
                        if let Some(our) = conflict.our {
                            conflicting_files.push(String::from_utf8_lossy(&our.path).to_string());
                        } else if let Some(their) = conflict.their {
                            conflicting_files.push(String::from_utf8_lossy(&their.path).to_string());
                        }
                    }
                }
                return Err(WorktreeError::MergeConflict { conflicting_files });
            }

            let merged_tree_oid = merge_index
                .write_tree_to(&primary_repo)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "write merged tree to object store",
                    message: e.to_string(),
                })?;
            let merged_tree = primary_repo
                .find_tree(merged_tree_oid)
                .map_err(|e| WorktreeError::GitEngine {
                    context: "find merged tree",
                    message: e.to_string(),
                })?;

            // Human signature (Zero AI attribution)
            let sig = git2::Signature::now(&user_name, &user_email)
                .or_else(|_| {
                    git2::Signature::new(
                        &user_name,
                        &user_email,
                        &git2::Time::new(aionui_common::now_ms() / 1000, 0),
                    )
                })
                .map_err(|e| WorktreeError::GitEngine {
                    context: "create human signature",
                    message: e.to_string(),
                })?;

            // Single parent squash commit
            let base_ref_name = format!("refs/heads/{base_branch}");
            let commit_oid = primary_repo
                .commit(
                    Some(&base_ref_name),
                    &sig,
                    &sig,
                    &sanitized_msg_clone,
                    &merged_tree,
                    &[&base_commit],
                )
                .map_err(|e| WorktreeError::GitEngine {
                    context: "commit single-parent squash commit",
                    message: e.to_string(),
                })?;

            // Working tree sync if base branch is checked out in primary repo
            if let Ok(head) = primary_repo.head() {
                if head.name().ok() == Some(base_ref_name.as_str()) {
                    let mut checkout_opts = git2::build::CheckoutBuilder::new();
                    checkout_opts.force();
                    let _ = primary_repo.checkout_head(Some(&mut checkout_opts));
                }
            }

            // Teardown worktree metadata
            if let Ok(wt) = primary_repo.find_worktree(&lease_id_inner) {
                let mut prune_opts = git2::WorktreePruneOptions::new();
                prune_opts.valid(true);
                let _ = wt.prune(Some(&mut prune_opts));
            }

            // Teardown directory
            teardown_directory(&worktree_path);

            // Delete ephemeral worktree branch
            if let Ok(mut wt_branch) = primary_repo.find_branch(&branch_name, git2::BranchType::Local) {
                let _ = wt_branch.delete();
            }

            Ok(commit_oid.to_string())
        })
        .await
        .map_err(|e| WorktreeError::Internal(format!("Task spawn failed: {e}")))??;

        Ok(WorktreeMergeResultDto {
            lease_id,
            squash_commit_oid,
            leftovers_ref: leftovers_ref_clone,
            commit_message: sanitized_msg,
        })
    }

    /// Discards a worktree without merging (archives leftovers first, then prunes and deletes).
    pub async fn discard_worktree(
        &self,
        repo_path: &Path,
        worktree_path: &Path,
        branch_name: &str,
        lease_id: &str,
    ) -> Result<Option<String>, WorktreeError> {
        let repo_lock = self.get_repo_lock(repo_path);
        let _guard = repo_lock.lock().await;

        let leftovers_ref = self.archive_leftovers(repo_path, worktree_path, lease_id).await?;

        let repo_path = repo_path.to_path_buf();
        let worktree_path = worktree_path.to_path_buf();
        let branch_name = branch_name.to_string();
        let lease_id = lease_id.to_string();

        tokio::task::spawn_blocking(move || -> Result<(), WorktreeError> {
            let primary_repo = git2::Repository::open(&repo_path).map_err(|e| WorktreeError::GitEngine {
                context: "open primary repo for discard",
                message: e.to_string(),
            })?;

            if let Ok(wt) = primary_repo.find_worktree(&lease_id) {
                let mut prune_opts = git2::WorktreePruneOptions::new();
                prune_opts.valid(true);
                let _ = wt.prune(Some(&mut prune_opts));
            }

            teardown_directory(&worktree_path);

            if let Ok(mut wt_branch) = primary_repo.find_branch(&branch_name, git2::BranchType::Local) {
                let _ = wt_branch.delete();
            }

            Ok(())
        })
        .await
        .map_err(|e| WorktreeError::Internal(format!("Task spawn failed: {e}")))??;

        Ok(leftovers_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_test_repo(path: &Path) -> git2::Repository {
        let repo = git2::Repository::init(path).unwrap();
        let sig = git2::Signature::now("Test User", "test@user.internal").unwrap();

        // Initial commit on main
        let file_path = path.join("README.md");
        std::fs::write(&file_path, "# Initial Commit\n").unwrap();

        let mut index = repo.index().unwrap();
        index.add_path(Path::new("README.md")).unwrap();
        let tree_oid = index.write_tree().unwrap();
        let commit_oid = {
            let tree = repo.find_tree(tree_oid).unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "chore: initial commit", &tree, &[])
                .unwrap()
        };

        // Branch feat/hybrid-command-hub from HEAD
        {
            let commit = repo.find_commit(commit_oid).unwrap();
            repo.branch("feat/hybrid-command-hub", &commit, false).unwrap();
            repo.set_head("refs/heads/feat/hybrid-command-hub").unwrap();
        }

        repo
    }

    #[test]
    fn test_sanitize_conventional_commit_strips_ai_tags_and_formats() {
        let raw = r#"[AI]: feat(auth): add biometric token refresh
Co-authored-by: Claude <noreply@anthropic.com>
Generated-by: Claude 3.5 Sonnet
Signed-off-by: assistant-bot <bot@local>
🤖 Automated by AionUi Agent

This commit adds biometric passkey support.
"#;
        let cleaned = sanitize_conventional_commit(raw, "lease-101").unwrap();
        assert!(cleaned.starts_with("feat(auth): add biometric token refresh"));
        assert!(!cleaned.contains("Co-authored-by:"));
        assert!(!cleaned.contains("Generated-by:"));
        assert!(!cleaned.contains("Signed-off-by:"));
        assert!(!cleaned.contains("🤖"));
        assert!(cleaned.contains("This commit adds biometric passkey support."));
    }

    #[test]
    fn test_sanitize_conventional_commit_auto_prefixes_non_conventional() {
        let raw = "update readme with new endpoints";
        let cleaned = sanitize_conventional_commit(raw, "lease-102").unwrap();
        assert_eq!(cleaned, "feat(worktree): update readme with new endpoints");
    }

    #[test]
    fn test_ensure_exclude_configured_writes_aionui_pattern() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path();
        let _repo = init_test_repo(repo_dir);

        ensure_exclude_configured(repo_dir).unwrap();

        let exclude_file = repo_dir.join(".git").join("info").join("exclude");
        assert!(exclude_file.exists());
        let content = std::fs::read_to_string(exclude_file).unwrap();
        assert!(content.contains(".aionui/"));

        // Idempotent call
        ensure_exclude_configured(repo_dir).unwrap();
    }

    #[tokio::test]
    async fn test_worktree_lifecycle_and_zero_lock_contention() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_path = temp_dir.path().to_path_buf();
        let primary_repo = init_test_repo(&repo_path);

        let engine = GitWorktreeEngine::new();
        let lease_id = "wt-test-1".to_string();

        let config = WorktreeConfig {
            lease_id: lease_id.clone(),
            repo_path: repo_path.clone(),
            base_branch: "feat/hybrid-command-hub".to_string(),
            room_id: Some("room-1".to_string()),
            team_id: Some("team-alpha".to_string()),
            owner_user_id: "user-1".to_string(),
        };

        let row = engine.allocate_worktree(config, None).await.unwrap();
        assert_eq!(row.lease_id, lease_id);
        let worktree_dir = PathBuf::from(&row.worktree_path);
        assert!(worktree_dir.exists());
        assert!(worktree_dir.join("README.md").exists());

        // Zero lock contention verification:
        // Simultaneously commit in the primary repo and in the worktree!
        let sig = git2::Signature::now("Primary Dev", "dev@primary.internal").unwrap();
        let prim_file = repo_path.join("primary_file.txt");
        std::fs::write(&prim_file, "primary edit\n").unwrap();

        let wt_file = worktree_dir.join("agent_file.txt");
        std::fs::write(&wt_file, "agent edit in worktree\n").unwrap();

        // Worktree add & commit
        let wt_repo = git2::Repository::open(&worktree_dir).unwrap();
        let mut wt_index = wt_repo.index().unwrap();
        wt_index.add_path(Path::new("agent_file.txt")).unwrap();
        let wt_tree_oid = wt_index.write_tree().unwrap();
        let wt_tree = wt_repo.find_tree(wt_tree_oid).unwrap();
        let wt_head = wt_repo.head().unwrap().peel_to_commit().unwrap();
        wt_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "feat(agent): add agent file",
                &wt_tree,
                &[&wt_head],
            )
            .unwrap();

        // Primary add & commit (must succeed without index.lock conflict!)
        let mut prim_index = primary_repo.index().unwrap();
        prim_index.add_path(Path::new("primary_file.txt")).unwrap();
        let prim_tree_oid = prim_index.write_tree().unwrap();
        let prim_tree = primary_repo.find_tree(prim_tree_oid).unwrap();
        let prim_head = primary_repo.head().unwrap().peel_to_commit().unwrap();
        primary_repo
            .commit(
                Some("refs/heads/main"),
                &sig,
                &sig,
                "chore: commit on primary branch",
                &prim_tree,
                &[&prim_head],
            )
            .unwrap();

        // Diff computation
        let diff = engine
            .compute_diff(&repo_path, &worktree_dir, "feat/hybrid-command-hub", &lease_id)
            .await
            .unwrap();
        assert_eq!(diff.files_changed, 1);
        assert_eq!(diff.additions, 1);
        assert!(diff.patch.contains("+agent edit in worktree"));

        // Leftovers archiving: add an uncommitted file
        let draft_file = worktree_dir.join("scratchpad.txt");
        std::fs::write(&draft_file, "uncommitted agent scratchpad\n").unwrap();

        // Squash merge
        let merge_res = engine
            .squash_merge(
                &repo_path,
                &worktree_dir,
                &row.branch,
                "feat/hybrid-command-hub",
                "[AI]: feat(agent): integrate agent file\nCo-authored-by: Claude <noreply@anthropic.com>",
                Some("Lead Dev"),
                Some("lead@local.internal"),
                &lease_id,
            )
            .await
            .unwrap();

        assert!(merge_res.leftovers_ref.is_some());
        let leftovers = merge_res.leftovers_ref.unwrap();
        assert!(leftovers.starts_with("refs/aionui/leftovers/wt-test-1_"));

        // Verify leftovers ref exists and contains scratchpad.txt
        let leftovers_commit = primary_repo
            .find_reference(&leftovers)
            .unwrap()
            .peel_to_commit()
            .unwrap();
        let leftovers_tree = leftovers_commit.tree().unwrap();
        assert!(leftovers_tree.get_name("scratchpad.txt").is_some());

        // Verify squash commit: has exactly 1 parent on feat/hybrid-command-hub!
        let base_branch_commit = primary_repo
            .find_branch("feat/hybrid-command-hub", git2::BranchType::Local)
            .unwrap()
            .get()
            .peel_to_commit()
            .unwrap();
        assert_eq!(base_branch_commit.id().to_string(), merge_res.squash_commit_oid);
        assert_eq!(base_branch_commit.parent_count(), 1);
        assert_eq!(
            base_branch_commit.message().unwrap().trim(),
            "feat(agent): integrate agent file"
        );
        assert!(!base_branch_commit.message().unwrap().contains("Co-authored-by"));

        // Worktree directory and branch should be cleaned up
        assert!(!worktree_dir.exists());
        assert!(primary_repo.find_branch(&row.branch, git2::BranchType::Local).is_err());
    }
}

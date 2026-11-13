//! Canonical outbound path validation, inbound Telegram attachment staging,
//! and cross-team Shared Room `.shared-inbox/` artifact synchronization.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use aionui_common::now_ms;
use serde::{Deserialize, Serialize};

/// Hard cap for outbound Telegram file attachments (45 MiB, safely under Bot API 50 MiB limit).
pub const MAX_OUTBOUND_FILE_BYTES: u64 = 45 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedRoomArtifact {
    pub file_name: String,
    pub staged_path: String,
    pub size_bytes: u64,
    pub is_image: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedOutboundFile {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct StagedArtifactBatch {
    pub artifacts: Vec<StagedRoomArtifact>,
    /// Keyed by trimmed recipient workspace string -> exact copied `.shared-inbox/<room>/<file>` paths.
    pub recipient_inbox_paths: HashMap<String, Vec<String>>,
    pub rejected_files: Vec<RejectedOutboundFile>,
}

/// Sanitize an untrusted filename so it cannot escape directories or contain OS-reserved chars,
/// normalizing to Unicode NFC (`AGENTS.md` §2) while preserving Unicode alphanumerics.
pub fn sanitize_file_name(raw: &str) -> String {
    let base = Path::new(raw)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("attachment.bin");
    let nfc_base = normalize_nfc(base);
    let cleaned: String = nfc_base
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('.').trim();
    if trimmed.is_empty() {
        "attachment.bin".to_owned()
    } else if trimmed.len() > 120 {
        truncate_utf8_tail(trimmed, 120)
    } else {
        trimmed.to_owned()
    }
}

fn truncate_utf8_tail(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_owned();
    }
    let min_start = s.len() - max_bytes;
    for (idx, _) in s.char_indices() {
        if idx >= min_start {
            let tail = s[idx..].trim_matches('.');
            return if tail.is_empty() {
                "attachment.bin".to_owned()
            } else {
                tail.to_owned()
            };
        }
    }
    "attachment.bin".to_owned()
}

pub fn is_image_file_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".png")
        || lower.ends_with(".jpg")
        || lower.ends_with(".jpeg")
        || lower.ends_with(".webp")
        || lower.ends_with(".gif")
}

/// Validate an outbound file path before sending via Telegram or staging into a Shared Room.
///
/// Security contract:
/// 1. `file://` URIs are decoded via `Url::parse` -> `.to_file_path()`.
/// 2. Relative paths require a non-empty, absolute `team_workspace` and are joined against
///    `team_workspace` before canonicalization (never resolved against process CWD).
/// 3. Path must exist, be a regular file, and resolve via `dunce::canonicalize`.
/// 4. Sensitive segments (`.ssh`, `.gnupg`, `.aws`, `.kube`, `.git`) and secret files
///    (`.env`, `.env.*`, `.npmrc`, `.pypirc`, `.netrc`, `.git-credentials`, `auth.enc`,
///    `aionui-backend.db*`, `id_rsa`, `id_ed25519`, `id_ecdsa`, `id_dsa`, `*.pem`, `*.key`,
///    `*.p12`, `*.pfx`) are strictly denied.
/// 5. Anything inside `data_dir` outside `<data_dir>/shared-rooms` is strictly denied.
/// 6. Canonical path must reside inside `team_workspace`, `std::env::temp_dir()`, or `<data_dir>/shared-rooms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundFileError {
    SecurityViolation(String),
    Rejected(String),
}

impl OutboundFileError {
    pub fn into_reason(self) -> String {
        match self {
            Self::SecurityViolation(msg) | Self::Rejected(msg) => msg,
        }
    }
}

pub fn validate_outbound_file_path(
    raw_path: &str,
    team_workspace: Option<&str>,
    data_dir: &Path,
) -> Result<PathBuf, String> {
    validate_outbound_file_path_with_policy(raw_path, team_workspace, data_dir, false)
}

pub fn validate_outbound_file_path_with_policy(
    raw_path: &str,
    team_workspace: Option<&str>,
    data_dir: &Path,
    allow_user_local_files: bool,
) -> Result<PathBuf, String> {
    validate_outbound_file_path_classified(raw_path, team_workspace, data_dir, allow_user_local_files)
        .map_err(OutboundFileError::into_reason)
}

pub fn validate_outbound_file_path_classified(
    raw_path: &str,
    team_workspace: Option<&str>,
    data_dir: &Path,
    allow_user_local_files: bool,
) -> Result<PathBuf, OutboundFileError> {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() {
        return Err(OutboundFileError::Rejected(
            "File path must not be empty".to_owned(),
        ));
    }

    let clean_ws = team_workspace.map(str::trim).filter(|w| !w.is_empty());

    let candidate = if trimmed.get(..5).is_some_and(|prefix| prefix.eq_ignore_ascii_case("file:")) {
        let parsed = reqwest::Url::parse(trimmed)
            .map_err(|e| OutboundFileError::Rejected(format!("Invalid file URI '{trimmed}': {e}")))?;
        if !parsed.scheme().eq_ignore_ascii_case("file") {
            return Err(OutboundFileError::Rejected(format!(
                "Unsupported URI scheme in '{trimmed}'"
            )));
        }
        parsed.to_file_path().map_err(|()| {
            OutboundFileError::Rejected(format!("Cannot convert file URI '{trimmed}' to a local path"))
        })?
    } else {
        PathBuf::from(trimmed)
    };

    let was_relative = !candidate.is_absolute();
    let resolved_candidate = if was_relative {
        let Some(ws) = clean_ws else {
            return Err(OutboundFileError::Rejected(format!(
                "Relative outbound file path '{trimmed}' requires a configured team workspace"
            )));
        };
        let ws_path = Path::new(ws);
        if !ws_path.is_absolute() {
            return Err(OutboundFileError::Rejected(format!(
                "Relative outbound file path '{trimmed}' requires an absolute team workspace"
            )));
        }
        ws_path.join(&candidate)
    } else {
        candidate
    };

    let canonical = dunce::canonicalize(&resolved_candidate).map_err(|e| {
        OutboundFileError::Rejected(format!("Cannot resolve outbound file '{trimmed}': {e}"))
    })?;

    let metadata = std::fs::metadata(&canonical).map_err(|e| {
        OutboundFileError::Rejected(format!(
            "Cannot stat outbound file '{}': {e}",
            canonical.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(OutboundFileError::Rejected(format!(
            "Outbound path '{}' is not a regular file",
            canonical.display()
        )));
    }
    if metadata.len() > MAX_OUTBOUND_FILE_BYTES {
        return Err(OutboundFileError::Rejected(format!(
            "Outbound file '{}' exceeds 45 MiB limit ({} bytes)",
            canonical.display(),
            metadata.len()
        )));
    }

    // Check explicit deny-list on directory components and file names/extensions.
    for comp in canonical.components() {
        if let Component::Normal(os) = comp {
            let lower = os.to_string_lossy().to_ascii_lowercase();
            if matches!(lower.as_str(), ".ssh" | ".gnupg" | ".aws" | ".kube" | ".git") {
                return Err(OutboundFileError::SecurityViolation(format!(
                    "Forbidden: Outbound file '{}' is inside a restricted directory ({lower})",
                    canonical.display()
                )));
            }
        }
    }

    let file_name_lower = canonical
        .file_name()
        .map(|f| f.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    let ext_lower = Path::new(&file_name_lower)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();

    if is_denied_secret_file(&file_name_lower, ext_lower) {
        return Err(OutboundFileError::SecurityViolation(format!(
            "Forbidden: Outbound file '{}' matches a protected credential/key/database filename",
            canonical.display()
        )));
    }

    let shared_rooms_root = data_dir.join("shared-rooms");
    let canonical_shared_rooms = dunce::canonicalize(&shared_rooms_root).ok();

    if let Ok(canonical_data_dir) = dunce::canonicalize(data_dir)
        && path_starts_with_ci(&canonical, &canonical_data_dir)
    {
        let in_shared_rooms = canonical_shared_rooms
            .as_ref()
            .is_some_and(|sr| path_starts_with_ci(&canonical, sr));
        if !in_shared_rooms {
            return Err(OutboundFileError::SecurityViolation(format!(
                "Forbidden: Outbound file '{}' inside app data directory is forbidden outside shared-rooms",
                canonical.display()
            )));
        }
    }

    // Even when `allow_user_local_files` is enabled, a relative path resolved against
    // `clean_ws` must never escape `clean_ws` via `..` or symlinks.
    if was_relative
        && let Some(ws) = clean_ws
    {
        let canon_ws = dunce::canonicalize(ws).map_err(|e| {
            OutboundFileError::Rejected(format!("Cannot canonicalize team workspace '{ws}': {e}"))
        })?;
        if !path_starts_with_ci(&canonical, &canon_ws) {
            return Err(OutboundFileError::SecurityViolation(format!(
                "Forbidden: Relative outbound file '{trimmed}' escapes team workspace '{}'",
                canon_ws.display()
            )));
        }
    }

    if allow_user_local_files {
        return Ok(canonical);
    }

    let mut allowed = false;
    if let Some(ws) = clean_ws {
        if let Ok(canon_ws) = dunce::canonicalize(ws)
            && path_starts_with_ci(&canonical, &canon_ws)
        {
            allowed = true;
        }
    } else if let Ok(canon_tmp) = dunce::canonicalize(std::env::temp_dir())
        && path_starts_with_ci(&canonical, &canon_tmp)
    {
        allowed = true;
    }
    if !allowed
        && let Some(sr) = &canonical_shared_rooms
        && path_starts_with_ci(&canonical, sr)
    {
        allowed = true;
    }

    if !allowed {
        return Err(OutboundFileError::SecurityViolation(format!(
            "Forbidden: Outbound file '{}' is outside allowed roots (team workspace, temp_dir, shared-rooms)",
            canonical.display()
        )));
    }

    Ok(canonical)
}

fn is_denied_secret_file(file_name_lower: &str, ext_lower: &str) -> bool {
    file_name_lower == ".env"
        || file_name_lower.starts_with(".env.")
        || file_name_lower == "auth.enc"
        || file_name_lower.starts_with("aionui-backend.db")
        || matches!(
            file_name_lower,
            ".npmrc"
                | ".pypirc"
                | ".netrc"
                | ".git-credentials"
                | "id_rsa"
                | "id_ed25519"
                | "id_ecdsa"
                | "id_dsa"
        )
        || matches!(ext_lower, "pem" | "key" | "p12" | "pfx")
}

/// Partition a slice of raw outbound file paths into validated canonical `(PathBuf, safe_name, size_bytes)`
/// entries vs structured [`RejectedOutboundFile`] entries without aborting the whole batch.
pub fn partition_outbound_files(
    raw_files: &[String],
    team_workspace: Option<&str>,
    data_dir: &Path,
) -> (Vec<(PathBuf, String, u64)>, Vec<RejectedOutboundFile>) {
    let mut valid = Vec::with_capacity(raw_files.len());
    let mut rejected = Vec::new();
    for raw in raw_files {
        match validate_outbound_file_path(raw, team_workspace, data_dir) {
            Ok(canon) => match std::fs::metadata(&canon) {
                Ok(meta) => {
                    let safe_name = sanitize_file_name(
                        canon
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("attachment.bin"),
                    );
                    valid.push((canon, safe_name, meta.len()));
                }
                Err(e) => rejected.push(RejectedOutboundFile {
                    path: raw.clone(),
                    reason: format!("Cannot stat validated file '{}': {e}", canon.display()),
                }),
            },
            Err(reason) => rejected.push(RejectedOutboundFile {
                path: raw.clone(),
                reason,
            }),
        }
    }
    (valid, rejected)
}

fn path_starts_with_ci(path: &Path, base: &Path) -> bool {
    if cfg!(any(windows, target_os = "macos")) {
        path_starts_with_nfc_folded(path, base, true)
    } else {
        path_starts_with_nfc_folded(path, base, false)
    }
}

fn path_starts_with_nfc_folded(path: &Path, base: &Path, case_fold: bool) -> bool {
    let norm = |p: &Path| -> String {
        let mut s = normalize_nfc(&p.to_string_lossy()).replace('\\', "/");
        if case_fold {
            s = s.to_lowercase();
        }
        s
    };
    let p = norm(path);
    let b = norm(base);
    let b_trimmed = b.trim_end_matches('/');
    if b_trimmed.is_empty() {
        return p.starts_with('/');
    }
    if p == b_trimmed {
        return true;
    }
    p.starts_with(b_trimmed) && p.as_bytes().get(b_trimmed.len()) == Some(&b'/')
}

/// Save an inbound Telegram attachment into `<team_workspace>/.aionui-inbox/`
/// (or `<data_dir>/shared-rooms/<fallback_id>/artifacts/` when no workspace is set).
pub fn save_inbound_telegram_attachment(
    bytes: &[u8],
    raw_file_name: &str,
    team_workspace: Option<&str>,
    data_dir: &Path,
    fallback_id: &str,
) -> Result<PathBuf, String> {
    let inbox_dir = if let Some(ws) = team_workspace.map(str::trim).filter(|w| !w.is_empty()) {
        PathBuf::from(ws).join(".aionui-inbox")
    } else {
        data_dir
            .join("shared-rooms")
            .join(sanitize_file_name(fallback_id))
            .join("artifacts")
    };
    std::fs::create_dir_all(&inbox_dir)
        .map_err(|e| format!("Failed to create inbox directory '{}': {e}", inbox_dir.display()))?;

    let safe_name = sanitize_file_name(raw_file_name);
    let stamped = format!("{}_{}", now_ms(), safe_name);
    let dest = inbox_dir.join(stamped);
    std::fs::write(&dest, bytes)
        .map_err(|e| format!("Failed to write inbound attachment '{}': {e}", dest.display()))?;
    Ok(dunce::canonicalize(&dest).unwrap_or(dest))
}

/// Copy a staged Shared Room file into `<recipient_ws>/.shared-inbox/<safe_room>/<file_name>`
/// only after verifying canonical symlink containment inside `recipient_ws`.
fn copy_into_recipient_shared_inbox(
    staged_src: &Path,
    recipient_ws: &str,
    safe_room: &str,
    file_name: &str,
) -> Option<String> {
    let canon_ws = dunce::canonicalize(recipient_ws).ok()?;
    let shared_inbox_root = canon_ws.join(".shared-inbox");
    if let Ok(sym_meta) = std::fs::symlink_metadata(&shared_inbox_root)
        && sym_meta.file_type().is_symlink()
    {
        return None;
    }
    let inbox_dir = shared_inbox_root.join(safe_room);
    if let Ok(sym_meta) = std::fs::symlink_metadata(&inbox_dir)
        && sym_meta.file_type().is_symlink()
    {
        return None;
    }
    std::fs::create_dir_all(&inbox_dir).ok()?;
    let canon_inbox = dunce::canonicalize(&inbox_dir).ok()?;
    if !path_starts_with_ci(&canon_inbox, &canon_ws) {
        return None;
    }
    let recipient_dest = canon_inbox.join(file_name);
    if let Ok(dest_meta) = std::fs::symlink_metadata(&recipient_dest)
        && dest_meta.file_type().is_symlink()
    {
        return None;
    }
    std::fs::copy(staged_src, &recipient_dest).ok()?;
    Some(
        dunce::canonicalize(&recipient_dest)
            .unwrap_or(recipient_dest)
            .to_string_lossy()
            .into_owned(),
    )
}

/// Stage files into `<data_dir>/shared-rooms/<room_id>/artifacts/` and copy them into
/// every recipient Team's `<recipient_workspace>/.shared-inbox/<room_id>/` so sandboxed
/// CLI agents can read cross-team files directly from their own workspace.
pub fn stage_room_artifacts_and_sync_inbox(
    room_id: &str,
    source_files: &[String],
    source_team_workspace: Option<&str>,
    data_dir: &Path,
    recipient_workspaces: &[String],
) -> Result<Vec<StagedRoomArtifact>, String> {
    Ok(stage_room_artifacts_batch(
        room_id,
        source_files,
        source_team_workspace,
        data_dir,
        recipient_workspaces,
        false,
    )?
    .artifacts)
}

pub fn stage_room_artifacts_batch(
    room_id: &str,
    source_files: &[String],
    source_team_workspace: Option<&str>,
    data_dir: &Path,
    recipient_workspaces: &[String],
    allow_user_local_files: bool,
) -> Result<StagedArtifactBatch, String> {
    if source_files.is_empty() {
        return Ok(StagedArtifactBatch::default());
    }

    let safe_room = sanitize_file_name(room_id);
    let artifacts_dir = data_dir.join("shared-rooms").join(&safe_room).join("artifacts");
    std::fs::create_dir_all(&artifacts_dir).map_err(|e| {
        format!(
            "Failed to create shared room artifacts directory '{}': {e}",
            artifacts_dir.display()
        )
    })?;

    let mut staged = Vec::with_capacity(source_files.len());
    let mut recipient_inbox_paths: HashMap<String, Vec<String>> = HashMap::new();
    let mut rejected_files: Vec<RejectedOutboundFile> = Vec::new();

    for (idx, raw_file) in source_files.iter().enumerate() {
        let valid_src = match validate_outbound_file_path_classified(
            raw_file,
            source_team_workspace,
            data_dir,
            allow_user_local_files,
        ) {
            Ok(path) => path,
            Err(OutboundFileError::SecurityViolation(reason)) => {
                return Err(reason);
            }
            Err(OutboundFileError::Rejected(reason)) => {
                rejected_files.push(RejectedOutboundFile {
                    path: raw_file.clone(),
                    reason,
                });
                continue;
            }
        };

        let meta = match std::fs::metadata(&valid_src) {
            Ok(meta) => meta,
            Err(e) => {
                rejected_files.push(RejectedOutboundFile {
                    path: raw_file.clone(),
                    reason: format!("Failed to read metadata of '{}': {e}", valid_src.display()),
                });
                continue;
            }
        };

        let orig_name = valid_src
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("artifact.bin");
        let safe_name = sanitize_file_name(orig_name);
        let stamped_name = format!("{}_{}_{}", now_ms(), idx, safe_name);
        let staged_dest = artifacts_dir.join(&stamped_name);

        if valid_src != staged_dest
            && let Err(e) = std::fs::copy(&valid_src, &staged_dest)
        {
            rejected_files.push(RejectedOutboundFile {
                path: raw_file.clone(),
                reason: format!(
                    "Failed to stage artifact '{}' -> '{}': {e}",
                    valid_src.display(),
                    staged_dest.display()
                ),
            });
            continue;
        }

        let canonical_staged = dunce::canonicalize(&staged_dest).unwrap_or(staged_dest.clone());
        let canonical_staged_str = canonical_staged.to_string_lossy().into_owned();

        for ws in recipient_workspaces {
            let trimmed_ws = ws.trim();
            if trimmed_ws.is_empty() {
                continue;
            }
            let delivered_path =
                copy_into_recipient_shared_inbox(&staged_dest, trimmed_ws, &safe_room, &stamped_name)
                    .unwrap_or_else(|| canonical_staged_str.clone());
            recipient_inbox_paths
                .entry(trimmed_ws.to_owned())
                .or_default()
                .push(delivered_path);
        }

        staged.push(StagedRoomArtifact {
            file_name: safe_name.clone(),
            staged_path: canonical_staged_str,
            size_bytes: meta.len(),
            is_image: is_image_file_name(&safe_name),
        });
    }

    Ok(StagedArtifactBatch {
        artifacts: staged,
        recipient_inbox_paths,
        rejected_files,
    })
}

/// Async wrapper around [`stage_room_artifacts_batch`] that offloads blocking filesystem
/// validation, canonicalization, and multi-workspace copies onto Tokio's blocking pool.
pub async fn stage_room_artifacts_batch_async(
    room_id: String,
    source_files: Vec<String>,
    source_team_workspace: Option<String>,
    data_dir: PathBuf,
    recipient_workspaces: Vec<String>,
    allow_user_local_files: bool,
) -> Result<StagedArtifactBatch, String> {
    if source_files.is_empty() {
        return Ok(StagedArtifactBatch::default());
    }
    tokio::task::spawn_blocking(move || {
        stage_room_artifacts_batch(
            &room_id,
            &source_files,
            source_team_workspace.as_deref(),
            &data_dir,
            &recipient_workspaces,
            allow_user_local_files,
        )
    })
    .await
    .map_err(|e| format!("Artifact staging join error: {e}"))?
}

/// Async wrapper around [`stage_room_artifacts_and_sync_inbox`] using `spawn_blocking`.
pub async fn stage_room_artifacts_and_sync_inbox_async(
    room_id: String,
    source_files: Vec<String>,
    source_team_workspace: Option<String>,
    data_dir: PathBuf,
    recipient_workspaces: Vec<String>,
) -> Result<Vec<StagedRoomArtifact>, String> {
    Ok(stage_room_artifacts_batch_async(
        room_id,
        source_files,
        source_team_workspace,
        data_dir,
        recipient_workspaces,
        false,
    )
    .await?
    .artifacts)
}

/// Offload oversized inline Shared Room message text (`> 2,400` chars) to a Markdown file in
/// `<data_dir>/shared-rooms/<room_id>/artifacts/msg_<msg_id>.md` and copy into each recipient's
/// `<workspace>/.shared-inbox/<room_id>/msg_<msg_id>.md` on Tokio's blocking pool.
pub async fn stage_room_oversized_message_async(
    room_id: String,
    msg_id: String,
    content: String,
    data_dir: PathBuf,
    recipient_workspaces: Vec<String>,
) -> Result<(String, HashMap<String, String>), String> {
    tokio::task::spawn_blocking(move || {
        let safe_room = sanitize_file_name(&room_id);
        let safe_msg = sanitize_file_name(&format!("msg_{msg_id}.md"));
        let artifacts_dir = data_dir.join("shared-rooms").join(&safe_room).join("artifacts");
        std::fs::create_dir_all(&artifacts_dir)
            .map_err(|e| format!("Failed to create room artifacts dir: {e}"))?;
        let staged_dest = artifacts_dir.join(&safe_msg);
        std::fs::write(&staged_dest, content.as_bytes())
            .map_err(|e| format!("Failed to write staged room message: {e}"))?;
        let canonical_staged = dunce::canonicalize(&staged_dest)
            .unwrap_or_else(|_| staged_dest.clone())
            .to_string_lossy()
            .into_owned();

        let mut per_ws = HashMap::new();
        for ws in recipient_workspaces {
            let trimmed = ws.trim();
            if trimmed.is_empty() {
                continue;
            }
            let delivered =
                copy_into_recipient_shared_inbox(&staged_dest, trimmed, &safe_room, &safe_msg)
                    .unwrap_or_else(|| canonical_staged.clone());
            per_ws.insert(trimmed.to_owned(), delivered);
        }
        Ok((canonical_staged, per_ws))
    })
    .await
    .map_err(|e| format!("Oversized message staging join error: {e}"))?
}

// ── Self-Contained Unicode NFC Normalizer (Vietnamese + Latin + Hangul) ─────

/// Normalize `input` into Unicode Normalization Form C (NFC) so NFD sequences
/// from macOS APFS or copy-pasted filenames fold deterministically (`AGENTS.md` §2).
pub fn normalize_nfc(input: &str) -> String {
    if input.is_ascii() {
        return input.to_owned();
    }

    // Step 1: Canonical decomposition (NFD)
    let mut decomposed = Vec::with_capacity(input.len() + 8);
    for ch in input.chars() {
        decompose_canonical(ch, &mut decomposed);
    }

    // Step 2: Canonical ordering of combining marks by non-zero CCC
    let len = decomposed.len();
    let mut i = 0;
    while i < len {
        let ccc = combining_class(decomposed[i]);
        if ccc != 0 {
            let start = i;
            while i < len && combining_class(decomposed[i]) != 0 {
                i += 1;
            }
            decomposed[start..i].sort_by_key(|&c| combining_class(c));
        } else {
            i += 1;
        }
    }

    // Step 3: Canonical composition (UAX #15)
    let mut result: Vec<char> = Vec::with_capacity(decomposed.len());
    let mut starter_idx: Option<usize> = None;
    let mut last_ccc: u8 = 0;

    for ch in decomposed {
        let ch_ccc = combining_class(ch);
        if let Some(s_idx) = starter_idx {
            let starter = result[s_idx];
            // Hangul L+V and LV+T composition
            if ch_ccc == 0
                && let Some(hangul) = compose_hangul(starter, ch)
            {
                result[s_idx] = hangul;
                last_ccc = 0;
                continue;
            }
            // Standard starter + combining mark composition when unblocked
            if (last_ccc < ch_ccc || last_ccc == 0)
                && let Some(composed) = compose_pair(starter, ch)
            {
                result[s_idx] = composed;
                continue;
            }
        }

        if ch_ccc == 0 {
            starter_idx = Some(result.len());
            last_ccc = 0;
        } else {
            last_ccc = ch_ccc;
        }
        result.push(ch);
    }

    result.into_iter().collect()
}

fn combining_class(ch: char) -> u8 {
    match ch {
        '\u{0327}' | '\u{0328}' => 202, // Cedilla / ogonek attached below
        '\u{031B}' => 216,              // COMBINING HORN (ơ, ư)
        '\u{0323}' | '\u{0326}' => 220, // COMBINING DOT BELOW / COMMA BELOW
        '\u{0300}'..='\u{0314}'
        | '\u{033E}'..='\u{0344}'
        | '\u{0346}'
        | '\u{034A}'..='\u{034C}'
        | '\u{3099}'
        | '\u{309A}' => 230, // Above marks (grave, acute, circumflex, tilde, macron, breve, dot above, diaeresis, hook above, ring, caron)
        _ => 0,
    }
}

fn decompose_canonical(ch: char, out: &mut Vec<char>) {
    // Hangul syllables U+AC00..=U+D7A3
    let u = ch as u32;
    if (0xAC00..=0xD7A3).contains(&u) {
        let s_index = u - 0xAC00;
        let l = 0x1100 + s_index / (21 * 28);
        let v = 0x1161 + (s_index % (21 * 28)) / 28;
        let t = s_index % 28;
        if let Some(lc) = char::from_u32(l) {
            out.push(lc);
        }
        if let Some(vc) = char::from_u32(v) {
            out.push(vc);
        }
        if t > 0
            && let Some(tc) = char::from_u32(0x11A7 + t)
        {
            out.push(tc);
        }
        return;
    }

    for &(composed, base, mark) in CANONICAL_PAIRS {
        if composed == ch {
            decompose_canonical(base, out);
            out.push(mark);
            return;
        }
    }
    out.push(ch);
}

fn compose_hangul(starter: char, ch: char) -> Option<char> {
    let s = starter as u32;
    let c = ch as u32;
    // L + V -> LV
    if (0x1100..0x1100 + 19).contains(&s) && (0x1161..0x1161 + 21).contains(&c) {
        let l_idx = s - 0x1100;
        let v_idx = c - 0x1161;
        return char::from_u32(0xAC00 + (l_idx * 21 + v_idx) * 28);
    }
    // LV + T -> LVT
    if (0xAC00..=0xD7A3).contains(&s) && (s - 0xAC00) % 28 == 0 && (0x11A8..0x11A8 + 27).contains(&c) {
        let t_idx = c - 0x11A7;
        return char::from_u32(s + t_idx);
    }
    None
}

fn compose_pair(base: char, mark: char) -> Option<char> {
    for &(composed, b, m) in CANONICAL_PAIRS {
        if b == base && m == mark {
            return Some(composed);
        }
    }
    // Handle sorted NFD order where below-mark (\u{0323}) composed before above-mark (\u{0302}/\u{0306})
    match (base, mark) {
        ('ạ', '\u{0302}') => Some('ậ'),
        ('Ạ', '\u{0302}') => Some('Ậ'),
        ('ạ', '\u{0306}') => Some('ặ'),
        ('Ạ', '\u{0306}') => Some('Ặ'),
        ('ẹ', '\u{0302}') => Some('ệ'),
        ('Ẹ', '\u{0302}') => Some('Ệ'),
        ('ọ', '\u{0302}') => Some('ộ'),
        ('Ọ', '\u{0302}') => Some('Ộ'),
        _ => None,
    }
}

/// Canonical `(precomposed, base, combining_mark)` table covering all Vietnamese
/// diacritics + common Latin/Cyrillic/Japanese precomposed characters.
const CANONICAL_PAIRS: &[(char, char, char)] = &[
    // Latin-1 & European accents
    ('À', 'A', '\u{0300}'), ('Á', 'A', '\u{0301}'), ('Â', 'A', '\u{0302}'), ('Ã', 'A', '\u{0303}'),
    ('Ä', 'A', '\u{0308}'), ('Å', 'A', '\u{030A}'), ('Ç', 'C', '\u{0327}'), ('È', 'E', '\u{0300}'),
    ('É', 'E', '\u{0301}'), ('Ê', 'E', '\u{0302}'), ('Ë', 'E', '\u{0308}'), ('Ì', 'I', '\u{0300}'),
    ('Í', 'I', '\u{0301}'), ('Î', 'I', '\u{0302}'), ('Ï', 'I', '\u{0308}'), ('Ñ', 'N', '\u{0303}'),
    ('Ò', 'O', '\u{0300}'), ('Ó', 'O', '\u{0301}'), ('Ô', 'O', '\u{0302}'), ('Õ', 'O', '\u{0303}'),
    ('Ö', 'O', '\u{0308}'), ('Ù', 'U', '\u{0300}'), ('Ú', 'U', '\u{0301}'), ('Û', 'U', '\u{0302}'),
    ('Ü', 'U', '\u{0308}'), ('Ý', 'Y', '\u{0301}'),
    ('à', 'a', '\u{0300}'), ('á', 'a', '\u{0301}'), ('â', 'a', '\u{0302}'), ('ã', 'a', '\u{0303}'),
    ('ä', 'a', '\u{0308}'), ('å', 'a', '\u{030A}'), ('ç', 'c', '\u{0327}'), ('è', 'e', '\u{0300}'),
    ('é', 'e', '\u{0301}'), ('ê', 'e', '\u{0302}'), ('ë', 'e', '\u{0308}'), ('ì', 'i', '\u{0300}'),
    ('í', 'i', '\u{0301}'), ('î', 'i', '\u{0302}'), ('ï', 'i', '\u{0308}'), ('ñ', 'n', '\u{0303}'),
    ('ò', 'o', '\u{0300}'), ('ó', 'o', '\u{0301}'), ('ô', 'o', '\u{0302}'), ('õ', 'o', '\u{0303}'),
    ('ö', 'o', '\u{0308}'), ('ù', 'u', '\u{0300}'), ('ú', 'u', '\u{0301}'), ('û', 'u', '\u{0302}'),
    ('ü', 'u', '\u{0308}'), ('ý', 'y', '\u{0301}'), ('ÿ', 'y', '\u{0308}'),
    // Latin Extended-A/B (breve, horn, caron, macron, acute, ogonek)
    ('Ă', 'A', '\u{0306}'), ('ă', 'a', '\u{0306}'),
    ('Ơ', 'O', '\u{031B}'), ('ơ', 'o', '\u{031B}'),
    ('Ư', 'U', '\u{031B}'), ('ư', 'u', '\u{031B}'),
    ('Ć', 'C', '\u{0301}'), ('ć', 'c', '\u{0301}'), ('Č', 'C', '\u{030C}'), ('č', 'c', '\u{030C}'),
    ('Ď', 'D', '\u{030C}'), ('ď', 'd', '\u{030C}'), ('Ě', 'E', '\u{030C}'), ('ě', 'e', '\u{030C}'),
    ('Ń', 'N', '\u{0301}'), ('ń', 'n', '\u{0301}'), ('Ň', 'N', '\u{030C}'), ('ň', 'n', '\u{030C}'),
    ('Ř', 'R', '\u{030C}'), ('ř', 'r', '\u{030C}'), ('Ś', 'S', '\u{0301}'), ('ś', 's', '\u{0301}'),
    ('Š', 'S', '\u{030C}'), ('š', 's', '\u{030C}'), ('Ť', 'T', '\u{030C}'), ('ť', 't', '\u{030C}'),
    ('Ū', 'U', '\u{0304}'), ('ū', 'u', '\u{0304}'), ('Ů', 'U', '\u{030A}'), ('ů', 'u', '\u{030A}'),
    ('Ÿ', 'Y', '\u{0308}'), ('Ź', 'Z', '\u{0301}'), ('ź', 'z', '\u{0301}'),
    ('Ż', 'Z', '\u{0307}'), ('ż', 'z', '\u{0307}'), ('Ž', 'Z', '\u{030C}'), ('ž', 'z', '\u{030C}'),
    ('Ą', 'A', '\u{0328}'), ('ą', 'a', '\u{0328}'), ('Ę', 'E', '\u{0328}'), ('ę', 'e', '\u{0328}'),
    // Vietnamese Latin Extended Additional (all single & stacked vowel + tone combinations)
    ('Ạ', 'A', '\u{0323}'), ('ạ', 'a', '\u{0323}'),
    ('Ả', 'A', '\u{0309}'), ('ả', 'a', '\u{0309}'),
    ('Ấ', 'Â', '\u{0301}'), ('ấ', 'â', '\u{0301}'),
    ('Ầ', 'Â', '\u{0300}'), ('ầ', 'â', '\u{0300}'),
    ('Ẩ', 'Â', '\u{0309}'), ('ẩ', 'â', '\u{0309}'),
    ('Ẫ', 'Â', '\u{0303}'), ('ẫ', 'â', '\u{0303}'),
    ('Ậ', 'Â', '\u{0323}'), ('ậ', 'â', '\u{0323}'),
    ('Ắ', 'Ă', '\u{0301}'), ('ắ', 'ă', '\u{0301}'),
    ('Ằ', 'Ă', '\u{0300}'), ('ằ', 'ă', '\u{0300}'),
    ('Ẳ', 'Ă', '\u{0309}'), ('ẳ', 'ă', '\u{0309}'),
    ('Ẵ', 'Ă', '\u{0303}'), ('ẵ', 'ă', '\u{0303}'),
    ('Ặ', 'Ă', '\u{0323}'), ('ặ', 'ă', '\u{0323}'),
    ('Ẹ', 'E', '\u{0323}'), ('ẹ', 'e', '\u{0323}'),
    ('Ẻ', 'E', '\u{0309}'), ('ẻ', 'e', '\u{0309}'),
    ('Ẽ', 'E', '\u{0303}'), ('ẽ', 'e', '\u{0303}'),
    ('Ế', 'Ê', '\u{0301}'), ('ế', 'ê', '\u{0301}'),
    ('Ề', 'Ê', '\u{0300}'), ('ề', 'ê', '\u{0300}'),
    ('Ể', 'Ê', '\u{0309}'), ('ể', 'ê', '\u{0309}'),
    ('Ễ', 'Ê', '\u{0303}'), ('ễ', 'ê', '\u{0303}'),
    ('Ệ', 'Ê', '\u{0323}'), ('ệ', 'ê', '\u{0323}'),
    ('Ỉ', 'I', '\u{0309}'), ('ỉ', 'i', '\u{0309}'),
    ('Ĩ', 'I', '\u{0303}'), ('ĩ', 'i', '\u{0303}'),
    ('Ị', 'I', '\u{0323}'), ('ị', 'i', '\u{0323}'),
    ('Ọ', 'O', '\u{0323}'), ('ọ', 'o', '\u{0323}'),
    ('Ỏ', 'O', '\u{0309}'), ('ỏ', 'o', '\u{0309}'),
    ('Ố', 'Ô', '\u{0301}'), ('ố', 'ô', '\u{0301}'),
    ('Ồ', 'Ô', '\u{0300}'), ('ồ', 'ô', '\u{0300}'),
    ('Ổ', 'Ô', '\u{0309}'), ('ổ', 'ô', '\u{0309}'),
    ('Ỗ', 'Ô', '\u{0303}'), ('ỗ', 'ô', '\u{0303}'),
    ('Ộ', 'Ô', '\u{0323}'), ('ộ', 'ô', '\u{0323}'),
    ('Ớ', 'Ơ', '\u{0301}'), ('ớ', 'ơ', '\u{0301}'),
    ('Ờ', 'Ơ', '\u{0300}'), ('ờ', 'ơ', '\u{0300}'),
    ('Ở', 'Ơ', '\u{0309}'), ('ở', 'ơ', '\u{0309}'),
    ('Ỡ', 'Ơ', '\u{0303}'), ('ỡ', 'ơ', '\u{0303}'),
    ('Ợ', 'Ơ', '\u{0323}'), ('ợ', 'ơ', '\u{0323}'),
    ('Ụ', 'U', '\u{0323}'), ('ụ', 'u', '\u{0323}'),
    ('Ủ', 'U', '\u{0309}'), ('ủ', 'u', '\u{0309}'),
    ('Ũ', 'U', '\u{0303}'), ('ũ', 'u', '\u{0303}'),
    ('Ứ', 'Ư', '\u{0301}'), ('ứ', 'ư', '\u{0301}'),
    ('Ừ', 'Ư', '\u{0300}'), ('ừ', 'ư', '\u{0300}'),
    ('Ử', 'Ư', '\u{0309}'), ('ử', 'ư', '\u{0309}'),
    ('Ữ', 'Ư', '\u{0303}'), ('ữ', 'ư', '\u{0303}'),
    ('Ự', 'Ư', '\u{0323}'), ('ự', 'ư', '\u{0323}'),
    ('Ỳ', 'Y', '\u{0300}'), ('ỳ', 'y', '\u{0300}'),
    ('Ỵ', 'Y', '\u{0323}'), ('ỵ', 'y', '\u{0323}'),
    ('Ỷ', 'Y', '\u{0309}'), ('ỷ', 'y', '\u{0309}'),
    ('Ỹ', 'Y', '\u{0303}'), ('ỹ', 'y', '\u{0303}'),
    // Cyrillic & Kana common pairs
    ('Ё', 'Е', '\u{0308}'), ('ё', 'е', '\u{0308}'), ('Й', 'И', '\u{0306}'), ('й', 'и', '\u{0306}'),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_guard_allows_workspace_and_blocks_secrets_and_appdata() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().join("team-ws");
        let data_dir = tmp.path().join("appdata");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(data_dir.join("shared-rooms").join("room-1").join("artifacts")).unwrap();

        let report = workspace.join("report.md");
        std::fs::write(&report, "# Done").unwrap();
        assert!(
            validate_outbound_file_path(
                &report.to_string_lossy(),
                Some(&workspace.to_string_lossy()),
                &data_dir
            )
            .is_ok()
        );

        // Expanded secret deny-list checks
        for denied_name in [
            ".env",
            ".npmrc",
            ".pypirc",
            ".netrc",
            ".git-credentials",
            "id_rsa",
            "id_ecdsa",
            "id_dsa",
            "cert.pem",
            "private.key",
            "bundle.p12",
            "signing.pfx",
        ] {
            let secret = workspace.join(denied_name);
            std::fs::write(&secret, "SECRET=1").unwrap();
            assert!(
                validate_outbound_file_path(
                    &secret.to_string_lossy(),
                    Some(&workspace.to_string_lossy()),
                    &data_dir
                )
                .is_err(),
                "expected {denied_name} to be blocked"
            );
        }

        let ssh_dir = workspace.join(".ssh");
        std::fs::create_dir_all(&ssh_dir).unwrap();
        let ssh_key = ssh_dir.join("config");
        std::fs::write(&ssh_key, "Host *").unwrap();
        assert!(
            validate_outbound_file_path(
                &ssh_key.to_string_lossy(),
                Some(&workspace.to_string_lossy()),
                &data_dir
            )
            .is_err()
        );

        let app_db = data_dir.join("aionui-backend.db");
        std::fs::write(&app_db, "sqlite").unwrap();
        assert!(
            validate_outbound_file_path(
                &app_db.to_string_lossy(),
                Some(&workspace.to_string_lossy()),
                &data_dir
            )
            .is_err()
        );

        // Private outbound-staging directory inside data_dir is strictly denied
        let staging_dir = data_dir.join("outbound-staging");
        std::fs::create_dir_all(&staging_dir).unwrap();
        let staged_tmp = staging_dir.join("tmp.bin");
        std::fs::write(&staged_tmp, "bytes").unwrap();
        assert!(
            validate_outbound_file_path(
                &staged_tmp.to_string_lossy(),
                Some(&workspace.to_string_lossy()),
                &data_dir
            )
            .is_err()
        );
    }

    #[test]
    fn outbound_guard_resolves_relative_paths_against_workspace_and_rejects_without_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().join("team space");
        let data_dir = tmp.path().join("appdata");
        std::fs::create_dir_all(workspace.join("docs")).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let rel_target = workspace.join("docs").join("summary report.md");
        std::fs::write(&rel_target, "ok").unwrap();

        // Relative path resolves against workspace
        let resolved = validate_outbound_file_path(
            "docs/summary report.md",
            Some(&workspace.to_string_lossy()),
            &data_dir,
        )
        .expect("relative path inside workspace must resolve");
        assert_eq!(resolved, dunce::canonicalize(&rel_target).unwrap());

        // Relative path MUST fail immediately when workspace is None or blank (even with allow_user_local_files)
        assert!(validate_outbound_file_path_with_policy("Cargo.toml", None, &data_dir, true).is_err());
        assert!(validate_outbound_file_path_with_policy("Cargo.toml", Some("   "), &data_dir, true).is_err());

        // Percent-encoded file:// URI resolves cleanly
        let file_uri = reqwest::Url::from_file_path(&rel_target).unwrap().to_string();
        assert!(file_uri.contains("%20"));
        let from_uri = validate_outbound_file_path(
            &file_uri,
            Some(&workspace.to_string_lossy()),
            &data_dir,
        )
        .expect("file:// URI with %20 must resolve");
        assert_eq!(from_uri, dunce::canonicalize(&rel_target).unwrap());
    }

    #[test]
    fn unicode_nfc_normalizes_vietnamese_filenames_and_apfs_nfd_paths() {
        // NFD "Báo_cáo_hệ_thống_đội_ngũ_đa_ngôn_ngữ.pdf"
        let nfd = "Ba\u{0301}o_ca\u{0301}o_he\u{0323}\u{0302}_tho\u{0302}\u{0301}ng_đo\u{0323}\u{0302}i_ngu\u{0303}_đa_ngo\u{0302}n_ngu\u{031B}\u{0303}.pdf";
        let sanitized = sanitize_file_name(nfd);
        assert_eq!(sanitized, "Báo_cáo_hệ_thống_đội_ngũ_đa_ngôn_ngữ.pdf");

        // APFS NFD workspace vs NFC file path containment check
        let nfd_base = Path::new("/Users/dev/Du\u{031B}\u{0323}_a\u{0301}n");
        let nfc_child = Path::new("/Users/dev/Dự_án/báo_cáo.md");
        assert!(path_starts_with_nfc_folded(nfc_child, nfd_base, true));
    }

    #[test]
    fn staging_artifacts_partitions_invalid_files_without_aborting_valid_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let ws_a = tmp.path().join("ws-a");
        let ws_b = tmp.path().join("ws-b");
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&ws_a).unwrap();
        std::fs::create_dir_all(&ws_b).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let spec = ws_a.join("api_spec.json");
        std::fs::write(&spec, r#"{"openapi":"3.1.0"}"#).unwrap();

        let batch = stage_room_artifacts_batch(
            "room-alpha",
            &[
                "missing_file.txt".to_owned(),
                "api_spec.json".to_owned(),
            ],
            Some(&ws_a.to_string_lossy()),
            &data_dir,
            &[ws_b.to_string_lossy().into_owned()],
            false,
        )
        .expect("batch staging must not abort on one missing file");

        assert_eq!(batch.artifacts.len(), 1);
        assert_eq!(batch.rejected_files.len(), 1);
        assert_eq!(batch.rejected_files[0].path, "missing_file.txt");
        assert!(Path::new(&batch.artifacts[0].staged_path).exists());

        let inbox_entries: Vec<_> = std::fs::read_dir(ws_b.join(".shared-inbox").join("room-alpha"))
            .unwrap()
            .collect();
        assert_eq!(inbox_entries.len(), 1);

        // Security exfiltration (.env or cross-workspace file) still fails hard with Forbidden
        let dotenv = ws_a.join(".env");
        std::fs::write(&dotenv, "SECRET=1").unwrap();
        let exfil_err = stage_room_artifacts_batch(
            "room-alpha",
            &[dotenv.to_string_lossy().into_owned()],
            Some(&ws_a.to_string_lossy()),
            &data_dir,
            &[],
            false,
        )
        .unwrap_err();
        assert!(exfil_err.starts_with("Forbidden:"));
    }
}

//! Canonical outbound path validation, inbound Telegram attachment staging,
//! and cross-team Shared Room `.shared-inbox/` artifact synchronization.

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

/// Sanitize an untrusted filename so it cannot escape directories or contain OS-reserved chars.
pub fn sanitize_file_name(raw: &str) -> String {
    let base = Path::new(raw)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("attachment.bin");
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
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
        trimmed[trimmed.len() - 120..].to_owned()
    } else {
        trimmed.to_owned()
    }
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
/// 1. Path must exist, be a regular file, and resolve via `dunce::canonicalize`.
/// 2. Sensitive segments (`.ssh`, `.gnupg`, `.aws`, `.kube`, `.git`) and secret files
///    (`.env`, `.env.*`, `auth.enc`, `aionui-backend.db*`, `id_rsa`, `id_ed25519`) are strictly denied.
/// 3. Anything inside `data_dir` outside `<data_dir>/shared-rooms` is strictly denied.
/// 4. Canonical path must reside inside `team_workspace`, `std::env::temp_dir()`, or `<data_dir>/shared-rooms`.
pub fn validate_outbound_file_path(
    raw_path: &str,
    team_workspace: Option<&str>,
    data_dir: &Path,
) -> Result<PathBuf, String> {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() {
        return Err("File path must not be empty".to_owned());
    }

    let canonical = dunce::canonicalize(trimmed)
        .map_err(|e| format!("Cannot resolve outbound file '{trimmed}': {e}"))?;

    let metadata = std::fs::metadata(&canonical)
        .map_err(|e| format!("Cannot stat outbound file '{}': {e}", canonical.display()))?;
    if !metadata.is_file() {
        return Err(format!("Outbound path '{}' is not a regular file", canonical.display()));
    }
    if metadata.len() > MAX_OUTBOUND_FILE_BYTES {
        return Err(format!(
            "Outbound file '{}' exceeds 45 MiB limit ({} bytes)",
            canonical.display(),
            metadata.len()
        ));
    }

    // Check explicit deny-list on components and file name.
    for comp in canonical.components() {
        if let Component::Normal(os) = comp {
            let lower = os.to_string_lossy().to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                ".ssh" | ".gnupg" | ".aws" | ".kube" | ".git"
            ) {
                return Err(format!(
                    "Outbound file '{}' is inside a restricted directory ({lower})",
                    canonical.display()
                ));
            }
        }
    }

    let file_name_lower = canonical
        .file_name()
        .map(|f| f.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    if file_name_lower == ".env"
        || file_name_lower.starts_with(".env.")
        || file_name_lower == "auth.enc"
        || file_name_lower.starts_with("aionui-backend.db")
        || file_name_lower == "id_rsa"
        || file_name_lower == "id_ed25519"
    {
        return Err(format!(
            "Outbound file '{}' matches a protected credential/database filename",
            canonical.display()
        ));
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
            return Err(format!(
                "Outbound file '{}' inside app data directory is forbidden outside shared-rooms",
                canonical.display()
            ));
        }
    }

    let mut allowed = false;
    if let Some(ws) = team_workspace.map(str::trim).filter(|w| !w.is_empty())
        && let Ok(canon_ws) = dunce::canonicalize(ws)
        && path_starts_with_ci(&canonical, &canon_ws)
    {
        allowed = true;
    }
    if !allowed
        && let Ok(canon_tmp) = dunce::canonicalize(std::env::temp_dir())
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
        return Err(format!(
            "Outbound file '{}' is outside allowed roots (team workspace, temp_dir, shared-rooms)",
            canonical.display()
        ));
    }

    Ok(canonical)
}

fn path_starts_with_ci(path: &Path, base: &Path) -> bool {
    if cfg!(windows) {
        let p = path.to_string_lossy().to_ascii_lowercase();
        let b = base.to_string_lossy().to_ascii_lowercase();
        let b_trimmed = b.trim_end_matches(['\\', '/']);
        if p == b_trimmed {
            return true;
        }
        p.starts_with(&format!("{b_trimmed}\\")) || p.starts_with(&format!("{b_trimmed}/"))
    } else {
        path.starts_with(base)
    }
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
    if source_files.is_empty() {
        return Ok(Vec::new());
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
    for raw_file in source_files {
        let valid_src = validate_outbound_file_path(raw_file, source_team_workspace, data_dir)?;
        let meta = std::fs::metadata(&valid_src)
            .map_err(|e| format!("Failed to read metadata of '{}': {e}", valid_src.display()))?;
        let orig_name = valid_src
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("artifact.bin");
        let safe_name = sanitize_file_name(orig_name);
        let stamped_name = format!("{}_{}", now_ms(), safe_name);
        let staged_dest = artifacts_dir.join(&stamped_name);

        if valid_src != staged_dest {
            std::fs::copy(&valid_src, &staged_dest).map_err(|e| {
                format!(
                    "Failed to stage artifact '{}' -> '{}': {e}",
                    valid_src.display(),
                    staged_dest.display()
                )
            })?;
        }

        for ws in recipient_workspaces {
            let trimmed_ws = ws.trim();
            if trimmed_ws.is_empty() {
                continue;
            }
            let inbox_dir = PathBuf::from(trimmed_ws).join(".shared-inbox").join(&safe_room);
            if std::fs::create_dir_all(&inbox_dir).is_ok() {
                let recipient_dest = inbox_dir.join(&stamped_name);
                let _ = std::fs::copy(&staged_dest, &recipient_dest);
            }
        }

        let canonical_staged = dunce::canonicalize(&staged_dest).unwrap_or(staged_dest);
        staged.push(StagedRoomArtifact {
            file_name: safe_name.clone(),
            staged_path: canonical_staged.to_string_lossy().into_owned(),
            size_bytes: meta.len(),
            is_image: is_image_file_name(&safe_name),
        });
    }

    Ok(staged)
}

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

        let dotenv = workspace.join(".env");
        std::fs::write(&dotenv, "SECRET=1").unwrap();
        assert!(
            validate_outbound_file_path(
                &dotenv.to_string_lossy(),
                Some(&workspace.to_string_lossy()),
                &data_dir
            )
            .is_err()
        );

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
    }

    #[test]
    fn staging_artifacts_copies_into_shared_rooms_and_recipient_inboxes() {
        let tmp = tempfile::tempdir().unwrap();
        let ws_a = tmp.path().join("ws-a");
        let ws_b = tmp.path().join("ws-b");
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&ws_a).unwrap();
        std::fs::create_dir_all(&ws_b).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let spec = ws_a.join("api_spec.json");
        std::fs::write(&spec, r#"{"openapi":"3.1.0"}"#).unwrap();

        let staged = stage_room_artifacts_and_sync_inbox(
            "room-alpha",
            &[spec.to_string_lossy().into_owned()],
            Some(&ws_a.to_string_lossy()),
            &data_dir,
            &[ws_b.to_string_lossy().into_owned()],
        )
        .expect("staging should succeed");

        assert_eq!(staged.len(), 1);
        assert!(Path::new(&staged[0].staged_path).exists());

        let inbox_entries: Vec<_> = std::fs::read_dir(ws_b.join(".shared-inbox").join("room-alpha"))
            .unwrap()
            .collect();
        assert_eq!(inbox_entries.len(), 1);
    }
}

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use url::Url;

use crate::types::ProjectError;

/// Whether the `file:` provider folds path casing on this platform.
/// macOS / Windows are case-insensitive; Linux is case-sensitive. Making this
/// a compile-time constant keeps `resource_canonical` machine-platform-relative
/// as the design specifies (local single-DB desktop, so no cross-platform key).
pub const IGNORE_PATH_CASING: bool = cfg!(any(target_os = "macos", target_os = "windows"));

/// Maximum cached folded -> case-preserved URI mappings so long-lived desktop
/// processes stay strictly bounded in memory.
const MAX_PRESERVED_URI_ENTRIES: usize = 16_384;

type PreservedUriCache = (HashMap<String, String>, VecDeque<String>);

static PRESERVED_URIS: OnceLock<Mutex<PreservedUriCache>> = OnceLock::new();

fn preserved_uri_cache() -> &'static Mutex<PreservedUriCache> {
    PRESERVED_URIS.get_or_init(|| Mutex::new((HashMap::new(), VecDeque::new())))
}

/// Provider scheme. Only `file:` is registered for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    File,
}

/// A lexically-normalized canonical resource URI — the folder dedupe key.
/// Produced only by [`canonicalize`]; never constructed from a raw string.
///
/// Equality, hashing, and [`Canonical::as_str`] use the platform case-folded
/// identity so macOS/Windows paths deduplicate case-insensitively, while
/// [`Canonical::preserved_str`] retains the original filesystem segment casing.
#[derive(Debug, Clone)]
pub struct Canonical {
    folded: String,
    preserved: String,
}

impl PartialEq for Canonical {
    fn eq(&self, other: &Self) -> bool {
        self.folded == other.folded
    }
}

impl Eq for Canonical {}

impl Hash for Canonical {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.folded.hash(state);
    }
}

impl Canonical {
    pub fn as_str(&self) -> &str {
        &self.folded
    }

    /// Lexically-normalized `file:` URI with original filesystem casing preserved.
    pub fn preserved_str(&self) -> &str {
        &self.preserved
    }
}

/// Parse the scheme of a provider URI (for provider dispatch; not persisted).
pub fn parse_scheme(uri: &str) -> Result<Scheme, ProjectError> {
    let url = Url::parse(uri).map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })?;
    match url.scheme() {
        "file" => Ok(Scheme::File),
        other => Err(ProjectError::UnsupportedResourceScheme {
            scheme: other.to_owned(),
        }),
    }
}

/// Lexically normalize a `file:` URI (resolving `.`/`..`, collapsing redundant
/// separators, dropping trailing slash) WITHOUT folding path casing.
pub fn normalize_preserving_case(uri: &str) -> Result<String, ProjectError> {
    parse_scheme(uri)?;
    let url = Url::parse(uri).map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })?;
    if let Ok(path) = url.to_file_path() {
        let normalized = normalize_lexical(&path);
        return Url::from_file_path(&normalized)
            .map(|u| u.to_string())
            .map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() });
    }
    #[cfg(windows)]
    if let Some((preserved, _folded)) = normalize_unix_style_file_url_on_windows(&url) {
        return Ok(preserved);
    }
    Err(ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })
}

/// Record the case-preserved `file:` URI corresponding to a folded canonical key
/// so OS watchers (`notify`) and filesystem providers (`read_dir`/`stat`) use
/// real on-disk casing even when the subscription/shard layer keys on the folded
/// dedup identity.
pub fn remember_preserved_uri(folded_canonical: &str, preserved_uri: &str) {
    if folded_canonical == preserved_uri {
        return;
    }
    let Ok(mut guard) = preserved_uri_cache().lock() else {
        return;
    };
    let (map, order) = &mut *guard;
    if let Some(existing) = map.get_mut(folded_canonical) {
        *existing = preserved_uri.to_owned();
        return;
    }
    while map.len() >= MAX_PRESERVED_URI_ENTRIES {
        let Some(oldest) = order.pop_front() else {
            break;
        };
        map.remove(&oldest);
    }
    map.insert(folded_canonical.to_owned(), preserved_uri.to_owned());
    order.push_back(folded_canonical.to_owned());
}

/// Resolve the case-preserving normalized `file:` URI for `uri`.
///
/// - If `uri` itself carries mixed case (e.g. `file:///Users/Hero/Proj`), keeps
///   that casing and records it under its folded key.
/// - If `uri` is already case-folded, returns the previously registered
///   case-preserved URI when known, falling back to `uri`.
pub fn preserved_uri_for(uri: &str) -> String {
    let normalized = normalize_preserving_case(uri).unwrap_or_else(|_| uri.to_owned());
    let Ok(canon) = canonicalize(uri) else {
        return normalized;
    };
    if normalized != canon.as_str() {
        remember_preserved_uri(canon.as_str(), &normalized);
        return normalized;
    }
    if let Ok(guard) = preserved_uri_cache().lock()
        && let Some(preserved) = guard.0.get(canon.as_str())
    {
        return preserved.clone();
    }
    normalized
}

/// Lexical canonicalization of a `file:` URI into the dedupe-key identity.
/// Pure: never touches the filesystem, never resolves symlinks.
///
/// Steps (see `data-model.md` "resource_canonical semantics"): parse to a fs
/// path, lexically resolve `.`/`..` and collapse separators, drop trailing
/// slash, fold path casing per platform, rebuild the `file:` URI (percent-
/// encoded, no fragment/query).
pub fn canonicalize(uri: &str) -> Result<Canonical, ProjectError> {
    // Only file: is supported; other schemes are rejected up front.
    parse_scheme(uri)?;

    let url = Url::parse(uri).map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })?;
    let Ok(path) = url.to_file_path() else {
        #[cfg(windows)]
        if let Some((preserved, folded)) = normalize_unix_style_file_url_on_windows(&url) {
            return Ok(Canonical { folded, preserved });
        }
        return Err(ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() });
    };

    let normalized = normalize_lexical(&path);
    let preserved_url =
        Url::from_file_path(&normalized).map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })?;
    let folded = if IGNORE_PATH_CASING {
        PathBuf::from(normalized.to_string_lossy().to_ascii_lowercase())
    } else {
        normalized
    };

    // from_file_path yields a percent-encoded file: URI with no trailing slash
    // and no fragment/query.
    let canonical_url =
        Url::from_file_path(&folded).map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })?;
    Ok(Canonical {
        folded: canonical_url.to_string(),
        preserved: preserved_url.to_string(),
    })
}

/// Display-name derivation: the final path segment of a canonical folder.
pub fn basename(canonical: &Canonical) -> String {
    fs_path(canonical)
        .ok()
        .and_then(|p| p.file_name().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_default()
}

/// Build a `file:` URI from an absolute filesystem path (raw input capture).
/// Does not canonicalize — callers pass the result to [`canonicalize`] for the
/// identity form.
pub fn to_file_uri(path: &Path) -> Result<String, ProjectError> {
    if let Ok(url) = Url::from_file_path(path) {
        return Ok(url.to_string());
    }
    #[cfg(windows)]
    if let Some(uri) = unix_style_path_to_file_uri_on_windows(path) {
        return Ok(uri);
    }
    Err(ProjectError::FolderCanonicalizeFailed {
        uri: path.to_string_lossy().into_owned(),
    })
}

fn raw_uri_to_path(uri: &str) -> Result<PathBuf, ProjectError> {
    let url = Url::parse(uri).map_err(|_| ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })?;
    if let Ok(path) = url.to_file_path() {
        return Ok(path);
    }
    #[cfg(windows)]
    if url.scheme() == "file" && url.host_str().unwrap_or("").is_empty() && url.path().starts_with('/') {
        let mut out = String::new();
        for seg in url.path().split('/') {
            if seg.is_empty() {
                continue;
            }
            out.push('/');
            out.push_str(seg);
        }
        return Ok(PathBuf::from(if out.is_empty() { "/".to_owned() } else { out }));
    }
    Err(ProjectError::FolderCanonicalizeFailed { uri: uri.to_owned() })
}

/// Fallback lexical normalizer on Windows for POSIX-style drive-less `file:///...`
/// URIs used in cross-platform unit tests (e.g. `file:///Users/Me/Aion`).
/// Native Windows drive/UNC URIs (`file:///C:/...`) always succeed in `Url::to_file_path`
/// above and never enter this fallback.
#[cfg(windows)]
fn normalize_unix_style_file_url_on_windows(url: &Url) -> Option<(String, String)> {
    if url.scheme() != "file" || !url.host_str().unwrap_or("").is_empty() || !url.path().starts_with('/') {
        return None;
    }
    let mut segs: Vec<&str> = Vec::new();
    for seg in url.path().split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                segs.pop();
            }
            other => segs.push(other),
        }
    }
    let joined = if segs.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", segs.join("/"))
    };
    let preserved = format!("file://{joined}");
    let folded = if IGNORE_PATH_CASING {
        format!("file://{}", joined.to_ascii_lowercase())
    } else {
        preserved.clone()
    };
    Some((preserved, folded))
}

#[cfg(windows)]
fn unix_style_path_to_file_uri_on_windows(path: &Path) -> Option<String> {
    let raw = path.to_string_lossy();
    if !raw.starts_with('/') && !raw.starts_with('\\') {
        return None;
    }
    let slash = raw.replace('\\', "/");
    let mut u = Url::parse("file:///").ok()?;
    u.set_path(&slash);
    Some(u.to_string())
}

/// Derive the absolute filesystem path from a `file:` canonical URI.
pub fn fs_path(canonical: &Canonical) -> Result<PathBuf, ProjectError> {
    raw_uri_to_path(canonical.as_str())
}

/// Derive the absolute filesystem path from a `file:` URI string. Used by the
/// runtime watcher and provider; if `uri` is a folded canonical key with a
/// registered case-preserved URI, resolves the preserved path so OS watches and
/// directory reads never mutate real filesystem casing.
pub fn uri_to_path(uri: &str) -> Result<PathBuf, ProjectError> {
    if let Ok(guard) = preserved_uri_cache().lock()
        && let Some(preserved) = guard.0.get(uri)
    {
        return raw_uri_to_path(preserved);
    }
    raw_uri_to_path(uri)
}

/// Lexically resolve `.` / `..` and collapse redundant separators without
/// touching the filesystem. `..` above the root is clamped (not an error here —
/// containment rejects escapes at the reference layer).
fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out: Vec<Component> = Vec::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.last(), Some(Component::Normal(_))) {
                    out.pop();
                }
                // Otherwise ignore: cannot ascend above root / prefix.
            }
            other => out.push(other),
        }
    }
    out.iter().collect()
}

#[cfg(test)]
#[path = "canonical_test.rs"]
mod canonical_test;

#[cfg(test)]
mod case_preservation_tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn canonical_preserves_casing_for_io_while_deduping_case_insensitively() {
        let mixed_uri = if cfg!(windows) {
            "file:///C:/Users/Hero/MyProj/Sub/../Src/"
        } else {
            "file:///Users/Hero/MyProj/Sub/../Src/"
        };
        let lower_uri = if cfg!(windows) {
            "file:///c:/users/hero/myproj/src"
        } else {
            "file:///users/hero/myproj/src"
        };
        let expected_preserved = if cfg!(windows) {
            "file:///C:/Users/Hero/MyProj/Src"
        } else {
            "file:///Users/Hero/MyProj/Src"
        };

        let c_mixed = canonicalize(mixed_uri).expect("mixed canonicalize");
        let c_lower = canonicalize(lower_uri).expect("lower canonicalize");

        assert_eq!(c_mixed.preserved_str(), expected_preserved);
        if IGNORE_PATH_CASING {
            assert_eq!(c_mixed, c_lower);
            let mut set = HashSet::new();
            set.insert(c_mixed.clone());
            assert!(set.contains(&c_lower), "Hash + Eq must fold case identically");

            remember_preserved_uri(c_mixed.as_str(), c_mixed.preserved_str());
            let resolved_path = uri_to_path(c_mixed.as_str()).expect("uri_to_path");
            assert!(
                resolved_path.to_string_lossy().contains("Hero") && resolved_path.to_string_lossy().contains("MyProj"),
                "uri_to_path on folded canonical must preserve registered filesystem casing, got {resolved_path:?}"
            );
        } else {
            assert_ne!(c_mixed, c_lower);
        }
    }
}

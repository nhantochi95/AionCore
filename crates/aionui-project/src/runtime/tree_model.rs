//! `TreeModel` — the app-lifetime, canonical-keyed, sparse file-fact tree.
//!
//! Each node is one directory level's listing (name → fact) plus its watch
//! handle. There is **no gen/version**: the WS transport is ordered, so applying
//! snapshots/deltas in arrival order is sufficient. Nodes produce canonical-
//! domain [`Snapshot`] / [`DeltaBatch`]; fan-out to the pe-keyed wire is the WS
//! handler's job (stage 1).
//!
//! `apply` never trusts a `notify` event's kind — it takes the affected child
//! names (or `All`) as a hint and reconciles against a fresh provider read, so
//! it is idempotent (repeated/stale events are no-ops) and absorbs coarse macOS
//! events (degrade to a full `read_dir`).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::canonical;

use super::error::FsError;
use super::fs_runtime::{FsRuntimeRegistry, IFsRuntime};
use super::noise::should_hide;
use super::provider::{EntryFact, Kind};
use super::watcher::WatchHandle;

/// A directory's full one-level listing (canonical domain).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub canonical: String,
    /// Sorted by name for deterministic output.
    pub entries: Vec<(String, EntryFact)>,
}

/// One reconciled change to a directory level. The tree tracks name+kind for
/// listing purposes (a kind change surfaces as removed + added) plus an mtime
/// used solely to detect that a file's content changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Added {
        name: String,
        kind: Kind,
    },
    Removed {
        name: String,
    },
    Renamed {
        from: String,
        to: String,
    },
    /// Same name, same kind, different mtime — the file's *content* changed.
    ///
    /// Deliberately carries no mtime value. The timestamp here is the one left by
    /// the external write, so a subscriber that fed it back as an optimistic
    /// concurrency precondition would make its own save look conflict-free
    /// against the very edit this signal exists to warn about. Withholding the
    /// value removes that misuse structurally rather than by convention.
    /// Subscribers re-read content on demand.
    Modified {
        name: String,
    },
}

/// A batch of reconciled changes for one canonical (canonical domain).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaBatch {
    pub canonical: String,
    pub changes: Vec<Change>,
}

/// What to re-check on `apply`: specific child names (precise Linux/Windows
/// events) or the whole level (coarse macOS events / rescan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hint {
    ChildNames(Vec<String>),
    All,
}

/// One mounted directory: its listing, case-preserved resource URI for IO, and
/// the watch keeping it live.
struct Node {
    resource_uri: String,
    entries: BTreeMap<String, EntryFact>,
    watch: WatchHandle,
}

/// The sparse canonical-keyed fact tree.
pub struct TreeModel {
    nodes: HashMap<String, Node>,
    runtimes: FsRuntimeRegistry,
}

/// Fold a URI to its platform dedup key (`Canonical::as_str`) when valid,
/// falling back to the raw string for synthetic non-path test URIs.
fn canonical_node_key(uri: &str) -> String {
    canonical::canonicalize(uri)
        .map(|c| c.as_str().to_owned())
        .unwrap_or_else(|_| uri.to_owned())
}

/// Resolve the runtime serving `canonical`'s scheme.
fn runtime_for(runtimes: &FsRuntimeRegistry, canonical: &str) -> Result<Arc<dyn IFsRuntime>, FsError> {
    let scheme = match canonical::parse_scheme(canonical) {
        Ok(canonical::Scheme::File) => "file",
        Err(_) => {
            return Err(FsError::Io {
                uri: canonical.to_owned(),
                message: "cannot parse scheme".to_owned(),
            });
        }
    };
    runtimes.get(scheme).ok_or_else(|| FsError::UnsupportedScheme {
        scheme: scheme.to_owned(),
    })
}

/// Build the child `file:` URI for `name` under directory `canonical`.
fn child_uri(canonical: &str, name: &str) -> Result<String, FsError> {
    let dir = canonical::uri_to_path(canonical).map_err(|_| FsError::Io {
        uri: canonical.to_owned(),
        message: "invalid canonical".to_owned(),
    })?;
    let child = dir.join(name);
    canonical::to_file_uri(&child).map_err(|_| FsError::Io {
        uri: canonical.to_owned(),
        message: "cannot build child uri".to_owned(),
    })
}

/// Whether two facts for the same surviving entry show a moved mtime.
///
/// Requires both sides to be known: an unknown mtime on either side means "no
/// evidence of a change", never "changed". That keeps a provider or filesystem
/// that cannot supply timestamps silent instead of reporting every entry as
/// modified on the first reconcile (see [`EntryFact::mtime_ms`]).
fn is_mtime_change(old: &EntryFact, fresh: &EntryFact) -> bool {
    match (old.mtime_ms, fresh.mtime_ms) {
        (Some(a), Some(b)) => a != b,
        _ => false,
    }
}

/// Reconcile `old` → `fresh` into added/removed/renamed/modified changes. A
/// same-inode removed+added pair is synthesized into a rename (falls back to
/// removed+added when the provider cannot supply an inode). A surviving file
/// whose mtime moved yields `Modified`.
fn diff(old: &BTreeMap<String, EntryFact>, fresh: &BTreeMap<String, EntryFact>) -> Vec<Change> {
    // Candidate removals/additions by name; a same-name kind change counts as
    // both (removed old kind + added new kind).
    let mut removed: Vec<(String, EntryFact)> = Vec::new();
    let mut added: Vec<(String, EntryFact)> = Vec::new();
    let mut modified: Vec<String> = Vec::new();
    for (name, of) in old {
        match fresh.get(name) {
            None => removed.push((name.clone(), of.clone())),
            Some(nf) if nf.kind != of.kind => {
                removed.push((name.clone(), of.clone()));
                added.push((name.clone(), nf.clone()));
            }
            // Survived with its kind intact: the listing is unchanged, but the
            // content may not be. Only files qualify — a directory's mtime also
            // moves when its children are added or removed, which the child
            // entries' own added/removed changes already express, and a symlink's
            // mtime describes the link rather than anything a subscriber reads.
            // Both would be noise for a content-changed signal.
            Some(nf) if of.kind == Kind::File && is_mtime_change(of, nf) => modified.push(name.clone()),
            Some(_) => {}
        }
    }
    for (name, nf) in fresh {
        // Same-name kind change already pushed above; same kind = unchanged.
        if !old.contains_key(name) {
            added.push((name.clone(), nf.clone()));
        }
    }

    // Synthesize renames from same-inode removed+added pairs (inode 0 = unknown,
    // never matched → degrades to removed+added).
    let mut changes = Vec::new();
    let mut used_added = vec![false; added.len()];
    removed.retain(|(rname, rf)| {
        if rf.inode != 0
            && let Some(idx) = added
                .iter()
                .enumerate()
                // A rename preserves kind. Requiring `af.kind == rf.kind` stops a
                // same-name file→dir kind change from being read as a rename when
                // the OS reuses the freed inode for the new entry (Linux does;
                // macOS assigns a fresh inode) — that case is remove + add.
                .position(|(i, (_, af))| !used_added[i] && af.inode == rf.inode && af.kind == rf.kind)
        {
            used_added[idx] = true;
            changes.push(Change::Renamed {
                from: rname.clone(),
                to: added[idx].0.clone(),
            });
            false
        } else {
            true
        }
    });

    for (i, (name, af)) in added.into_iter().enumerate() {
        if !used_added[i] {
            changes.push(Change::Added { name, kind: af.kind });
        }
    }
    for (name, _) in removed {
        changes.push(Change::Removed { name });
    }
    // Modified last: it never participates in rename synthesis (those entries
    // survived under the same name), so ordering is purely for stable output.
    for name in modified {
        changes.push(Change::Modified { name });
    }
    changes
}

impl TreeModel {
    pub fn new(runtimes: FsRuntimeRegistry) -> Self {
        Self {
            nodes: HashMap::new(),
            runtimes,
        }
    }

    /// Mount a canonical: arm watch, read baseline, store node. Ordering is the
    /// TOCTOU guard — watch is armed *before* the baseline read so no change
    /// between the two is lost (events buffered by the actor are replayed via
    /// idempotent `apply`). Returns the baseline snapshot. Re-mount is a no-op
    /// that returns the current snapshot (subscription layer guards duplicates).
    pub async fn mount(&mut self, canonical: &str) -> Result<Snapshot, FsError> {
        let key = canonical_node_key(canonical);
        let resource_uri = canonical::preserved_uri_for(canonical);
        canonical::remember_preserved_uri(&key, &resource_uri);

        if self.nodes.contains_key(&key) {
            return Ok(self.snapshot(canonical).expect("mounted node has snapshot"));
        }
        let runtime = runtime_for(&self.runtimes, &resource_uri)?;
        // 1. Arm watch first — any change from here on is buffered by the actor.
        //    Note: `LocalWatcher` resolves `canonical` through `canonical::uri_to_path`,
        //    which returns the case-preserved path registered above while keeping the
        //    watcher's `lexical` event attribution key aligned with `Shard`/`subs`.
        let watch = runtime.watcher().watch(canonical)?;
        // 2. Read the baseline listing using the case-preserved URI.
        let entries: BTreeMap<String, EntryFact> = runtime
            .provider()
            .read_dir(&resource_uri)
            .await?
            .into_iter()
            .collect();
        // 3. Store the node keyed by folded canonical identity.
        self.nodes.insert(
            key,
            Node {
                resource_uri,
                entries,
                watch,
            },
        );
        Ok(self.snapshot(canonical).expect("just-mounted node has snapshot"))
    }

    /// Unmount: drop the node and stop its watch.
    pub fn unmount(&mut self, canonical: &str) {
        let key = canonical_node_key(canonical);
        if let Some(node) = self.nodes.remove(&key)
            && let Ok(runtime) = runtime_for(&self.runtimes, &node.resource_uri)
        {
            runtime.watcher().unwatch(&node.watch);
        }
    }

    /// Reconcile the node against a fresh provider read per `hint`. `None` when
    /// nothing changed (idempotent) or the node is not mounted (in-flight event
    /// after unmount).
    pub async fn apply(&mut self, canonical: &str, hint: Hint) -> Result<Option<DeltaBatch>, FsError> {
        let key = canonical_node_key(canonical);
        // In-flight event after unmount (or never mounted) → drop silently.
        let Some((resource_uri, old)) = self
            .nodes
            .get(&key)
            .map(|n| (n.resource_uri.clone(), n.entries.clone()))
        else {
            return Ok(None);
        };
        let runtime = runtime_for(&self.runtimes, &resource_uri)?;

        let fresh: BTreeMap<String, EntryFact> = match hint {
            // Coarse event / rescan: re-read the whole level using case-preserved URI.
            Hint::All => runtime
                .provider()
                .read_dir(&resource_uri)
                .await?
                .into_iter()
                .collect(),
            // Precise event: re-stat only the named children, over the old base.
            Hint::ChildNames(child_names) => {
                let mut map = old.clone();
                for name in child_names {
                    // Noise never enters the tree — a watcher event for a hidden
                    // name (e.g. `.DS_Store` written mid-session) is ignored, so
                    // the tree stays consistent with `read_dir`/search.
                    if should_hide(&name) {
                        continue;
                    }
                    let uri = child_uri(&resource_uri, &name)?;
                    match runtime.provider().stat(&uri).await? {
                        Some(fact) => {
                            map.insert(name, fact);
                        }
                        None => {
                            map.remove(&name);
                        }
                    }
                }
                map
            }
        };

        let changes = diff(&old, &fresh);
        if changes.is_empty() {
            return Ok(None);
        }
        if let Some(node) = self.nodes.get_mut(&key) {
            node.entries = fresh;
        }
        Ok(Some(DeltaBatch {
            canonical: canonical.to_owned(),
            changes,
        }))
    }

    /// Current snapshot of a mounted node, or `None` if not mounted.
    pub fn snapshot(&self, canonical: &str) -> Option<Snapshot> {
        let key = canonical_node_key(canonical);
        self.nodes.get(&key).map(|node| Snapshot {
            canonical: canonical.to_owned(),
            entries: node.entries.iter().map(|(n, f)| (n.clone(), f.clone())).collect(),
        })
    }
}

#[cfg(test)]
#[path = "tree_model_test.rs"]
mod tree_model_test;

#[cfg(test)]
mod case_preserving_mount_tests {
    use super::*;
    use crate::runtime::fs_runtime::{IFsRuntime, IoDispatch};
    use crate::runtime::provider::IFsProvider;
    use crate::runtime::search::IFsSearchProvider;
    use crate::runtime::watcher::Watcher;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingCaseRuntime {
        watched_paths: Mutex<Vec<String>>,
        read_dir_uris: Mutex<Vec<String>>,
        stat_uris: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl IFsProvider for RecordingCaseRuntime {
        fn scheme(&self) -> &str {
            "file"
        }
        async fn read_dir(&self, uri: &str) -> Result<Vec<(String, EntryFact)>, FsError> {
            self.read_dir_uris.lock().unwrap().push(uri.to_owned());
            Ok(vec![])
        }
        async fn stat(&self, uri: &str) -> Result<Option<EntryFact>, FsError> {
            self.stat_uris.lock().unwrap().push(uri.to_owned());
            Ok(Some(EntryFact {
                kind: Kind::File,
                inode: 1,
                symlink_target: None,
                mtime_ms: Some(1_700_000_000_000),
            }))
        }
        async fn read(&self, _uri: &str) -> Result<Vec<u8>, FsError> {
            Ok(vec![])
        }
        async fn write(&self, _uri: &str, _data: &[u8]) -> Result<(), FsError> {
            Ok(())
        }
        async fn create_file(&self, _uri: &str) -> Result<(), FsError> {
            Ok(())
        }
        async fn mkdir(&self, _uri: &str) -> Result<(), FsError> {
            Ok(())
        }
        async fn remove(&self, _uri: &str, _recursive: bool) -> Result<(), FsError> {
            Ok(())
        }
        async fn rename(&self, _from: &str, _to: &str) -> Result<(), FsError> {
            Ok(())
        }
        async fn copy(&self, _from: &str, _to: &str, _recursive: bool) -> Result<(), FsError> {
            Ok(())
        }
    }

    impl Watcher for RecordingCaseRuntime {
        fn watch(&self, canonical: &str) -> Result<WatchHandle, FsError> {
            // Mirror `LocalWatcher::watch`, which resolves the OS path via `canonical::uri_to_path`.
            let path = canonical::uri_to_path(canonical)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| canonical.to_owned());
            self.watched_paths.lock().unwrap().push(path);
            Ok(WatchHandle {
                canonical: canonical.to_owned(),
            })
        }
        fn unwatch(&self, _handle: &WatchHandle) {}
    }

    impl IFsRuntime for RecordingCaseRuntime {
        fn provider(&self) -> &dyn IFsProvider {
            self
        }
        fn watcher(&self) -> &dyn Watcher {
            self
        }
        fn io_dispatch(&self) -> IoDispatch {
            IoDispatch::Inline
        }
        fn search_provider(&self) -> Option<Arc<dyn IFsSearchProvider>> {
            None
        }
    }

    #[tokio::test]
    async fn mount_preserves_filesystem_casing_for_watch_and_provider_while_deduping_keys() {
        let runtime = Arc::new(RecordingCaseRuntime::default());
        let mut registry = FsRuntimeRegistry::new();
        registry.register("file", Arc::clone(&runtime) as Arc<dyn IFsRuntime>);
        let mut tree = TreeModel::new(registry);

        let mixed = if cfg!(windows) {
            "file:///C:/Users/Hero/MixedCaseProj"
        } else {
            "file:///Users/Hero/MixedCaseProj"
        };
        let c = canonical::canonicalize(mixed).expect("canonicalize");
        canonical::remember_preserved_uri(c.as_str(), c.preserved_str());

        // Mount via folded canonical key (exactly as `Shard` does after `dispatch.rs`).
        tree.mount(c.as_str()).await.unwrap();

        assert_eq!(runtime.read_dir_uris.lock().unwrap().as_slice(), &[mixed]);
        let watched = runtime.watched_paths.lock().unwrap().clone();
        assert_eq!(watched.len(), 1);
        assert!(
            watched[0].contains("Hero") && watched[0].contains("MixedCaseProj"),
            "OS watch path must preserve MixedCaseProj casing, got {watched:?}"
        );

        // Re-mounting via mixed-case URI deduplicates on case-insensitive platforms.
        if canonical::IGNORE_PATH_CASING {
            tree.mount(mixed).await.unwrap();
            assert_eq!(
                runtime.read_dir_uris.lock().unwrap().len(),
                1,
                "case-folded node key must deduplicate second mount"
            );
        }

        // `apply` with `ChildNames` stats child under case-preserved parent URI.
        let delta = tree
            .apply(c.as_str(), Hint::ChildNames(vec!["App.tsx".to_owned()]))
            .await
            .unwrap()
            .expect("delta");
        assert_eq!(delta.changes.len(), 1);
        let expected_stat = format!("{mixed}/App.tsx");
        assert_eq!(runtime.stat_uris.lock().unwrap().as_slice(), &[expected_stat]);
    }
}

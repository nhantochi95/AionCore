//! Persistent warm `agy acp` transport for [`AntigravitySessionBackend`].
//!
//! Replaces the cold one-process-per-turn `agy -p` model with a single warm
//! `agy acp` process per session:
//! - Eliminates per-turn Go Language Server restarts (`[Notice] ... server restart`).
//! - Cuts Turn 2+ latency from ~15s cold-boot down to ~2.5s–4.5s warm RPCs.
//! - Supports `--include-default-customizations=false` (`antigravity_isolate_default_customizations`)
//!   on Team slots so broken/heavy global MCP servers never stall Turn 1.
//! - Preserves full effort-suffixed model IDs (e.g. `gemini-3.8-flash-low`) by
//!   ignoring `agy acp`'s family-only `config_option_update` notifications.
//! - Handles permissions natively via ACP `session/request_permission` (instant
//!   host-side auto-approval in `"yolo"` mode; interactive UI cards otherwise)
//!   without `.agents/hooks.json` or its 30s fail-open window.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use aionui_common::CommandSpec;
use aionui_process::BoxedStdin;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, oneshot};

use super::argv::FULL_AUTO_SENTINEL;
use super::conn::{AntigravitySessionBackend, store_models};
use crate::adapter::{AgentIo, ManagedProcessIo, STDERR_PEEK_LINES};
use crate::backend::SessionConfig;
use crate::backend::acp_conn::{
    build_mcp_servers, build_prompt_blocks, map_plan_priority, map_plan_status, parse_acp_result_usage,
    parse_acp_tool_content, parse_permission_metadata, synth_turn_result,
};
use crate::backend::types::{BackendError, ContentBlock, PermissionDecision};
use crate::capability::ModelInfo;
use crate::event::{CancelReason, PlanEntry, SessionEvent, SubagentKind, TurnOutcome};

/// Build argv for a persistent `agy acp` child process.
pub(crate) fn build_warm_acp_argv(config: &SessionConfig) -> Vec<String> {
    let mut args = vec!["acp".to_owned()];
    if config.antigravity_isolate_default_customizations {
        args.push("--include-default-customizations=false".to_owned());
    }
    if let Some(ws) = config.cwd.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        args.push("--add-dir".to_owned());
        args.push(ws.to_owned());
    }
    args.extend(config.extra_args.iter().cloned());
    args
}

/// Normalize AionUi's mode into an `agy acp` wire mode id.
/// `"yolo"` is AionUi's host-side full-auto sentinel (not an `agy` mode), so on
/// the wire `agy acp` stays in `"default"` while `is_full_auto()` auto-approves
/// `session/request_permission` immediately on the host.
fn wire_mode_for_acp(mode: Option<&str>) -> &'static str {
    match mode.map(str::trim) {
        Some("accept-edits") => "accept-edits",
        Some("plan") => "plan",
        Some(m) if m.eq_ignore_ascii_case(FULL_AUTO_SENTINEL) => "default",
        _ => "default",
    }
}

/// Unwrap `agy acp`'s generic `"call_mcp_tool"` title into `"mcp__<server>__<tool>"`
/// when `rawInput` carries `server_name` / `ServerName` and `tool_name` / `ToolName`.
fn display_tool_name(raw_title: &str, raw_input: &Value) -> String {
    if raw_title == "call_mcp_tool" {
        let server = raw_input
            .get("server_name")
            .or_else(|| raw_input.get("ServerName"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let tool = raw_input
            .get("tool_name")
            .or_else(|| raw_input.get("ToolName"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        if let (Some(s), Some(t)) = (server, tool) {
            return format!("mcp__{s}__{t}");
        }
    }
    raw_title.to_owned()
}

fn pick_permission_option_id(options: &[(String, String)], decision: PermissionDecision) -> String {
    let pick_by = |pred: &dyn Fn(&str, &str) -> bool| -> Option<String> {
        options
            .iter()
            .find(|(id, kind)| pred(id.as_str(), kind.as_str()))
            .map(|(id, _)| id.clone())
    };
    match decision {
        PermissionDecision::AllowAlways => pick_by(&|id, kind| kind == "allow_always" || id == "allow_always")
            .or_else(|| pick_by(&|id, kind| kind.starts_with("allow") || id == "allow" || id == "allow_once"))
            .unwrap_or_else(|| "allow".to_owned()),
        PermissionDecision::Approved => pick_by(&|id, kind| kind == "allow_once" || id == "allow")
            .or_else(|| pick_by(&|_, kind| kind.starts_with("allow")))
            .unwrap_or_else(|| "allow".to_owned()),
        PermissionDecision::Denied => pick_by(&|id, kind| kind.starts_with("reject") || id == "deny" || id == "reject")
            .unwrap_or_else(|| "deny".to_owned()),
    }
}

fn parse_offered_options(params: Option<&Value>) -> Vec<(String, String)> {
    params
        .and_then(|p| p.get("options"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|o| {
                    let id = o.get("optionId").and_then(Value::as_str)?.to_owned();
                    let kind = o.get("kind").and_then(Value::as_str).unwrap_or("").to_owned();
                    Some((id, kind))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_handshake_models(result: &Value) -> Vec<ModelInfo> {
    result
        .get("models")
        .and_then(|m| m.get("availableModels"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let id = item.get("modelId").and_then(Value::as_str)?.trim();
                    if id.is_empty() {
                        return None;
                    }
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or(id);
                    let description = item
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned);
                    Some(ModelInfo {
                        id: id.to_owned(),
                        name: name.to_owned(),
                        description,
                        reasoning_efforts: Vec::new(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn write_json_line(stdin: &Arc<Mutex<Option<BoxedStdin>>>, value: &Value) -> Result<(), BackendError> {
    let mut line = serde_json::to_vec(value).map_err(|e| BackendError::Transport(format!("serialize acp rpc: {e}")))?;
    line.push(b'\n');
    let mut guard = stdin.lock().await;
    let writer = guard
        .as_mut()
        .ok_or_else(|| BackendError::Transport("agy acp stdin closed".into()))?;
    writer
        .write_all(&line)
        .await
        .map_err(|e| BackendError::Transport(format!("write agy acp stdin: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| BackendError::Transport(format!("flush agy acp stdin: {e}")))?;
    Ok(())
}

pub(crate) struct WarmAcpClient {
    io: Arc<dyn AgentIo>,
    stdin: Arc<Mutex<Option<BoxedStdin>>>,
    next_rpc_id: AtomicU64,
    pending_rpcs: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    alive: Arc<AtomicBool>,
    suppress_updates: Arc<AtomicBool>,
    saw_streaming_occupancy_in_turn: Arc<AtomicBool>,
    active_turn_gen: Arc<AtomicU64>,
    bound_session_id: Mutex<Option<String>>,
    preamble_delivered: AtomicBool,
    applied_model: Mutex<Option<String>>,
    applied_mode: Mutex<Option<String>>,
    handshake_gate: Mutex<()>,
    reader_task: tokio::task::JoinHandle<()>,
}

impl WarmAcpClient {
    pub(crate) async fn from_io(
        io: Arc<dyn AgentIo>,
        backend: std::sync::Weak<AntigravitySessionBackend>,
    ) -> Result<Arc<Self>, BackendError> {
        let (stdin_raw, stdout_raw) = io
            .take_stdio()
            .await
            .ok_or_else(|| BackendError::Transport("agy acp stdio unavailable".into()))?;
        let stdin = Arc::new(Mutex::new(Some(stdin_raw)));
        let pending_rpcs: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>> = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let suppress_updates = Arc::new(AtomicBool::new(false));
        let saw_streaming_occupancy_in_turn = Arc::new(AtomicBool::new(false));
        let active_turn_gen = Arc::new(AtomicU64::new(0));

        let reader_stdin = Arc::clone(&stdin);
        let reader_pending = Arc::clone(&pending_rpcs);
        let reader_alive = Arc::clone(&alive);
        let reader_suppress = Arc::clone(&suppress_updates);
        let reader_saw_streaming = Arc::clone(&saw_streaming_occupancy_in_turn);
        let reader_turn_gen = Arc::clone(&active_turn_gen);

        let reader_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout_raw).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(frame) = serde_json::from_str::<Value>(trimmed) else {
                    continue;
                };
                if let Some(method) = frame.get("method").and_then(Value::as_str) {
                    if let Some(req_id) = frame.get("id").cloned().filter(|v| !v.is_null()) {
                        Self::handle_reverse_rpc(&reader_stdin, &backend, &reader_turn_gen, method, req_id, &frame)
                            .await;
                    } else if method == "session/update" && !reader_suppress.load(Ordering::SeqCst) {
                        Self::handle_session_update(&backend, &reader_turn_gen, &reader_saw_streaming, &frame);
                    }
                } else if let Some(id) = frame.get("id").and_then(Value::as_u64)
                    && let Some(tx) = reader_pending.lock().await.remove(&id)
                {
                    let _ = tx.send(frame);
                }
            }
            reader_alive.store(false, Ordering::SeqCst);
            if let Ok(mut guard) = reader_stdin.try_lock() {
                *guard = None;
            }
            reader_pending.lock().await.clear();
        });

        Ok(Arc::new(Self {
            io,
            stdin,
            next_rpc_id: AtomicU64::new(1),
            pending_rpcs,
            alive,
            suppress_updates,
            saw_streaming_occupancy_in_turn,
            active_turn_gen,
            bound_session_id: Mutex::new(None),
            preamble_delivered: AtomicBool::new(false),
            applied_model: Mutex::new(None),
            applied_mode: Mutex::new(None),
            handshake_gate: Mutex::new(()),
            reader_task,
        }))
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub(crate) async fn bound_session_id(&self) -> Option<String> {
        self.bound_session_id.lock().await.clone()
    }

    pub(crate) async fn shutdown(&self) {
        self.alive.store(false, Ordering::SeqCst);
        *self.stdin.lock().await = None;
        self.pending_rpcs.lock().await.clear();
        self.reader_task.abort();
        self.io.terminate().await;
    }

    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value, BackendError> {
        if !self.is_alive() {
            return Err(BackendError::Transport("agy acp process is not alive".into()));
        }
        let id = self.next_rpc_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending_rpcs.lock().await.insert(id, tx);
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(err) = write_json_line(&self.stdin, &req).await {
            self.pending_rpcs.lock().await.remove(&id);
            self.alive.store(false, Ordering::SeqCst);
            return Err(err);
        }
        rx.await.map_err(|_| {
            self.alive.store(false, Ordering::SeqCst);
            BackendError::Transport(format!("agy acp exited while awaiting `{method}`"))
        })
    }

    pub(crate) async fn send_cancel_notification(&self) -> bool {
        let Some(sid) = self.bound_session_id().await else {
            return false;
        };
        let notif = json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": { "sessionId": sid },
        });
        write_json_line(&self.stdin, &notif).await.is_ok()
    }

    pub(crate) async fn ensure_initialized(&self, backend: &AntigravitySessionBackend) -> Result<String, BackendError> {
        let _guard = self.handshake_gate.lock().await;
        if let Some(existing) = self.bound_session_id.lock().await.clone() {
            return Ok(existing);
        }

        let init_resp = self
            .rpc_call(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": { "readTextFile": false, "writeTextFile": false }
                    }
                }),
            )
            .await?;
        if let Some(err) = init_resp.get("error") {
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("agy acp initialize failed");
            return Err(BackendError::Transport(msg.to_owned()));
        }

        let cwd = backend.config_ref().cwd.clone().unwrap_or_else(|| ".".to_owned());
        let mcp_servers = build_mcp_servers(&backend.config_ref().init.mcp_servers);
        let resume_candidate = backend.current_anchor().await;

        let (sid, open_result, resumed) = if let Some(resume_sid) = resume_candidate.filter(|s| !s.trim().is_empty()) {
            self.suppress_updates.store(true, Ordering::SeqCst);
            let load_resp = self
                .rpc_call(
                    "session/load",
                    json!({
                        "sessionId": resume_sid,
                        "cwd": cwd,
                        "mcpServers": mcp_servers,
                    }),
                )
                .await;
            self.suppress_updates.store(false, Ordering::SeqCst);
            match load_resp {
                Ok(frame) if frame.get("error").is_none() => {
                    let result = frame.get("result").cloned().unwrap_or(Value::Null);
                    let bound = result
                        .get("sessionId")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or(resume_sid);
                    (bound, result, true)
                }
                Ok(frame) => {
                    tracing::warn!(
                        session_id = %backend.session_id_str(),
                        error = ?frame.get("error"),
                        "antigravity warm acp: session/load rejected anchor; falling back to session/new"
                    );
                    let fresh = self
                        .rpc_call(
                            "session/new",
                            json!({
                                "cwd": cwd,
                                "mcpServers": mcp_servers,
                            }),
                        )
                        .await?;
                    let result = Self::extract_open_result(&fresh)?;
                    let sid = Self::extract_new_sid(&result)?;
                    (sid, result, false)
                }
                Err(err) => return Err(err),
            }
        } else {
            let fresh = self
                .rpc_call(
                    "session/new",
                    json!({
                        "cwd": cwd,
                        "mcpServers": mcp_servers,
                    }),
                )
                .await?;
            let result = Self::extract_open_result(&fresh)?;
            let sid = Self::extract_new_sid(&result)?;
            (sid, result, false)
        };

        if resumed {
            self.preamble_delivered.store(true, Ordering::SeqCst);
        }

        let discovered_models = parse_handshake_models(&open_result);
        if !discovered_models.is_empty() {
            store_models(&discovered_models);
            backend.update_discovered_models(discovered_models);
        }

        if let Some(obs_mode) = open_result
            .get("modes")
            .and_then(|m| m.get("currentModeId"))
            .and_then(Value::as_str)
        {
            *self.applied_mode.lock().await = Some(obs_mode.to_owned());
        }
        if let Some(obs_model) = open_result
            .get("models")
            .and_then(|m| m.get("currentModelId"))
            .and_then(Value::as_str)
        {
            *self.applied_model.lock().await = Some(obs_model.to_owned());
        }

        *self.bound_session_id.lock().await = Some(sid.clone());
        backend.set_anchor(sid.clone()).await;
        backend.emit_event(
            backend.current_turn_gen(),
            SessionEvent::BackendBound {
                backend_session_id: Some(sid.clone()),
            },
        );
        Ok(sid)
    }

    fn extract_open_result(frame: &Value) -> Result<Value, BackendError> {
        if let Some(err) = frame.get("error") {
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("agy acp session/new failed");
            return Err(BackendError::Transport(msg.to_owned()));
        }
        Ok(frame.get("result").cloned().unwrap_or(Value::Null))
    }

    fn extract_new_sid(result: &Value) -> Result<String, BackendError> {
        result
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| BackendError::Transport("agy acp session/new returned no sessionId".into()))
    }

    pub(crate) async fn sync_desired_config(&self, sid: &str, desired_model: Option<String>, desired_mode: Option<String>) {
        let target_wire_mode = wire_mode_for_acp(desired_mode.as_deref());
        let need_mode = self
            .applied_mode
            .lock()
            .await
            .as_deref()
            != Some(target_wire_mode);
        if need_mode {
            match self
                .rpc_call(
                    "session/set_mode",
                    json!({
                        "sessionId": sid,
                        "modeId": target_wire_mode,
                    }),
                )
                .await
            {
                Ok(resp) if resp.get("error").is_none() => {
                    *self.applied_mode.lock().await = Some(target_wire_mode.to_owned());
                }
                Ok(resp) => {
                    tracing::warn!(error = ?resp.get("error"), mode = target_wire_mode, "antigravity warm acp: session/set_mode rejected");
                }
                Err(err) => {
                    tracing::warn!(error = %err, mode = target_wire_mode, "antigravity warm acp: session/set_mode transport error");
                }
            }
        }

        if let Some(model_id) = desired_model.filter(|m| !m.trim().is_empty()) {
            let need_model = self.applied_model.lock().await.as_deref() != Some(model_id.as_str());
            if need_model {
                match self
                    .rpc_call(
                        "session/set_model",
                        json!({
                            "sessionId": sid,
                            "modelId": model_id,
                        }),
                    )
                    .await
                {
                    Ok(resp) if resp.get("error").is_none() => {
                        *self.applied_model.lock().await = Some(model_id);
                    }
                    Ok(resp) => {
                        tracing::warn!(error = ?resp.get("error"), model = %model_id, "antigravity warm acp: session/set_model rejected");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, model = %model_id, "antigravity warm acp: session/set_model transport error");
                    }
                }
            }
        }
    }

    pub(crate) async fn run_prompt_turn(
        &self,
        backend: &AntigravitySessionBackend,
        turn_gen: u64,
        content: Vec<ContentBlock>,
    ) {
        self.active_turn_gen.store(turn_gen, Ordering::SeqCst);
        self.saw_streaming_occupancy_in_turn.store(false, Ordering::SeqCst);

        let sid = match self.ensure_initialized(backend).await {
            Ok(sid) => sid,
            Err(err) => {
                let tail = self.io.peek_stderr(STDERR_PEEK_LINES).await;
                let msg = if tail.trim().is_empty() {
                    err.to_string()
                } else {
                    format!("{err} ({})", tail.trim())
                };
                backend.emit_event(
                    turn_gen,
                    SessionEvent::TurnResult {
                        is_error: true,
                        api_error_status: None,
                        result_text: msg,
                        epoch: turn_gen,
                        outcome: TurnOutcome::Failed,
                    },
                );
                return;
            }
        };

        self.sync_desired_config(&sid, backend.effective_model_id(), backend.effective_mode_id())
            .await;

        let mut prompt_blocks = content;
        if !self.preamble_delivered.swap(true, Ordering::SeqCst)
            && let Some(prefix) = backend
                .config_ref()
                .init
                .preset_context
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            if let Some(ContentBlock::Text(first_text)) = prompt_blocks.first_mut() {
                *first_text = format!("{prefix}\n\n{first_text}");
            } else {
                prompt_blocks.insert(0, ContentBlock::Text(prefix.to_owned()));
            }
        }

        let wire_blocks = build_prompt_blocks(&prompt_blocks);
        let resp = self
            .rpc_call(
                "session/prompt",
                json!({
                    "sessionId": sid,
                    "prompt": wire_blocks,
                }),
            )
            .await;

        match resp {
            Ok(frame) => {
                let saw_streaming = self.saw_streaming_occupancy_in_turn.swap(false, Ordering::SeqCst);
                if let Some(usage_event) = parse_acp_result_usage(&frame, saw_streaming) {
                    backend.emit_event(turn_gen, usage_event);
                }
                let stderr_tail = if frame.get("error").is_some() {
                    Some(self.io.peek_stderr(STDERR_PEEK_LINES).await)
                } else {
                    None
                };
                let terminal = synth_turn_result(&frame, turn_gen, stderr_tail.as_deref());
                backend.emit_event(turn_gen, terminal);
            }
            Err(err) => {
                let was_cancelled = backend.was_turn_cancelled(turn_gen);
                if was_cancelled {
                    backend.emit_event(
                        turn_gen,
                        SessionEvent::TurnResult {
                            is_error: false,
                            api_error_status: None,
                            result_text: String::new(),
                            epoch: turn_gen,
                            outcome: TurnOutcome::Cancelled {
                                reason: CancelReason::UserCancel,
                            },
                        },
                    );
                } else {
                    let tail = self.io.peek_stderr(STDERR_PEEK_LINES).await;
                    let result_text = if tail.trim().is_empty() {
                        err.to_string()
                    } else {
                        tail
                    };
                    backend.emit_event(
                        turn_gen,
                        SessionEvent::TurnResult {
                            is_error: true,
                            api_error_status: None,
                            result_text,
                            epoch: turn_gen,
                            outcome: TurnOutcome::Failed,
                        },
                    );
                }
            }
        }
    }

    async fn handle_reverse_rpc(
        stdin: &Arc<Mutex<Option<BoxedStdin>>>,
        backend_weak: &std::sync::Weak<AntigravitySessionBackend>,
        active_turn_gen: &Arc<AtomicU64>,
        method: &str,
        req_id: Value,
        frame: &Value,
    ) {
        if method != "session/request_permission" {
            let err_frame = json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": { "code": -32601, "message": "method not handled by antigravity warm acp" }
            });
            let _ = write_json_line(stdin, &err_frame).await;
            return;
        }

        let params = frame.get("params");
        let options = parse_offered_options(params);
        let Some(backend) = backend_weak.upgrade() else {
            let option_id = pick_permission_option_id(&options, PermissionDecision::Denied);
            let _ = write_json_line(
                stdin,
                &json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": { "outcome": { "outcome": "selected", "optionId": option_id } }
                }),
            )
            .await;
            return;
        };

        if backend.is_full_auto_enabled() {
            let option_id = pick_permission_option_id(&options, PermissionDecision::Approved);
            let _ = write_json_line(
                stdin,
                &json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": { "outcome": { "outcome": "selected", "optionId": option_id } }
                }),
            )
            .await;
            return;
        }

        let tool_call = params.and_then(|p| p.get("toolCall"));
        let raw_title = tool_call
            .and_then(|tc| tc.get("title"))
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let raw_input = tool_call.and_then(|tc| tc.get("rawInput")).cloned().unwrap_or(Value::Null);
        let tool_name = display_tool_name(raw_title, &raw_input);
        let metadata = parse_permission_metadata(params);
        let turn_gen = active_turn_gen.load(Ordering::SeqCst);
        let stdin_clone = Arc::clone(stdin);

        tokio::spawn(async move {
            let decision = backend
                .await_interactive_acp_permission(turn_gen, tool_name, raw_input, metadata)
                .await;
            let option_id = pick_permission_option_id(&options, decision);
            let _ = write_json_line(
                &stdin_clone,
                &json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": { "outcome": { "outcome": "selected", "optionId": option_id } }
                }),
            )
            .await;
        });
    }

    fn handle_session_update(
        backend_weak: &std::sync::Weak<AntigravitySessionBackend>,
        active_turn_gen: &Arc<AtomicU64>,
        saw_streaming_occupancy_in_turn: &Arc<AtomicBool>,
        frame: &Value,
    ) {
        let Some(backend) = backend_weak.upgrade() else {
            return;
        };
        let turn_gen = active_turn_gen.load(Ordering::SeqCst);
        let update = frame
            .get("params")
            .and_then(|p| p.get("update"))
            .unwrap_or(&Value::Null);
        let kind = update.get("sessionUpdate").and_then(Value::as_str).unwrap_or("");
        match kind {
            "agent_message_chunk" => {
                let text = update
                    .get("content")
                    .and_then(|c| c.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !text.is_empty() {
                    backend.emit_event(
                        turn_gen,
                        SessionEvent::MessageDelta {
                            item_id: format!("agy-acp-msg-{turn_gen}"),
                            text: text.to_owned(),
                        },
                    );
                }
            }
            "agent_thought_chunk" => {
                let text = update
                    .get("content")
                    .and_then(|c| c.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !text.is_empty() {
                    backend.emit_event(
                        turn_gen,
                        SessionEvent::ThoughtDelta {
                            item_id: format!("agy-acp-thought-{turn_gen}"),
                            text: text.to_owned(),
                        },
                    );
                }
            }
            "tool_call" => {
                let id = update.get("toolCallId").and_then(Value::as_str).unwrap_or("");
                let raw_title = update.get("title").and_then(Value::as_str).unwrap_or("tool");
                let input = update.get("rawInput").cloned().unwrap_or(Value::Null);
                let name = display_tool_name(raw_title, &input);
                backend.emit_event(
                    turn_gen,
                    SessionEvent::ToolCall {
                        tool_use_id: id.to_owned(),
                        name,
                        subagent: SubagentKind::Inline,
                        input,
                        parent_tool_use_id: None,
                    },
                );
            }
            "tool_call_update" => {
                let status = update.get("status").and_then(Value::as_str).unwrap_or("");
                if matches!(status, "completed" | "failed") {
                    let id = update.get("toolCallId").and_then(Value::as_str).unwrap_or("");
                    backend.emit_event(
                        turn_gen,
                        SessionEvent::ToolResult {
                            tool_use_id: id.to_owned(),
                            is_error: status == "failed",
                            content: parse_acp_tool_content(update),
                            parent_tool_use_id: None,
                        },
                    );
                }
            }
            "usage_update" => {
                let input = update.get("inputTokens").and_then(Value::as_u64).unwrap_or(0);
                let output = update.get("outputTokens").and_then(Value::as_u64).unwrap_or(0);
                let total = update
                    .get("totalTokens")
                    .and_then(Value::as_u64)
                    .or_else(|| update.get("used").and_then(Value::as_u64))
                    .unwrap_or(input + output);
                let cost_usd = update.get("cost").and_then(|c| c.get("amount")).and_then(Value::as_f64);
                let context_window = update.get("size").and_then(Value::as_u64);
                if total > 0 {
                    saw_streaming_occupancy_in_turn.store(true, Ordering::SeqCst);
                }
                if total > 0 || cost_usd.is_some() || context_window.is_some() {
                    backend.emit_event(
                        turn_gen,
                        SessionEvent::UsageDelta {
                            input_tokens: input,
                            output_tokens: output,
                            total_tokens: total,
                            cost_usd,
                            context_window,
                            occupancy_tokens: (total > 0).then_some(total),
                            turn_billed_tokens: None,
                            breakdown: crate::event::UsageBreakdown::default(),
                        },
                    );
                }
            }
            "plan" => {
                let entries: Vec<PlanEntry> = update
                    .get("entries")
                    .and_then(Value::as_array)
                    .map(|es| {
                        es.iter()
                            .filter_map(|e| {
                                let content = e.get("content").and_then(Value::as_str)?.to_owned();
                                let status = map_plan_status(e.get("status").and_then(Value::as_str).unwrap_or(""));
                                let priority = map_plan_priority(e.get("priority").and_then(Value::as_str));
                                Some(PlanEntry {
                                    content,
                                    status,
                                    priority,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                backend.emit_event(
                    turn_gen,
                    SessionEvent::Plan {
                        entries,
                        explanation: None,
                    },
                );
            }
            // Deliberately do NOT emit ConfigChanged { model } from `config_option_update`:
            // `agy acp` reports family-only `currentValue` (e.g. `"gemini-3.8-flash"` for
            // `"gemini-3.8-flash-low"`), which would clobber effort-suffixed model IDs.
            _ => {}
        }
    }
}

impl Drop for WarmAcpClient {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
        self.reader_task.abort();
        let io = Arc::clone(&self.io);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                io.terminate().await;
            });
        }
    }
}

pub(crate) async fn spawn_warm_acp_process(
    backend: &AntigravitySessionBackend,
    self_weak: std::sync::Weak<AntigravitySessionBackend>,
) -> Result<Arc<WarmAcpClient>, BackendError> {
    let config = backend.config_ref();
    let mut spawn_env = config.spawn_env.clone();
    if let Some(cwd) = config.cwd.as_deref() {
        spawn_env.retain(|entry| entry.name != "PWD");
        spawn_env.push(aionui_common::EnvVar {
            name: "PWD".to_owned(),
            value: cwd.to_owned(),
        });
    }
    let spec = CommandSpec {
        command: config
            .cli_program
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("agy")),
        args: build_warm_acp_argv(config),
        env: spawn_env,
        cwd: config.cwd.clone(),
    };
    let proc = backend
        .spawner_ref()
        .spawn(spec, &[], backend.session_id_str())
        .await
        .map_err(|e| BackendError::Transport(format!("spawn agy acp: {e}")))?;
    let io: Arc<dyn AgentIo> = Arc::new(ManagedProcessIo::new(proc));
    WarmAcpClient::from_io(io, self_weak).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SessionBackend;
    use crate::backend::types::{CancelTarget, Command, CommandMeta};
    use crate::event::ExitStatusLite;
    use futures_util::StreamExt;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    struct DuplexAgentIo {
        stdio: Mutex<Option<(BoxedStdin, aionui_process::BoxedStdout)>>,
    }

    impl DuplexAgentIo {
        fn pair() -> (Arc<Self>, BufReader<tokio::io::ReadHalf<DuplexStream>>, tokio::io::WriteHalf<DuplexStream>) {
            let (client_side, server_side) = tokio::io::duplex(64 * 1024);
            let (client_read, client_write) = tokio::io::split(client_side);
            let (server_read, server_write) = tokio::io::split(server_side);
            let io = Arc::new(Self {
                stdio: Mutex::new(Some((Box::new(client_write), Box::new(client_read)))),
            });
            (io, BufReader::new(server_read), server_write)
        }
    }

    #[async_trait::async_trait]
    impl AgentIo for DuplexAgentIo {
        async fn take_stdio(&self) -> Option<(BoxedStdin, aionui_process::BoxedStdout)> {
            self.stdio.lock().await.take()
        }

        async fn wait_for_exit(&self) -> Option<ExitStatusLite> {
            std::future::pending().await
        }
    }

    async fn read_rpc(reader: &mut BufReader<tokio::io::ReadHalf<DuplexStream>>) -> Value {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await.expect("read rpc line");
        assert!(n > 0, "unexpected EOF on fake agy acp server");
        serde_json::from_str(line.trim()).expect("valid json-rpc")
    }

    async fn write_rpc(writer: &mut tokio::io::WriteHalf<DuplexStream>, frame: &Value) {
        let mut raw = serde_json::to_vec(frame).expect("serialize frame");
        raw.push(b'\n');
        writer.write_all(&raw).await.expect("write rpc line");
        writer.flush().await.expect("flush rpc line");
    }

    fn send_cmd(text: &str) -> Command {
        Command::Send {
            content: vec![ContentBlock::Text(text.to_owned())],
            metadata: CommandMeta::default(),
        }
    }

    #[test]
    fn warm_acp_argv_isolates_customizations_when_enabled() {
        let mut cfg = SessionConfig {
            cwd: Some("d:/work".into()),
            extra_args: vec!["--add-dir".into(), "d:/skills/s1".into()],
            ..Default::default()
        };
        assert_eq!(
            build_warm_acp_argv(&cfg),
            vec!["acp", "--add-dir", "d:/work", "--add-dir", "d:/skills/s1"]
        );

        cfg.antigravity_isolate_default_customizations = true;
        assert_eq!(
            build_warm_acp_argv(&cfg),
            vec![
                "acp",
                "--include-default-customizations=false",
                "--add-dir",
                "d:/work",
                "--add-dir",
                "d:/skills/s1",
            ]
        );
    }

    #[tokio::test]
    async fn warm_acp_reuses_single_process_across_multiple_turns_and_injects_preamble_once() {
        let mut cfg = SessionConfig {
            cwd: Some("/w".into()),
            mode: Some("yolo".into()),
            model: Some("gemini-3.8-flash-low".into()),
            ..Default::default()
        };
        cfg.init.preset_context = Some("TEAM_ROLE_PROMPT".into());

        let backend = AntigravitySessionBackend::new_warm_for_test(cfg, None);
        let (io, mut srv_in, mut srv_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io).await.unwrap();
        let mut events = backend.events();

        let server = tokio::spawn(async move {
            // Turn 1: initialize -> session/new -> session/set_model -> session/prompt
            let init_req = read_rpc(&mut srv_in).await;
            assert_eq!(init_req["method"], "initialize");
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}})).await;

            let new_req = read_rpc(&mut srv_in).await;
            assert_eq!(new_req["method"], "session/new");
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": new_req["id"],
                    "result": {
                        "sessionId": "agy-sid-42",
                        "modes": { "currentModeId": "default" },
                        "models": {
                            "currentModelId": "gemini-4-argon-high",
                            "availableModels": [
                                { "modelId": "gemini-4-argon-high", "name": "Gemini 4 Argon (High)" },
                                { "modelId": "gemini-3.8-flash-low", "name": "Gemini 3.8 Flash (Low)" }
                            ]
                        }
                    }
                }),
            )
            .await;

            let set_model_req = read_rpc(&mut srv_in).await;
            assert_eq!(set_model_req["method"], "session/set_model");
            assert_eq!(set_model_req["params"]["modelId"], "gemini-3.8-flash-low");
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":set_model_req["id"],"result":{}})).await;

            // Emit family-only config_option_update (must NOT overwrite `gemini-3.8-flash-low`)
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "agy-sid-42",
                        "update": {
                            "sessionUpdate": "config_option_update",
                            "configOptions": [{
                                "id": "model",
                                "category": "model",
                                "currentValue": "gemini-3.8-flash"
                            }]
                        }
                    }
                }),
            )
            .await;

            let p1 = read_rpc(&mut srv_in).await;
            assert_eq!(p1["method"], "session/prompt");
            let p1_text = p1["params"]["prompt"][0]["text"].as_str().unwrap().to_owned();
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "agy-sid-42",
                        "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "WARM_1" } }
                    }
                }),
            )
            .await;
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": p1["id"],
                    "result": {
                        "stopReason": "end_turn",
                        "usage": {
                            "inputTokens": 1200,
                            "outputTokens": 45,
                            "cachedReadTokens": 900,
                            "thoughtTokens": 30,
                            "totalTokens": 2175
                        }
                    }
                }),
            )
            .await;

            // Turn 2: NO initialize, NO session/new, NO duplicate set_model, NO duplicate preamble!
            let p2 = read_rpc(&mut srv_in).await;
            assert_eq!(p2["method"], "session/prompt");
            let p2_text = p2["params"]["prompt"][0]["text"].as_str().unwrap().to_owned();
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": p2["id"],
                    "result": { "stopReason": "end_turn" }
                }),
            )
            .await;
            (p1_text, p2_text)
        });

        backend.dispatch(send_cmd("first message")).await.unwrap();
        let mut saw_usage_breakdown = false;
        while let Some(env) = events.next().await {
            if let SessionEvent::UsageDelta { breakdown, .. } = &env.event {
                assert_eq!(breakdown.cached_read_tokens, 900);
                assert_eq!(breakdown.thought_tokens, 30);
                saw_usage_breakdown = true;
            }
            if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }
        assert!(saw_usage_breakdown, "UsageDelta must carry cachedReadTokens and thoughtTokens");
        assert_eq!(
            backend.capabilities().current_model.as_deref(),
            Some("gemini-3.8-flash-low"),
            "family-only config_option_update must not clobber effort-suffixed model ID"
        );
        assert_eq!(backend.current_anchor().await.as_deref(), Some("agy-sid-42"));

        // Wait briefly for `settle_turn_end` before dispatching Turn 2
        while backend.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        backend.dispatch(send_cmd("second message")).await.unwrap();
        while let Some(env) = events.next().await {
            if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }

        let (p1_text, p2_text) = server.await.unwrap();
        assert_eq!(p1_text, "TEAM_ROLE_PROMPT\n\nfirst message");
        assert_eq!(p2_text, "second message");
    }

    #[tokio::test]
    async fn warm_acp_auto_approves_permissions_in_yolo_and_prompts_in_default() {
        let cfg = SessionConfig {
            cwd: Some("/w".into()),
            mode: Some("yolo".into()),
            ..Default::default()
        };
        let backend = AntigravitySessionBackend::new_warm_for_test(cfg, Some("agy-sid-perm".into()));
        let (io, mut srv_in, mut srv_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io).await.unwrap();
        let mut events = backend.events();

        let server = tokio::spawn(async move {
            let init_req = read_rpc(&mut srv_in).await;
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}})).await;
            let load_req = read_rpc(&mut srv_in).await;
            assert_eq!(load_req["method"], "session/load");
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":load_req["id"],"result":{"sessionId":"agy-sid-perm","modes":{"currentModeId":"default"}}}),
            )
            .await;

            // Turn 1 in yolo: permission request is auto-approved without UI prompt
            let p1 = read_rpc(&mut srv_in).await;
            assert_eq!(p1["method"], "session/prompt");
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": 900,
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": "agy-sid-perm",
                        "options": [
                            {"optionId": "allow_once", "kind": "allow_once"},
                            {"optionId": "reject_once", "kind": "reject_once"}
                        ],
                        "toolCall": {"title": "run_command", "rawInput": {"command": "echo hi"}}
                    }
                }),
            )
            .await;
            let perm_resp_1 = read_rpc(&mut srv_in).await;
            assert_eq!(perm_resp_1["id"], 900);
            assert_eq!(perm_resp_1["result"]["outcome"]["optionId"], "allow_once");
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":p1["id"],"result":{"stopReason":"end_turn"}})).await;

            // Turn 2 after switching to `default`: permission request parks for interactive UI decision
            let p2 = read_rpc(&mut srv_in).await;
            assert_eq!(p2["method"], "session/prompt");
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": 901,
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": "agy-sid-perm",
                        "options": [
                            {"optionId": "allow_once", "kind": "allow_once"},
                            {"optionId": "reject_once", "kind": "reject_once"}
                        ],
                        "toolCall": {"title": "run_command", "rawInput": {"command": "rm -rf /"}}
                    }
                }),
            )
            .await;
            let perm_resp_2 = read_rpc(&mut srv_in).await;
            assert_eq!(perm_resp_2["id"], 901);
            assert_eq!(perm_resp_2["result"]["outcome"]["optionId"], "reject_once");
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":p2["id"],"result":{"stopReason":"end_turn"}})).await;
        });

        backend.dispatch(send_cmd("run yolo")).await.unwrap();
        while let Some(env) = events.next().await {
            assert!(
                !matches!(env.event, SessionEvent::Permission { .. }),
                "yolo mode must not surface interactive Permission events"
            );
            if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }
        while backend.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        // Switch out of yolo into default without respawning!
        backend
            .dispatch(Command::SetMode {
                mode: "default".into(),
            })
            .await
            .unwrap();

        backend.dispatch(send_cmd("run default")).await.unwrap();
        while let Some(env) = events.next().await {
            if let SessionEvent::Permission { request_id, .. } = env.event {
                backend
                    .dispatch(Command::AnswerPermission {
                        request_id,
                        decision: PermissionDecision::Denied,
                        selected: None,
                        answers: Vec::new(),
                    })
                    .await
                    .unwrap();
            } else if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn warm_acp_cancel_sends_rpc_without_killing_warm_session() {
        let cfg = SessionConfig {
            cwd: Some("/w".into()),
            mode: Some("yolo".into()),
            ..Default::default()
        };
        let backend = AntigravitySessionBackend::new_warm_for_test(cfg, Some("agy-sid-cancel".into()));
        let (io, mut srv_in, mut srv_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io).await.unwrap();
        let mut events = backend.events();

        let server = tokio::spawn(async move {
            let init_req = read_rpc(&mut srv_in).await;
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}})).await;
            let load_req = read_rpc(&mut srv_in).await;
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":load_req["id"],"result":{"sessionId":"agy-sid-cancel","modes":{"currentModeId":"default"}}}),
            )
            .await;

            let p1 = read_rpc(&mut srv_in).await;
            assert_eq!(p1["method"], "session/prompt");
            let cancel_notif = read_rpc(&mut srv_in).await;
            assert_eq!(cancel_notif["method"], "session/cancel");
            assert_eq!(cancel_notif["params"]["sessionId"], "agy-sid-cancel");
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":p1["id"],"result":{"stopReason":"cancelled"}}),
            )
            .await;

            // Subsequent turn still uses the SAME warm process!
            let p2 = read_rpc(&mut srv_in).await;
            assert_eq!(p2["method"], "session/prompt");
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":p2["id"],"result":{"stopReason":"end_turn"}})).await;
        });

        backend.dispatch(send_cmd("long turn")).await.unwrap();
        // Wait until session/load bound the session id so Cancel sends session/cancel
        loop {
            let bound = backend
                .warm_acp
                .lock()
                .await
                .as_ref()
                .unwrap()
                .bound_session_id()
                .await;
            if bound.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Small yield to let session/prompt write land
        tokio::task::yield_now().await;
        backend
            .dispatch(Command::Cancel {
                target: CancelTarget::Turn,
            })
            .await
            .unwrap();

        while let Some(env) = events.next().await {
            if let SessionEvent::TurnResult { outcome, .. } = env.event {
                assert!(matches!(outcome, TurnOutcome::Cancelled { .. }));
                break;
            }
        }
        while backend.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        backend.dispatch(send_cmd("next turn")).await.unwrap();
        while let Some(env) = events.next().await {
            if let SessionEvent::TurnResult { outcome, .. } = env.event {
                assert_eq!(
                    outcome,
                    TurnOutcome::Completed {
                        stop_reason: crate::event::StopReason::EndTurn,
                    }
                );
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn warm_acp_falls_back_to_session_new_when_session_load_fails() {
        let mut cfg = SessionConfig {
            cwd: Some("/w".into()),
            mode: Some("yolo".into()),
            ..Default::default()
        };
        cfg.init.preset_context = Some("FRESH_PREAMBLE".into());
        let backend = AntigravitySessionBackend::new_warm_for_test(cfg, Some("dead-anchor-99".into()));
        let (io, mut srv_in, mut srv_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io).await.unwrap();
        let mut events = backend.events();

        let server = tokio::spawn(async move {
            let init_req = read_rpc(&mut srv_in).await;
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}})).await;

            let load_req = read_rpc(&mut srv_in).await;
            assert_eq!(load_req["method"], "session/load");
            assert_eq!(load_req["params"]["sessionId"], "dead-anchor-99");
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":load_req["id"],"error":{"code":-32000,"message":"session not found"}}),
            )
            .await;

            let new_req = read_rpc(&mut srv_in).await;
            assert_eq!(new_req["method"], "session/new");
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":new_req["id"],"result":{"sessionId":"fresh-sid-100","modes":{"currentModeId":"default"}}}),
            )
            .await;

            let p1 = read_rpc(&mut srv_in).await;
            let text = p1["params"]["prompt"][0]["text"].as_str().unwrap().to_owned();
            write_rpc(&mut srv_out, &json!({"jsonrpc":"2.0","id":p1["id"],"result":{"stopReason":"end_turn"}})).await;
            text
        });

        backend.dispatch(send_cmd("hello")).await.unwrap();
        while let Some(env) = events.next().await {
            if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }
        let text = server.await.unwrap();
        assert_eq!(text, "FRESH_PREAMBLE\n\nhello", "fallback to session/new must deliver preamble");
        assert_eq!(backend.current_anchor().await.as_deref(), Some("fresh-sid-100"));
    }

    #[tokio::test]
    async fn warm_acp_mid_turn_crash_surfaces_failed_turn_and_resumes_via_session_load() {
        let mut cfg = SessionConfig {
            cwd: Some("/w".into()),
            mode: Some("yolo".into()),
            ..Default::default()
        };
        cfg.init.preset_context = Some("INITIAL_ROLE_PROMPT".into());

        let backend = AntigravitySessionBackend::new_warm_for_test(cfg, None);
        let (io1, mut srv1_in, mut srv1_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io1).await.unwrap();
        let mut events = backend.events();

        // Process #1: succeeds on Turn 1 (binding `crash-sid-77`), then crashes mid-Turn 2!
        let srv1 = tokio::spawn(async move {
            let init_req = read_rpc(&mut srv1_in).await;
            write_rpc(
                &mut srv1_out,
                &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}}),
            )
            .await;

            let new_req = read_rpc(&mut srv1_in).await;
            assert_eq!(new_req["method"], "session/new");
            write_rpc(
                &mut srv1_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": new_req["id"],
                    "result": { "sessionId": "crash-sid-77", "modes": { "currentModeId": "default" } }
                }),
            )
            .await;

            let p1 = read_rpc(&mut srv1_in).await;
            assert_eq!(p1["method"], "session/prompt");
            write_rpc(
                &mut srv1_out,
                &json!({"jsonrpc":"2.0","id":p1["id"],"result":{"stopReason":"end_turn"}}),
            )
            .await;

            // Turn 2 arrives, streams one partial chunk, then `agy.exe` dies abruptly (EOF)
            let p2 = read_rpc(&mut srv1_in).await;
            assert_eq!(p2["method"], "session/prompt");
            write_rpc(
                &mut srv1_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "crash-sid-77",
                        "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "PARTIAL" } }
                    }
                }),
            )
            .await;
            drop(srv1_out);
            drop(srv1_in);
        });

        // Turn 1 succeeds
        backend.dispatch(send_cmd("turn 1")).await.unwrap();
        while let Some(env) = events.next().await {
            if let SessionEvent::TurnResult { is_error, .. } = env.event {
                assert!(!is_error);
                break;
            }
        }
        while backend.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert_eq!(backend.current_anchor().await.as_deref(), Some("crash-sid-77"));

        // Turn 2 crashes mid-flight -> must surface `TurnResult { is_error: true, outcome: Failed }` and settle cleanly
        backend.dispatch(send_cmd("turn 2 crash")).await.unwrap();
        let mut saw_crash_failure = false;
        while let Some(env) = events.next().await {
            if let SessionEvent::TurnResult {
                is_error,
                outcome,
                ..
            } = env.event
            {
                assert!(is_error, "mid-turn EOF must report is_error=true");
                assert_eq!(outcome, TurnOutcome::Failed);
                saw_crash_failure = true;
                break;
            }
        }
        assert!(saw_crash_failure);
        srv1.await.unwrap();
        while backend.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        // Inject fresh Process #2 for Turn 3 -> must resume `crash-sid-77` via `session/load`,
        // suppress history replay chunks, and NOT re-inject `INITIAL_ROLE_PROMPT`!
        let (io2, mut srv2_in, mut srv2_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io2).await.unwrap();
        let srv2 = tokio::spawn(async move {
            let init_req = read_rpc(&mut srv2_in).await;
            write_rpc(
                &mut srv2_out,
                &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}}),
            )
            .await;

            let load_req = read_rpc(&mut srv2_in).await;
            assert_eq!(load_req["method"], "session/load");
            assert_eq!(load_req["params"]["sessionId"], "crash-sid-77");
            // Simulate replayed history during `session/load` before responding to `load_req`
            write_rpc(
                &mut srv2_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "crash-sid-77",
                        "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "OLD_REPLAY_NOISE" } }
                    }
                }),
            )
            .await;
            write_rpc(
                &mut srv2_out,
                &json!({"jsonrpc":"2.0","id":load_req["id"],"result":{"modes":{"currentModeId":"default"}}}),
            )
            .await;

            let p3 = read_rpc(&mut srv2_in).await;
            assert_eq!(p3["method"], "session/prompt");
            let p3_text = p3["params"]["prompt"][0]["text"].as_str().unwrap().to_owned();
            write_rpc(
                &mut srv2_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "crash-sid-77",
                        "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "RECOVERED_OK" } }
                    }
                }),
            )
            .await;
            write_rpc(
                &mut srv2_out,
                &json!({"jsonrpc":"2.0","id":p3["id"],"result":{"stopReason":"end_turn"}}),
            )
            .await;
            p3_text
        });

        backend.dispatch(send_cmd("turn 3 after crash")).await.unwrap();
        let mut streamed_turn3 = String::new();
        while let Some(env) = events.next().await {
            if let SessionEvent::MessageDelta { text, .. } = &env.event {
                streamed_turn3.push_str(text);
            }
            if let SessionEvent::TurnResult { is_error, .. } = env.event {
                assert!(!is_error);
                break;
            }
        }
        let p3_text = srv2.await.unwrap();
        assert_eq!(p3_text, "turn 3 after crash", "session/load recovery must not duplicate preamble");
        assert_eq!(streamed_turn3, "RECOVERED_OK", "session/load history replay must be suppressed");
    }

    #[tokio::test]
    async fn warm_acp_multistep_result_usage_keeps_occupancy_none_when_streaming_reported() {
        let backend = AntigravitySessionBackend::new_warm_for_test(SessionConfig::default(), None);
        let (io, mut srv_in, mut srv_out) = DuplexAgentIo::pair();
        backend.inject_warm_acp_client_for_test(io).await.unwrap();
        let mut events = backend.events();

        let srv = tokio::spawn(async move {
            let init_req = read_rpc(&mut srv_in).await;
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":init_req["id"],"result":{"protocolVersion":1}}),
            )
            .await;
            let new_req = read_rpc(&mut srv_in).await;
            write_rpc(
                &mut srv_out,
                &json!({"jsonrpc":"2.0","id":new_req["id"],"result":{"sessionId":"sid-occ-1","modes":{"currentModeId":"default"}}}),
            )
            .await;

            // Turn 1: <1M multi-step (48k occupancy, 700k cumulative turn total)
            let p1 = read_rpc(&mut srv_in).await;
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "sid-occ-1",
                        "update": { "sessionUpdate": "usage_update", "used": 48_000, "size": 1_000_000 }
                    }
                }),
            )
            .await;
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": p1["id"],
                    "result": {
                        "stopReason": "end_turn",
                        "usage": {
                            "inputTokens": 40_000,
                            "outputTokens": 10_000,
                            "cachedReadTokens": 650_000,
                            "totalTokens": 700_000
                        }
                    }
                }),
            )
            .await;

            // Turn 2: >1M multi-step (42k occupancy, 27.4M cumulative turn total)
            let p2 = read_rpc(&mut srv_in).await;
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "sid-occ-1",
                        "update": { "sessionUpdate": "usage_update", "used": 42_150, "size": 1_000_000 }
                    }
                }),
            )
            .await;
            write_rpc(
                &mut srv_out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": p2["id"],
                    "result": {
                        "stopReason": "end_turn",
                        "usage": {
                            "inputTokens": 410_000,
                            "outputTokens": 90_000,
                            "cachedReadTokens": 26_900_000,
                            "totalTokens": 27_400_000
                        }
                    }
                }),
            )
            .await;
        });

        backend.dispatch(send_cmd("sub-1m multistep")).await.unwrap();
        let mut t1_usages = Vec::new();
        while let Some(env) = events.next().await {
            if let SessionEvent::UsageDelta {
                occupancy_tokens,
                turn_billed_tokens,
                ..
            } = env.event
            {
                t1_usages.push((occupancy_tokens, turn_billed_tokens));
            } else if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }
        assert_eq!(
            t1_usages,
            vec![(Some(48_000), None), (None, Some(700_000))],
            "700k <1M multi-step turn must NOT overwrite 48k streaming occupancy"
        );
        while backend.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        backend.dispatch(send_cmd("27m multistep")).await.unwrap();
        let mut t2_usages = Vec::new();
        while let Some(env) = events.next().await {
            if let SessionEvent::UsageDelta {
                occupancy_tokens,
                turn_billed_tokens,
                ..
            } = env.event
            {
                t2_usages.push((occupancy_tokens, turn_billed_tokens));
            } else if matches!(env.event, SessionEvent::TurnResult { .. }) {
                break;
            }
        }
        assert_eq!(
            t2_usages,
            vec![(Some(42_150), None), (None, Some(27_400_000))],
            "27.4M multi-step turn must set occupancy_tokens=None and turn_billed_tokens=Some(27.4M)"
        );
        srv.await.unwrap();
    }
}

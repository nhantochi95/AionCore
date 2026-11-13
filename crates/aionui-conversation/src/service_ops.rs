//! Agent-session operations on ConversationService.
//!
//! These forward to the active AgentInstance (via `self.task(id)`) for
//! config-options/usage/slash-commands/side-question queries, plus workspace
//! browsing that needs the conversations.extra.workspace field.
//!
//! Kept in a separate file from service.rs to avoid pushing that file
//! over 2000 lines.

use std::collections::HashMap;
use std::path::Component;

use aionui_ai_agent::{AcpError, AgentError};
use aionui_api_types::{
    AcpConfigOptionDto, AcpConfigSelectOptionDto, ConfigOptionConfirmation, GetConfigOptionsResponse,
    SetConfigOptionRequest, SetConfigOptionResponse, SideQuestionRequest, SideQuestionResponse, SlashCommandItem,
    WorkspaceBrowseQuery, WorkspaceEntry,
};
use aionui_common::{AgentKillReason, ErrorChain, normalize_keys_to_snake_case};
use aionui_db::SaveRuntimeStateParams;
use tracing::warn;

use crate::ConversationError;
use crate::service::{AssistantRuntimePreferenceUpdate, ConversationService};

const MAX_DIR_DEPTH: usize = 10;

impl ConversationService {
    // ── Config Options ──────────────────────────────────────────────

    pub async fn get_config_options(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<GetConfigOptionsResponse, ConversationError> {
        self.ensure_owned_conversation(user_id, conversation_id).await?;
        if self.runtime_state().is_restarting(conversation_id) {
            return Err(ConversationError::RuntimeRestarting {
                conversation_id: conversation_id.to_owned(),
            });
        }
        match self.task(conversation_id) {
            Ok(agent) => agent.get_config_options().await.map_err(ConversationError::from),
            Err(ConversationError::ActiveAgentNotFound { .. }) => {
                let config_options = self.synthesize_dormant_config_options(user_id, conversation_id).await?;
                Ok(GetConfigOptionsResponse { config_options })
            }
            Err(err) => Err(err),
        }
    }

    /// Return the last model selection that was confirmed by the runtime.
    ///
    /// Generic config selections and the assistant snapshot are checked before
    /// the legacy ACP current-model field. Older Team builds could persist the
    /// first two after an observed switch while leaving that field stale.
    pub async fn confirmed_model_id(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<String>, ConversationError> {
        self.ensure_owned_conversation(user_id, conversation_id).await?;
        let runtime_state = self
            .acp_session_repo()
            .load_runtime_state_for_user(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load runtime model state: {e}")))?;
        let config_selection = runtime_state
            .as_ref()
            .and_then(|state| state.config_selections_json.as_deref())
            .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(raw).ok())
            .and_then(|selections| selections.get("model").cloned())
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if config_selection.is_some() {
            return Ok(config_selection);
        }

        let snapshot_model = self
            .conversation_repo()
            .get_assistant_snapshot(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load assistant model snapshot: {e}")))?
            .and_then(|snapshot| snapshot.resolved_model_id)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if snapshot_model.is_some() {
            return Ok(snapshot_model);
        }

        Ok(runtime_state
            .and_then(|state| state.current_model_id)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty()))
    }

    /// Persist an already-observed model selection into every conversation
    /// source used when a Team member runtime is rebuilt.
    pub async fn persist_confirmed_model(
        &self,
        user_id: &str,
        conversation_id: &str,
        model: &str,
    ) -> Result<(), ConversationError> {
        let model = model.trim();
        if model.is_empty() {
            return Err(ConversationError::BadRequest {
                reason: "model must not be empty".into(),
            });
        }
        self.update_extra(
            user_id,
            conversation_id,
            serde_json::json!({ "current_model_id": model }),
        )
        .await?;

        let runtime_state = self
            .acp_session_repo()
            .load_runtime_state_for_user(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load runtime model state: {e}")))?;
        let mut selections = runtime_state
            .and_then(|state| state.config_selections_json)
            .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(&raw).ok())
            .unwrap_or_default();
        selections.insert("model".to_owned(), model.to_owned());
        let selections_json = serde_json::to_string(&selections)
            .map_err(|e| ConversationError::internal(format!("Failed to serialize runtime model selection: {e}")))?;
        self.acp_session_repo()
            .save_runtime_state_for_user(
                user_id,
                conversation_id,
                &SaveRuntimeStateParams {
                    current_model_id: Some(Some(model)),
                    config_selections_json: Some(Some(&selections_json)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to persist runtime model: {e}")))?;
        self.persist_runtime_assistant_snapshot(
            user_id,
            conversation_id,
            AssistantRuntimePreferenceUpdate {
                model: Some(model),
                permission: None,
                thought_level: None,
            },
        )
        .await?;
        Ok(())
    }

    pub async fn set_config_option(
        &self,
        user_id: &str,
        conversation_id: &str,
        option_id: &str,
        req: SetConfigOptionRequest,
    ) -> Result<SetConfigOptionResponse, ConversationError> {
        if option_id.trim().is_empty() {
            return Err(ConversationError::BadRequest {
                reason: "option_id must not be empty".into(),
            });
        }
        if req.value.trim().is_empty() {
            return Err(ConversationError::BadRequest {
                reason: "value must not be empty".into(),
            });
        }
        self.ensure_owned_conversation(user_id, conversation_id).await?;
        if self.runtime_state().is_restarting(conversation_id) {
            return Err(ConversationError::RuntimeRestarting {
                conversation_id: conversation_id.to_owned(),
            });
        }
        let agent = match self.task(conversation_id) {
            Ok(agent) => agent,
            Err(ConversationError::ActiveAgentNotFound { .. }) => {
                return self
                    .set_dormant_config_option(user_id, conversation_id, option_id.trim(), req.value.trim())
                    .await;
            }
            Err(err) => return Err(err),
        };
        let response = match agent.set_config_option(option_id, &req.value).await {
            Ok(response) => response,
            Err(err @ AgentError::Acp(AcpError::NotConnected)) => {
                warn!(
                    conversation_id,
                    option_id,
                    reason = ?AgentKillReason::AgentErrorRecovery,
                    error = %ErrorChain(&err),
                    "ACP config option failed because protocol is disconnected; evicting task"
                );
                self.task_manager()
                    .kill_and_wait(conversation_id, Some(AgentKillReason::AgentErrorRecovery))
                    .await;
                return Err(ConversationError::from(err));
            }
            Err(err) => return Err(ConversationError::from(err)),
        };

        // Mirror runtime model/mode/thought-level switches into the persisted assistant
        // snapshot + preference so the next conversation seeded from this
        // assistant in `auto` mode reflects the latest pick.
        //
        // `PendingNextTurn` counts: it means the agent WILL apply the value from the next
        // turn, so it is the user's settled choice, merely not in force yet. codex reports
        // every mode switch that way (its schema documents `permissions` as "for
        // subsequent turns"), so excluding it would strip preference memory from every
        // codex conversation. `CommandAck` still does NOT count — it means nothing could
        // be established either way. Persistence failures are logged but do not roll back
        // the user-facing config switch.
        if matches!(
            response.confirmation,
            ConfigOptionConfirmation::Observed | ConfigOptionConfirmation::PendingNextTurn
        ) {
            let category = response
                .config_options
                .as_ref()
                .and_then(|options| options.iter().find(|option| option.id == option_id))
                .and_then(|option| option.category.as_deref())
                .unwrap_or(option_id);
            let updates = match category {
                "model" => Some(AssistantRuntimePreferenceUpdate {
                    model: Some(req.value.as_str()),
                    permission: None,
                    thought_level: None,
                }),
                "mode" => Some(AssistantRuntimePreferenceUpdate {
                    model: None,
                    permission: Some(req.value.as_str()),
                    thought_level: None,
                }),
                "thought_level" | "reasoning_effort" => Some(AssistantRuntimePreferenceUpdate {
                    model: None,
                    permission: None,
                    thought_level: Some(req.value.as_str()),
                }),
                _ => None,
            };
            if let Some(updates) = updates {
                if let Err(err) = self
                    .persist_runtime_assistant_snapshot(user_id, conversation_id, updates)
                    .await
                {
                    warn!(
                        conversation_id,
                        option_id,
                        error = %ErrorChain(&err),
                        "Failed to persist runtime assistant snapshot after set_config_option",
                    );
                }
                if let Err(err) = self
                    .persist_runtime_assistant_preferences(user_id, conversation_id, updates)
                    .await
                {
                    warn!(
                        conversation_id,
                        option_id,
                        error = %ErrorChain(&err),
                        "Failed to persist runtime assistant preferences after set_config_option",
                    );
                }
            }
        }

        Ok(response)
    }

    // ── Usage / Slash commands ──────────────────────────────────────

    pub async fn get_usage(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<serde_json::Value>, ConversationError> {
        self.ensure_owned_conversation(user_id, conversation_id).await?;
        // A reaped task must NOT mean "no usage". The indicator's whole point is
        // to survive switching away and back, and the snapshot it needs is
        // already durable in `acp_session.session_config.runtime.context_usage`
        // — `SessionAgentTask::get_usage` reads it from there too. Requiring a
        // live task here made the figure vanish exactly when the user returned
        // to an idle conversation.
        if let Ok(task) = self.task(conversation_id) {
            return task.get_usage().await.map_err(ConversationError::from);
        }
        let state = self
            .acp_session_repo()
            .load_runtime_state_for_user(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load usage state: {e}")))?;
        Ok(state
            .and_then(|s| s.context_usage_json)
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()))
    }

    pub async fn get_slash_commands(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<SlashCommandItem>, ConversationError> {
        self.ensure_owned_conversation(user_id, conversation_id).await?;
        self.task(conversation_id)?
            .get_slash_commands()
            .await
            .map_err(ConversationError::from)
    }

    // ── Side question ───────────────────────────────────────────────

    pub async fn handle_side_question(
        &self,
        user_id: &str,
        conversation_id: &str,
        req: SideQuestionRequest,
    ) -> Result<SideQuestionResponse, ConversationError> {
        self.ensure_owned_conversation(user_id, conversation_id).await?;
        // `AgentInstance::handle_side_question` already validates that the
        // question is non-empty; no need to duplicate the check here.
        self.task(conversation_id)?
            .handle_side_question(req)
            .await
            .map_err(ConversationError::from)
    }

    async fn ensure_owned_conversation(&self, user_id: &str, conversation_id: &str) -> Result<(), ConversationError> {
        let exists = self
            .conversation_repo()
            .get(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load conversation: {e}")))?
            .is_some();
        if exists {
            Ok(())
        } else {
            Err(ConversationError::NotFound {
                id: conversation_id.to_owned(),
            })
        }
    }

    // ── Workspace browsing ──────────────────────────────────────────

    /// Enumerate entries under `query.path` inside the conversation's
    /// workspace root. Enforces workspace isolation (no traversal outside
    /// the root, with an allowance for symlinked sub-directories) and a
    /// depth cap of [`MAX_DIR_DEPTH`].
    pub async fn browse_workspace(
        &self,
        user_id: &str,
        conversation_id: &str,
        query: WorkspaceBrowseQuery,
    ) -> Result<Vec<WorkspaceEntry>, ConversationError> {
        if query.path.trim().is_empty() {
            return Err(ConversationError::BadRequest {
                reason: "path must not be empty".into(),
            });
        }

        let row = self
            .conversation_repo()
            .get(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load conversation: {e}")))?
            .ok_or_else(|| ConversationError::NotFound {
                id: conversation_id.to_owned(),
            })?;

        let extra: serde_json::Value = serde_json::from_str(&row.extra)
            .map_err(|e| ConversationError::internal(format!("Invalid extra JSON: {e}")))?;
        let workspace = extra
            .get("workspace")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_owned();
        if workspace.is_empty() {
            return Err(ConversationError::BadRequest {
                reason: "Conversation has no workspace assigned".into(),
            });
        }

        let relative_path = query.path.trim_start_matches('/');
        let relative_path_obj = std::path::Path::new(relative_path);
        if relative_path_obj
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(ConversationError::BadRequest {
                reason: "Path traversal outside workspace is not allowed".into(),
            });
        }

        // Resolve the browsed path relative to the workspace root
        let base = std::path::Path::new(&workspace);
        let browse_path = if relative_path.is_empty() {
            base.to_path_buf()
        } else {
            base.join(relative_path_obj)
        };

        // Security: reject direct traversal outside the workspace root, but allow
        // symlinked directories mounted inside the workspace (e.g. native skill
        // dirs that point at the builtin skills corpus under data-dir).
        let canonical_base = base
            .canonicalize()
            .map_err(|e| ConversationError::internal(format!("Failed to resolve workspace path: {e}")))?;
        let canonical_browse = browse_path
            .canonicalize()
            .map_err(|_| ConversationError::not_found_reason("Directory not found"))?;
        if !browse_path.starts_with(base) && !canonical_browse.starts_with(&canonical_base) {
            return Err(ConversationError::BadRequest {
                reason: "Path traversal outside workspace is not allowed".into(),
            });
        }

        // Check depth limit
        let depth = relative_path_obj.components().count();
        if depth > MAX_DIR_DEPTH {
            return Err(ConversationError::BadRequest {
                reason: format!("Directory depth exceeds maximum of {MAX_DIR_DEPTH}"),
            });
        }

        let mut entries = Vec::new();
        let mut dir_reader = tokio::fs::read_dir(&canonical_browse)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to read directory: {e}")))?;

        while let Ok(Some(entry)) = dir_reader.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();

            // Apply search filter if provided
            if let Some(ref search) = query.search
                && !search.is_empty()
                && !name.to_lowercase().contains(&search.to_lowercase())
            {
                continue;
            }

            let entry_path = entry.path();
            let metadata = tokio::fs::metadata(&entry_path)
                .await
                .map_err(|e| ConversationError::internal(format!("Failed to read entry metadata: {e}")))?;

            let entry_type = if metadata.is_dir() { "directory" } else { "file" };

            entries.push(WorkspaceEntry {
                name,
                entry_type: entry_type.into(),
            });
        }

        // Sort: directories first, then alphabetically
        entries.sort_by(|a, b| {
            let type_cmp = a.entry_type.cmp(&b.entry_type);
            if type_cmp == std::cmp::Ordering::Equal {
                a.name.to_lowercase().cmp(&b.name.to_lowercase())
            } else {
                type_cmp
            }
        });

        Ok(entries)
    }

    // ── Dormant Config Options Synthesis & Persistence ───────────────

    async fn synthesize_dormant_config_options(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<AcpConfigOptionDto>, ConversationError> {
        let conversation = self
            .conversation_repo()
            .get(user_id, conversation_id)
            .await?
            .ok_or_else(|| ConversationError::NotFound {
                id: conversation_id.to_owned(),
            })?;
        let extra: serde_json::Value =
            serde_json::from_str(&conversation.extra).unwrap_or_else(|_| serde_json::json!({}));

        let acp_row = self
            .acp_session_repo()
            .get_for_user(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load acp_session row: {e}")))?;
        let runtime_state = self
            .acp_session_repo()
            .load_runtime_state_for_user(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load runtime state: {e}")))?;
        let snapshot = self
            .conversation_repo()
            .get_assistant_snapshot(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load assistant snapshot: {e}")))?;

        let agent_id_candidate = acp_row
            .as_ref()
            .map(|row| row.agent_id.trim())
            .filter(|id| !id.is_empty())
            .or_else(|| {
                snapshot
                    .as_ref()
                    .map(|snap| snap.agent_id.trim())
                    .filter(|id| !id.is_empty())
            })
            .or_else(|| non_empty_json_str(&extra, &["agent_id", "agentId"]));

        let mut metadata_row = if let Some(agent_id) = agent_id_candidate {
            self.agent_metadata_repo()
                .get_for_user(user_id, agent_id)
                .await
                .map_err(|e| ConversationError::internal(format!("agent_metadata lookup failed: {e}")))?
        } else {
            None
        };

        if metadata_row.is_none() {
            let backend_candidate = non_empty_json_str(&extra, &["backend"]).or_else(|| {
                let conv_type = conversation.r#type.trim();
                (!conv_type.is_empty() && conv_type != "acp").then_some(conv_type)
            });
            if let Some(backend) = backend_candidate {
                metadata_row = self
                    .agent_metadata_repo()
                    .find_builtin_by_backend_for_user(user_id, backend)
                    .await
                    .map_err(|e| ConversationError::internal(format!("agent_metadata backend lookup failed: {e}")))?;
            }
        }

        let mut options = parse_dormant_catalog_options(metadata_row.as_ref());

        let selections: HashMap<String, String> = runtime_state
            .as_ref()
            .and_then(|state| state.config_selections_json.as_deref())
            .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(raw).ok())
            .unwrap_or_default();

        let persisted_model = selections
            .get("model")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                snapshot
                    .as_ref()
                    .and_then(|snap| snap.resolved_model_id.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                runtime_state
                    .as_ref()
                    .and_then(|state| state.current_model_id.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            })
            .or_else(|| non_empty_json_str(&extra, &["current_model_id", "currentModelId"]).map(ToOwned::to_owned))
            .or_else(|| {
                conversation
                    .model
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            });

        let persisted_mode = selections
            .get("mode")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                runtime_state
                    .as_ref()
                    .and_then(|state| state.current_mode_id.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                non_empty_json_str(
                    &extra,
                    &["current_mode_id", "currentModeId", "session_mode", "sessionMode"],
                )
                .map(ToOwned::to_owned)
            })
            .or_else(|| {
                snapshot
                    .as_ref()
                    .and_then(|snap| snap.resolved_permission_value.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            });

        let persisted_thought = selections
            .get("thought_level")
            .or_else(|| selections.get("reasoning_effort"))
            .or_else(|| selections.get("effort"))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| non_empty_json_str(&extra, &["thought_level", "thoughtLevel"]).map(ToOwned::to_owned))
            .or_else(|| {
                snapshot
                    .as_ref()
                    .and_then(|snap| snap.resolved_thought_level_value.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            });

        for opt in &mut options {
            let category = opt.category.as_deref().unwrap_or(opt.id.as_str());
            let overlaid = selections
                .get(&opt.id)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| match category {
                    "model" => persisted_model.clone(),
                    "mode" => persisted_mode.clone(),
                    "thought_level" | "reasoning_effort" | "effort" => persisted_thought.clone(),
                    _ => None,
                });

            if let Some(val) = overlaid {
                opt.current_value = Some(val);
            } else if opt
                .current_value
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
                && let Some(first) = opt.options.first()
            {
                opt.current_value = Some(first.value.clone());
            }
        }

        Ok(options)
    }

    async fn set_dormant_config_option(
        &self,
        user_id: &str,
        conversation_id: &str,
        option_id: &str,
        value: &str,
    ) -> Result<SetConfigOptionResponse, ConversationError> {
        let mut config_options = self
            .synthesize_dormant_config_options(user_id, conversation_id)
            .await?;

        let Some(target_idx) = config_options.iter().position(|opt| opt.id == option_id) else {
            return Err(ConversationError::BadRequest {
                reason: format!("Unknown config option '{option_id}' for dormant conversation"),
            });
        };

        if !config_options[target_idx].options.is_empty()
            && !config_options[target_idx]
                .options
                .iter()
                .any(|candidate| candidate.value == value)
        {
            return Err(ConversationError::BadRequest {
                reason: format!("Invalid value '{value}' for config option '{option_id}'"),
            });
        }

        let category = config_options[target_idx]
            .category
            .clone()
            .unwrap_or_else(|| option_id.to_owned());

        match category.as_str() {
            "model" => {
                self.update_extra(
                    user_id,
                    conversation_id,
                    serde_json::json!({ "current_model_id": value }),
                )
                .await?;
            }
            "mode" => {
                self.update_extra(
                    user_id,
                    conversation_id,
                    serde_json::json!({ "session_mode": value, "current_mode_id": value }),
                )
                .await?;
            }
            "thought_level" | "reasoning_effort" | "effort" => {
                self.update_extra(
                    user_id,
                    conversation_id,
                    serde_json::json!({ "thought_level": value }),
                )
                .await?;
            }
            _ => {}
        }

        let runtime_state = self
            .acp_session_repo()
            .load_runtime_state_for_user(user_id, conversation_id)
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to load dormant runtime state: {e}")))?;
        let mut selections = runtime_state
            .and_then(|state| state.config_selections_json)
            .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(&raw).ok())
            .unwrap_or_default();
        selections.insert(option_id.to_owned(), value.to_owned());
        if category == "model" && option_id != "model" {
            selections.insert("model".to_owned(), value.to_owned());
        } else if category == "mode" && option_id != "mode" {
            selections.insert("mode".to_owned(), value.to_owned());
        }
        let selections_json = serde_json::to_string(&selections)
            .map_err(|e| ConversationError::internal(format!("Failed to serialize dormant config selections: {e}")))?;
        self.acp_session_repo()
            .save_runtime_state_for_user(
                user_id,
                conversation_id,
                &SaveRuntimeStateParams {
                    current_model_id: (category == "model").then_some(Some(value)),
                    current_mode_id: (category == "mode").then_some(Some(value)),
                    config_selections_json: Some(Some(&selections_json)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| ConversationError::internal(format!("Failed to persist dormant runtime state: {e}")))?;

        let updates = match category.as_str() {
            "model" => Some(AssistantRuntimePreferenceUpdate {
                model: Some(value),
                permission: None,
                thought_level: None,
            }),
            "mode" => Some(AssistantRuntimePreferenceUpdate {
                model: None,
                permission: Some(value),
                thought_level: None,
            }),
            "thought_level" | "reasoning_effort" | "effort" => Some(AssistantRuntimePreferenceUpdate {
                model: None,
                permission: None,
                thought_level: Some(value),
            }),
            _ => None,
        };
        if let Some(updates) = updates {
            if let Err(err) = self
                .persist_runtime_assistant_snapshot(user_id, conversation_id, updates)
                .await
            {
                warn!(
                    conversation_id,
                    option_id,
                    error = %ErrorChain(&err),
                    "Failed to persist runtime assistant snapshot after dormant set_config_option",
                );
            }
            if let Err(err) = self
                .persist_runtime_assistant_preferences(user_id, conversation_id, updates)
                .await
            {
                warn!(
                    conversation_id,
                    option_id,
                    error = %ErrorChain(&err),
                    "Failed to persist runtime assistant preferences after dormant set_config_option",
                );
            }
        }

        config_options[target_idx].current_value = Some(value.to_owned());
        Ok(SetConfigOptionResponse {
            confirmation: ConfigOptionConfirmation::Observed,
            config_options: Some(config_options),
        })
    }
}

fn non_empty_json_str<'a>(value: &'a serde_json::Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    })
}

fn parse_dormant_catalog_options(metadata: Option<&aionui_db::models::AgentMetadataRow>) -> Vec<AcpConfigOptionDto> {
    let Some(metadata) = metadata else {
        return Vec::new();
    };

    let mut options: Vec<AcpConfigOptionDto> = metadata
        .config_options
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|val| {
            let mut arr = if val.is_array() {
                val
            } else {
                val.get("config_options")
                    .or_else(|| val.get("configOptions"))
                    .cloned()?
            };
            normalize_keys_to_snake_case(&mut arr);
            normalize_select_option_groups(&mut arr);
            serde_json::from_value::<Vec<AcpConfigOptionDto>>(arr).ok()
        })
        .unwrap_or_default();

    let has_mode = options
        .iter()
        .any(|opt| opt.id == "mode" || opt.category.as_deref() == Some("mode"));
    if !has_mode
        && let Some(mode_opt) = metadata
            .available_modes
            .as_deref()
            .and_then(parse_fallback_mode_option)
    {
        options.insert(0, mode_opt);
    }

    let has_model = options
        .iter()
        .any(|opt| opt.id == "model" || opt.category.as_deref() == Some("model"));
    if !has_model
        && let Some(model_opt) = metadata
            .available_models
            .as_deref()
            .and_then(parse_fallback_model_option)
    {
        options.push(model_opt);
    }

    options
}

fn normalize_select_option_groups(arr_val: &mut serde_json::Value) {
    let Some(items) = arr_val.as_array_mut() else {
        return;
    };
    for item in items {
        let Some(obj) = item.as_object_mut() else {
            continue;
        };
        if !obj.contains_key("type") {
            obj.insert("type".to_owned(), serde_json::Value::String("select".to_owned()));
        }
        if let Some(raw_opts) = obj.get_mut("options").and_then(serde_json::Value::as_array_mut) {
            let mut flattened = Vec::with_capacity(raw_opts.len());
            for entry in raw_opts.drain(..) {
                if entry.get("value").and_then(serde_json::Value::as_str).is_some() {
                    flattened.push(entry);
                } else if let Some(group_opts) = entry.get("options").and_then(serde_json::Value::as_array) {
                    for sub in group_opts {
                        if sub.get("value").and_then(serde_json::Value::as_str).is_some() {
                            flattened.push(sub.clone());
                        }
                    }
                }
            }
            *raw_opts = flattened;
        }
    }
}

fn parse_fallback_mode_option(raw: &str) -> Option<AcpConfigOptionDto> {
    let mut val = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    normalize_keys_to_snake_case(&mut val);
    let arr = if let Some(a) = val.as_array() {
        a
    } else {
        val.get("available_modes")?.as_array()?
    };
    let select_opts: Vec<AcpConfigSelectOptionDto> = arr
        .iter()
        .filter_map(|entry| {
            let value = non_empty_json_str(entry, &["id", "value"])?.to_owned();
            let name = non_empty_json_str(entry, &["name", "label"]).map(ToOwned::to_owned);
            let description = non_empty_json_str(entry, &["description"]).map(ToOwned::to_owned);
            Some(AcpConfigSelectOptionDto {
                value,
                label: name.clone(),
                name,
                description,
            })
        })
        .collect();
    if select_opts.is_empty() {
        return None;
    }
    let current_value = non_empty_json_str(&val, &["current_mode_id"])
        .map(ToOwned::to_owned)
        .or_else(|| Some(select_opts[0].value.clone()));
    Some(AcpConfigOptionDto {
        id: "mode".to_owned(),
        name: Some("Mode".to_owned()),
        label: Some("Mode".to_owned()),
        description: None,
        category: Some("mode".to_owned()),
        option_type: "select".to_owned(),
        current_value,
        options: select_opts,
    })
}

fn parse_fallback_model_option(raw: &str) -> Option<AcpConfigOptionDto> {
    let mut val = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    normalize_keys_to_snake_case(&mut val);
    let arr = if let Some(a) = val.as_array() {
        a
    } else {
        val.get("available_models")?.as_array()?
    };
    let select_opts: Vec<AcpConfigSelectOptionDto> = arr
        .iter()
        .filter_map(|entry| {
            let value = non_empty_json_str(entry, &["id", "model_id", "value"])?.to_owned();
            let label = non_empty_json_str(entry, &["label", "name"]).map(ToOwned::to_owned);
            let description = non_empty_json_str(entry, &["description"]).map(ToOwned::to_owned);
            Some(AcpConfigSelectOptionDto {
                value,
                name: label.clone(),
                label,
                description,
            })
        })
        .collect();
    if select_opts.is_empty() {
        return None;
    }
    let current_value = non_empty_json_str(&val, &["current_model_id"])
        .map(ToOwned::to_owned)
        .or_else(|| Some(select_opts[0].value.clone()));
    Some(AcpConfigOptionDto {
        id: "model".to_owned(),
        name: Some("Model".to_owned()),
        label: Some("Model".to_owned()),
        description: None,
        category: Some("model".to_owned()),
        option_type: "select".to_owned(),
        current_value,
        options: select_opts,
    })
}

#[cfg(test)]
mod dormant_config_options_tests {
    use super::*;
    use std::sync::Arc;

    use aionui_ai_agent::agent_task::AgentInstance;
    use aionui_ai_agent::types::BuildTaskOptions;
    use aionui_ai_agent::{AgentError, IWorkerTaskManager};
    use aionui_api_types::{
        ConfigOptionConfirmation, CreateConversationRequest, SetConfigOptionRequest, WebSocketMessage,
    };
    use aionui_common::{AgentKillReason, TimestampMs};
    use aionui_db::{
        IAcpSessionRepository, IAgentMetadataRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository,
        SqliteConversationRepository, init_database_memory,
    };
    use aionui_realtime::EventBroadcaster;

    struct NoopBroadcaster;
    impl EventBroadcaster for NoopBroadcaster {
        fn broadcast(&self, _message: WebSocketMessage<serde_json::Value>) {}
    }

    struct EmptyTaskManager;
    #[async_trait::async_trait]
    impl IWorkerTaskManager for EmptyTaskManager {
        fn get_task(&self, _id: &str) -> Option<AgentInstance> {
            None
        }
        async fn get_or_build_task(
            &self,
            _id: &str,
            _opts: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::bad_gateway("dormant slot"))
        }
        fn kill(&self, _id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }
        fn kill_and_wait(
            &self,
            _id: &str,
            _reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            Box::pin(std::future::ready(()))
        }
        async fn clear(&self) {}
        fn active_count(&self) -> usize {
            0
        }
        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn dormant_get_and_put_config_options_normalize_snake_case_and_persist_to_acp_session() {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        sqlx::query("INSERT OR IGNORE INTO users (id, username, password_hash, created_at, updated_at) VALUES ('u1', 'dormant_test_user', 'h', 1, 1)")
            .execute(&pool)
            .await
            .unwrap();

        let catalog_json = serde_json::json!([
            {
                "id": "model",
                "name": "Model",
                "category": "model",
                "type": "select",
                "currentValue": "gpt-5.3-codex",
                "options": [
                    { "value": "gpt-5.3-codex", "name": "GPT-5.3 Codex" },
                    { "value": "gpt-5.4", "name": "GPT-5.4" }
                ]
            },
            {
                "id": "mode",
                "name": "Mode",
                "category": "mode",
                "type": "select",
                "currentValue": "default",
                "options": [
                    { "value": "default", "name": "Default" },
                    { "value": "yolo", "name": "Full Auto" }
                ]
            }
        ])
        .to_string();

        sqlx::query("UPDATE agent_metadata SET config_options = ? WHERE backend = 'codex'")
            .bind(&catalog_json)
            .execute(&pool)
            .await
            .unwrap();

        let conv_repo = Arc::new(SqliteConversationRepository::new(pool.clone()));
        let agent_meta_repo = Arc::new(SqliteAgentMetadataRepository::new(pool.clone()));
        let acp_session_repo = Arc::new(SqliteAcpSessionRepository::new(pool.clone()));

        assert!(agent_meta_repo.list_all().await.unwrap().iter().any(|r| r.backend.as_deref() == Some("codex")));

        let svc = ConversationService::new(
            std::env::temp_dir(),
            Arc::new(NoopBroadcaster),
            Arc::new(crate::skill_resolver::FixedSkillResolver { names: vec![] }),
            Arc::new(EmptyTaskManager),
            conv_repo,
            agent_meta_repo,
            acp_session_repo.clone(),
        );

        let tmp = tempfile::tempdir().unwrap();
        let create_req: CreateConversationRequest = serde_json::from_value(serde_json::json!({
            "type": "acp",
            "name": "Dormant Worker",
            "extra": {
                "backend": "codex",
                "workspace": tmp.path().to_string_lossy(),
                "currentModelId": "gpt-5.3-codex"
            }
        }))
        .unwrap();
        let created = svc.create("u1", create_req).await.unwrap();

        // 1. Dormant GET returns 200 OK with normalized `current_value`
        let got = svc.get_config_options("u1", &created.id).await.unwrap();
        assert_eq!(got.config_options.len(), 2);
        let model_opt = got.config_options.iter().find(|o| o.id == "model").unwrap();
        assert_eq!(model_opt.current_value.as_deref(), Some("gpt-5.3-codex"));

        // 2. Dormant PUT validates and persists to `acp_session` + `conversations.extra`
        let updated = svc
            .set_config_option(
                "u1",
                &created.id,
                "model",
                SetConfigOptionRequest {
                    value: "gpt-5.4".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.confirmation, ConfigOptionConfirmation::Observed);
        let updated_model = updated
            .config_options
            .as_ref()
            .unwrap()
            .iter()
            .find(|o| o.id == "model")
            .unwrap();
        assert_eq!(updated_model.current_value.as_deref(), Some("gpt-5.4"));

        let session_state = acp_session_repo
            .load_runtime_state_for_user("u1", &created.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session_state.current_model_id.as_deref(), Some("gpt-5.4"));

        // 3. Dormant PUT rejects unknown value with BadRequest (400)
        let bad = svc
            .set_config_option(
                "u1",
                &created.id,
                "model",
                SetConfigOptionRequest {
                    value: "nonexistent-model".into(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(bad, ConversationError::BadRequest { .. }));
    }
}

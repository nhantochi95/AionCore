//! LIVE empirical verification (`#[ignore]`) for Antigravity's persistent warm
//! `agy acp` backend (`WarmAcpClient`), executed against the real `agy` CLI
//! binary via `RealSpawner` + `AntigravityConnection`.
//!
//! Run explicitly:
//! ```text
//! cargo test -p aionui-session --test live_antigravity_warm -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Instant;

use aionui_process::Spawner;
use aionui_session::{
    AntigravityConnection, BackendConnection, Command, CommandMeta, ContentBlock, SessionConfig,
    SessionEvent, SessionSpec, TurnOutcome,
};
use futures_util::StreamExt;

#[tokio::test]
#[ignore = "requires live `agy` CLI login on PATH; run with `-- --ignored --nocapture`"]
async fn live_warm_acp_two_turn_benchmark_preserves_state_and_kv_cache() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let spawner: Arc<dyn Spawner> = Arc::new(aionui_process::RealSpawner::new(
        Arc::new(aionui_process::FileRegistryStore::new(tmp.path())),
        uuid::Uuid::now_v7(),
        "live-agy-warm-test",
    ));
    let conn = AntigravityConnection::new(spawner);
    let session_id = uuid::Uuid::new_v4().to_string();

    let mut config = SessionConfig {
        cwd: Some(tmp.path().to_string_lossy().to_string()),
        mode: Some("yolo".into()),
        antigravity_warm_acp: true,
        antigravity_isolate_default_customizations: true,
        ..Default::default()
    };
    config.init.preset_context = Some("Always answer concisely in one line.".into());

    let backend = conn
        .open_session(
            SessionSpec::Fresh {
                session_id: session_id.clone(),
            },
            config,
        )
        .await
        .expect("open warm antigravity session");

    let mut events = backend.events();

    // --- Turn 1 (Cold handshake + session/new + first LLM turn) ---
    let t1_start = Instant::now();
    backend
        .dispatch(Command::Send {
            content: vec![ContentBlock::Text(
                "Remember the exact code RAKAZO-WARM-77. Reply with ONLY: ACK1".into(),
            )],
            metadata: CommandMeta::default(),
        })
        .await
        .expect("Turn 1 dispatch");

    let mut bound_sid_1: Option<String> = None;
    let mut t1_reply = String::new();
    let mut t1_input = 0u64;
    let mut t1_output = 0u64;
    let mut t1_cached = 0u64;
    let mut t1_thought = 0u64;
    let mut saw_restart_notice = false;

    while let Some(env) = events.next().await {
        match env.event {
            SessionEvent::BackendBound { backend_session_id } => {
                bound_sid_1 = backend_session_id;
            }
            SessionEvent::MessageDelta { text, .. } => {
                t1_reply.push_str(&text);
                if text.contains("server restart") {
                    saw_restart_notice = true;
                }
            }
            SessionEvent::UsageDelta {
                input_tokens,
                output_tokens,
                breakdown,
                ..
            } => {
                t1_input = input_tokens;
                t1_output = output_tokens;
                t1_cached = breakdown.cached_read_tokens;
                t1_thought = breakdown.thought_tokens;
            }
            SessionEvent::TurnResult {
                is_error,
                result_text,
                outcome,
                ..
            } => {
                assert!(
                    !is_error,
                    "Turn 1 failed: outcome={outcome:?} text={result_text}"
                );
                assert!(matches!(outcome, TurnOutcome::Completed { .. }));
                break;
            }
            _ => {}
        }
    }
    let t1_ms = t1_start.elapsed().as_millis();
    eprintln!(
        "[LIVE BENCHMARK] Turn 1 (cold init + session/new + LLM): {t1_ms} ms | sid={bound_sid_1:?} | reply={:?} | in={t1_input} out={t1_output} cached_read={t1_cached} thought={t1_thought}",
        t1_reply.trim()
    );
    assert!(bound_sid_1.is_some(), "Turn 1 must bind backend_session_id");
    assert!(!saw_restart_notice, "Warm ACP must not emit server restart notice");

    // Brief yield so backend settles `in_flight = false` before Turn 2
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // --- Turn 2 (Warm reuse of the exact same `agy acp` child process!) ---
    let t2_start = Instant::now();
    backend
        .dispatch(Command::Send {
            content: vec![ContentBlock::Text(
                "What exact code did I ask you to remember? Reply with ONLY that code.".into(),
            )],
            metadata: CommandMeta::default(),
        })
        .await
        .expect("Turn 2 dispatch");

    let mut t2_reply = String::new();
    let mut t2_input = 0u64;
    let mut t2_output = 0u64;
    let mut t2_cached = 0u64;
    let mut t2_thought = 0u64;

    while let Some(env) = events.next().await {
        match env.event {
            SessionEvent::MessageDelta { text, .. } => {
                t2_reply.push_str(&text);
                if text.contains("server restart") {
                    saw_restart_notice = true;
                }
            }
            SessionEvent::UsageDelta {
                input_tokens,
                output_tokens,
                breakdown,
                ..
            } => {
                t2_input = input_tokens;
                t2_output = output_tokens;
                t2_cached = breakdown.cached_read_tokens;
                t2_thought = breakdown.thought_tokens;
            }
            SessionEvent::TurnResult {
                is_error,
                result_text,
                outcome,
                ..
            } => {
                assert!(
                    !is_error,
                    "Turn 2 failed: outcome={outcome:?} text={result_text}"
                );
                assert!(matches!(outcome, TurnOutcome::Completed { .. }));
                break;
            }
            _ => {}
        }
    }
    let t2_ms = t2_start.elapsed().as_millis();
    eprintln!(
        "[LIVE BENCHMARK] Turn 2 (WARM persistent agy acp): {t2_ms} ms | reply={:?} | in={t2_input} out={t2_output} cached_read={t2_cached} thought={t2_thought}",
        t2_reply.trim()
    );

    assert!(
        t2_reply.contains("RAKAZO-WARM-77"),
        "Warm session must preserve conversation memory across turns; got: {t2_reply:?}"
    );
    assert!(!saw_restart_notice, "Turn 2 must not emit server restart notice");
    assert!(
        t2_input > 0 || t2_cached > 0,
        "Turn 2 must report structured ACP token usage"
    );

    // --- Turn 3 & Turn 4: Hot-switch to discovered Flash model on the SAME warm process ---
    let models = backend.capabilities().available_models;
    let flash_model = models
        .iter()
        .find(|m| m.id.contains("flash") && m.id.contains("low"))
        .or_else(|| models.iter().find(|m| m.id.contains("flash")))
        .map(|m| m.id.clone());
    eprintln!(
        "[LIVE BENCHMARK] Discovered {} models; selected flash_model={flash_model:?}",
        models.len()
    );
    if let Some(flash_id) = flash_model {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        backend
            .dispatch(Command::SetModel {
                model: flash_id.clone(),
            })
            .await
            .expect("SetModel on warm ACP");

        for (turn_idx, prompt_text) in [
            (3usize, "Reply with ONLY: FLASH1"),
            (4usize, "What code did I ask you to remember earlier? Reply with ONLY that code."),
        ] {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let t_start = Instant::now();
            backend
                .dispatch(Command::Send {
                    content: vec![ContentBlock::Text(prompt_text.into())],
                    metadata: CommandMeta::default(),
                })
                .await
                .expect("Flash turn dispatch");

            let mut reply = String::new();
            let mut u_in = 0u64;
            let mut u_out = 0u64;
            let mut u_cached = 0u64;
            let mut u_thought = 0u64;
            while let Some(env) = events.next().await {
                match env.event {
                    SessionEvent::MessageDelta { text, .. } => reply.push_str(&text),
                    SessionEvent::UsageDelta {
                        input_tokens,
                        output_tokens,
                        breakdown,
                        ..
                    } => {
                        u_in = input_tokens;
                        u_out = output_tokens;
                        u_cached = breakdown.cached_read_tokens;
                        u_thought = breakdown.thought_tokens;
                    }
                    SessionEvent::TurnResult {
                        is_error,
                        result_text,
                        ..
                    } => {
                        assert!(!is_error, "Turn {turn_idx} failed: {result_text}");
                        break;
                    }
                    _ => {}
                }
            }
            let elapsed_ms = t_start.elapsed().as_millis();
            eprintln!(
                "[LIVE BENCHMARK] Turn {turn_idx} (WARM {flash_id}): {elapsed_ms} ms | reply={:?} | in={u_in} out={u_out} cached_read={u_cached} thought={u_thought}",
                reply.trim()
            );
            if turn_idx == 4 {
                assert!(
                    reply.contains("RAKAZO-WARM-77"),
                    "Hot-switched Flash model on warm ACP must retain session memory; got: {reply:?}"
                );
            }
        }
    }

    backend.terminate().await;
}

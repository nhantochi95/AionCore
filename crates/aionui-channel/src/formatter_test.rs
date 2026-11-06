use super::*;

#[test]
fn slack_escapes_special_chars() {
    let out = format_text_for_platform("a < b & c > d", PluginType::Slack);
    assert_eq!(out, "a &lt; b &amp; c &gt; d");
}

#[test]
fn slack_converts_bold() {
    assert_eq!(format_text_for_platform("**bold**", PluginType::Slack), "*bold*");
    assert_eq!(format_text_for_platform("__bold__", PluginType::Slack), "*bold*");
}

#[test]
fn slack_converts_strikethrough() {
    assert_eq!(format_text_for_platform("~~gone~~", PluginType::Slack), "~gone~");
}

#[test]
fn slack_converts_link() {
    assert_eq!(
        format_text_for_platform("[docs](https://example.com)", PluginType::Slack),
        "<https://example.com|docs>"
    );
}

#[test]
fn slack_leaves_inline_code_untouched() {
    assert_eq!(format_text_for_platform("`code`", PluginType::Slack), "`code`");
}

#[test]
fn slack_combined() {
    let out = format_text_for_platform("See **[link](https://x.io)** now", PluginType::Slack);
    assert_eq!(out, "See *<https://x.io|link>* now");
}

#[test]
fn slack_not_plain_escape_fallback() {
    let out = format_text_for_platform("**x**", PluginType::Slack);
    assert_ne!(out, "**x**");
    assert_eq!(out, "*x*");
}

#[test]
fn discord_passes_markdown_through_unchanged() {
    let input = "**bold** _i_ `code` <@123> a < b & c";
    assert_eq!(format_text_for_platform(input, PluginType::Discord), input);
}

#[test]
fn telegram_converts_gfm_table_and_preserves_snake_case_code() {
    let md = "\
### Đề xuất đội hình
| Vai trò | Assistant | Nhiệm vụ |
| --- | --- | --- |
| Backend | `claude_code` | Viết `team_notify_user` |
| QA | `codex_cli` | Chạy `cargo_test` |";

    let html = format_text_for_platform(md, PluginType::Telegram);
    assert!(html.contains("<b>Đề xuất đội hình</b>"));
    assert!(html.contains("▸ <b>Vai trò:</b> Backend"));
    assert!(html.contains("<code>claude_code</code>"));
    assert!(html.contains("<code>team_notify_user</code>"));
    assert!(!html.contains("| --- |"));
}

#[test]
fn telegram_chunking_balances_pre_code_blocks() {
    let long_code = "let x = 1;\n".repeat(60);
    let html = markdown_to_telegram_html(&format!("Intro\n```rust\n{long_code}```\nOutro"));
    let chunks = chunk_telegram_html(&html, 300);
    assert!(chunks.len() >= 2);
    for chunk in &chunks {
        assert_eq!(
            chunk.matches("<pre><code>").count(),
            chunk.matches("</code></pre>").count(),
            "unbalanced <pre><code> in chunk: {chunk}"
        );
    }
}

#[test]
fn torture_telegram_12k_vietnamese_emojis_unspaced_unicode_and_xss_escaping() {
    let vi_para = "**Báo cáo tiến độ dự án NhanTown 👑🪑:** Hệ thống đã xử lý thành công `<script>alert('xss') & drop()</script>` và kiểm tra độ ổn định UTF-8 tiếng Việt (ế, ộ, ư, đ).\n\n";
    let huge_md = format!(
        "{}{}",
        vi_para.repeat(75),
        "👑ếộưđ".repeat(900) // Continuous unspaced 3- and 4-byte UTF-8 sequence!
    );
    assert!(huge_md.chars().count() > 12_000);

    let html = markdown_to_telegram_html(&huge_md);
    assert!(!html.contains("<script>"), "raw <script> tag must be escaped");
    assert!(html.contains("&lt;script&gt;"));

    let chunks = chunk_telegram_html(&html, TELEGRAM_SAFE_CHUNK_CHARS);
    assert!(chunks.len() >= 4);
    for (idx, chunk) in chunks.iter().enumerate() {
        assert!(
            chunk.chars().count() <= TELEGRAM_SAFE_CHUNK_CHARS,
            "chunk #{idx} exceeded safe char limit: {}",
            chunk.chars().count()
        );
        assert_eq!(
            chunk.matches("<b>").count(),
            chunk.matches("</b>").count(),
            "unbalanced <b> in chunk #{idx}"
        );
        assert_eq!(
            chunk.matches("<code>").count(),
            chunk.matches("</code>").count(),
            "unbalanced <code> in chunk #{idx}"
        );
    }
}

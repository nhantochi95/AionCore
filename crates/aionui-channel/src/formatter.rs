use std::sync::LazyLock;

use regex::Regex;

use crate::types::PluginType;

/// Safe chunk threshold for Telegram HTML messages (below Telegram's 4096-char hard limit).
pub const TELEGRAM_SAFE_CHUNK_CHARS: usize = 3800;

/// Threshold above which a report is better attached as a `.md` document alongside a summary.
pub const TELEGRAM_DOCUMENT_THRESHOLD_CHARS: usize = 8000;

/// Convert text to the target IM platform format.
///
/// - Telegram: convert GFM tables to mobile cards, protect code spans, escape HTML, then convert markdown → HTML tags
/// - Lark/DingTalk: convert HTML tags → markdown
/// - Slack: escape `&<>`, then convert markdown → Slack mrkdwn
/// - Discord: pass markdown through unchanged (Discord renders it natively;
///   accidental @mentions are suppressed at send time via `allowed_mentions`)
/// - WeChat: strip all HTML
pub fn format_text_for_platform(text: &str, platform: PluginType) -> String {
    match platform {
        PluginType::Telegram => markdown_to_telegram_html(text),
        PluginType::Lark | PluginType::Dingtalk => html_to_markdown(text),
        PluginType::Slack => markdown_to_slack_mrkdwn(text),
        PluginType::Discord => text.to_string(),
        PluginType::Weixin => strip_html(text),
    }
}

/// Strip Telegram HTML tags and decode entities back to plain text (used as automatic
/// fallback if Telegram's Bot API rejects an HTML payload with entity parse errors).
pub fn strip_telegram_html_to_plain(html: &str) -> String {
    strip_html(html)
}

/// Convert GFM pipe tables outside fenced code blocks into mobile-friendly bullet cards.
pub fn convert_gfm_tables_to_cards(text: &str) -> String {
    let mut out_lines: Vec<String> = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut idx = 0;
    let mut in_fence = false;

    while idx < lines.len() {
        let trimmed = lines[idx].trim();
        if trimmed.starts_with("```") {
            in_fence = !in_fence;
            out_lines.push(lines[idx].to_owned());
            idx += 1;
            continue;
        }

        if !in_fence
            && idx + 1 < lines.len()
            && is_pipe_row(lines[idx])
            && is_table_separator_row(lines[idx + 1])
        {
            let headers = split_pipe_cells(lines[idx]);
            idx += 2;
            let mut rows: Vec<Vec<String>> = Vec::new();
            while idx < lines.len() && is_pipe_row(lines[idx]) && !lines[idx].trim().starts_with("```") {
                if !is_table_separator_row(lines[idx]) {
                    rows.push(split_pipe_cells(lines[idx]));
                }
                idx += 1;
            }

            if !headers.is_empty() && !rows.is_empty() {
                for row in rows {
                    if row.iter().all(|c| c.is_empty()) {
                        continue;
                    }
                    let primary = row.first().map(String::as_str).unwrap_or("");
                    let head_label = headers.first().map(String::as_str).unwrap_or("");
                    if !primary.is_empty() {
                        if head_label.is_empty() {
                            out_lines.push(format!("▸ **{primary}**"));
                        } else {
                            out_lines.push(format!("▸ **{head_label}:** {primary}"));
                        }
                    }
                    for (col_idx, cell) in row.iter().enumerate().skip(1) {
                        if cell.is_empty() {
                            continue;
                        }
                        let col_name = headers.get(col_idx).map(String::as_str).unwrap_or("Info");
                        out_lines.push(format!("  • **{col_name}:** {cell}"));
                    }
                }
                continue;
            }
        }

        out_lines.push(lines[idx].to_owned());
        idx += 1;
    }

    out_lines.join("\n")
}

fn is_pipe_row(line: &str) -> bool {
    let t = line.trim();
    t.contains('|') && (t.starts_with('|') || t.ends_with('|') || t.matches('|').count() >= 2)
}

fn is_table_separator_row(line: &str) -> bool {
    let cells = split_pipe_cells(line);
    !cells.is_empty()
        && cells.iter().all(|cell| {
            let c = cell.trim();
            !c.is_empty() && c.chars().all(|ch| matches!(ch, '-' | ':' | ' ')) && c.contains('-')
        })
}

fn split_pipe_cells(line: &str) -> Vec<String> {
    let mut trimmed = line.trim();
    if let Some(rest) = trimmed.strip_prefix('|') {
        trimmed = rest;
    }
    if let Some(rest) = trimmed.strip_suffix('|') {
        trimmed = rest;
    }
    trimmed.split('|').map(|c| c.trim().to_owned()).collect()
}

/// Paginate a formatted Telegram message into chunks of at most `max_chars` Unicode scalar values,
/// preferring paragraph/line boundaries and keeping `<pre><code>...</code></pre>` balanced.
pub fn chunk_telegram_html(html: &str, max_chars: usize) -> Vec<String> {
    let limit = max_chars.max(200);
    if html.chars().count() <= limit {
        return vec![html.to_owned()];
    }

    let mut chunks = Vec::new();
    let mut remaining = html.to_owned();

    while !remaining.is_empty() {
        if remaining.chars().count() <= limit {
            chunks.push(remaining);
            break;
        }

        // Reserve 48 chars in case we need to close/reopen `<pre><code>` or inline tags.
        let effective_limit = limit.saturating_sub(48);
        let split_byte = find_safe_split_byte(&remaining, effective_limit);
        let mut head = remaining[..split_byte].to_owned();
        let mut tail = remaining[split_byte..].trim_start_matches('\n').to_owned();

        // Balance `<pre><code>` or inline tags (`<b>`, `<i>`, `<code>`) across chunk boundary.
        let open_pre = head.matches("<pre><code>").count();
        let close_pre = head.matches("</code></pre>").count();
        if open_pre > close_pre {
            head.push_str("</code></pre>");
            tail = format!("<pre><code>{tail}");
        } else {
            for (open_tag, close_tag) in [("<b>", "</b>"), ("<i>", "</i>"), ("<code>", "</code>")] {
                if head.matches(open_tag).count() > head.matches(close_tag).count() {
                    head.push_str(close_tag);
                    tail = format!("{open_tag}{tail}");
                }
            }
        }

        chunks.push(head);
        remaining = tail;
    }

    chunks
}

fn find_safe_split_byte(text: &str, max_chars: usize) -> usize {
    let mut last_byte = text.len();
    for (count, (byte_idx, _)) in text.char_indices().enumerate() {
        if count == max_chars {
            last_byte = byte_idx;
            break;
        }
    }

    let window = &text[..last_byte];
    // Avoid splitting inside an HTML entity (`&lt;`) or HTML tag (`<a href="...">`).
    let safe_end = avoid_mid_tag_or_entity(window);
    let candidate = &text[..safe_end];

    if let Some(pos) = candidate.rfind("\n\n")
        && pos > safe_end / 3
    {
        return pos;
    }
    if let Some(pos) = candidate.rfind('\n')
        && pos > safe_end / 4
    {
        return pos;
    }
    if let Some(pos) = candidate.rfind(' ')
        && pos > safe_end / 2
    {
        return pos;
    }
    if safe_end == 0 {
        text.chars().next().map_or(0, char::len_utf8)
    } else {
        safe_end
    }
}

fn avoid_mid_tag_or_entity(window: &str) -> usize {
    if let Some(lt_pos) = window.rfind('<') {
        let after_lt = &window[lt_pos..];
        if !after_lt.contains('>') && lt_pos > 0 {
            return lt_pos;
        }
    }
    if let Some(amp_pos) = window.rfind('&') {
        let after_amp = &window[amp_pos..];
        if after_amp.len() <= 8 && !after_amp.contains(';') && amp_pos > 0 {
            return amp_pos;
        }
    }
    window.len()
}

// ── Slack (mrkdwn) ───────────────────────────────────────────────

static RE_MD_LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap());
static RE_MD_BOLD_STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*\*(.+?)\*\*").unwrap());
static RE_MD_BOLD_UNDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"__(.+?)__").unwrap());
static RE_MD_STRIKE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"~~(.+?)~~").unwrap());

/// Convert standard markdown to Slack mrkdwn.
fn markdown_to_slack_mrkdwn(text: &str) -> String {
    let s = escape_html(text);
    let s = RE_MD_LINK.replace_all(&s, "<$2|$1>");
    let s = RE_MD_BOLD_STAR.replace_all(&s, "*$1*");
    let s = RE_MD_BOLD_UNDER.replace_all(&s, "*$1*");
    let s = RE_MD_STRIKE.replace_all(&s, "~$1~");
    s.into_owned()
}

// ── Telegram ─────────────────────────────────────────────────────

static RE_CODE_BLOCK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"```(?:\w*)\r?\n?([\s\S]*?)```").unwrap());
static RE_INLINE_CODE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`([^`\n]+)`").unwrap());
static RE_HEADING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?m)^#{1,6}\s+(.+)$").unwrap());
static RE_BOLD_STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*\*(.+?)\*\*").unwrap());
static RE_BOLD_UNDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"__(.+?)__").unwrap());
static RE_ITALIC_STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*([^\*\n]+?)\*").unwrap());
static RE_LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap());

pub fn markdown_to_telegram_html(text: &str) -> String {
    let table_normalized = convert_gfm_tables_to_cards(text);

    // Extract fenced code blocks and inline code spans BEFORE running bold/italic/link
    // formatting so snake_case identifiers (`team_notify_user`, `slot_id`) inside code
    // never produce overlapping HTML tags.
    let mut code_blocks: Vec<String> = Vec::new();
    let with_block_tokens = RE_CODE_BLOCK
        .replace_all(&table_normalized, |caps: &regex::Captures| {
            let idx = code_blocks.len();
            code_blocks.push(format!("<pre><code>{}</code></pre>", escape_html(&caps[1])));
            format!("\u{0000}CB{idx}\u{0000}")
        })
        .into_owned();

    let mut inline_codes: Vec<String> = Vec::new();
    let with_inline_tokens = RE_INLINE_CODE
        .replace_all(&with_block_tokens, |caps: &regex::Captures| {
            let idx = inline_codes.len();
            inline_codes.push(format!("<code>{}</code>", escape_html(&caps[1])));
            format!("\u{0000}IC{idx}\u{0000}")
        })
        .into_owned();

    let s = escape_html(&with_inline_tokens);
    let s = RE_HEADING.replace_all(&s, "<b>$1</b>");
    let s = RE_BOLD_STAR.replace_all(&s, "<b>$1</b>");
    let s = RE_BOLD_UNDER.replace_all(&s, "<b>$1</b>");
    let s = RE_ITALIC_STAR.replace_all(&s, "<i>$1</i>");
    let s = RE_LINK.replace_all(&s, r#"<a href="$2">$1</a>"#);

    let mut restored = s.into_owned();
    for (idx, html) in inline_codes.iter().enumerate() {
        let token = format!("\u{0000}IC{idx}\u{0000}");
        restored = restored.replace(&token, html);
    }
    for (idx, html) in code_blocks.iter().enumerate() {
        let token = format!("\u{0000}CB{idx}\u{0000}");
        restored = restored.replace(&token, html);
    }

    restored
}

// ── Lark / DingTalk ──────────────────────────────────────────────

static RE_PRE_CODE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<pre><code[^>]*>([\s\S]*?)</code></pre>").unwrap());
static RE_HTML_CODE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<code>([^<]+)</code>").unwrap());
static RE_HTML_B: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<b>([\s\S]*?)</b>").unwrap());
static RE_HTML_STRONG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<strong>([\s\S]*?)</strong>").unwrap());
static RE_HTML_I: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<i>([\s\S]*?)</i>").unwrap());
static RE_HTML_EM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<em>([\s\S]*?)</em>").unwrap());
static RE_HTML_SAFE_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"<a\s+href="((?:https?://|mailto:|/)[^"]*)"[^>]*>([^<]*)</a>"#).unwrap());
static RE_HTML_TAG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<[^>]+>").unwrap());

fn html_to_markdown(text: &str) -> String {
    let s = decode_safe_entities(text);
    let s = RE_PRE_CODE.replace_all(&s, "```\n$1```");
    let s = RE_HTML_CODE.replace_all(&s, "`$1`");
    let s = RE_HTML_B.replace_all(&s, "**$1**");
    let s = RE_HTML_STRONG.replace_all(&s, "**$1**");
    let s = RE_HTML_I.replace_all(&s, "*$1*");
    let s = RE_HTML_EM.replace_all(&s, "*$1*");
    let s = RE_HTML_SAFE_LINK.replace_all(&s, "[$2]($1)");
    strip_tags_loop(s.as_ref())
}

// ── WeChat ───────────────────────────────────────────────────────

fn strip_html(text: &str) -> String {
    let s = strip_tags_loop(text);
    let s = decode_all_entities(&s);
    s.replace(['<', '>'], "")
}

// ── Helpers ──────────────────────────────────────────────────────

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn strip_tags_loop(text: &str) -> String {
    let mut result = text.to_owned();
    loop {
        let stripped = RE_HTML_TAG.replace_all(&result, "");
        if stripped == result {
            break;
        }
        result = stripped.into_owned();
    }
    result
}

/// Decode only safe entities (quotes, numeric). Never decode &lt;/&gt;/&amp;
/// to prevent tag injection in Lark/DingTalk output.
fn decode_safe_entities(text: &str) -> String {
    static RE_HEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"&#x([0-9a-fA-F]+);").unwrap());
    static RE_DEC: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"&#(\d+);").unwrap());

    let s = text.replace("&quot;", "\"");
    let s = s.replace("&#39;", "'");
    let s = s.replace("&apos;", "'");
    let s = RE_HEX.replace_all(&s, |caps: &regex::Captures| {
        u32::from_str_radix(&caps[1], 16)
            .ok()
            .and_then(char::from_u32)
            .map(|c| c.to_string())
            .unwrap_or_else(|| caps[0].to_owned())
    });
    let s = RE_DEC.replace_all(&s, |caps: &regex::Captures| {
        caps[1]
            .parse::<u32>()
            .ok()
            .and_then(char::from_u32)
            .map(|c| c.to_string())
            .unwrap_or_else(|| caps[0].to_owned())
    });
    s.into_owned()
}

/// Decode all common HTML entities (for WeChat / plain-text fallback).
fn decode_all_entities(text: &str) -> String {
    let s = decode_safe_entities(text);
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
}

#[cfg(test)]
#[path = "formatter_test.rs"]
mod formatter_test;

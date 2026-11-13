use std::sync::LazyLock;

use regex::Regex;
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use tracing::{debug, warn};

use crate::error::ChannelError;

use super::types::{
    AnswerCallbackQueryRequest, EditMessageTextRequest, ReplyMarkup, SendMessageRequest, TgFile, TgMessage,
    TgResponse, TgUpdate, TgUser,
};

const TELEGRAM_API_BASE: &str = "https://api.telegram.org";
/// Maximum incoming Telegram file download size (25 MiB).
const MAX_TELEGRAM_DOWNLOAD_BYTES: usize = 25 * 1024 * 1024;

static BOTFATHER_TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:bot)?\d{6,15}(?::|%3[Aa])[A-Za-z0-9_-]{30,64}")
        .expect("valid BotFather token redaction regex")
});

#[derive(Debug, Clone)]
pub(crate) struct SendMediaMultipartParams {
    pub chat_id: i64,
    pub message_thread_id: Option<i64>,
    pub file_name: String,
    pub bytes: axum::body::Bytes,
    pub mime_type: Option<String>,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub reply_to_message_id: Option<i64>,
    pub reply_markup: Option<ReplyMarkup>,
    pub disable_notification: Option<bool>,
}

/// HTTP client for the Telegram Bot API.
pub(crate) struct TelegramApi {
    client: Client,
    bot_token: String,
    base_url: String,
    file_base_url: String,
}

impl TelegramApi {
    /// Create a new API client for the given bot token.
    pub fn new(client: Client, token: &str) -> Self {
        Self {
            client,
            bot_token: token.to_owned(),
            base_url: format!("{TELEGRAM_API_BASE}/bot{token}"),
            file_base_url: format!("{TELEGRAM_API_BASE}/file/bot{token}"),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_base_urls(client: Client, token: &str, base_url: String, file_base_url: String) -> Self {
        Self {
            client,
            bot_token: token.to_owned(),
            base_url,
            file_base_url,
        }
    }

    fn scrub_token_str(&self, input: &str) -> String {
        let mut out = input.to_owned();
        if !self.bot_token.is_empty() {
            let raw_body = self.bot_token.strip_prefix("bot").unwrap_or(&self.bot_token);
            let mut exact_variants = vec![self.bot_token.clone()];
            if !raw_body.is_empty() && raw_body != self.bot_token {
                exact_variants.push(raw_body.to_owned());
            }
            if raw_body.contains(':') {
                exact_variants.push(raw_body.replace(':', "%3A"));
                exact_variants.push(raw_body.replace(':', "%3a"));
            }
            if raw_body.contains("%3A") || raw_body.contains("%3a") {
                exact_variants.push(raw_body.replace("%3A", ":").replace("%3a", ":"));
            }
            if self.bot_token.contains(':') && raw_body != self.bot_token {
                exact_variants.push(self.bot_token.replace(':', "%3A"));
                exact_variants.push(self.bot_token.replace(':', "%3a"));
            }

            for variant in &exact_variants {
                if variant.is_empty() {
                    continue;
                }
                let bot_prefixed = format!("bot{variant}");
                if out.contains(&bot_prefixed) {
                    out = out.replace(&bot_prefixed, "bot[REDACTED]");
                }
                if out.contains(variant) {
                    if variant.starts_with("bot") {
                        out = out.replace(variant, "bot[REDACTED]");
                    } else {
                        out = out.replace(variant, "[REDACTED]");
                    }
                }
            }
        }

        BOTFATHER_TOKEN_RE
            .replace_all(&out, |caps: &regex::Captures<'_>| {
                if caps[0].starts_with("bot") {
                    "bot[REDACTED]"
                } else {
                    "[REDACTED]"
                }
            })
            .into_owned()
    }

    pub(crate) fn sanitize_reqwest_err(&self, context: &str, err: reqwest::Error) -> String {
        let scrubbed_full = self.scrub_token_str(&err.to_string());
        let safe_detail = if !self.bot_token.is_empty() && scrubbed_full.contains(&self.bot_token) {
            self.scrub_token_str(&err.without_url().to_string())
        } else {
            scrubbed_full
        };
        format!("{context}: {safe_detail}")
    }

    /// `getMe` — returns the bot's user identity.
    pub async fn get_me(&self) -> Result<TgUser, ChannelError> {
        let url = format!("{}/getMe", self.base_url);
        let resp: TgResponse<TgUser> = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("getMe request failed", e)))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("getMe parse failed", e)))?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            return Err(ChannelError::ConnectionFailed(format!("Telegram getMe failed: {desc}")));
        }

        resp.result
            .ok_or_else(|| ChannelError::PlatformApi("getMe returned no result".into()))
    }

    /// `getUpdates` — long-poll for new updates.
    pub async fn get_updates(&self, offset: Option<i64>, timeout: u32) -> Result<Vec<TgUpdate>, ChannelError> {
        let url = format!("{}/getUpdates", self.base_url);

        let mut params = vec![("timeout", timeout.to_string())];
        if let Some(off) = offset {
            params.push(("offset", off.to_string()));
        }

        let resp: TgResponse<Vec<TgUpdate>> = self
            .client
            .get(&url)
            .query(&params)
            .send()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("getUpdates request failed", e)))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("getUpdates parse failed", e)))?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            warn!("Telegram getUpdates error: {desc}");
            return Err(ChannelError::PlatformApi(format!("getUpdates failed: {desc}")));
        }

        Ok(resp.result.unwrap_or_default())
    }

    /// `sendMessage` — send a text message. Returns the sent message.
    pub async fn send_message(&self, req: &SendMessageRequest) -> Result<TgMessage, ChannelError> {
        let url = format!("{}/sendMessage", self.base_url);
        debug!(chat_id = req.chat_id, thread_id = ?req.message_thread_id, "Sending Telegram message");

        let resp: TgResponse<TgMessage> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(self.sanitize_reqwest_err("sendMessage request failed", e)))?
            .json()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(self.sanitize_reqwest_err("sendMessage parse failed", e)))?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            return Err(ChannelError::MessageSendFailed(format!("sendMessage failed: {desc}")));
        }

        resp.result
            .ok_or_else(|| ChannelError::MessageSendFailed("sendMessage returned no result".into()))
    }

    /// `editMessageText` — edit an existing text message.
    pub async fn edit_message_text(&self, req: &EditMessageTextRequest) -> Result<(), ChannelError> {
        let url = format!("{}/editMessageText", self.base_url);
        debug!(
            chat_id = req.chat_id,
            message_id = req.message_id,
            "Editing Telegram message"
        );

        let resp: TgResponse<TgMessage> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| {
                ChannelError::MessageSendFailed(self.sanitize_reqwest_err("editMessageText request failed", e))
            })?
            .json()
            .await
            .map_err(|e| {
                ChannelError::MessageSendFailed(self.sanitize_reqwest_err("editMessageText parse failed", e))
            })?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            return Err(ChannelError::MessageSendFailed(format!(
                "editMessageText failed: {desc}"
            )));
        }

        Ok(())
    }

    /// `answerCallbackQuery` — acknowledge a callback query.
    pub async fn answer_callback_query(&self, req: &AnswerCallbackQueryRequest) -> Result<(), ChannelError> {
        let url = format!("{}/answerCallbackQuery", self.base_url);

        let resp: TgResponse<bool> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| {
                ChannelError::PlatformApi(self.sanitize_reqwest_err("answerCallbackQuery request failed", e))
            })?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("answerCallbackQuery parse failed", e)))?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            warn!("answerCallbackQuery error: {desc}");
        }

        Ok(())
    }

    /// `getFile` — resolve a `file_id` into a downloadable `file_path`.
    pub async fn get_file(&self, file_id: &str) -> Result<TgFile, ChannelError> {
        let url = format!("{}/getFile", self.base_url);
        let resp: TgResponse<TgFile> = self
            .client
            .get(&url)
            .query(&[("file_id", file_id)])
            .send()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("getFile request failed", e)))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("getFile parse failed", e)))?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            return Err(ChannelError::PlatformApi(format!("getFile failed: {desc}")));
        }

        resp.result
            .ok_or_else(|| ChannelError::PlatformApi("getFile returned no result".into()))
    }

    /// Downloads raw file bytes for `file_id`, returning `(bytes, remote_file_path)`.
    pub async fn download_file(&self, file_id: &str) -> Result<(Vec<u8>, Option<String>), ChannelError> {
        let tg_file = self.get_file(file_id).await?;
        if let Some(size) = tg_file.file_size
            && size as usize > MAX_TELEGRAM_DOWNLOAD_BYTES
        {
            return Err(ChannelError::PlatformApi(format!(
                "Telegram file exceeds 25 MiB limit ({size} bytes)"
            )));
        }

        let rel_path = tg_file
            .file_path
            .clone()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ChannelError::PlatformApi("Telegram getFile returned empty file_path".into()))?;

        let download_url = format!("{}/{}", self.file_base_url, rel_path.trim_start_matches('/'));
        let resp = self
            .client
            .get(&download_url)
            .send()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("Telegram file download failed", e)))?;

        if !resp.status().is_success() {
            return Err(ChannelError::PlatformApi(format!(
                "Telegram file download returned HTTP {}",
                resp.status()
            )));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ChannelError::PlatformApi(self.sanitize_reqwest_err("Telegram file body read failed", e)))?;

        if bytes.len() > MAX_TELEGRAM_DOWNLOAD_BYTES {
            return Err(ChannelError::PlatformApi(format!(
                "Telegram file exceeds 25 MiB limit ({} bytes)",
                bytes.len()
            )));
        }

        Ok((bytes.to_vec(), Some(rel_path)))
    }

    /// `sendPhoto` via multipart upload.
    pub async fn send_photo_bytes(&self, params: SendMediaMultipartParams) -> Result<TgMessage, ChannelError> {
        self.send_multipart_media("sendPhoto", "photo", params).await
    }

    /// `sendDocument` via multipart upload.
    pub async fn send_document_bytes(&self, params: SendMediaMultipartParams) -> Result<TgMessage, ChannelError> {
        self.send_multipart_media("sendDocument", "document", params).await
    }

    async fn send_multipart_media(
        &self,
        endpoint: &str,
        field_name: &str,
        params: SendMediaMultipartParams,
    ) -> Result<TgMessage, ChannelError> {
        let url = format!("{}/{endpoint}", self.base_url);
        let byte_len = params.bytes.len() as u64;
        let part = match &params.mime_type {
            Some(mime) => Part::stream_with_length(reqwest::Body::from(params.bytes.clone()), byte_len)
                .file_name(params.file_name.clone())
                .mime_str(mime)
                .unwrap_or_else(|_| {
                    Part::stream_with_length(reqwest::Body::from(params.bytes), byte_len).file_name(params.file_name)
                }),
            None => Part::stream_with_length(reqwest::Body::from(params.bytes), byte_len).file_name(params.file_name),
        };

        let mut form = Form::new()
            .text("chat_id", params.chat_id.to_string())
            .part(field_name.to_owned(), part);

        if let Some(tid) = params.message_thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }
        if let Some(caption) = params.caption
            && !caption.is_empty()
        {
            form = form.text("caption", caption);
        }
        if let Some(pm) = params.parse_mode {
            form = form.text("parse_mode", pm);
        }
        if let Some(reply_to) = params.reply_to_message_id {
            form = form.text("reply_to_message_id", reply_to.to_string());
        }
        if let Some(markup) = params.reply_markup
            && let Ok(markup_json) = serde_json::to_string(&markup)
        {
            form = form.text("reply_markup", markup_json);
        }
        if let Some(silent) = params.disable_notification {
            form = form.text("disable_notification", silent.to_string());
        }

        let req_ctx = format!("{endpoint} request failed");
        let parse_ctx = format!("{endpoint} parse failed");
        let resp: TgResponse<TgMessage> = self
            .client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(self.sanitize_reqwest_err(&req_ctx, e)))?
            .json()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(self.sanitize_reqwest_err(&parse_ctx, e)))?;

        if !resp.ok {
            let desc = self.scrub_token_str(&resp.description.unwrap_or_default());
            return Err(ChannelError::MessageSendFailed(format!("{endpoint} failed: {desc}")));
        }

        resp.result
            .ok_or_else(|| ChannelError::MessageSendFailed(format!("{endpoint} returned no result")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn api_constructs_correct_base_url() {
        let client = Client::new();
        let api = TelegramApi::new(client, "123:ABC");
        assert_eq!(api.bot_token, "123:ABC");
        assert_eq!(api.base_url, "https://api.telegram.org/bot123:ABC");
        assert_eq!(api.file_base_url, "https://api.telegram.org/file/bot123:ABC");
    }

    #[test]
    fn scrub_token_str_handles_short_botfather_and_percent_encoded_variants() {
        let short_api = TelegramApi::new(Client::new(), "bot123:ABC");
        let scrubbed_short = short_api.scrub_token_str(
            "error for url (https://api.telegram.org/bot123:ABC/getUpdates and https://api.telegram.org/bot123%3AABC/getUpdates and 123%3aABC)",
        );
        assert!(!scrubbed_short.contains("123:ABC"), "{scrubbed_short}");
        assert!(!scrubbed_short.contains("123%3AABC"), "{scrubbed_short}");
        assert!(!scrubbed_short.contains("123%3aABC"), "{scrubbed_short}");
        assert!(scrubbed_short.contains("[REDACTED]"), "{scrubbed_short}");

        let real_token = "7123456789:AAH_abcdefghijklmnopqrstuvwxyz012345";
        let real_api = TelegramApi::new(Client::new(), "unrelated-init-token");
        let raw = format!(
            "prefixed=https://api.telegram.org/bot{real_token}/getUpdates \
             unprefixed={real_token} \
             upper=https://api.telegram.org/file/bot7123456789%3AAAH_abcdefghijklmnopqrstuvwxyz012345/doc.pdf \
             lower=7123456789%3aAAH_abcdefghijklmnopqrstuvwxyz012345"
        );
        let scrubbed = real_api.scrub_token_str(&raw);
        assert!(!scrubbed.contains("7123456789"), "{scrubbed}");
        assert!(!scrubbed.contains("AAH_abcdefghijklmnopqrstuvwxyz012345"), "{scrubbed}");
        assert!(scrubbed.contains("bot[REDACTED]"), "{scrubbed}");
        assert!(scrubbed.contains("[REDACTED]"), "{scrubbed}");
    }

    async fn unused_local_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        port
    }

    #[tokio::test]
    async fn get_updates_scrubs_short_prefixed_unprefixed_and_encoded_tokens_on_failure() {
        let closed_port = unused_local_port().await;
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .expect("client");

        let tokens = [
            "bot123:ABC",
            "7123456789:AAH_abcdefghijklmnopqrstuvwxyz012345",
            "bot7123456789:AAH_abcdefghijklmnopqrstuvwxyz012345",
            "7123456789%3AAAH_abcdefghijklmnopqrstuvwxyz012345",
            "7123456789%3aAAH_abcdefghijklmnopqrstuvwxyz012345",
        ];

        for token in tokens {
            let api = TelegramApi::with_base_urls(
                client.clone(),
                token,
                format!("http://127.0.0.1:{closed_port}/bot{token}"),
                format!("http://127.0.0.1:{closed_port}/file/bot{token}"),
            );
            let err_msg = api
                .get_updates(None, 0)
                .await
                .expect_err("closed port must fail")
                .to_string();

            assert!(err_msg.contains("[REDACTED]"), "missing [REDACTED] for {token}: {err_msg}");
            assert!(!err_msg.contains(token), "leaked raw token {token}: {err_msg}");
            assert!(!err_msg.contains("123:ABC"), "leaked short secret: {err_msg}");
            assert!(!err_msg.contains("7123456789"), "leaked bot id: {err_msg}");
            assert!(
                !err_msg.contains("AAH_abcdefghijklmnopqrstuvwxyz012345"),
                "leaked token body: {err_msg}"
            );
        }
    }

    #[tokio::test]
    async fn download_file_scrubs_tokens_on_network_and_body_failures() {
        let closed_file_port = unused_local_port().await;
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .expect("client");

        let tokens = [
            "bot123:ABC",
            "7123456789:AAH_abcdefghijklmnopqrstuvwxyz012345",
            "bot7123456789:AAH_abcdefghijklmnopqrstuvwxyz012345",
            "7123456789%3AAAH_abcdefghijklmnopqrstuvwxyz012345",
        ];

        for token in tokens {
            let api_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock getFile");
            let api_port = api_listener.local_addr().expect("addr").port();
            tokio::spawn(async move {
                if let Ok((mut stream, _)) = api_listener.accept().await {
                    let mut req_buf = [0u8; 1024];
                    let _ = stream.read(&mut req_buf).await;
                    let body = r#"{"ok":true,"result":{"file_id":"f1","file_path":"docs/report.pdf","file_size":42}}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            });

            let api = TelegramApi::with_base_urls(
                client.clone(),
                token,
                format!("http://127.0.0.1:{api_port}/bot{token}"),
                format!("http://127.0.0.1:{closed_file_port}/file/bot{token}"),
            );
            let err_msg = api
                .download_file("f1")
                .await
                .expect_err("file download to closed port must fail")
                .to_string();

            assert!(err_msg.contains("[REDACTED]"), "missing [REDACTED] for {token}: {err_msg}");
            assert!(!err_msg.contains(token), "leaked raw token {token}: {err_msg}");
            assert!(!err_msg.contains("123:ABC"), "leaked short secret: {err_msg}");
            assert!(!err_msg.contains("7123456789"), "leaked bot id: {err_msg}");
            assert!(
                !err_msg.contains("AAH_abcdefghijklmnopqrstuvwxyz012345"),
                "leaked token body: {err_msg}"
            );
        }
    }
}

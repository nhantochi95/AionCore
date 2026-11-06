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

#[derive(Debug, Clone)]
pub(crate) struct SendMediaMultipartParams {
    pub chat_id: i64,
    pub message_thread_id: Option<i64>,
    pub file_name: String,
    pub bytes: Vec<u8>,
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
    base_url: String,
    file_base_url: String,
}

impl TelegramApi {
    /// Create a new API client for the given bot token.
    pub fn new(client: Client, token: &str) -> Self {
        Self {
            client,
            base_url: format!("{TELEGRAM_API_BASE}/bot{token}"),
            file_base_url: format!("{TELEGRAM_API_BASE}/file/bot{token}"),
        }
    }

    /// `getMe` — returns the bot's user identity.
    pub async fn get_me(&self) -> Result<TgUser, ChannelError> {
        let url = format!("{}/getMe", self.base_url);
        let resp: TgResponse<TgUser> = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ChannelError::PlatformApi(format!("getMe request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(format!("getMe parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
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
            .map_err(|e| ChannelError::PlatformApi(format!("getUpdates request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(format!("getUpdates parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
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
            .map_err(|e| ChannelError::MessageSendFailed(format!("sendMessage request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(format!("sendMessage parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
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
            .map_err(|e| ChannelError::MessageSendFailed(format!("editMessageText request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(format!("editMessageText parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
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
            .map_err(|e| ChannelError::PlatformApi(format!("answerCallbackQuery request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(format!("answerCallbackQuery parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
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
            .map_err(|e| ChannelError::PlatformApi(format!("getFile request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::PlatformApi(format!("getFile parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
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
            .map_err(|e| ChannelError::PlatformApi(format!("Telegram file download failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(ChannelError::PlatformApi(format!(
                "Telegram file download returned HTTP {}",
                resp.status()
            )));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ChannelError::PlatformApi(format!("Telegram file body read failed: {e}")))?;

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
        let part = match &params.mime_type {
            Some(mime) => Part::bytes(params.bytes.clone())
                .file_name(params.file_name.clone())
                .mime_str(mime)
                .unwrap_or_else(|_| Part::bytes(params.bytes).file_name(params.file_name)),
            None => Part::bytes(params.bytes).file_name(params.file_name),
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

        let resp: TgResponse<TgMessage> = self
            .client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(format!("{endpoint} request failed: {e}")))?
            .json()
            .await
            .map_err(|e| ChannelError::MessageSendFailed(format!("{endpoint} parse failed: {e}")))?;

        if !resp.ok {
            let desc = resp.description.unwrap_or_default();
            return Err(ChannelError::MessageSendFailed(format!("{endpoint} failed: {desc}")));
        }

        resp.result
            .ok_or_else(|| ChannelError::MessageSendFailed(format!("{endpoint} returned no result")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_constructs_correct_base_url() {
        let client = Client::new();
        let api = TelegramApi::new(client, "123:ABC");
        assert_eq!(api.base_url, "https://api.telegram.org/bot123:ABC");
        assert_eq!(api.file_base_url, "https://api.telegram.org/file/bot123:ABC");
    }
}

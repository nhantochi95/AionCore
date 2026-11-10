//! Localized notification strings and templates for the Telegram Team Hub bridge.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubLocale {
    EnUs,
    ViVn,
}

impl HubLocale {
    pub fn from_lang(lang: &str) -> Self {
        if lang.starts_with("vi") {
            Self::ViVn
        } else {
            Self::EnUs
        }
    }
}

pub struct HubI18n;

impl HubI18n {
    #[inline]
    pub fn report_preview_footnote(locale: HubLocale, file_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("\n\n… *(Báo cáo dài — bản đầy đủ đính kèm trong file `{file_name}` bên dưới)*"),
            HubLocale::EnUs => format!("\n\n… *(Long report — full version attached in `{file_name}` below)*"),
        }
    }

    #[inline]
    pub fn btn_full_report(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "📄 Báo cáo chi tiết đầy đủ",
            HubLocale::EnUs => "📄 Full Detailed Report",
        }
    }

    #[inline]
    pub fn btn_approve_and_continue(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "✅ Duyệt & Chạy tiếp",
            HubLocale::EnUs => "✅ Approve & Continue",
        }
    }

    #[inline]
    pub fn btn_task_board(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "📋 Bảng Task",
            HubLocale::EnUs => "📋 Task Board",
        }
    }

    #[inline]
    pub fn btn_stop(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "⏹ Dừng",
            HubLocale::EnUs => "⏹ Stop",
        }
    }

    #[inline]
    pub fn btn_retry_member(locale: HubLocale, member_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("🔁 Thử lại {member_name}"),
            HubLocale::EnUs => format!("🔁 Retry {member_name}"),
        }
    }

    #[inline]
    pub fn member_warning(locale: HubLocale, team_name: &str, member_name: &str, reason: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "⚠️ **[{team_name} · Cảnh báo Member]**\n• **Thành viên:** `{member_name}`\n• **Chi tiết:** {reason}"
            ),
            HubLocale::EnUs => format!(
                "⚠️ **[{team_name} · Member Warning]**\n• **Member:** `{member_name}`\n• **Details:** {reason}"
            ),
        }
    }

    #[inline]
    pub fn run_error_notice(locale: HubLocale, team_name: &str, lead_name: &str, run_id: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "❌ **[{team_name} · {lead_name} 👑]**\nPhiên thực thi (`{run_id}`) gặp sự cố và đã dừng. Anh có thể nhắn lại để tiếp tục hoặc gõ `/status` kiểm tra."
            ),
            HubLocale::EnUs => format!(
                "❌ **[{team_name} · {lead_name} 👑]**\nRun execution (`{run_id}`) failed and stopped. You can reply to resume or send `/status` to check."
            ),
        }
    }

    #[inline]
    pub fn telegram_attachment_notice(locale: HubLocale, count: usize) -> String {
        match locale {
            HubLocale::ViVn => format!("Anh vừa gửi {} file đính kèm qua Telegram, hãy kiểm tra.", count),
            HubLocale::EnUs => format!("You sent {} attachment(s) via Telegram, please check.", count),
        }
    }

    #[inline]
    pub fn request_received_processing(locale: HubLocale, team_name: &str, lead_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("⏳ **[{team_name} · {lead_name} 👑]** đã nhận yêu cầu và đang xử lý..."),
            HubLocale::EnUs => format!("⏳ **[{team_name} · {lead_name} 👑]** received the request and is processing..."),
        }
    }

    #[inline]
    pub fn unable_to_send_team_message(locale: HubLocale, team_name: &str, err: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("⚠️ Không thể gửi tin tới Team **{}**: `{}`", team_name, err),
            HubLocale::EnUs => format!("⚠️ Unable to send message to Team **{}**: `{}`", team_name, err),
        }
    }

    #[inline]
    pub fn shared_room_attached_files(locale: HubLocale, count: usize) -> String {
        match locale {
            HubLocale::ViVn => format!("Anh vừa đính kèm {} file vào phòng họp.", count),
            HubLocale::EnUs => format!("You attached {} file(s) to the shared room.", count),
        }
    }

    #[inline]
    pub fn shared_room_routed_notice(locale: HubLocale, count: usize, leaders: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("📨 **[Phòng Họp Liên Team]** Đã định tuyến tới {} Leader ({})", count, leaders),
            HubLocale::EnUs => format!("📨 **[Shared Room]** Routed to {} Leader(s) ({})", count, leaders),
        }
    }

    #[inline]
    pub fn status_running(locale: HubLocale, status: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("🟢 Đang chạy (`{status}`)"),
            HubLocale::EnUs => format!("🟢 Running (`{status}`)"),
        }
    }

    #[inline]
    pub fn status_idle(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "⚪ Đang nghỉ (Idle)",
            HubLocale::EnUs => "⚪ Idle",
        }
    }

    #[inline]
    pub fn status_board_header(locale: HubLocale, team_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("📊 **[{} · Bảng Trạng Thái]**", team_name),
            HubLocale::EnUs => format!("📊 **[{} · Status Board]**", team_name),
        }
    }

    #[inline]
    pub fn status_board_status(locale: HubLocale, badge: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("• **Trạng thái:** {badge}"),
            HubLocale::EnUs => format!("• **Status:** {badge}"),
        }
    }

    #[inline]
    pub fn status_board_members(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "\n**Thành viên:**",
            HubLocale::EnUs => "\n**Members:**",
        }
    }

    #[inline]
    pub fn taskboard_empty(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "\n📋 **TaskBoard:** Chưa có task nào.",
            HubLocale::EnUs => "\n📋 **TaskBoard:** No tasks yet.",
        }
    }

    #[inline]
    pub fn taskboard_counts(locale: HubLocale, total: usize, completed: usize, in_prog: usize, pending: usize) -> String {
        match locale {
            HubLocale::ViVn => format!("\n📋 **TaskBoard ({total} tasks):** ✅ {completed} xong · 🔄 {in_prog} đang làm · ⏳ {pending} chờ"),
            HubLocale::EnUs => format!("\n📋 **TaskBoard ({total} tasks):** ✅ {completed} done · 🔄 {in_prog} in progress · ⏳ {pending} pending"),
        }
    }

    #[inline]
    pub fn shared_room_header(locale: HubLocale, name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("🏛 **[Phòng Họp: {}]**", name),
            HubLocale::EnUs => format!("🏛 **[Shared Room: {}]**", name),
        }
    }

    #[inline]
    pub fn shared_room_hops(locale: HubLocale, current: i64, max: i64) -> String {
        match locale {
            HubLocale::ViVn => format!("• **Lượt thảo luận tự động:** `{}/{}`", current, max),
            HubLocale::EnUs => format!("• **Auto-discussion hops:** `{}/{}`", current, max),
        }
    }

    #[inline]
    pub fn shared_room_participating(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "\n**Các Team tham gia (Leader Hub):**",
            HubLocale::EnUs => "\n**Participating Teams (Leader Hub):**",
        }
    }

    #[inline]
    pub fn role_chair(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🪑 Chủ trì",
            HubLocale::EnUs => "🪑 Host",
        }
    }

    #[inline]
    pub fn role_leader(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "👑 Leader",
            HubLocale::EnUs => "👑 Leader",
        }
    }

    #[inline]
    pub fn handling_handoff_indicator(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => " _(đang xử lý handoff)_",
            HubLocale::EnUs => " _(handling handoff)_",
        }
    }

    #[inline]
    pub fn cross_team_tasks_empty(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "\n📋 **Cross-Team Tasks:** Chưa có task liên team.",
            HubLocale::EnUs => "\n📋 **Cross-Team Tasks:** No cross-team tasks yet.",
        }
    }

    #[inline]
    pub fn cross_team_tasks_header(locale: HubLocale, count: usize) -> String {
        match locale {
            HubLocale::ViVn => format!("\n📋 **Cross-Team Tasks ({}):**", count),
            HubLocale::EnUs => format!("\n📋 **Cross-Team Tasks ({}):**", count),
        }
    }

    #[inline]
    pub fn blocked_tag(locale: HubLocale, blocker: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(" [chờ: {}]", blocker),
            HubLocale::EnUs => format!(" [waiting for: {}]", blocker),
        }
    }

    #[inline]
    pub fn btn_allow_more_hops(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "✅ Cho phép thảo luận tiếp (+5 lượt)",
            HubLocale::EnUs => "✅ Allow more discussion (+5 hops)",
        }
    }

    #[inline]
    pub fn max_hops_paused_notice(locale: HubLocale, hops: i64) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "⏸ Đã tạm dừng chuyển lượt tự động sau **{hops}** lượt trao đổi liên tiếp giữa các Leader để tránh tốn token ngoài ý muốn. Bấm **Cho phép thảo luận tiếp (+5 lượt)** hoặc gửi tin nhắn mới để tiếp tục."
            ),
            HubLocale::EnUs => format!(
                "⏸ Auto-discussion paused after **{hops}** consecutive leader hops to avoid unintended token spend. Tap **Allow more discussion (+5 hops)** or send a new message to continue."
            ),
        }
    }

    #[inline]
    pub fn no_teams_info(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Chưa có Team nào trong AionUi. Anh tạo Team trên app trước nhé!",
            HubLocale::EnUs => "No Teams found in AionUi. Please create a Team in the app first!",
        }
    }

    #[inline]
    pub fn bind_topic_info(locale: HubLocale, team_name: &str, lead_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "📌 Đã gắn nhóm/topic này với Team **[{team_name} · {lead_name} 👑]**!\nTừ giờ mọi tin nhắn tại đây sẽ gửi thẳng tới Leader `{lead_name}`."
            ),
            HubLocale::EnUs => format!(
                "📌 Bound this group/topic to Team **[{team_name} · {lead_name} 👑]**!\nAll messages sent here will go directly to Leader `{lead_name}`."
            ),
        }
    }

    #[inline]
    pub fn switch_team_info(locale: HubLocale, team_name: &str, lead_name: &str, count: usize) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "✅ Đã chuyển kênh chat sang Team **[{team_name} · {lead_name} 👑]** ({count} thành viên).\nAnh nhắn trực tiếp ở đây để giao việc cho Leader nhé! _(Gõ `/solo` nếu muốn về chat đơn)_"
            ),
            HubLocale::EnUs => format!(
                "✅ Switched chat channel to Team **[{team_name} · {lead_name} 👑]** ({count} members).\nSend messages here to assign tasks to the Leader! _(Type `/solo` to switch back to solo chat)_"
            ),
        }
    }

    #[inline]
    pub fn btn_switch_solo(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🏠 Chuyển về Solo Chat riêng",
            HubLocale::EnUs => "🏠 Switch to Solo Chat",
        }
    }

    #[inline]
    pub fn select_team_bind_prompt(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "📌 **Chọn Team để gắn với nhóm / Topic này:**",
            HubLocale::EnUs => "📌 **Select a Team to bind to this group / Topic:**",
        }
    }

    #[inline]
    pub fn select_team_chat_prompt(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "👑 **Chọn Team anh muốn trò chuyện trực tiếp với Leader:**\n_(Mẹo: Khi bất kỳ Team nào báo cáo chủ động, anh chỉ cần vuốt **Reply** vào tin nhắn đó là trả lời đúng Team ngay!)_",
            HubLocale::EnUs => "👑 **Select a Team to chat directly with its Leader:**\n_(Tip: When a Team reports proactively, swipe **Reply** on that message to reply directly!)_",
        }
    }

    #[inline]
    pub fn no_shared_rooms_info(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🏛 Chưa có **Phòng Họp Liên Team (Shared Room)** nào.\nAnh có thể bấm `+` ở mục **Shared Rooms** trên thanh bên trái AionUi để tạo phòng họp giữa nhiều Leader!",
            HubLocale::EnUs => "🏛 No **Shared Rooms** found yet.\nClick `+` under **Shared Rooms** in the AionUi sidebar to create a room between Leaders!",
        }
    }

    #[inline]
    pub fn shared_room_joined_info(locale: HubLocale, room_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "🏛 Đã kết nối tới Phòng Họp Liên Team **#{}**!\nAnh có thể tag `@TeamName`, `@all` hoặc nhắn tự nhiên để các Leader phối hợp.",
                room_name
            ),
            HubLocale::EnUs => format!(
                "🏛 Connected to Shared Room **#{}**!\nYou can mention `@TeamName`, `@all` or message freely for Leaders to collaborate.",
                room_name
            ),
        }
    }

    #[inline]
    pub fn select_shared_room_prompt(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🏛 **Chọn Phòng Họp Liên Team (Shared Room):**",
            HubLocale::EnUs => "🏛 **Select a Shared Room:**",
        }
    }

    #[inline]
    pub fn switched_to_solo_info(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🏠 Đã chuyển về chế độ **Solo Chat** riêng. Gõ `/team` bất kỳ lúc nào để kết nối lại với các Team!",
            HubLocale::EnUs => "🏠 Switched to **Solo Chat** mode. Type `/team` anytime to reconnect with Teams!",
        }
    }

    #[inline]
    pub fn no_bound_team_info(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Hiện không có Team nào đang gắn với cuộc trò chuyện này.",
            HubLocale::EnUs => "No Team is currently bound to this conversation.",
        }
    }

    #[inline]
    pub fn no_active_runs_to_stop(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "⚪ Hiện không có phiên chạy (active run) nào cần dừng.",
            HubLocale::EnUs => "⚪ No active run to stop.",
        }
    }

    #[inline]
    pub fn stopped_active_runs_info(locale: HubLocale, count: usize) -> String {
        match locale {
            HubLocale::ViVn => format!("⏹ Đã gửi lệnh dừng cho `{}` phiên đang chạy!", count),
            HubLocale::EnUs => format!("⏹ Sent stop command to `{}` active run(s)!", count),
        }
    }

    #[inline]
    pub fn action_expired(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "⏱ Nút thao tác này đã hết hạn.",
            HubLocale::EnUs => "⏱ This action button has expired.",
        }
    }

    #[inline]
    pub fn connected_team_notice(locale: HubLocale, team_name: &str, lead_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "✅ Đang kết nối trực tiếp với **[{team_name} · {lead_name} 👑]**!\nAnh gửi tin nhắn hoặc ảnh/tài liệu để giao việc cho Leader nhé."
            ),
            HubLocale::EnUs => format!(
                "✅ Connected directly to **[{team_name} · {lead_name} 👑]**!\nSend messages or attachments to assign tasks to the Leader."
            ),
        }
    }

    #[inline]
    pub fn selected_team_toast(locale: HubLocale, team_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("Đã chọn Team {}", team_name),
            HubLocale::EnUs => format!("Selected Team {}", team_name),
        }
    }

    #[inline]
    pub fn in_shared_room_notice(locale: HubLocale, room_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "🏛 Đang ở **Phòng Họp Liên Team #{room_name}**!\nNhắn `@TeamName`, `@all` hoặc giao việc chung để các Leader phối hợp."
            ),
            HubLocale::EnUs => format!(
                "🏛 Currently in **Shared Room #{room_name}**!\nMention `@TeamName`, `@all` or assign tasks for Leaders to collaborate."
            ),
        }
    }

    #[inline]
    pub fn opened_room_toast(locale: HubLocale, room_name: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("Đã mở phòng #{}", room_name),
            HubLocale::EnUs => format!("Opened room #{}", room_name),
        }
    }

    #[inline]
    pub fn switched_to_default_solo_notice(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🏠 Đã chuyển về **Solo Chat** mặc định. Gõ `/team` khi cần quay lại Team!",
            HubLocale::EnUs => "🏠 Switched to default **Solo Chat**. Type `/team` to return to Team!",
        }
    }

    #[inline]
    pub fn read_taskboard_error(locale: HubLocale, err: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("⚠️ Không đọc được bảng Task: {err}"),
            HubLocale::EnUs => format!("⚠️ Failed to read TaskBoard: {err}"),
        }
    }

    #[inline]
    pub fn action_already_processed(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "✅ Thao tác này đã được xử lý trước đó.",
            HubLocale::EnUs => "✅ This action has already been processed.",
        }
    }

    #[inline]
    pub fn default_approval_prompt(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Anh đã duyệt phương án đề xuất (Approved). Hãy tiến hành triển khai và báo cáo khi hoàn thành.",
            HubLocale::EnUs => "Proposal approved. Please proceed with implementation and report when completed.",
        }
    }

    #[inline]
    pub fn approval_confirmed_markdown(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "✅ **Đã gửi xác nhận duyệt** tới Leader! Hệ thống đang tiếp tục triển khai...",
            HubLocale::EnUs => "✅ **Sent approval confirmation** to Leader! Resuming execution...",
        }
    }

    #[inline]
    pub fn run_stopped_markdown(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "⏹ **Đã gửi lệnh dừng phiên chạy!**",
            HubLocale::EnUs => "⏹ **Sent stop command to run!**",
        }
    }

    #[inline]
    pub fn member_runtime_restarted_markdown(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🔁 **Đã gửi lệnh khởi động lại runtime cho Member!**",
            HubLocale::EnUs => "🔁 **Sent runtime restart command for Member!**",
        }
    }

    #[inline]
    pub fn more_hops_granted_markdown(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "✅ **Đã cấp thêm +5 lượt thảo luận tự động** cho Phòng Họp Liên Team!",
            HubLocale::EnUs => "✅ **Granted +5 auto-discussion hops** for Shared Room!",
        }
    }

    #[inline]
    pub fn unknown_action_toast(locale: HubLocale, other: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("Thao tác không xác định: {other}"),
            HubLocale::EnUs => format!("Unknown action: {other}"),
        }
    }

    #[inline]
    pub fn task_status_badge(locale: HubLocale, kind: &str) -> &'static str {
        match (locale, kind) {
            (HubLocale::ViVn, "approval_needed") => "🙋 Cần anh duyệt",
            (HubLocale::ViVn, "completed") => "✅ Hoàn thành",
            (HubLocale::ViVn, "blocker") => "⚠️ Vướng mắc cần hỗ trợ",
            (HubLocale::ViVn, _) => "📌 Cập nhật tiến độ",

            (HubLocale::EnUs, "approval_needed") => "🙋 Approval needed",
            (HubLocale::EnUs, "completed") => "✅ Completed",
            (HubLocale::EnUs, "blocker") => "⚠️ Blocker needing help",
            (HubLocale::EnUs, _) => "📌 Progress update",
        }
    }

    // --- Action Inbox & Dual Wallet Locales ---

    #[inline]
    pub fn inbox_worktree_merge_title(locale: HubLocale, team: &str, branch: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("Duyệt gộp nhánh Git Worktree `{branch}` ({team})"),
            HubLocale::EnUs => format!("Approve Git Worktree Squash Merge `{branch}` ({team})"),
        }
    }

    #[inline]
    pub fn inbox_worktree_merge_summary(
        locale: HubLocale,
        files: usize,
        additions: usize,
        deletions: usize,
    ) -> String {
        match locale {
            HubLocale::ViVn => format!("Thay đổi: {files} tệp (+{additions} / -{deletions})"),
            HubLocale::EnUs => format!("Diff: {files} file(s) changed (+{additions} / -{deletions})"),
        }
    }

    #[inline]
    pub fn btn_worktree_approve(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "✅ Duyệt & Gộp nhánh",
            HubLocale::EnUs => "✅ Approve & Squash Merge",
        }
    }

    #[inline]
    pub fn btn_worktree_discard(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "❌ Hủy & Lưu nháp",
            HubLocale::EnUs => "❌ Discard Worktree",
        }
    }

    #[inline]
    pub fn inbox_choice_title(locale: HubLocale, question: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("Câu hỏi từ Agent: {question}"),
            HubLocale::EnUs => format!("Agent Question: {question}"),
        }
    }

    #[inline]
    pub fn inbox_budget_soft_warning_title(locale: HubLocale, spent: f64, limit: f64) -> String {
        match locale {
            HubLocale::ViVn => format!("Cảnh báo mềm: Đạt 80% hạn mức API Key (${spent:.2} / ${limit:.2})"),
            HubLocale::EnUs => format!("Soft Budget Warning: 80% API Key Cap Reached (${spent:.2} / ${limit:.2})"),
        }
    }

    #[inline]
    pub fn inbox_budget_soft_warning_summary(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Các Agent dùng gói thuê bao Claude/Codex ($0) vẫn chạy bình thường. Chỉ nhánh gọi API lẻ chạm mốc 80% hạn mức.",
            HubLocale::EnUs => "CLI subscription runs (Claude/Codex) remain free ($0.00). Only pay-per-token API key usage reached 80% of cap.",
        }
    }

    #[inline]
    pub fn btn_budget_ack(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "👌 Đã hiểu & Giữ trần",
            HubLocale::EnUs => "👌 Acknowledged & Keep Cap",
        }
    }

    #[inline]
    pub fn inbox_budget_hard_fuse_title(locale: HubLocale, spent: f64, limit: f64) -> String {
        match locale {
            HubLocale::ViVn => format!("Cầu chì ngân sách 100%: Tự động tạm dừng các Agent dùng API Key (${spent:.2} / ${limit:.2})"),
            HubLocale::EnUs => format!("100% Hard Budget Fuse: Paused API Key Agents (${spent:.2} / ${limit:.2})"),
        }
    }

    #[inline]
    pub fn inbox_budget_hard_fuse_summary(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Đã chạm 100% hạn mức API Key. Tạm dừng các Agent trả phí. Các Agent dùng gói thuê bao CLI ($0) vẫn tiếp tục hoạt động.",
            HubLocale::EnUs => "API Key cap reached 100%. Paused paid agents. CLI subscription agents ($0.00) continue executing normally.",
        }
    }

    #[inline]
    pub fn btn_budget_extend(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "➕ Nâng hạn mức +$20",
            HubLocale::EnUs => "➕ Extend Budget +$20",
        }
    }

    #[inline]
    pub fn room_user_sender_label(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Anh (User)",
            HubLocale::EnUs => "You",
        }
    }

    #[inline]
    pub fn inbox_resolved_on_desktop(locale: HubLocale, title: &str, action: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "✅ **Đã xử lý trên Desktop** (`{action}`)\n• **Mục:** {title}"
            ),
            HubLocale::EnUs => format!(
                "✅ **Resolved via Desktop** (`{action}`)\n• **Item:** {title}"
            ),
        }
    }

    #[inline]
    pub fn wallet_hard_blocked_notice(locale: HubLocale, team_name: &str, spent: f64, limit: f64) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "🛑 **[Cầu chì ngân sách API 100%]** Tạm ngưng gọi Team `{team_name}` (dùng API Key trả phí: `${spent:.2} / ${limit:.2}`). Các Team dùng gói CLI OAuth (`$0.00`) vẫn hoạt động bình thường."
            ),
            HubLocale::EnUs => format!(
                "🛑 **[100% API Budget Fuse]** Paused metered API-Key Team `{team_name}` (`${spent:.2} / ${limit:.2}`). CLI OAuth subscription teams (`$0.00`) continue unimpeded."
            ),
        }
    }

    // --- Context Compaction & Connected Model Hot-Swap Locales ---

    #[inline]
    pub fn btn_compact_now(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "🧹 Nén ngữ cảnh ngay",
            HubLocale::EnUs => "🧹 Compact Now",
        }
    }

    #[inline]
    pub fn btn_switch_all_members(locale: HubLocale, label: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("👑 Tất cả: {label}"),
            HubLocale::EnUs => format!("👑 All: {label}"),
        }
    }

    #[inline]
    pub fn btn_switch_teammates_only(locale: HubLocale, label: &str) -> String {
        match locale {
            HubLocale::ViVn => format!("🤖 Teammates: {label}"),
            HubLocale::EnUs => format!("🤖 Workers: {label}"),
        }
    }

    #[inline]
    pub fn compact_completed_markdown(
        locale: HubLocale,
        team_name: &str,
        slot_count: usize,
        avg_after_pct: i64,
    ) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "🧹 **[{team_name} · Đã nén ngữ cảnh]**\n• **Thành viên đã nén:** `{slot_count}` (gồm cả Leader & Teammates)\n• **Mức ngữ cảnh sau nén:** `~{avg_after_pct}%` (đã neo bản tóm tắt 5 mục an toàn)"
            ),
            HubLocale::EnUs => format!(
                "🧹 **[{team_name} · Context Compacted]**\n• **Compacted members:** `{slot_count}` (Leader & Teammates)\n• **Post-compaction context:** `~{avg_after_pct}%` (5-section anchored handover saved)"
            ),
        }
    }

    #[inline]
    pub fn model_picker_prompt(locale: HubLocale, team_name: &str, current_summary: &str) -> String {
        match locale {
            HubLocale::ViVn => format!(
                "🔀 **[{team_name} · Đổi Model kết nối]**\n{current_summary}\n\nChọn chế độ đổi cho **Tất cả thành viên** (gồm Leader) hoặc **Chỉ Teammates** (giữ nguyên Model của Leader):"
            ),
            HubLocale::EnUs => format!(
                "🔀 **[{team_name} · Switch Connected Models]**\n{current_summary}\n\nChoose **All Members** (including Leader) or **Workers Only** (keep Leader model):"
            ),
        }
    }

    #[inline]
    pub fn no_connected_models_info(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "⚠️ Chưa phát hiện CLI OAuth đang Online hoặc API Provider nào đang bật trong AionUi.",
            HubLocale::EnUs => "⚠️ No online CLI OAuth agents or enabled API providers found in AionUi.",
        }
    }

    #[inline]
    pub fn model_switched_markdown(
        locale: HubLocale,
        team_name: &str,
        scope_label: &str,
        target_label: &str,
        switched_count: usize,
        queued_count: usize,
    ) -> String {
        match locale {
            HubLocale::ViVn => {
                if queued_count > 0 {
                    format!(
                        "✅ **[{team_name} · Đã cập nhật Model]**\n• **Phạm vi:** {scope_label}\n• **Model mới:** `{target_label}`\n• **Kết quả:** Đổi ngay `{switched_count}` thành viên, xếp hàng chờ hết turn cho `{queued_count}` thành viên."
                    )
                } else {
                    format!(
                        "✅ **[{team_name} · Đã đổi Model thành công]**\n• **Phạm vi:** {scope_label}\n• **Model mới:** `{target_label}`\n• **Thành viên áp dụng:** `{switched_count}` (giữ nguyên lịch sử & neo ngữ cảnh)"
                    )
                }
            }
            HubLocale::EnUs => {
                if queued_count > 0 {
                    format!(
                        "✅ **[{team_name} · Model Switch Updated]**\n• **Scope:** {scope_label}\n• **Target:** `{target_label}`\n• **Outcome:** Switched `{switched_count}` member(s) immediately, queued `{queued_count}` active member(s) after turn."
                    )
                } else {
                    format!(
                        "✅ **[{team_name} · Models Switched]**\n• **Scope:** {scope_label}\n• **Target:** `{target_label}`\n• **Updated members:** `{switched_count}` (chat history & anchored handover preserved)"
                    )
                }
            }
        }
    }

    #[inline]
    pub fn scope_all_members_label(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Tất cả thành viên (gồm Leader)",
            HubLocale::EnUs => "All Members (including Leader)",
        }
    }

    #[inline]
    pub fn scope_teammates_only_label(locale: HubLocale) -> &'static str {
        match locale {
            HubLocale::ViVn => "Chỉ Teammates (giữ Leader)",
            HubLocale::EnUs => "Teammates Only (keep Leader)",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hub_locale_resolution() {
        assert_eq!(HubLocale::from_lang("en-US"), HubLocale::EnUs);
        assert_eq!(HubLocale::from_lang("en"), HubLocale::EnUs);
        assert_eq!(HubLocale::from_lang("vi-VN"), HubLocale::ViVn);
        assert_eq!(HubLocale::from_lang("vi"), HubLocale::ViVn);
        assert_eq!(HubLocale::from_lang(""), HubLocale::EnUs);
    }

    #[test]
    fn test_hub_i18n_strings() {
        assert_eq!(HubI18n::btn_stop(HubLocale::EnUs), "⏹ Stop");
        assert_eq!(HubI18n::btn_stop(HubLocale::ViVn), "⏹ Dừng");

        assert_eq!(HubI18n::btn_approve_and_continue(HubLocale::EnUs), "✅ Approve & Continue");
        assert_eq!(HubI18n::btn_approve_and_continue(HubLocale::ViVn), "✅ Duyệt & Chạy tiếp");

        let footnote_en = HubI18n::report_preview_footnote(HubLocale::EnUs, "doc.md");
        assert!(footnote_en.contains("doc.md"));
        assert!(footnote_en.contains("full version"));

        let footnote_vi = HubI18n::report_preview_footnote(HubLocale::ViVn, "doc.md");
        assert!(footnote_vi.contains("doc.md"));
        assert!(footnote_vi.contains("bản đầy đủ"));

        // Action Inbox & Budget Fuse tests
        assert_eq!(HubI18n::btn_worktree_approve(HubLocale::EnUs), "✅ Approve & Squash Merge");
        assert_eq!(HubI18n::btn_worktree_approve(HubLocale::ViVn), "✅ Duyệt & Gộp nhánh");
        assert_eq!(HubI18n::btn_budget_ack(HubLocale::EnUs), "👌 Acknowledged & Keep Cap");
        assert_eq!(HubI18n::btn_budget_ack(HubLocale::ViVn), "👌 Đã hiểu & Giữ trần");
        assert_eq!(HubI18n::btn_budget_extend(HubLocale::EnUs), "➕ Extend Budget +$20");
        assert_eq!(HubI18n::btn_budget_extend(HubLocale::ViVn), "➕ Nâng hạn mức +$20");

        let soft_en = HubI18n::inbox_budget_soft_warning_title(HubLocale::EnUs, 40.0, 50.0);
        assert!(soft_en.contains("80%"));
        assert!(soft_en.contains("$40.00 / $50.00"));

        let soft_vi = HubI18n::inbox_budget_soft_warning_title(HubLocale::ViVn, 40.0, 50.0);
        assert!(soft_vi.contains("80%"));
        assert!(soft_vi.contains("$40.00 / $50.00"));

        let hard_en = HubI18n::inbox_budget_hard_fuse_title(HubLocale::EnUs, 50.0, 50.0);
        assert!(hard_en.contains("100%"));

        let hard_vi = HubI18n::inbox_budget_hard_fuse_title(HubLocale::ViVn, 50.0, 50.0);
        assert!(hard_vi.contains("100%"));

        // Compact & Model switch NFC checks
        let samples = [
            HubI18n::btn_compact_now(HubLocale::ViVn).to_owned(),
            HubI18n::btn_switch_all_members(HubLocale::ViVn, "codex/gpt-5"),
            HubI18n::btn_switch_teammates_only(HubLocale::ViVn, "aionrs/claude-sonnet-4"),
            HubI18n::compact_completed_markdown(HubLocale::ViVn, "Alpha", 3, 12),
            HubI18n::model_picker_prompt(HubLocale::ViVn, "Alpha", "3 slots"),
            HubI18n::no_connected_models_info(HubLocale::ViVn).to_owned(),
            HubI18n::model_switched_markdown(HubLocale::ViVn, "Alpha", "Tất cả", "codex", 3, 0),
        ];
        for s in &samples {
            assert!(
                !s.chars().any(|c| ('\u{0300}'..='\u{036F}').contains(&c)),
                "Vietnamese string must be 100% Unicode NFC with zero combining marks: {s}"
            );
        }
    }
}

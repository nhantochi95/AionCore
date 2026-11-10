//! Telegram <-> Team Leader Bridge & Multi-Team Shared Rooms (Leader Hub).

pub mod context_compactor;
pub mod file_guard;
pub mod hub;
pub mod judge;
pub mod locales;
pub mod routes;

pub use hub::TelegramTeamHub;
pub use routes::{SharedRoomRouterState, shared_room_routes};

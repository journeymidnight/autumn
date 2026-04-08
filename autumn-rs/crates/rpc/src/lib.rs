//! autumn-rpc: wire protocol framing for custom binary RPC.
//!
//! Provides a 10-byte-header wire protocol with request multiplexing IDs.
//!
//! # Wire Format
//!
//! ```text
//! [req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload]
//! ```

pub mod error;
pub mod frame;

pub use error::{RpcError, Result, StatusCode};
pub use frame::{Frame, FrameDecoder, HEADER_LEN};

/// Handler result type for RPC dispatch.
pub type HandlerResult = std::result::Result<bytes::Bytes, (StatusCode, String)>;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;

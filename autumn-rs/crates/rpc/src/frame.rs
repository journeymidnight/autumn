//! Wire protocol framing for autumn-rpc.
//!
//! Frame format (10-byte header + payload):
//! ```text
//! ┌───────────┬──────────┬───────────┬──────────────┬─────────────────┐
//! │ req_id    │ msg_type │ flags     │ payload_len  │ payload         │
//! │ u32 LE    │ u8       │ u8        │ u32 LE       │ N bytes         │
//! └───────────┴──────────┴───────────┴──────────────┴─────────────────┘
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Frame header size in bytes.
pub const HEADER_LEN: usize = 10;

/// Maximum payload size: 4 GB - 1 (u32::MAX).
/// Individual services should enforce their own practical limits.
pub const MAX_PAYLOAD_LEN: u32 = u32::MAX;

// Flag bits
pub const FLAG_RESPONSE: u8 = 0x01;
pub const FLAG_ERROR: u8 = 0x02;
pub const FLAG_STREAM_END: u8 = 0x04;

/// A single RPC frame on the wire.
#[derive(Debug, Clone)]
pub struct Frame {
    pub req_id: u32,
    pub msg_type: u8,
    pub flags: u8,
    pub payload: Bytes,
}

impl Frame {
    /// Create a new request frame.
    pub fn request(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: 0,
            payload,
        }
    }

    /// Create a response frame.
    pub fn response(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE,
            payload,
        }
    }

    /// Create an error response frame.
    pub fn error(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE | FLAG_ERROR,
            payload,
        }
    }

    pub fn is_response(&self) -> bool {
        self.flags & FLAG_RESPONSE != 0
    }

    pub fn is_error(&self) -> bool {
        self.flags & FLAG_ERROR != 0
    }

    pub fn is_stream_end(&self) -> bool {
        self.flags & FLAG_STREAM_END != 0
    }

    /// Encode this frame into bytes (header + payload).
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.payload.len());
        buf.put_u32_le(self.req_id);
        buf.put_u8(self.msg_type);
        buf.put_u8(self.flags);
        buf.put_u32_le(self.payload.len() as u32);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Encode only the header into a fixed-size array (for vectored writes).
    pub fn encode_header(&self) -> [u8; HEADER_LEN] {
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..4].copy_from_slice(&self.req_id.to_le_bytes());
        hdr[4] = self.msg_type;
        hdr[5] = self.flags;
        hdr[6..10].copy_from_slice(&(self.payload.len() as u32).to_le_bytes());
        hdr
    }
}

/// Decode state machine for reading frames from a byte stream.
pub struct FrameDecoder {
    buf: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    /// Feed new data into the decoder buffer.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to decode the next complete frame from the buffer.
    /// Returns `None` if not enough data is available yet.
    pub fn try_decode(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }

        let payload_len =
            u32::from_le_bytes(self.buf[6..10].try_into().unwrap());

        if payload_len > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        let total = HEADER_LEN + payload_len as usize;
        if self.buf.len() < total {
            // Reserve capacity for the rest of the frame to reduce reallocations.
            self.buf.reserve(total - self.buf.len());
            return Ok(None);
        }

        let req_id = u32::from_le_bytes(self.buf[0..4].try_into().unwrap());
        let msg_type = self.buf[4];
        let flags = self.buf[5];

        self.buf.advance(HEADER_LEN);
        let payload = self.buf.split_to(payload_len as usize).freeze();

        Ok(Some(Frame {
            req_id,
            msg_type,
            flags,
            payload,
        }))
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("payload too large: {0} bytes (max {MAX_PAYLOAD_LEN})")]
    PayloadTooLarge(u32),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let frame = Frame::request(42, 7, Bytes::from_static(b"hello world"));
        let encoded = frame.encode();
        assert_eq!(encoded.len(), HEADER_LEN + 11);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.msg_type, 7);
        assert_eq!(decoded.flags, 0);
        assert_eq!(decoded.payload, Bytes::from_static(b"hello world"));
    }

    #[test]
    fn decode_partial_header() {
        let frame = Frame::request(1, 2, Bytes::from_static(b"x"));
        let encoded = frame.encode();

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded[..5]); // partial header
        assert!(decoder.try_decode().unwrap().is_none());

        decoder.feed(&encoded[5..]); // rest
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.req_id, 1);
        assert_eq!(decoded.payload, Bytes::from_static(b"x"));
    }

    #[test]
    fn decode_partial_payload() {
        let payload = Bytes::from(vec![0xAB; 100]);
        let frame = Frame::request(10, 3, payload.clone());
        let encoded = frame.encode();

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded[..HEADER_LEN + 50]); // header + half payload
        assert!(decoder.try_decode().unwrap().is_none());

        decoder.feed(&encoded[HEADER_LEN + 50..]); // rest of payload
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn decode_multiple_frames() {
        let f1 = Frame::request(1, 1, Bytes::from_static(b"aaa"));
        let f2 = Frame::response(2, 2, Bytes::from_static(b"bbb"));

        let mut all = BytesMut::new();
        all.extend_from_slice(&f1.encode());
        all.extend_from_slice(&f2.encode());

        let mut decoder = FrameDecoder::new();
        decoder.feed(&all);

        let d1 = decoder.try_decode().unwrap().unwrap();
        assert_eq!(d1.req_id, 1);
        assert!(!d1.is_response());

        let d2 = decoder.try_decode().unwrap().unwrap();
        assert_eq!(d2.req_id, 2);
        assert!(d2.is_response());

        assert!(decoder.try_decode().unwrap().is_none());
    }

    // payload_too_large test removed: MAX_PAYLOAD_LEN == u32::MAX, so no u32
    // value can exceed it. The guard `payload_len > MAX_PAYLOAD_LEN` is dead code
    // for the current wire format.

    #[test]
    fn response_and_error_flags() {
        let f = Frame::error(99, 5, Bytes::from_static(b"oops"));
        assert!(f.is_response());
        assert!(f.is_error());
        assert!(!f.is_stream_end());
    }

    #[test]
    fn empty_payload() {
        let frame = Frame::request(0, 0, Bytes::new());
        let encoded = frame.encode();
        assert_eq!(encoded.len(), HEADER_LEN);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 0);
    }

    #[test]
    fn encode_header_matches_encode() {
        let frame = Frame::request(123, 45, Bytes::from_static(b"test"));
        let full = frame.encode();
        let hdr = frame.encode_header();
        assert_eq!(&full[..HEADER_LEN], &hdr);
    }
}

//! One call's bidi stream as hyper's IO, abandoned on the wire when dropped before it completes.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use wtransport::{RecvStream, SendStream, VarInt};

pub(crate) fn io(
    send: SendStream,
    recv: RecvStream,
    abandonment: Option<crate::Abandonment>,
) -> TokioIo<Stream> {
    TokioIo::new(Stream {
        send,
        recv,
        finished: false,
        ended: false,
        abandonment,
    })
}

/// Completed means our side was finished or the peer's side was read to its end.
/// Dropped otherwise, the call was abandoned: the send side is reset and the receive side stopped,
/// rather than the FIN that would make a truncated exchange look whole. The code is the
/// [`Abandonment`](crate::Abandonment)'s when it has one.
pub(crate) struct Stream {
    send: SendStream,
    recv: RecvStream,
    finished: bool,
    ended: bool,
    abandonment: Option<crate::Abandonment>,
}

impl Drop for Stream {
    fn drop(&mut self) {
        if self.finished || self.ended {
            return;
        }
        let code = http3_code(
            self.abandonment
                .as_ref()
                .and_then(crate::Abandonment::code)
                .unwrap_or(crate::CANCELED),
        );
        let _ = self.send.reset(code);
        let _ = self
            .recv
            .quic_stream_mut()
            .stop(wtransport::quinn::VarInt::from_u64(code.into_inner()).unwrap_or_default());
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.recv).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() == before {
            self.ended = true;
        }
        polled
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.send).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.send).poll_shutdown(cx);
        if matches!(polled, Poll::Ready(Ok(()))) {
            self.finished = true;
        }
        polled
    }
}

const FIRST: u64 = 0x52e4_a40f_a8db;

/// A WebTransport stream error code in HTTP/3's error space (draft-ietf-webtrans-http3 §4.4).
pub(crate) fn http3_code(code: u32) -> VarInt {
    let n = u64::from(code);
    VarInt::try_from_u64(FIRST + n + n / 0x1e).expect("within the reserved range")
}

/// The WebTransport stream error code an HTTP/3 one carries, if it is one.
#[cfg(any(feature = "server", test))]
pub(crate) fn webtransport_code(h3: u64) -> Option<u32> {
    let shifted = h3.checked_sub(FIRST)?;
    if (h3 - 0x21).is_multiple_of(0x1f) {
        return None;
    }
    u32::try_from(shifted - shifted / 0x1f).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_map_into_http3_and_back() {
        assert_eq!(http3_code(0).into_inner(), FIRST);
        assert_eq!(http3_code(0x1d).into_inner(), FIRST + 0x1d);
        // 0x1e skips the reserved HTTP/3 code in between.
        assert_eq!(http3_code(0x1e).into_inner(), FIRST + 0x1f);
        assert_eq!(http3_code(u32::MAX).into_inner(), 0x52e5_ac98_3162);
        for code in [0, 1, 0x1d, 0x1e, 0x1f, 1000, u32::MAX] {
            assert_eq!(webtransport_code(http3_code(code).into_inner()), Some(code));
        }
        assert_eq!(webtransport_code(7), None);
    }
}

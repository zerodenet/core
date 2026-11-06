//! Poll the exact length and payload; an empty payload is a packet, never EOF.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};
use zero_core::packet::Datagram;
use zero_platform_tokio::contract::{datagram::DatagramRx, BoxFuture, CloseReason};
pub struct Rx {
    shared: Arc<Shared>,
    bytes: Vec<u8>,
    offset: usize,
    length: Option<usize>,
    cancelled: BoxFuture<'static, ()>,
}
impl Rx {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            cancelled: shared.cancellation(),
            shared,
            bytes: vec![0; 2],
            offset: 0,
            length: None,
        }
    }
    fn packet(&mut self) -> Datagram {
        let payload = self.bytes[2..].to_vec().into();
        self.bytes.clear();
        self.bytes.resize(2, 0);
        self.offset = 0;
        self.length = None;
        Datagram {
            association: self.shared.association,
            address: self.shared.target.clone(),
            payload,
        }
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.shared.cancel.is_cancelled() || Future::poll(this.cancelled.as_mut(), cx).is_ready()
        {
            this.shared.stop(CloseReason::Cancelled);
            return Poll::Ready(Err(this.shared.error(
                Stage::Receive,
                Reason::Cancelled,
                DeliveryState::Unknown,
            )));
        }
        loop {
            if this.offset == this.bytes.len() {
                if this.length.is_none() {
                    let len = u16::from_be_bytes([this.bytes[0], this.bytes[1]]) as usize;
                    this.length = Some(len);
                    this.bytes.resize(len + 2, 0);
                }
                if this.offset == this.bytes.len() {
                    return Poll::Ready(Ok(Some(this.packet())));
                }
            }
            let mut state = this.shared.state.lock().unwrap();
            let stream = state
                .stream
                .as_mut()
                .expect("actual retained VLESS UDP view");
            let mut buf = ReadBuf::new(&mut this.bytes[this.offset..]);
            let result = std::task::ready!(Pin::new(stream).poll_read(cx, &mut buf));
            let n = buf.filled().len();
            drop(state);
            result.map_err(|source| {
                this.shared.stop(CloseReason::ProtocolFailure);
                this.shared
                    .error(Stage::Receive, Reason::Io, DeliveryState::Unknown)
                    .with_source(source)
            })?;
            if n == 0 {
                if this.offset == 0 {
                    return Poll::Ready(Ok(None));
                }
                this.shared.stop(CloseReason::ProtocolFailure);
                return Poll::Ready(Err(this
                    .shared
                    .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                    .with_source(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))));
            }
            this.offset += n;
        }
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.shared.returned();
    }
}

use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_core::{packet::Datagram, request::Destination};
use zero_platform_tokio::contract::{
    datagram::{DatagramRx, DatagramTx},
    BoxFuture, SendFailure, SendReceipt,
};
pub struct Tx {
    shared: Arc<Shared>,
    limits: DatagramLimits,
    bytes: Vec<u8>,
    pos: usize,
    payload: Option<usize>,
    cancel: BoxFuture<'static, ()>,
}
pub struct Rx {
    shared: Arc<Shared>,
    bytes: Vec<u8>,
    pos: usize,
    cancel: BoxFuture<'static, ()>,
}
impl Tx {
    pub(super) fn new(shared: Arc<Shared>, limits: DatagramLimits) -> Self {
        Self {
            cancel: shared.cancellation(),
            shared,
            limits,
            bytes: vec![],
            pos: 0,
            payload: None,
        }
    }
}
impl Rx {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            cancel: shared.cancellation(),
            shared,
            bytes: vec![0; 2],
            pos: 0,
        }
    }
}
fn cancelled(shared: &Shared, cancel: &mut BoxFuture<'static, ()>, cx: &mut Context<'_>) -> bool {
    if shared.cancel.is_cancelled() {
        return true;
    }
    if cancel.as_mut().poll(cx).is_ready() {
        shared.cancel.cancel();
        return true;
    }
    false
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        let failure = |reason, delivery| {
            SendFailure::new(this.shared.error(Stage::Send, reason, delivery), delivery)
        };
        if cancelled(&this.shared, &mut this.cancel, cx) {
            return Poll::Ready(Err(failure(Reason::Cancelled, DeliveryState::Unknown)));
        }
        if let Some(n) = this.payload {
            if frame.association != this.shared.association
                || this.limits.validate(frame).is_err()
                || n != frame.payload.len()
                || this.bytes[this.bytes.len() - n..] != *frame.payload
            {
                this.shared.cancel.cancel();
                return Poll::Ready(Err(failure(Reason::Protocol, DeliveryState::Unknown)));
            }
        } else {
            this.limits.validate(frame)?;
            if frame.association != this.shared.association {
                return Poll::Ready(Err(failure(Reason::StalePlan, DeliveryState::NotSubmitted)));
            }
            this.bytes = crate::udp::encode_udp_flow_packet(
                &frame.address.address,
                frame.address.port,
                &frame.payload,
            )
            .map_err(|source| {
                SendFailure::new(
                    this.shared
                        .error(Stage::Send, Reason::Protocol, DeliveryState::NotSubmitted)
                        .with_source(source),
                    DeliveryState::NotSubmitted,
                )
            })?;
            this.payload = Some(frame.payload.len());
            this.pos = 0;
        }
        let mut state = this.shared.state.lock().unwrap();
        let stream = state.stream.as_mut().expect("live VMess UDP views");
        while this.pos < this.bytes.len() {
            match Pin::new(&mut *stream).poll_write(cx, &this.bytes[this.pos..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(n)) if n > 0 => this.pos += n,
                result => {
                    drop(state);
                    this.shared.cancel.cancel();
                    let source = match result {
                        Poll::Ready(Err(error)) => error,
                        _ => std::io::ErrorKind::WriteZero.into(),
                    };
                    return Poll::Ready(Err(SendFailure::new(
                        this.shared
                            .error(Stage::Send, Reason::Io, DeliveryState::Unknown)
                            .with_source(source),
                        DeliveryState::Unknown,
                    )));
                }
            }
        }
        let flushed = std::task::ready!(Pin::new(stream).poll_flush(cx));
        drop(state);
        flushed.map_err(|source| {
            this.shared.cancel.cancel();
            SendFailure::new(
                this.shared
                    .error(Stage::Send, Reason::Io, DeliveryState::Unknown)
                    .with_source(source),
                DeliveryState::Unknown,
            )
        })?;
        if let Some(observer) = &this.shared.observer {
            observer.datagram_sent(this.shared.session, this.bytes.len() as u64);
        }
        let n = this.payload.take().unwrap();
        this.bytes.clear();
        this.pos = 0;
        Poll::Ready(Ok(SendReceipt {
            accepted_bytes: n,
            delivery: DeliveryState::LocallyAccepted,
        }))
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if cancelled(&this.shared, &mut this.cancel, cx) {
            return Poll::Ready(Err(this.shared.error(
                Stage::Receive,
                Reason::Cancelled,
                DeliveryState::Unknown,
            )));
        }
        loop {
            let mut state = this.shared.state.lock().unwrap();
            let stream = state.stream.as_mut().expect("live VMess UDP views");
            let mut buf = ReadBuf::new(&mut this.bytes[this.pos..]);
            let read = std::task::ready!(Pin::new(stream).poll_read(cx, &mut buf));
            let n = buf.filled().len();
            drop(state);
            read.map_err(|source| {
                this.shared.cancel.cancel();
                this.shared
                    .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                    .with_source(source)
            })?;
            if n == 0 {
                return if this.pos == 0 {
                    Poll::Ready(Ok(None))
                } else {
                    this.shared.cancel.cancel();
                    Poll::Ready(Err(this
                        .shared
                        .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                        .with_source(std::io::Error::from(
                            std::io::ErrorKind::UnexpectedEof,
                        ))))
                };
            }
            this.pos += n;
            if this.pos < this.bytes.len() {
                continue;
            }
            if this.bytes.len() == 2 {
                let len = u16::from_be_bytes([this.bytes[0], this.bytes[1]]) as usize;
                if len == 0 {
                    this.shared.cancel.cancel();
                    return Poll::Ready(Err(this.shared.error(
                        Stage::Receive,
                        Reason::Protocol,
                        DeliveryState::Unknown,
                    )));
                }
                this.bytes.resize(len + 2, 0);
                continue;
            }
            let decoded = crate::udp::decode_udp_flow_packet(&this.bytes).map_err(|source| {
                this.shared.cancel.cancel();
                this.shared
                    .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                    .with_source(source)
            })?;
            let (address, port, payload) = decoded.into_parts();
            this.bytes.clear();
            this.bytes.resize(2, 0);
            this.pos = 0;
            return Poll::Ready(Ok(Some(Datagram {
                association: this.shared.association,
                address: Destination::new(address, port),
                payload: payload.into(),
            })));
        }
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
        self.shared.returned();
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.shared.returned();
    }
}

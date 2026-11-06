//! One retained Trojan frame; Pending never accepts a different packet on retry.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;
use zero_core::packet::Datagram;
use zero_platform_tokio::contract::{
    datagram::DatagramTx, BoxFuture, CloseReason, SendFailure, SendReceipt,
};
pub struct Tx {
    shared: Arc<Shared>,
    limits: DatagramLimits,
    bytes: Vec<u8>,
    offset: usize,
    payload: Option<usize>,
    started: bool,
    target: Option<zero_core::request::Destination>,
    cancelled: BoxFuture<'static, ()>,
}
impl Tx {
    pub(super) fn new(shared: Arc<Shared>, limits: DatagramLimits) -> Self {
        Self {
            cancelled: shared.cancellation(),
            shared,
            limits,
            bytes: vec![],
            offset: 0,
            payload: None,
            started: false,
            target: None,
        }
    }
    fn delivery(&self) -> DeliveryState {
        if self.started {
            DeliveryState::Unknown
        } else {
            DeliveryState::NotSubmitted
        }
    }
    fn failure(&self, reason: Reason) -> SendFailure {
        let delivery = self.delivery();
        SendFailure::new(self.shared.error(Stage::Send, reason, delivery), delivery)
    }
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.shared.cancel.is_cancelled() || Future::poll(this.cancelled.as_mut(), cx).is_ready()
        {
            this.shared.stop(CloseReason::Cancelled);
            return Poll::Ready(Err(this.failure(Reason::Cancelled)));
        }
        if let Some(payload) = this.payload {
            if frame.association != this.shared.association
                || this.target.as_ref() != Some(&frame.address)
                || payload != frame.payload.len()
                || this.bytes[this.bytes.len() - payload..] != *frame.payload
            {
                this.shared.stop(CloseReason::DeliveryUnknown);
                return Poll::Ready(Err(this.failure(Reason::Protocol)));
            }
        } else {
            this.limits.validate(frame)?;
            if frame.association != this.shared.association {
                return Poll::Ready(Err(this.failure(Reason::StalePlan)));
            }
            if matches!(&frame.address.address,zero_core::Address::Domain(name) if name.len()>255) {
                return Poll::Ready(Err(this.failure(Reason::InvalidConfig)));
            }
            this.bytes = crate::shared::build_udp_packet(
                &frame.address.address,
                frame.address.port,
                &frame.payload,
            );
            this.target = Some(frame.address.clone());
            this.payload = Some(frame.payload.len());
        }
        let mut state = this.shared.state.lock().unwrap();
        let stream = state
            .stream
            .as_mut()
            .expect("actual retained VLESS UDP view");
        while this.offset < this.bytes.len() {
            this.started = true;
            match Pin::new(&mut *stream).poll_write(cx, &this.bytes[this.offset..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(n)) if n > 0 => this.offset += n,
                result => {
                    drop(state);
                    this.shared.stop(CloseReason::ProtocolFailure);
                    let source = match result {
                        Poll::Ready(Err(source)) => source,
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
            this.shared.stop(CloseReason::ProtocolFailure);
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
        let payload = this.payload.take().unwrap();
        this.bytes.clear();
        this.offset = 0;
        this.started = false;
        this.target = None;
        Poll::Ready(Ok(SendReceipt::accepted(payload)))
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
        self.shared.returned();
    }
}

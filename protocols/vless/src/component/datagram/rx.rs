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
    bytes: memory::Buffer,
    output: Option<memory::Output>,
    offset: usize,
    length: Option<usize>,
    response_pending: bool,
    cancelled: BoxFuture<'static, ()>,
    _cancel_metadata: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    shared: SharedRef,
}
impl Rx {
    pub(super) fn new(
        shared: SharedRef,
        mut bytes: memory::Buffer,
        output: Option<memory::Output>,
        cancel_metadata: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    ) -> Self {
        bytes.resize(2, 0);
        Self {
            cancelled: shared.cancellation(),
            shared,
            bytes,
            output,
            _cancel_metadata: cancel_metadata,
            offset: 0,
            length: None,
            response_pending: true,
        }
    }
    fn packet(&mut self, cx: &mut Context<'_>) -> Poll<Result<Datagram, ChannelError>> {
        let (payload, address) = match &mut self.output {
            Some(output) => {
                std::task::ready!(output.poll_copy(cx, &self.bytes[2..], &self.shared.target))?
            }
            None => (self.bytes[2..].to_vec().into(), self.shared.target.clone()),
        };
        self.bytes.clear();
        self.bytes.resize(2, 0);
        self.offset = 0;
        self.length = None;
        Poll::Ready(Ok(Datagram {
            association: self.shared.association,
            address,
            payload,
        }))
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
            if this.response_pending && this.offset >= 1 {
                if let Err(source) = crate::shared::validate_response_version(this.bytes[0]) {
                    if let Some(observer) = &this.shared.observer {
                        observer.record(this.shared.session, ConnectFact::PeerRejected);
                    }
                    this.shared.stop(CloseReason::ProtocolFailure);
                    return Poll::Ready(Err(this
                        .shared
                        .error(Stage::Handshake, Reason::Protocol, DeliveryState::Unknown)
                        .with_source(source)));
                }
            }
            if this.offset == this.bytes.len() {
                if this.length.is_none() {
                    let len = if this.response_pending {
                        this.bytes[1] as usize
                    } else {
                        u16::from_be_bytes([this.bytes[0], this.bytes[1]]) as usize
                    };
                    this.length = Some(len);
                    this.bytes.resize(len + 2, 0);
                }
                if this.offset == this.bytes.len() {
                    if this.response_pending {
                        let wire = this.bytes.len();
                        this.response_pending = false;
                        this.bytes.clear();
                        this.bytes.resize(2, 0);
                        this.offset = 0;
                        this.length = None;
                        if let Some(observer) = &this.shared.observer {
                            observer.record(this.shared.session, ConnectFact::PeerConfirmed);
                            observer.protocol_traffic(this.shared.session, wire as u64, 0);
                        }
                        continue;
                    }
                    return this.packet(cx).map(|result| result.map(Some));
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
                    .error(
                        if this.response_pending {
                            Stage::Handshake
                        } else {
                            Stage::Receive
                        },
                        Reason::Io,
                        DeliveryState::Unknown,
                    )
                    .with_source(source)
            })?;
            if n == 0 {
                if this.offset == 0 && !this.response_pending {
                    return Poll::Ready(Ok(None));
                }
                this.shared.stop(CloseReason::ProtocolFailure);
                return Poll::Ready(Err(this
                    .shared
                    .error(
                        if this.response_pending {
                            Stage::Handshake
                        } else {
                            Stage::Receive
                        },
                        if this.response_pending {
                            Reason::Io
                        } else {
                            Reason::Protocol
                        },
                        DeliveryState::Unknown,
                    )
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

//! Keep the exact partial address/header/payload until the same poll completes.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};
use zero_core::{packet::Datagram, request::Destination, Address};
use zero_platform_tokio::contract::{datagram::DatagramRx, BoxFuture, CloseReason};
pub struct Rx {
    shared: Arc<Shared>,
    bytes: Vec<u8>,
    offset: usize,
    address_end: Option<usize>,
    target: Option<Destination>,
    cancelled: BoxFuture<'static, ()>,
}
impl Rx {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            cancelled: shared.cancellation(),
            shared,
            bytes: vec![0],
            offset: 0,
            address_end: None,
            target: None,
        }
    }
    fn error(&self, source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
        self.shared.stop(CloseReason::ProtocolFailure);
        self.shared
            .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
            .with_source(source)
    }
    fn advance(&mut self) -> Result<Option<Datagram>, ChannelError> {
        if self.target.is_some() {
            let target = self.target.take().unwrap();
            let header = self.address_end.unwrap() + 6;
            let payload = self.bytes[header..].to_vec().into();
            self.bytes.clear();
            self.bytes.resize(1, 0);
            self.offset = 0;
            self.address_end = None;
            return Ok(Some(Datagram {
                association: self.shared.association,
                address: target,
                payload,
            }));
        }
        if self.address_end.is_none() {
            let end = match self.bytes[0] {
                1 => 5,
                4 => 17,
                3 if self.bytes.len() == 1 => {
                    self.bytes.resize(2, 0);
                    return Ok(None);
                }
                3 => 2 + self.bytes[1] as usize,
                _ => {
                    return Err(self.error(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid Trojan UDP address type",
                    )))
                }
            };
            self.address_end = Some(end);
            self.bytes.resize(end + 6, 0);
            return Ok(None);
        }
        let end = self.address_end.unwrap();
        if self.bytes[end + 4..end + 6] != [b'\r', b'\n'] {
            return Err(self.error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Trojan UDP CRLF missing",
            )));
        }
        let address = match self.bytes[0] {
            1 => Address::Ipv4(self.bytes[1..5].try_into().unwrap()),
            4 => Address::Ipv6(self.bytes[1..17].try_into().unwrap()),
            3 => Address::Domain(
                std::str::from_utf8(&self.bytes[2..end])
                    .map_err(|source| self.error(source))?
                    .to_owned(),
            ),
            _ => unreachable!("validated original address type"),
        };
        let port = u16::from_be_bytes([self.bytes[end], self.bytes[end + 1]]);
        let payload = u16::from_be_bytes([self.bytes[end + 2], self.bytes[end + 3]]) as usize;
        self.target = Some(Destination::new(address, port));
        self.bytes.resize(end + 6 + payload, 0);
        Ok(None)
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
                if let Some(packet) = this.advance()? {
                    return Poll::Ready(Ok(Some(packet)));
                }
                if this.offset == this.bytes.len() {
                    continue;
                }
            }
            let mut state = this.shared.state.lock().unwrap();
            let stream = state
                .stream
                .as_mut()
                .expect("actual retained Trojan packet view");
            let mut buffer = ReadBuf::new(&mut this.bytes[this.offset..]);
            let result = std::task::ready!(Pin::new(stream).poll_read(cx, &mut buffer));
            let count = buffer.filled().len();
            drop(state);
            result.map_err(|source| {
                this.shared.stop(CloseReason::ProtocolFailure);
                this.shared
                    .error(Stage::Receive, Reason::Io, DeliveryState::Unknown)
                    .with_source(source)
            })?;
            if count == 0 {
                if this.offset == 0 {
                    return Poll::Ready(Ok(None));
                }
                return Poll::Ready(Err(
                    this.error(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
                ));
            }
            this.offset += count;
        }
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.shared.returned();
    }
}

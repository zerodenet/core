use super::format_authority;
use crate::body::HttpBodyKind;
use alloc::{string::String, vec::Vec};
use zero_core::{Network, ProtocolType, Session};

#[derive(Debug)]
pub struct HttpForwardCommand {
    pub(super) destination: zero_core::request::Destination,
    pub(super) method: String,
    pub(super) head: Vec<u8>,
    pub(super) body: HttpBodyKind,
    pub(super) close_after_response: bool,
    pub(super) upgrade_requested: bool,
    pub(super) expect_continue: bool,
    pub(super) supports_chunked_response: bool,
}

impl HttpForwardCommand {
    pub fn destination(&self) -> &zero_core::request::Destination {
        &self.destination
    }

    pub(super) fn into_legacy(self) -> HttpForwardRequest {
        HttpForwardRequest {
            session: Session::new(
                0,
                self.destination.address.clone(),
                self.destination.port,
                Network::Tcp,
                ProtocolType::new("http"),
            ),
            command: self,
        }
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn head(&self) -> &[u8] {
        &self.head
    }

    pub fn body(&self) -> HttpBodyKind {
        self.body
    }

    pub fn close_after_response(&self) -> bool {
        self.close_after_response
    }

    pub fn upgrade_requested(&self) -> bool {
        self.upgrade_requested
    }

    pub fn expect_continue(&self) -> bool {
        self.expect_continue
    }

    pub fn supports_chunked_response(&self) -> bool {
        self.supports_chunked_response
    }

    pub fn set_effective_authority(&mut self, target: &zero_core::Address, port: u16) {
        let authority = format_authority(target, port);
        let Some(host_start) = self
            .head
            .windows(6)
            .position(|window| window.eq_ignore_ascii_case(b"Host: "))
        else {
            return;
        };
        let value_start = host_start + 6;
        let Some(relative_end) = self.head[value_start..]
            .windows(2)
            .position(|window| window == b"\r\n")
        else {
            return;
        };
        self.head
            .splice(value_start..value_start + relative_end, authority.bytes());
        self.destination.address = target.clone();
        self.destination.port = port;
    }
}
#[derive(Debug)]
pub struct HttpForwardRequest {
    session: Session,
    command: HttpForwardCommand,
}
impl core::ops::Deref for HttpForwardRequest {
    type Target = HttpForwardCommand;
    fn deref(&self) -> &Self::Target {
        &self.command
    }
}
impl HttpForwardRequest {
    pub fn session(&self) -> &Session {
        &self.session
    }
    pub fn into_session(self) -> Session {
        self.session
    }
    pub fn set_effective_authority(&mut self, target: &zero_core::Address, port: u16) {
        self.command.set_effective_authority(target, port);
        self.session.target = self.command.destination.address.clone();
        self.session.port = self.command.destination.port;
    }
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Session,
        String,
        Vec<u8>,
        HttpBodyKind,
        bool,
        bool,
        bool,
        bool,
    ) {
        (
            self.session,
            self.command.method,
            self.command.head,
            self.command.body,
            self.command.close_after_response,
            self.command.upgrade_requested,
            self.command.expect_continue,
            self.command.supports_chunked_response,
        )
    }
}

//! Application-owned control client. The daemon's budget is never borrowed.
use std::{io, sync::Arc};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlMessageLimits};
mod document;
mod frames;
mod native;
mod owner;
mod wire;
pub(crate) use owner::Caller;
pub(crate) use wire::{RawInput, Reply};
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub frame: usize,
    pub total: usize,
}
impl Default for Limits {
    fn default() -> Self {
        let limits = ControlMessageLimits::default();
        Self {
            frame: limits.ipc_frame_bytes,
            total: limits.response_bytes_total,
        }
    }
}
impl Limits {
    pub(crate) fn validate(self) -> io::Result<Self> {
        if self.frame == 0 || self.frame > self.total || self.total > 256 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control client requires 1 <= frame <= total <= 268435456",
            ));
        }
        Ok(self)
    }
    fn budget_limits(self) -> ControlBudgetLimits {
        let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
        limits.connections = 1;
        limits.messages.ipc_frame_bytes = self.frame;
        limits.messages.response_bytes = self.frame;
        limits.messages.request_bytes_total = self.total;
        limits.messages.response_bytes_total = self.total;
        limits
    }
}
/// Only private framing; this does not dispatch commands or acquire IO.
#[derive(serde::Serialize)]
pub(crate) struct Command<'a, P: serde::Serialize> {
    #[serde(rename = "type")]
    kind: &'static str,
    id: Option<()>,
    method: &'a str,
    params: P,
}
impl<'a, P: serde::Serialize> Command<'a, P> {
    pub(crate) fn new(method: &'a str, params: P) -> Self {
        Self {
            kind: "command",
            id: None,
            method,
            params,
        }
    }
}
impl Caller {
    pub(crate) async fn request(
        &self,
        path: &str,
        request: &impl serde::Serialize,
    ) -> io::Result<Reply> {
        let input = frames::Frame::request(request, &self.original.book)?;
        self.original
            .request(path, input, false)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "no response from control socket",
                )
            })
    }
    pub(crate) async fn events(
        &self,
        path: &str,
        request: &impl serde::Serialize,
    ) -> io::Result<()> {
        let input = frames::Frame::request(request, &self.original.book)?;
        self.original.request(path, input, true).await?;
        Ok(())
    }
}
#[cfg(test)]
mod tests;

impl Caller {
    pub(crate) fn raw_json<'a>(&self, text: &'a str) -> io::Result<wire::RawInput<'a>> {
        wire::RawInput::new(text, &self.original.book)
    }
}

impl Caller {
    pub(crate) fn read_document(&self, path: &str) -> io::Result<document::Document> {
        let source = Arc::new(zero_runtime::configuration::ConfigurationSource::default());
        document::load(&source, std::path::Path::new(path), &self.original.book)
    }
}

//! A real child-owned native socket with a separate pending close operation.
use std::{future::Future, sync::Arc};
use tokio::{io::AsyncReadExt, sync::Notify, time::Instant};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, ResourceLifecycle},
        ChannelError, CloseReason, Impact, Reason, Stage,
    },
    TokioDatagramSocket,
};

pub(super) struct NativeGate {
    pub(super) socket: Option<TokioDatagramSocket>,
    pub(super) address: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
    pub(super) gate: tokio::net::TcpStream,
    pub(super) pending: Arc<Notify>,
    pub(super) gate_done: bool,
}
fn error(source: std::io::Error) -> ChannelError {
    ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(source)
}
impl ResourceLifecycle for NativeGate {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let socket = TokioDatagramSocket::bind("127.0.0.1:0")
            .await
            .map_err(error)?;
        let address = socket.local_addr().map_err(error)?;
        self.socket = Some(socket);
        self.address.take().unwrap().send(address).unwrap();
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if self.socket.is_none() {
            return Ok(CloseReport::default());
        }
        if !self.gate_done {
            let read = self.gate.read_u8();
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    self.pending.notify_one();
                }
                result
            })
            .await
            .map_err(error)?;
            self.gate_done = true;
        }
        self.socket.take().unwrap().close().map_err(error)?;
        Ok(CloseReport {
            closed_io: vec!["same actual child UDP original explicitly closed".into()],
            ..CloseReport::default()
        })
    }
}

pub(super) fn original_no_inbounds(error: &(dyn std::error::Error + 'static)) -> bool {
    if error
        .downcast_ref::<zero_engine::EngineError>()
        .is_some_and(|source| matches!(source, zero_engine::EngineError::NoInbounds))
    {
        return true;
    }
    if let Some(source) = error
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        if original_no_inbounds(source) {
            return true;
        }
    }
    error.source().is_some_and(original_no_inbounds)
}

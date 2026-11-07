use super::*;
pub(super) struct Control {
    pub(super) original: ClientTcpSocketControl,
    pub(super) completion: crate::runtime::packet_flow::FlowLogicalClose,
    pub(super) closing:
        Option<Pin<Box<dyn Future<Output = zero_stack::tcp::client::ClientTcpClosed> + Send>>>,
    pub(super) deadline: Option<Instant>,
    pub(super) closed: Option<zero_stack::tcp::client::ClientTcpClosed>,
}
impl ChannelControl for Control {
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.begin();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        match self.poll_closed(&mut cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "same logical TCP close continuation remains pending".into(),
                    ),
                ],
                ..Default::default()
            }),
        }
    }

    fn cancel(&self, _: CloseReason) {
        self.original.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        let end = *self.deadline.get_or_insert(deadline);
        self.begin();
        tokio::time::timeout_at(end, poll_fn(|cx| self.poll_closed(cx)))
            .await
            .map_err(|source| self.completion.deadline_failure(source))?
    }
}
impl Control {
    fn begin(&mut self) {
        if self.closed.is_none() && self.closing.is_none() {
            let actual = self.original.clone();
            self.closing = Some(Box::pin(async move { actual.close().await }));
            self.completion.continuation_started();
        }
    }
    fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<Result<CloseReport, ChannelError>> {
        if let Some(closed) = &self.closed {
            assert!(closed.matches(&self.original));
            return Poll::Ready(Ok(CloseReport::default()));
        }
        let closed = std::task::ready!(self
            .closing
            .as_mut()
            .expect("same original close future")
            .as_mut()
            .poll(cx));
        if !closed.matches(&self.original) {
            return Poll::Ready(Err(self.completion.terminal_failure(Arc::new(
                ChannelError::new(Stage::Close, Reason::Protocol, Impact::Component),
            ))));
        }
        self.closed = Some(closed);
        let completed = self.closing.take();
        drop(completed);
        self.completion.confirm();
        Poll::Ready(Ok(CloseReport::default())) // logical TCP only; no Native/Scope proof
    }
}

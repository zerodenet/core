use tokio::task::JoinSet;

pub(crate) struct MuxSessionLoop {
    pub(crate) inbound_tag: String,
    pub(crate) protocol: &'static str,
    pub(crate) panic_message: &'static str,
    pub(crate) abort_on_end: bool,
}

pub(crate) trait MuxOpenedDispatcher {
    type Error;

    fn dispatch_next(
        &mut self,
        tasks: &mut JoinSet<()>,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;
}

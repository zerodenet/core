//! Prepare one caller control before its existing answer future can be polled.
use super::*;
impl DnsSystem {
    /// Persist the control before polling the returned single-use answer future.
    /// Its Drop returns only this waiter; a shared query keeps its other callers.
    pub fn prepare_udp_query<'a>(
        &'a self,
        query: &'a [u8],
    ) -> (DnsQueryCall, BoxFuture<'a, io::Result<Vec<u8>>>) {
        self.prepare_wire_query(query, false)
    }
    /// The same actual caller/worker ownership for DNS-over-TCP framing.
    pub fn prepare_tcp_query<'a>(
        &'a self,
        query: &'a [u8],
    ) -> (DnsQueryCall, BoxFuture<'a, io::Result<Vec<u8>>>) {
        self.prepare_wire_query(query, true)
    }
    fn prepare_wire_query<'a>(
        &'a self,
        query: &'a [u8],
        tcp: bool,
    ) -> (DnsQueryCall, BoxFuture<'a, io::Result<Vec<u8>>>) {
        let state = State::new();
        let returned = Returned(state.clone());
        let original = DnsQueryCall(state.clone());
        let work = Box::pin(async move {
            let _returned = returned;
            let cancel = state.cancel.clone();
            CALL.scope(state, async move {
                tokio::select! { biased;
                    _ = cancel.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted,"DNS wire caller cancelled")),
                    result = async { if tcp {self.answer_tcp_query(query).await} else {self.answer_udp_query(query).await} } => result,
                }
            }).await
        });
        (original, work)
    }
}

impl State {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(State {
            cancel: CancellationToken::new(),
            returned: AtomicBool::new(false),
            registration: Mutex::new(None),
            conflict: AtomicBool::new(false),
            changed: Notify::new(),
            closed: OnceLock::new(),
            cleanup_failure: OnceLock::new(),
        })
    }
}

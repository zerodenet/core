//! Request-bound peer facts for real SIP022 responses only.
use super::*;
impl<S> ShadowsocksAeadStream<S> {
    pub(crate) fn with_connect_observer(
        mut self,
        session: Option<zero_core::component::SessionId>,
        observer: Option<
            std::sync::Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>,
        >,
    ) -> Self {
        // Only SIP022 binds its authenticated response to this request salt.
        if self.is_2022 {
            self.connect_observer = observer.map(|observer| (session, observer));
        }
        self
    }
    pub(super) fn report_peer(
        &mut self,
        fact: zero_platform_tokio::contract::context::ConnectFact,
    ) {
        if let Some((session, observer)) = self.connect_observer.take() {
            observer.record(session, fact);
        }
    }
}

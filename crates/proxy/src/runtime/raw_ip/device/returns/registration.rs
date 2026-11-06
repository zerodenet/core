//! Original source/conversation admission, shared by legacy and standard roles.
use super::*;
impl PacketReturns {
    #[cfg(test)]
    pub(crate) fn register(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
    ) -> io::Result<()> {
        self.register_observed(source, ingress_id, replies, None, None)
    }
    pub(crate) fn register_observed(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        conversation: Option<packet::PacketConversationKey>,
    ) -> io::Result<()> {
        self.register_source(
            source,
            ReturnSource::Legacy(ingress_id),
            replies,
            observer,
            conversation,
        )
    }
    pub(super) fn register_source(
        &self,
        source: IpAddr,
        ingress_id: ReturnSource,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        conversation: Option<packet::PacketConversationKey>,
    ) -> io::Result<()> {
        let now = Instant::now();
        // Admit both route and observation before publishing either. A rejected
        // overlapping ingress must not replace the accepted flow's observer.
        let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(route) = routes.get(&source) {
            let differs = route.ingress_id != ingress_id
                || (matches!(ingress_id, ReturnSource::Endpoint { .. })
                    && !route.replies.same_channel(&replies));
            if differs
                && now.duration_since(route.touched) < IDLE_TIMEOUT
                && !route.replies.is_closed()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "overlapping packet source on shared outbound",
                ));
            }
        } else {
            if routes.len() >= MAX_RETURN_ROUTES {
                routes.retain(|_, route| {
                    now.duration_since(route.touched) < IDLE_TIMEOUT && !route.replies.is_closed()
                });
            }
            if routes.len() >= MAX_RETURN_ROUTES {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "packet return route limit",
                ));
            }
        }
        // Return channel ownership is mandatory execution state, with its own
        // bounded capacity. It is distinct from optional metric observations.
        let mut conversations = self.conversations.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(key) = conversation {
            if !conversations.contains_key(&key) && conversations.len() >= 8192 {
                conversations.retain(|_, c| c.touched.elapsed() < Duration::from_secs(600));
                if conversations.len() >= 8192 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "packet return conversation limit",
                    ));
                }
            }
            conversations.insert(
                key,
                ReturnConversation {
                    replies: replies.clone(),
                    touched: now,
                },
            );
        }
        let mut observations = self.observations.lock().unwrap_or_else(|e| e.into_inner());
        if let (Some(observer), Some(key)) = (observer, conversation) {
            if self
                .receive_incomplete
                .load(std::sync::atomic::Ordering::Acquire)
            {
                observer.receive_coverage_lost();
            }
            if !observations.contains_key(&key) && observations.len() >= MAX_OBSERVATIONS {
                observer.receive_coverage_lost();
            } else {
                observations.insert(
                    key,
                    ReturnObservation {
                        observer: Some(observer),
                        touched: now,
                    },
                );
            }
        }
        routes.insert(
            source,
            ReturnRoute {
                ingress_id,
                replies,
                touched: now,
            },
        );
        Ok(())
    }
}

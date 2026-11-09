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
    pub(in crate::runtime::raw_ip::device) fn register_source(
        &self,
        source: IpAddr,
        ingress_id: ReturnSource,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        conversation: Option<packet::PacketConversationKey>,
    ) -> io::Result<()> {
        let _notifications = self.budget.defer_notifications();
        let memory = self.budget.metadata();
        let now = Instant::now();
        self.prune_registration(source, conversation, now);
        let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, row)) = routes.iter().find(|(key, _)| *key == source) {
            if (row.ingress_id != ingress_id || !row.replies.same_channel(&replies))
                && !row.replies.is_closed()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "overlapping live original packet source",
                ));
            }
        }
        let route_index = routes.iter().position(|(key, _)| *key == source);
        if route_index.is_none() {
            routes
                .try_reserve_one_with_metadata(&memory, self.row_limits.routes)
                .map_err(table_capacity)?;
        }
        let mut conversations = self.conversations.lock().unwrap_or_else(|e| e.into_inner());
        let conversation_index = conversation
            .and_then(|key| conversations.iter().position(|(actual, _)| *actual == key));
        if let Some(index) = conversation_index {
            let original = &conversations[index].1;
            if !original.replies.same_channel(&replies) && !original.replies.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "live return conversation belongs to another original source",
                ));
            }
        }
        if conversation.is_some() && conversation_index.is_none() {
            conversations
                .try_reserve_one_with_metadata(&memory, self.row_limits.conversations)
                .map_err(table_capacity)?;
        }
        // All mandatory capacities are prepaid before publishing either row.
        let mut observations = self.observations.lock().unwrap_or_else(|e| e.into_inner());
        let mut old_observation = None;
        let mut coverage = None;
        if let (Some(observer), Some(key)) = (observer, conversation) {
            if self
                .receive_incomplete
                .load(std::sync::atomic::Ordering::Acquire)
            {
                coverage = Some(observer.clone());
            }
            let value = ReturnObservation {
                replies: replies.clone(),
                observer: Some(observer.clone()),
                touched: now,
            };
            if let Some(index) = observations.iter().position(|(actual, _)| *actual == key) {
                old_observation = Some(std::mem::replace(&mut observations[index].1, value));
            } else if observations
                .try_reserve_one_with_metadata(&memory, self.row_limits.observations)
                .is_ok()
            {
                observations
                    .push_reserved((key, value))
                    .unwrap_or_else(|_| unreachable!("same locked prepaid observation"));
            } else {
                coverage = Some(observer);
                old_observation = Some(value);
            }
        }
        let mut old_conversation = None;
        if let Some(key) = conversation {
            let value = ReturnConversation {
                replies: replies.clone(),
                touched: now,
            };
            if let Some(index) = conversation_index {
                old_conversation = Some(std::mem::replace(&mut conversations[index].1, value));
            } else {
                conversations
                    .push_reserved((key, value))
                    .unwrap_or_else(|_| unreachable!("same locked prepaid conversation"));
            }
        }
        let value = ReturnRoute {
            ingress_id,
            replies,
            touched: now,
        };
        let old_route = if let Some(index) = route_index {
            Some(std::mem::replace(&mut routes[index].1, value))
        } else {
            routes
                .push_reserved((source, value))
                .unwrap_or_else(|_| unreachable!("same locked prepaid route"));
            None
        };
        drop(observations);
        drop(conversations);
        drop(routes);
        if let Some(observer) = coverage {
            observer.receive_coverage_lost();
        }
        drop((old_observation, old_conversation, old_route));
        Ok(())
    }
}
fn table_capacity(source: zero_tun::packet_memory::PacketMemoryError) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, source)
}

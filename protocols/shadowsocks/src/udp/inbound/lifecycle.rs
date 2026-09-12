use super::*;

impl ShadowsocksInboundUdpSession {
    pub(super) fn maintain(&mut self) {
        self.bindings.prune();
        self.codec.maintain();
    }
}
impl ShadowsocksInboundUdpResponder {
    pub(super) fn maintain(&mut self) {
        let now = tokio::time::Instant::now();
        if now < self.maintenance_at {
            return;
        }
        self.maintenance_at = now + state::MAINTENANCE;
        self.associations.prune();
        match &mut self.mode {
            ShadowsocksInboundUdpResponderMode::Single(session) => session.maintain(),
            ShadowsocksInboundUdpResponderMode::Profile {
                profile,
                sessions,
                proxy_users,
                current_user,
                current_auth,
            } => {
                let users = profile.users_snapshot();
                let active: std::collections::HashSet<_> = users
                    .iter()
                    .map(crate::inbound::ShadowsocksUser::cache_key)
                    .collect();
                sessions.retain(|key, session| {
                    session.maintain();
                    active.contains(key)
                });
                proxy_users.retain(|id, user| {
                    sessions.get(user).is_some_and(|s| s.bindings.contains(*id))
                });
                if current_user
                    .as_ref()
                    .is_some_and(|key| !active.contains(key))
                {
                    *current_user = None;
                    *current_auth = None;
                }
            }
        }
    }
    pub(super) fn touch_response(&mut self, id: Option<u64>) {
        let Some(id) = id else {
            return;
        };
        match &mut self.mode {
            ShadowsocksInboundUdpResponderMode::Single(session) => {
                session.bindings.touch(id);
                session
                    .codec
                    .touch_association(session.bindings.session(id));
                if let Some(client) = session.bindings.client(id) {
                    self.associations
                        .touch(None, session.bindings.session(id), client);
                }
            }
            ShadowsocksInboundUdpResponderMode::Profile {
                sessions,
                proxy_users,
                ..
            } => {
                if let Some(user) = proxy_users.get(&id) {
                    if let Some(session) = sessions.get_mut(user) {
                        session.bindings.touch(id);
                        session
                            .codec
                            .touch_association(session.bindings.session(id));
                        if let Some(client) = session.bindings.client(id) {
                            self.associations.touch(
                                Some(user),
                                session.bindings.session(id),
                                client,
                            );
                        }
                    }
                }
            }
        }
    }
}

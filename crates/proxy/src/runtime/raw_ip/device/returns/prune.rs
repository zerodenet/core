//! Detach ONE retired row, release ALL owner locks, then drop its original.
use super::*;
impl PacketReturns {
    pub(super) fn prune_registration(
        &self,
        source: IpAddr,
        conversation: Option<packet::PacketConversationKey>,
        now: Instant,
    ) {
        for _ in 0..self.row_limits.routes {
            let old = {
                let mut rows = self.routes.lock().unwrap_or_else(|e| e.into_inner());
                if rows.iter().any(|(key, _)| *key == source) || rows.len() < self.row_limits.routes
                {
                    None
                } else {
                    let index = rows.iter().position(|(_, row)| {
                        row.replies.is_closed() && now.duration_since(row.touched) >= IDLE_TIMEOUT
                    });
                    index.map(|index| rows.remove(index))
                }
            };
            match old {
                Some(old) => drop(old),
                None => break,
            }
        }
        if let Some(key) = conversation {
            for _ in 0..self.row_limits.conversations {
                let old = {
                    let mut rows = self.conversations.lock().unwrap_or_else(|e| e.into_inner());
                    if rows.iter().any(|(actual, _)| *actual == key)
                        || rows.len() < self.row_limits.conversations
                    {
                        None
                    } else {
                        let index = rows.iter().position(|(_, row)| {
                            row.replies.is_closed()
                                && now.duration_since(row.touched) >= Duration::from_secs(600)
                        });
                        index.map(|index| rows.remove(index))
                    }
                };
                match old {
                    Some(old) => drop(old),
                    None => break,
                }
            }
            for _ in 0..self.row_limits.observations {
                let old = {
                    let mut rows = self.observations.lock().unwrap_or_else(|e| e.into_inner());
                    if rows.iter().any(|(actual, _)| *actual == key)
                        || rows.len() < self.row_limits.observations
                    {
                        None
                    } else {
                        let index = rows.iter().position(|(_, row)| {
                            row.replies.is_closed()
                                && now.duration_since(row.touched) >= IDLE_TIMEOUT
                        });
                        index.map(|index| rows.remove(index))
                    }
                };
                match old {
                    Some(old) => drop(old),
                    None => break,
                }
            }
        }
    }
}

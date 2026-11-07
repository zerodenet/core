use super::*;
#[must_use]
pub struct PacketSendNotifications {
    _guard: NotificationGuard,
}
enum NotificationGuard {
    Root {
        _original: zero_tun::packet_memory::PacketRootNotifications,
    },
    View {
        _original: zero_tun::packet_memory::PacketMemoryNotifications,
    },
}

impl super::super::PacketSender {
    /// Synchronous owner operations release all their locks before any memory
    /// notification calls an external waker. Never keep this across an await.
    pub fn defer_memory_notifications(&self) -> Option<PacketSendNotifications> {
        let guard = match &self.memory.as_ref()?.origin {
            Origin::Standalone(root) => NotificationGuard::Root {
                _original: root.defer_notifications(),
            },
            Origin::Original(view) => NotificationGuard::View {
                _original: view.defer_notifications(),
            },
            Origin::PendingOriginal(cell) => NotificationGuard::View {
                _original: cell.get()?.defer_notifications(),
            },
            Origin::ExistingStorage => return None,
        };
        Some(PacketSendNotifications { _guard: guard })
    }
}

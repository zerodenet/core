//! Explicit accepted-original and same-entry task occupancy, with no defaults.
use std::num::NonZeroUsize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedTcpLimits {
    /// Live, retiring and Unknown originals, never lifetime healthy accepts.
    pub max_sockets: NonZeroUsize,
    /// Primary handler plus all selected same-entry users/drivers. This is not
    /// borrowed from listener bind attempts, datagram peers or outer Scope one.
    pub max_tasks_per_socket: NonZeroUsize,
    /// Original auxiliary owners retained by this exact accepted entry.
    /// Zero grants no resource installation; this never grants native access.
    pub max_resources_per_socket: usize,
}
impl AcceptedTcpLimits {
    /// The host must add this checked ceiling to its original driver/internal
    /// tasks before creating the actual Scope or performing native acquisition.
    pub fn task_slots(self) -> Option<NonZeroUsize> {
        self.max_sockets
            .get()
            .checked_mul(self.max_tasks_per_socket.get())
            .and_then(NonZeroUsize::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_ceiling_is_checked_without_a_numeric_default() {
        let limits = AcceptedTcpLimits {
            max_sockets: NonZeroUsize::new(3).unwrap(),
            max_tasks_per_socket: NonZeroUsize::new(2).unwrap(),
            max_resources_per_socket: 0,
        };
        assert_eq!(limits.task_slots().unwrap().get(), 6);
        let overflow = AcceptedTcpLimits {
            max_sockets: NonZeroUsize::new(usize::MAX).unwrap(),
            ..limits
        };
        assert!(overflow.task_slots().is_none());
    }
}

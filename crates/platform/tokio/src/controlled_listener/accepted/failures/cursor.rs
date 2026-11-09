use super::*;
/// No raw TaskJoined/Arc leaves this new observation path. The actual header
/// originals drop before the same paid Book can release its final promises.
pub struct AcceptedTaskFailureView {
    joined: TaskJoined,
    key: rows::Key,
    receiver: AcceptedTaskFailures,
}
impl Clone for AcceptedTaskFailureView {
    fn clone(&self) -> Self {
        Self {
            joined: self.joined.clone(),
            key: self.key,
            receiver: self.receiver.clone(),
        }
    }
}
impl AcceptedTaskFailureView {
    pub fn id(&self) -> zero_core::component::TaskId {
        self.joined.id()
    }
    pub fn class(&self) -> crate::contract::scope::TaskClass {
        self.joined.class()
    }
    pub fn result(&self) -> Result<(), &ChannelError> {
        self.joined.error_ref().map_or(Ok(()), Err)
    }
    pub fn matches_joined(&self, joined: &TaskJoined) -> bool {
        let rows = self.receiver.0.rows.lock().unwrap();
        rows.values.get(self.key.index).is_some_and(|row| {
            row.serial == self.key.serial
                && row.witness.as_ref().is_some_and(|witness| {
                    witness.matches_joined(&self.joined) && witness.matches_joined(joined)
                })
        })
    }
}
/// Fixed-end pure cursor; no snapshot Vec or naked original-header escape.
pub struct AcceptedTaskFailureCursor {
    pub(super) receiver: AcceptedTaskFailures,
    pub(super) next: usize,
    pub(super) end: usize,
}
impl Iterator for AcceptedTaskFailureCursor {
    type Item = AcceptedTaskFailureView;
    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.end {
            let index = self.next;
            self.next += 1;
            let actual = self
                .receiver
                .0
                .rows
                .lock()
                .unwrap()
                .values
                .get(index)
                .and_then(|row| {
                    row.failure
                        .as_ref()
                        .map(|joined| (row.serial, joined.clone()))
                });
            if let Some((serial, joined)) = actual {
                return Some(AcceptedTaskFailureView {
                    joined,
                    key: rows::Key { index, serial },
                    receiver: self.receiver.clone(),
                });
            }
        }
        None
    }
}

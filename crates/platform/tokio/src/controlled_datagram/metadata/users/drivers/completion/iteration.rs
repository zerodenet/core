//! Stack cursor over the original sealed registrations, never a copied Vec.
use super::*;
#[derive(Clone)]
pub(in crate::controlled_datagram::metadata::users) enum RegistrationSequence {
    Empty,
    Library(Arc<[TaskJoinWitness]>),
    Original(DriverRegistrationsClosed),
}
pub struct RegistrationIter {
    source: RegistrationSequence,
    index: usize,
    after: Option<zero_core::component::TaskId>,
}
impl RegistrationSequence {
    pub(in crate::controlled_datagram::metadata::users) fn iter(&self) -> RegistrationIter {
        RegistrationIter {
            source: self.clone(),
            index: 0,
            after: None,
        }
    }
}
impl RegistrationIter {
    /// Retained records only. A row can disappear solely through actual Host
    /// ACK and original reclaim, never through a body/Scope/view inference.
    pub fn len(&self) -> usize {
        match &self.source {
            RegistrationSequence::Empty => 0,
            RegistrationSequence::Library(values) => values.len().saturating_sub(self.index),
            RegistrationSequence::Original(original) => original.remaining(self.after),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn iter(self) -> Self {
        self
    }
}
impl Iterator for RegistrationIter {
    type Item = TaskJoinWitness;
    fn next(&mut self) -> Option<Self::Item> {
        match &self.source {
            RegistrationSequence::Empty => None,
            RegistrationSequence::Library(values) => {
                let witness = values.get(self.index)?.clone();
                self.index += 1;
                Some(witness)
            }
            RegistrationSequence::Original(original) => {
                let (id, witness) = original.next(self.after)?;
                self.after = Some(id);
                Some(witness)
            }
        }
    }
}

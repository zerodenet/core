//! Only this inspector's finite rows, using the same existing Host metadata Root.
use zero_platform_tokio::contract::component::{PacketDriverMetadataView, PacketMetadataVec};
use zero_traits::packet_metadata::PacketMemoryError;
pub(crate) enum Rows<T> {
    Library(Vec<T>),
    Source {
        values: PacketMetadataVec<T>,
        memory: PacketDriverMetadataView,
    },
}
impl<T> Rows<T> {
    pub(crate) fn new(memory: Option<&PacketDriverMetadataView>) -> Self {
        match memory {
            Some(memory) => Self::Source {
                values: PacketMetadataVec::default(),
                memory: memory.clone(),
            },
            None => Self::Library(Vec::new()),
        }
    }
    pub(crate) fn try_push(
        &mut self,
        value: T,
        maximum: usize,
    ) -> Result<(), (PacketMemoryError, T)> {
        match self {
            Self::Library(values) if values.len() < maximum => {
                values.push(value);
                Ok(())
            }
            Self::Library(_) => Err((PacketMemoryError::Capacity, value)),
            Self::Source { values, memory } => {
                values.try_push_with_metadata(value, memory, maximum)
            }
        }
    }
    pub(crate) fn remove(&mut self, index: usize) -> T {
        match self {
            Self::Library(values) => values.remove(index),
            Self::Source { values, .. } => values.remove(index),
        }
    }
    pub(crate) fn memory(&self) -> Option<&PacketDriverMetadataView> {
        match self {
            Self::Library(_) => None,
            Self::Source { memory, .. } => Some(memory),
        }
    }
    pub(crate) fn clear(&mut self) {
        while !self.is_empty() {
            drop(self.remove(self.len() - 1));
        }
    }
}
impl<T> std::ops::Deref for Rows<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        match self {
            Self::Library(values) => values,
            Self::Source { values, .. } => values,
        }
    }
}
impl<T> std::ops::DerefMut for Rows<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            Self::Library(values) => values,
            Self::Source { values, .. } => values,
        }
    }
}

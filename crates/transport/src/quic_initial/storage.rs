use std::ops::{Deref, DerefMut};
use zero_platform_tokio::contract::component::{PacketDriverMetadataView, PacketMetadataVec};
use zero_traits::packet_metadata::PacketMemoryError;
pub(super) enum Rows<T> {
    Library(Vec<T>),
    Source {
        values: PacketMetadataVec<T>,
        memory: PacketDriverMetadataView,
    },
}
impl<T> Default for Rows<T> {
    fn default() -> Self {
        Self::Library(Vec::new())
    }
}
impl<T> Rows<T> {
    pub(super) fn source(memory: PacketDriverMetadataView) -> Self {
        Self::Source {
            values: Default::default(),
            memory,
        }
    }
    pub(super) fn push(&mut self, value: T, max: usize) -> Result<(), PacketMemoryError> {
        match self {
            Self::Library(values) => {
                if values.len() >= max {
                    return Err(PacketMemoryError::Capacity);
                }
                values.push(value);
                Ok(())
            }
            Self::Source { values, memory } => values
                .try_push_with_metadata(value, memory, max)
                .map_err(|(source, _)| source),
        }
    }
    pub(super) fn resize(
        &mut self,
        length: usize,
        value: T,
        max: usize,
    ) -> Result<(), PacketMemoryError>
    where
        T: Copy,
    {
        if length > max {
            return Err(PacketMemoryError::Capacity);
        }
        while self.len() < length {
            self.push(value, max)?;
        }
        Ok(())
    }
}
impl<T> Deref for Rows<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        match self {
            Self::Library(v) => v,
            Self::Source { values, .. } => values,
        }
    }
}
impl<T> DerefMut for Rows<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            Self::Library(v) => v,
            Self::Source { values, .. } => values,
        }
    }
}

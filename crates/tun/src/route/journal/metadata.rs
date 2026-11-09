//! Concrete custody of this journal's strings and two row arrays only.
use crate::packet_memory::{
    PacketDriverMetadataView, PacketMemoryError, PacketMetadataCredit, PacketMetadataLayout,
};
use std::{fmt, mem};
#[derive(Default)]
pub(in crate::route) struct Owner {
    pub(super) failure: Option<PacketMemoryError>,
    view: Option<PacketDriverMetadataView>,
    credit: Option<PacketMetadataCredit>,
}
impl fmt::Debug for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalMetadata")
            .field(
                "layout",
                &self.credit.as_ref().map(PacketMetadataCredit::layout),
            )
            .finish_non_exhaustive()
    }
}
impl Owner {
    pub(super) fn new(view: Option<PacketDriverMetadataView>) -> Self {
        Self {
            view,
            ..Self::default()
        }
    }
    pub(super) fn view(&self) -> Option<&PacketDriverMetadataView> {
        self.view.as_ref()
    }
    fn admit(&mut self, layout: PacketMetadataLayout) -> Result<(), PacketMemoryError> {
        if layout.allocations() == 0 && layout.bytes() == 0 {
            return Ok(());
        }
        let Some(view) = &self.view else {
            return Ok(());
        };
        match &mut self.credit {
            Some(credit) => view.grow_credit(credit, layout)?,
            None => self.credit = Some(view.reserve(layout)?),
        }
        Ok(())
    }
    pub(super) fn string(&mut self, value: &str) -> Result<String, PacketMemoryError> {
        self.admit(string_layout(value.len()))?;
        Ok(value.to_owned())
    }
    pub(super) fn path(
        &mut self,
        value: &std::path::Path,
    ) -> Result<std::path::PathBuf, PacketMemoryError> {
        self.admit(string_layout(value.as_os_str().len()))?;
        Ok(value.to_owned())
    }
    pub(super) fn push<T>(
        &mut self,
        rows: &mut Vec<T>,
        value: T,
    ) -> Result<(), (PacketMemoryError, T)> {
        if rows.len() == rows.capacity() {
            if let Err(error) = self.grow(rows) {
                return Err((error, value));
            }
        }
        rows.push(value);
        Ok(())
    }
    fn grow<T>(&mut self, rows: &mut Vec<T>) -> Result<(), PacketMemoryError> {
        let capacity = rows
            .capacity()
            .checked_mul(2)
            .and_then(|n| n.checked_add(usize::from(n == 0)))
            .ok_or(PacketMemoryError::Overflow)?;
        let layout = PacketMetadataLayout::array::<T>(capacity)?;
        self.admit(layout)?;
        // Exact Box-array storage, local to journal's IpAddr and String tables.
        let slots = Box::<[T]>::new_uninit_slice(capacity);
        let pointer = Box::into_raw(slots) as *mut T;
        let next = unsafe { Vec::from_raw_parts(pointer, 0, capacity) };
        let mut previous = mem::replace(rows, next);
        rows.append(&mut previous);
        let old = previous.capacity();
        drop(previous);
        if old != 0 {
            self.release(PacketMetadataLayout::array::<T>(old)?)?;
        }
        Ok(())
    }
    pub(super) fn release_string(&mut self, value: String) -> Result<(), PacketMemoryError> {
        let layout = string_layout(value.capacity());
        drop(value);
        self.release(layout)
    }
    pub(super) fn release(
        &mut self,
        layout: PacketMetadataLayout,
    ) -> Result<(), PacketMemoryError> {
        if let (Some(view), Some(credit)) = (&self.view, &mut self.credit) {
            view.shrink_credit(credit, layout)?;
        }
        Ok(())
    }
    pub(super) fn error<E: serde::de::Error>(&mut self, source: PacketMemoryError) -> E {
        self.failure.get_or_insert(source);
        E::custom("same journal metadata admission failed")
    }
}
fn string_layout(bytes: usize) -> PacketMetadataLayout {
    PacketMetadataLayout::new(usize::from(bytes != 0), bytes)
}

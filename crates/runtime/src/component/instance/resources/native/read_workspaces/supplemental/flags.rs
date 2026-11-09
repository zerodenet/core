//! Actual finite receiver-registration bits, freed before their own credit.
use super::*;
#[derive(Default)]
pub(in crate::component::instance::resources::native::read_workspaces) struct ReceivedFlags {
    values: Box<[bool]>,
    _metadata: Option<PacketMetadataCredit>,
}
impl ReceivedFlags {
    pub(super) fn prepare(
        root: &Arc<zero_tun::packet_memory::PacketReturnTotals>,
        count: usize,
    ) -> Result<Self, zero_tun::packet_memory::PacketMemoryError> {
        let credit = root.reserve_driver_metadata(PacketMetadataLayout::array::<bool>(count)?)?;
        let mut values = Box::<[bool]>::new_uninit_slice(count);
        for value in &mut values {
            value.write(false);
        }
        Ok(Self {
            values: unsafe { values.assume_init() },
            _metadata: Some(credit),
        })
    }
}
impl std::ops::Deref for ReceivedFlags {
    type Target = [bool];
    fn deref(&self) -> &[bool] {
        &self.values
    }
}
impl std::ops::DerefMut for ReceivedFlags {
    fn deref_mut(&mut self) -> &mut [bool] {
        &mut self.values
    }
}

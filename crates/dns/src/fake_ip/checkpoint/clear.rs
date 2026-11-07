//! Same membership-fixed checkpoint for a real clear; no domain clones.
use super::*;
use crate::fake_ip::management::ClearSelection;
impl<'a> Checkpoint<'a> {
    pub(in crate::fake_ip) fn clearing(
        state: &'a AllocatorState,
        selected: &ClearSelection,
        reusable_after: u64,
    ) -> io::Result<Self> {
        let mut checkpoint = Self::new(state, None, None)?;
        let addresses = checkpoint
            .rows
            .iter()
            .filter(|(domain, mapping)| selected.matches(domain, mapping))
            .map(|(_, mapping)| mapping.addresses().count())
            .sum();
        checkpoint
            .extra_retired
            .try_reserve_exact(addresses)
            .map_err(io::Error::other)?;
        for (domain, mapping) in &checkpoint.rows {
            if selected.matches(domain, mapping) {
                checkpoint
                    .extra_retired
                    .extend(mapping.addresses().map(|ip| (ip, reusable_after)));
            }
        }
        checkpoint
            .rows
            .retain(|(domain, mapping)| !selected.matches(domain, mapping));
        Ok(checkpoint)
    }
}

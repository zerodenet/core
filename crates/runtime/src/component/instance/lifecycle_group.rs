//! Exact catalog-bound members, never protocol/native completion evidence.
use super::*;
impl ComponentSlot {
    pub(in crate::component) fn bind_lifecycle_group(&self, members: Arc<[ComponentInstanceId]>) {
        assert!(members.contains(&self.instance_id()));
        if let Some(previous) = self.lifecycle_group.get() {
            assert_eq!(
                previous.as_ref(),
                members.as_ref(),
                "reused owner changed its lifecycle group"
            );
        } else if let Err(actual) = self.lifecycle_group.set(members) {
            assert_eq!(
                self.lifecycle_group.get().unwrap().as_ref(),
                actual.as_ref()
            );
        }
    }
    pub(in crate::component) fn lifecycle_group(&self) -> Option<&[ComponentInstanceId]> {
        self.lifecycle_group.get().map(AsRef::as_ref)
    }
    pub(in crate::component) fn same_lifecycle_group(&self, other: &Self) -> bool {
        self.lifecycle_group().is_some_and(|members| {
            members.contains(&other.instance_id()) && other.lifecycle_group() == Some(members)
        })
    }
}

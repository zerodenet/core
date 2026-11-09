//! The existing schema, including exact incremental cleanup persistence.
use super::*;
use serde::ser::{SerializeSeq, SerializeStruct};
pub(super) fn visible<T: PartialEq>(rows: &[T], removed: usize) -> bool {
    let (pending, successful) = rows.split_at(rows.len() - removed);
    pending.iter().any(|row| !successful.contains(row))
}
struct Visible<'a, T> {
    rows: &'a [T],
    removed: usize,
}
impl<T: Serialize + PartialEq> Serialize for Visible<'_, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let split = self.rows.len() - self.removed;
        let (pending, successful) = self.rows.split_at(split);
        let mut seq = serializer.serialize_seq(None)?;
        for row in pending {
            if !successful.contains(row) {
                seq.serialize_element(row)?;
            }
        }
        seq.end()
    }
}
impl Serialize for RouteJournal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut journal = serializer.serialize_struct("RouteJournal", 8)?;
        journal.serialize_field("tun_name", &self.tun_name)?;
        journal.serialize_field("ipv6", &self.ipv6)?;
        journal.serialize_field("tun_index", &self.tun_index)?;
        journal.serialize_field("egress", &self.egress)?;
        journal.serialize_field("gateway", &self.gateway)?;
        journal.serialize_field(
            "excluded",
            &Visible {
                rows: &self.excluded,
                removed: self._removed_exclusions,
            },
        )?;
        journal.serialize_field(
            "installed",
            &Visible {
                rows: &self.installed,
                removed: self._removed_routes,
            },
        )?;
        journal.serialize_field("scoped_bypass", &self.scoped_bypass)?;
        journal.end()
    }
}

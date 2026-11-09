use super::*;
#[derive(Default)]
pub(super) struct Rows {
    pub(super) next: u64,
    pub(super) values: PacketMetadataVec<Cell>,
}
pub(super) struct Cell {
    pub(super) serial: u64,
    pub(super) reserved: bool,
    pub(super) witness: Option<TaskJoinWitness>,
    pub(super) failure: Option<TaskJoined>,
    pub(super) success: Option<TaskJoined>,
    pub(super) liability: Option<PacketMetadataArc<liability::HeaderLiability>>,
}
impl Cell {
    pub(super) fn free(liability: Option<PacketMetadataArc<liability::HeaderLiability>>) -> Self {
        Self {
            serial: 0,
            reserved: false,
            witness: None,
            failure: None,
            success: None,
            liability,
        }
    }
}
#[derive(Clone, Copy)]
pub(super) struct Key {
    pub(super) index: usize,
    pub(super) serial: u64,
}
/// Only allocation custody for this actual caller. No task/IO authority.
pub struct AcceptedTaskFailureReservation {
    pub(super) key: Key,
    liability: Option<PacketMetadataArc<liability::HeaderLiability>>,
    pub(super) receiver: AcceptedTaskFailures,
    uncertain: bool,
    transferred: bool,
}
#[derive(Clone)]
pub struct AcceptedTaskFailureRegistration {
    pub(super) key: Key,
    pub(super) witness: Option<TaskJoinWitness>,
    pub(super) liability: Option<PacketMetadataArc<liability::HeaderLiability>>,
    pub(super) receiver: AcceptedTaskFailures,
}
impl AcceptedTaskFailures {
    pub fn reserve(&self) -> Result<AcceptedTaskFailureReservation, ChannelError> {
        self.reserve_prepaid(None)
    }
    pub(super) fn reserve_prepaid(
        &self,
        prepaid: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    ) -> Result<AcceptedTaskFailureReservation, ChannelError> {
        self.reap_successes();
        let _notifications = self.0.memory.notifications();
        let mut rows = self.0.rows.lock().unwrap();
        let serial = rows
            .next
            .checked_add(1)
            .ok_or_else(|| capacity(PacketMemoryError::Overflow))?;
        let plan = super::plan::required(&mut rows)?;
        let available = plan.available;
        let table_layout = plan.table;
        let header_layout = plan.header;
        let needs_liability = header_layout.allocations() != 0;
        let mut credit = match prepaid {
            Some(credit) if credit.layout() == plan.total => credit,
            Some(_) => return Err(capacity(PacketMemoryError::Busy)),
            None => self.0.memory.reserve(plan.total).map_err(capacity)?,
        };
        let returned = if let Some(layout) = table_layout {
            rows.values
                .grow_prepaid(credit.split(layout).map_err(capacity)?, usize::MAX)
                .map_err(|(source, _)| capacity(source))?
        } else {
            None
        };
        let headers = if needs_liability {
            Some(liability::HeaderLiability::from_prepaid(
                credit.split(header_layout).map_err(capacity)?,
            )?)
        } else {
            None
        };
        let index = if let Some(index) = available {
            index
        } else {
            rows.values
                .push_reserved(Cell::free(None))
                .map_err(|_| capacity(PacketMemoryError::Capacity))?;
            rows.values.len() - 1
        };
        let old = if let Some(headers) = headers {
            rows.values[index].liability.replace(headers)
        } else {
            None
        };
        rows.next = serial;
        let row = &mut rows.values[index];
        row.serial = serial;
        row.reserved = true;
        let actual = AcceptedTaskFailureReservation {
            receiver: self.clone(),
            key: Key { index, serial },
            liability: row.liability.clone(),
            uncertain: false,
            transferred: false,
        };
        drop(rows);
        drop((returned, old, credit));
        Ok(actual)
    }
    fn release_unused(&self, key: Key) {
        let old = {
            let mut rows = self.0.rows.lock().unwrap();
            let Some(row) = rows.values.get_mut(key.index) else {
                return;
            };
            if row.serial != key.serial || row.failure.is_some() {
                return;
            }
            row.reserved = false;
            row.witness.take()
        };
        drop(old);
        self.0.changed.notify_waiters();
    }
}
impl AcceptedTaskFailureReservation {
    /// A registrar call may unwind after accepting a real task. Such a gap
    /// remains charged; dropping this handle is never NoTask evidence.
    pub fn begin_registration(&mut self) {
        self.uncertain = true;
    }
    /// Only a real registrar rejection before acceptance reaches this path.
    pub fn known_not_registered(mut self) {
        self.uncertain = false;
    }
    pub fn registered(
        mut self,
        witness: &TaskJoinWitness,
    ) -> Result<AcceptedTaskFailureRegistration, ChannelError> {
        if !witness.scope_identity().same_scope(&self.receiver.0.scope)
            || witness.task_id().is_none()
        {
            return Err(foreign());
        }
        {
            let mut rows = self.receiver.0.rows.lock().unwrap();
            let row = rows.values.get_mut(self.key.index).ok_or_else(foreign)?;
            if row.serial != self.key.serial
                || !row.reserved
                || row.witness.is_some()
                || row.failure.is_some()
            {
                return Err(foreign());
            }
            row.witness = Some(witness.clone());
        }
        self.transferred = true;
        Ok(AcceptedTaskFailureRegistration {
            key: self.key,
            witness: Some(witness.clone()),
            liability: self.liability.clone(),
            receiver: self.receiver.clone(),
        })
    }
}
impl Drop for AcceptedTaskFailureReservation {
    fn drop(&mut self) {
        if !self.transferred && !self.uncertain {
            self.receiver.release_unused(self.key);
        }
    }
}

impl Drop for AcceptedTaskFailureRegistration {
    fn drop(&mut self) {
        drop(self.witness.take());
        drop(self.liability.take());
        self.receiver.0.changed.notify_waiters();
    }
}

use super::*;
impl AcceptedTaskFailures {
    pub(super) fn reap_successes(&self) {
        let count = self.0.rows.lock().unwrap().values.len();
        for index in 0..count {
            let taken = {
                let mut rows = self.0.rows.lock().unwrap();
                let Some(row) = rows.values.get_mut(index) else {
                    continue;
                };
                if row.reserved || row.failure.is_some() || row.success.is_none() {
                    continue;
                }
                let Some(witness) = row.witness.take() else {
                    continue;
                };
                let joined = row.success.take().unwrap();
                row.reserved = true; // no concurrent reuse during off-lock drop
                (row.serial, witness, joined)
            };
            let (serial, witness, joined) = taken;
            let result = TaskJoinWitness::try_retire_success_headers(witness, joined);
            let retired = result.is_ok();
            {
                let mut rows = self.0.rows.lock().unwrap();
                let row = rows
                    .values
                    .get_mut(index)
                    .expect("same original success row");
                assert_eq!(row.serial, serial);
                if let Err((witness, joined)) = result {
                    row.witness = Some(witness);
                    row.success = Some(joined);
                }
                row.reserved = false;
            }
            if retired {
                self.0.changed.notify_waiters();
            }
        }
    }
}

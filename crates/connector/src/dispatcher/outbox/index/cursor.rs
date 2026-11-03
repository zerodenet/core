use super::*;

impl PendingIndex {
    pub(in crate::dispatcher::outbox) fn visit(
        &self,
        mut visitor: impl FnMut(u64, (String, String)) -> ConnectorResult<bool>,
    ) -> ConnectorResult<()> {
        let original = self.db.lock().unwrap();
        let db = original
            .connection
            .as_ref()
            .expect("live original database");
        // Covering index iteration has no ORDER BY sort or full-key Vec.
        let mut statement = db
            .prepare("SELECT offset,sink,event FROM pending ORDER BY offset")
            .map_err(|e| self.error(e))?;
        let mut rows = statement.query([]).map_err(|e| self.error(e))?;
        while let Some(row) = rows.next().map_err(|e| self.error(e))? {
            let offset: i64 = row.get(0).map_err(|e| self.error(e))?;
            let offset = u64::try_from(offset).map_err(|e| self.error(e))?;
            let key = (
                row.get(1).map_err(|e| self.error(e))?,
                row.get(2).map_err(|e| self.error(e))?,
            );
            if !visitor(offset, key)? {
                break;
            }
        }
        Ok(())
    }
}

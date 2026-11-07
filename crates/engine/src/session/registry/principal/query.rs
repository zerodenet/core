//! Actual principal index membership and immutable values, under registry guard.
use super::*;
use serde::{ser::SerializeSeq, Serialize, Serializer};
#[derive(Serialize)]
struct PrincipalRow<'a> {
    principal_key: &'a str,
    active_flows: u64,
    last_transition_revision: u64,
    observed_at_unix_ms: u64,
}
pub(crate) struct PrincipalQueryRows<'a> {
    pub(crate) revision: u64,
    rows: Vec<PrincipalRow<'a>>,
}
impl PrincipalFlowIndex {
    pub(in crate::session::registry) fn freeze_query_paid(
        &self,
        workspace: &mut crate::runtime_query::workspace::RowCredit<'_>,
    ) -> std::io::Result<PrincipalQueryRows<'_>> {
        let mut rows = crate::runtime_query::workspace::vector_paid::<PrincipalRow<'_>>(
            self.active.len(),
            workspace,
        )?;
        for (key, state) in &self.active {
            rows.push(PrincipalRow {
                principal_key: key,
                active_flows: state.active_flows,
                last_transition_revision: state.last_transition_revision,
                observed_at_unix_ms: state.observed_at_unix_ms,
            });
        }
        // In-place sorting cannot allocate an uncharged temporary merge buffer.
        rows.sort_unstable_by(|a, b| a.principal_key.cmp(b.principal_key));
        Ok(PrincipalQueryRows {
            revision: self.revision,
            rows,
        })
    }
}
impl Serialize for PrincipalQueryRows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.rows.len()))?;
        for row in &self.rows {
            seq.serialize_element(row)?;
        }
        seq.end()
    }
}

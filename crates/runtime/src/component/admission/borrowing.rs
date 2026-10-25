//! Actual generation edges, not I/O or close receipts. One edge per owner pair.
use super::*;
use std::collections::BTreeSet;
pub(in crate::component) type LoanEdges = BTreeSet<(ComponentInstanceId, ComponentInstanceId)>;
impl HostAdmission {
    pub(in crate::component) fn loan_snapshot(&self) -> (u64, LoanEdges) {
        let view = self.0.read().expect("host admission poisoned");
        (view.loan_revision, view.loans.clone())
    }
    pub(in crate::component) fn finish_shutdown(&self) {
        let mut view = self.0.write().expect("host admission poisoned");
        if !view.loans.is_empty() {
            view.loan_revision = view.loan_revision.saturating_add(1);
        }
        view.loans.clear();
    }
    pub(in crate::component) fn freeze_loans(self: &Arc<Self>) -> LoanFreeze {
        self.0.write().expect("host admission poisoned").loan_frozen = true;
        LoanFreeze(Some(self.clone()))
    }
    pub(in crate::component) fn borrow_from(
        &self,
        consumer: (ComponentInstanceId, Arc<()>),
        donors: Vec<(ComponentInstanceId, Arc<()>)>,
        declared: &LoanEdges,
    ) -> Result<(), ChannelError> {
        let mut view = self.0.write().expect("host admission poisoned");
        let installed = |id, owner: &Arc<()>| {
            // Background acquisitions use the exact staged union. Ordinary
            // business admission retains its existing publication boundary.
            let actual = if view.loan_staged {
                view.preparing.get(&id)
            } else {
                view.current.get(&id)
            };
            actual.is_some_and(|actual| Arc::ptr_eq(actual, owner))
        };
        if view.loan_frozen
            || !installed(consumer.0, &consumer.1)
            || donors.iter().any(|(id, owner)| !installed(*id, owner))
        {
            return Err(invalid(
                Reason::StalePlan,
                "background borrowing is fenced by the actual host transition",
            ));
        }
        let mut edges = view.loans.clone();
        for (donor, _) in donors {
            if donor == consumer.0 {
                continue;
            }
            edges.insert((consumer.0, donor));
        }
        let all: LoanEdges = declared.union(&edges).copied().collect();
        if edges.iter().any(|(from, to)| reaches(&all, *to, *from)) {
            return Err(invalid(
                Reason::InvalidConfig,
                "actual background dependency graph contains a cycle",
            ));
        }
        if edges != view.loans {
            view.loan_revision = view.loan_revision.checked_add(1).ok_or_else(|| {
                invalid(Reason::Capacity, "background dependency revision exhausted")
            })?;
            view.loans = edges;
        }
        Ok(())
    }
}
fn reaches(edges: &LoanEdges, from: ComponentInstanceId, to: ComponentInstanceId) -> bool {
    let mut pending = vec![from];
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if id == to {
            return true;
        }
        if seen.insert(id) {
            pending.extend(edges.iter().filter(|(a, _)| *a == id).map(|(_, b)| *b));
        }
    }
    false
}
pub(in crate::component) struct LoanFreeze(Option<Arc<HostAdmission>>);
impl LoanFreeze {
    /// The retained replacement now owns the fence, including cancelled waits.
    pub(in crate::component) fn retain(mut self) {
        self.0 = None;
    }
}
impl Drop for LoanFreeze {
    fn drop(&mut self) {
        if let Some(host) = self.0.take() {
            host.0.write().expect("host admission poisoned").loan_frozen = false;
        }
    }
}

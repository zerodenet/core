//! All actual per-query heap workspace is prepaid in the same response book.
use std::io;
use zero_runtime::configuration::ControlBudgetPermit;
pub(crate) fn bytes<T>(count: usize) -> io::Result<usize> {
    count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| io::Error::other("control query workspace size overflow"))
}
pub(crate) fn vector<T>(count: usize, workspace: &mut ControlBudgetPermit) -> io::Result<Vec<T>> {
    vector_paid(count, &mut RowCredit::Control(workspace))
}
/// Only the two concrete actual byte owners used by these same frozen rows.
pub(crate) enum RowCredit<'a> {
    Control(&'a mut ControlBudgetPermit),
    Subscriber(&'a mut crate::observability::SubscriberStorage),
}
impl RowCredit<'_> {
    pub(crate) fn grow(&mut self, bytes: usize) -> io::Result<()> {
        match self {
            Self::Control(credit) => credit.grow(bytes),
            Self::Subscriber(credit) => credit.grow(bytes),
        }
    }
}
pub(crate) fn vector_paid<T>(count: usize, workspace: &mut RowCredit<'_>) -> io::Result<Vec<T>> {
    workspace.grow(bytes::<T>(count)?)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(count).map_err(io::Error::other)?;
    Ok(rows)
}

//! Bounded, fenced child handles retained outside the actual listener future.
pub(crate) mod capacity;
pub(crate) mod input;
mod output;
pub(crate) mod scoped;
pub(crate) use output::TaskOutput;
pub(crate) type Source = std::sync::Arc<dyn std::error::Error + Send + Sync>;
#[cfg(test)]
#[path = "tasks/tests/legacy.rs"]
mod legacy;
#[cfg(test)]
pub(crate) use legacy::RetainedTasks;

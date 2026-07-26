//! Common rule model, compiler, ZRS codec, verifier, mmap view, and lookup API
//! for Zero matcher sets.
//!
//! This crate owns rule semantics and local immutable rule artifacts. It does
//! not own route actions, downloads, update scheduling, or adapters for
//! third-party rule formats.

mod compile;
mod error;
mod model;
mod normalize;
pub mod protocol;
mod query;
pub mod zrs;

pub use compile::RuleSetCompiler;
pub use error::CompileError;
pub use model::{
    CompileReport, CompiledRuleSet, Ipv4Range, Ipv6Range, Rule, RuleSet, DISPLAY_NAME_CAPACITY,
    DISPLAY_NAME_MAX_BYTES, MAX_RULES, MAX_RULE_VALUE_BYTES,
};
pub use query::{PreparedRuleQuery, QueryError, RuleMatch, RuleMatcher};

impl RuleMatcher for CompiledRuleSet {
    fn lookup(&self, query: &PreparedRuleQuery) -> Option<RuleMatch> {
        CompiledRuleSet::lookup(self, query)
    }

    fn requires_destination_ip(&self) -> bool {
        !self.ipv4_ranges().is_empty() || !self.ipv6_ranges().is_empty()
    }
}

use std::fmt;
use std::sync::Arc;

use zero_rule::{PreparedRuleQuery, RuleMatcher};

/// A named, immutable matcher set referenced by one or more route rules.
///
/// The matcher remains owned by the compiled router snapshot. This lets an
/// engine reload replace the complete routing snapshot while in-flight users
/// of the previous snapshot keep its mmap or in-memory index alive.
#[derive(Clone)]
pub struct RuleSetMatcher {
    tag: Arc<str>,
    matcher: Arc<dyn RuleMatcher>,
}

impl RuleSetMatcher {
    pub fn new(tag: impl Into<Arc<str>>, matcher: Arc<dyn RuleMatcher>) -> Self {
        Self {
            tag: tag.into(),
            matcher,
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn matches(&self, query: &PreparedRuleQuery) -> bool {
        self.matcher.matches(query)
    }

    pub fn requires_destination_ip(&self) -> bool {
        self.matcher.requires_destination_ip()
    }
}

impl fmt::Debug for RuleSetMatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuleSetMatcher")
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}

impl PartialEq for RuleSetMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && Arc::ptr_eq(&self.matcher, &other.matcher)
    }
}

impl Eq for RuleSetMatcher {}

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
    source_digest: Option<[u8; 32]>,
}

impl RuleSetMatcher {
    pub fn new(tag: impl Into<Arc<str>>, matcher: Arc<dyn RuleMatcher>) -> Self {
        Self {
            tag: tag.into(),
            matcher,
            source_digest: None,
        }
    }

    /// Content identity of the exact validated source bytes and format used to
    /// prepare this immutable matcher. This is domain data, never native-close
    /// evidence. Opaque matchers without source evidence retain Arc equality.
    pub fn with_source_digest(mut self, digest: [u8; 32]) -> Self {
        self.source_digest = Some(digest);
        self
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
        self.tag == other.tag
            && match (self.source_digest, other.source_digest) {
                (Some(left), Some(right)) => left == right,
                (None, None) => Arc::ptr_eq(&self.matcher, &other.matcher),
                _ => false,
            }
    }
}

impl Eq for RuleSetMatcher {}

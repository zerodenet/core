use std::net::IpAddr;

use thiserror::Error;

use crate::normalize;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QueryError {
    #[error("rule query must contain a domain or destination IP")]
    Empty,
    #[error("rule query contains invalid domain `{value}`: {reason}")]
    InvalidDomain { value: String, reason: String },
}

/// Normalized request facts shared by every matcher set in one route lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRuleQuery {
    domain: Option<String>,
    destination_ip: Option<IpAddr>,
}

/// Common lookup contract for compiled and file-backed matcher sets.
pub trait RuleMatcher: Send + Sync {
    fn lookup(&self, query: &PreparedRuleQuery) -> Option<RuleMatch>;

    fn matches(&self, query: &PreparedRuleQuery) -> bool {
        self.lookup(query).is_some()
    }

    /// Whether this matcher can accept a destination IP.
    ///
    /// Unknown implementations conservatively return `true` so callers do not
    /// skip DNS-backed routing facts that the matcher may need.
    fn requires_destination_ip(&self) -> bool {
        true
    }
}

/// The semantic rule category that accepted a query.
///
/// When more than one category matches, lookup uses the stable order documented
/// by [`RuleMatcher::lookup`]: domain exact, domain suffix, domain keyword,
/// IPv4 range, then IPv6 range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleMatch {
    DomainExact,
    DomainSuffix,
    DomainKeyword,
    Ipv4Range,
    Ipv6Range,
}

impl PreparedRuleQuery {
    pub fn new(domain: Option<&str>, destination_ip: Option<IpAddr>) -> Result<Self, QueryError> {
        if domain.is_none() && destination_ip.is_none() {
            return Err(QueryError::Empty);
        }
        let domain = domain
            .map(|value| {
                normalize::domain(value).map_err(|reason| QueryError::InvalidDomain {
                    value: value.to_owned(),
                    reason,
                })
            })
            .transpose()?;
        Ok(Self {
            domain,
            destination_ip,
        })
    }

    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    pub fn destination_ip(&self) -> Option<IpAddr> {
        self.destination_ip
    }
}

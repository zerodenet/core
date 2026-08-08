//! Engine target plan and outbound resolution.

mod resolve;

pub(crate) use resolve::{
    resolve_target_chains, resolve_target_id, resolve_target_id_with_urltest_selector,
};
pub use resolve::{OutboundIdentity, ResolvedLeafOutbound, ResolvedOutbound};

use std::collections::HashMap;
use std::time::Duration;

use zero_config::{OutboundGroupKind, OutboundRuntimeKind, RuntimeConfig};

use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TargetId(usize);

impl TargetId {
    pub fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnginePlan {
    targets: Box<[TargetNode]>,
    targets_by_tag: HashMap<String, TargetId>,
    selector_groups: Box<[TargetId]>,
    urltest_groups: Box<[TargetId]>,
    loadbalance_groups: Box<[TargetId]>,
}

impl EnginePlan {
    pub fn build(config: &RuntimeConfig) -> Result<Self, EngineError> {
        let targets_by_tag = build_target_index(config)?;
        let mut targets = Vec::with_capacity(targets_by_tag.len());
        let mut selector_groups = Vec::new();
        let mut urltest_groups = Vec::new();
        let mut loadbalance_groups = Vec::new();

        for (outbound_index, outbound) in config.outbounds.iter().enumerate() {
            let kind = TargetKind::Outbound(OutboundTarget {
                outbound_index,
                runtime_kind: outbound.protocol.runtime_kind(),
            });

            targets.push(TargetNode {
                tag: outbound.tag.clone(),
                kind,
            });
        }

        for group in &config.outbound_groups {
            let group_id = target_id_by_tag(&targets_by_tag, group.tag())?;
            let kind = match &group.group {
                OutboundGroupKind::Selector { outbounds, .. } => {
                    selector_groups.push(group_id);
                    TargetKind::Selector(SelectorGroupPlan {
                        members: resolve_members(outbounds, &targets_by_tag)?,
                        initial_member: target_id_by_tag(
                            &targets_by_tag,
                            group
                                .active_outbound()
                                .ok_or_else(|| EngineError::InvalidPlan {
                                    message: format!(
                                        "selector group `{}` does not have an initial member",
                                        group.tag()
                                    ),
                                })?,
                        )?,
                    })
                }
                OutboundGroupKind::Fallback { outbounds } => {
                    TargetKind::Fallback(FallbackGroupPlan {
                        members: resolve_members(outbounds, &targets_by_tag)?,
                    })
                }
                OutboundGroupKind::UrlTest {
                    outbounds,
                    url,
                    interval_seconds,
                    tolerance_ms,
                } => {
                    urltest_groups.push(group_id);
                    TargetKind::UrlTest(UrlTestGroupPlan {
                        members: resolve_members(outbounds, &targets_by_tag)?,
                        initial_member: target_id_by_tag(
                            &targets_by_tag,
                            group
                                .active_outbound()
                                .ok_or_else(|| EngineError::InvalidPlan {
                                    message: format!(
                                        "urltest group `{}` does not have an initial member",
                                        group.tag()
                                    ),
                                })?,
                        )?,
                        url: url.clone().unwrap_or_else(|| {
                            config.runtime.effective_latency_test_url().to_owned()
                        }),
                        interval: Duration::from_secs(*interval_seconds),
                        tolerance_ms: *tolerance_ms,
                    })
                }
                OutboundGroupKind::Relay { proxies } => {
                    if proxies.len() < 2 {
                        return Err(EngineError::InvalidPlan {
                            message: format!(
                                "relay group `{}` requires at least 2 proxies",
                                group.tag()
                            ),
                        });
                    }
                    TargetKind::Relay(RelayGroupPlan {
                        chain: resolve_members(proxies, &targets_by_tag)?,
                    })
                }
                OutboundGroupKind::LoadBalance {
                    outbounds,
                    default: _,
                    strategy,
                } => {
                    loadbalance_groups.push(group_id);
                    TargetKind::LoadBalance(LoadBalanceGroupPlan {
                        members: resolve_members(outbounds, &targets_by_tag)?,
                        strategy: strategy.clone(),
                        initial_member: target_id_by_tag(
                            &targets_by_tag,
                            group
                                .active_outbound()
                                .ok_or_else(|| EngineError::InvalidPlan {
                                    message: format!(
                                        "loadbalance group `{}` does not have an initial member",
                                        group.tag()
                                    ),
                                })?,
                        )?,
                    })
                }
            };

            targets.push(TargetNode {
                tag: group.tag.clone(),
                kind,
            });
        }

        Ok(Self {
            targets: targets.into_boxed_slice(),
            targets_by_tag,
            selector_groups: selector_groups.into_boxed_slice(),
            urltest_groups: urltest_groups.into_boxed_slice(),
            loadbalance_groups: loadbalance_groups.into_boxed_slice(),
        })
    }

    pub fn target_id(&self, tag: &str) -> Option<TargetId> {
        self.targets_by_tag.get(tag).copied()
    }

    pub fn target(&self, id: TargetId) -> Option<&TargetNode> {
        self.targets.get(id.index())
    }

    pub fn selector_groups(&self) -> &[TargetId] {
        &self.selector_groups
    }

    pub fn urltest_groups(&self) -> &[TargetId] {
        &self.urltest_groups
    }

    pub fn loadbalance_groups(&self) -> &[TargetId] {
        &self.loadbalance_groups
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetNode {
    tag: String,
    kind: TargetKind,
}

impl TargetNode {
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Return the raw kind.  Prefer the `as_*` / `is_*` query methods
    /// below; use this only when exhaustive matching is required (e.g. in
    /// `resolve.rs`).
    pub fn kind(&self) -> &TargetKind {
        &self.kind
    }

    pub fn is_outbound(&self) -> bool {
        matches!(self.kind, TargetKind::Outbound(_))
    }

    pub fn as_outbound(&self) -> Option<&OutboundTarget> {
        match &self.kind {
            TargetKind::Outbound(o) => Some(o),
            _ => None,
        }
    }

    pub fn as_selector(&self) -> Option<&SelectorGroupPlan> {
        match &self.kind {
            TargetKind::Selector(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_fallback(&self) -> Option<&FallbackGroupPlan> {
        match &self.kind {
            TargetKind::Fallback(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_urltest(&self) -> Option<&UrlTestGroupPlan> {
        match &self.kind {
            TargetKind::UrlTest(u) => Some(u),
            _ => None,
        }
    }

    pub fn as_relay(&self) -> Option<&RelayGroupPlan> {
        match &self.kind {
            TargetKind::Relay(r) => Some(r),
            _ => None,
        }
    }

    pub fn as_loadbalance(&self) -> Option<&LoadBalanceGroupPlan> {
        match &self.kind {
            TargetKind::LoadBalance(lb) => Some(lb),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetKind {
    Outbound(OutboundTarget),
    Selector(SelectorGroupPlan),
    Fallback(FallbackGroupPlan),
    UrlTest(UrlTestGroupPlan),
    Relay(RelayGroupPlan),
    LoadBalance(LoadBalanceGroupPlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundTarget {
    outbound_index: usize,
    runtime_kind: OutboundRuntimeKind,
}

impl OutboundTarget {
    pub fn outbound_index(&self) -> usize {
        self.outbound_index
    }

    pub fn runtime_kind(&self) -> OutboundRuntimeKind {
        self.runtime_kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorGroupPlan {
    members: Box<[TargetId]>,
    initial_member: TargetId,
}

impl SelectorGroupPlan {
    pub fn members(&self) -> &[TargetId] {
        &self.members
    }

    pub fn initial_member(&self) -> TargetId {
        self.initial_member
    }

    pub fn contains_member(&self, target: TargetId) -> bool {
        self.members.contains(&target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackGroupPlan {
    members: Box<[TargetId]>,
}

impl FallbackGroupPlan {
    pub fn members(&self) -> &[TargetId] {
        &self.members
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlTestGroupPlan {
    members: Box<[TargetId]>,
    initial_member: TargetId,
    url: String,
    interval: Duration,
    tolerance_ms: u64,
}

impl UrlTestGroupPlan {
    pub fn members(&self) -> &[TargetId] {
        &self.members
    }

    pub fn initial_member(&self) -> TargetId {
        self.initial_member
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn tolerance_ms(&self) -> u64 {
        self.tolerance_ms
    }

    pub fn select(
        &self,
        current: Option<TargetId>,
        healthy_members: &[(TargetId, u64)],
    ) -> UrlTestSelection {
        let best = healthy_members
            .iter()
            .min_by_key(|(_, latency_ms)| *latency_ms)
            .copied();
        let current_latency_ms = current.and_then(|current_id| {
            healthy_members
                .iter()
                .find(|(member_id, _)| *member_id == current_id)
                .map(|(_, latency_ms)| *latency_ms)
        });

        let (selected, reason) = match (current, current_latency_ms, best) {
            (_, _, None) => (
                current.unwrap_or(self.initial_member),
                UrlTestSelectionReason::NoHealthyMember,
            ),
            (None, _, Some((best_id, _))) => (best_id, UrlTestSelectionReason::Initial),
            (Some(_), None, Some((best_id, _))) => {
                (best_id, UrlTestSelectionReason::CurrentUnhealthy)
            }
            (Some(current_id), Some(current_latency), Some((best_id, best_latency))) => {
                if current_latency > best_latency.saturating_add(self.tolerance_ms) {
                    (best_id, UrlTestSelectionReason::BetterBeyondTolerance)
                } else {
                    (current_id, UrlTestSelectionReason::WithinTolerance)
                }
            }
        };

        UrlTestSelection {
            previous: current,
            selected,
            best: best.map(|(member_id, _)| member_id),
            current_latency_ms,
            best_latency_ms: best.map(|(_, latency_ms)| latency_ms),
            tolerance_ms: self.tolerance_ms,
            switched: current.is_some_and(|current_id| current_id != selected),
            reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlTestSelectionReason {
    Initial,
    CurrentUnhealthy,
    BetterBeyondTolerance,
    WithinTolerance,
    NoHealthyMember,
}

impl UrlTestSelectionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::CurrentUnhealthy => "current_unhealthy",
            Self::BetterBeyondTolerance => "better_beyond_tolerance",
            Self::WithinTolerance => "within_tolerance",
            Self::NoHealthyMember => "no_healthy_member",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlTestSelection {
    pub previous: Option<TargetId>,
    pub selected: TargetId,
    pub best: Option<TargetId>,
    pub current_latency_ms: Option<u64>,
    pub best_latency_ms: Option<u64>,
    pub tolerance_ms: u64,
    pub switched: bool,
    pub reason: UrlTestSelectionReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayGroupPlan {
    /// Ordered chain of proxy target IDs.  Each hop's TCP stream is
    /// established through the previous hop's connection.
    pub chain: Box<[TargetId]>,
}

impl RelayGroupPlan {
    pub fn chain(&self) -> &[TargetId] {
        &self.chain
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadBalanceGroupPlan {
    members: Box<[TargetId]>,
    strategy: zero_config::LoadBalanceStrategy,
    initial_member: TargetId,
}

impl LoadBalanceGroupPlan {
    pub fn members(&self) -> &[TargetId] {
        &self.members
    }

    pub fn strategy(&self) -> &zero_config::LoadBalanceStrategy {
        &self.strategy
    }

    pub fn initial_member(&self) -> TargetId {
        self.initial_member
    }

    pub fn contains_member(&self, target: TargetId) -> bool {
        self.members.contains(&target)
    }
}

fn build_target_index(config: &RuntimeConfig) -> Result<HashMap<String, TargetId>, EngineError> {
    let mut targets_by_tag =
        HashMap::with_capacity(config.outbounds.len() + config.outbound_groups.len());

    for (index, outbound) in config.outbounds.iter().enumerate() {
        insert_target_tag(&mut targets_by_tag, &outbound.tag, TargetId(index))?;
    }

    let group_base = config.outbounds.len();
    for (offset, group) in config.outbound_groups.iter().enumerate() {
        insert_target_tag(
            &mut targets_by_tag,
            group.tag(),
            TargetId(group_base + offset),
        )?;
    }

    Ok(targets_by_tag)
}

fn insert_target_tag(
    targets_by_tag: &mut HashMap<String, TargetId>,
    tag: &str,
    id: TargetId,
) -> Result<(), EngineError> {
    match targets_by_tag.insert(tag.to_owned(), id) {
        Some(_) => Err(EngineError::InvalidPlan {
            message: format!("duplicate target tag `{tag}` found while building engine plan"),
        }),
        None => Ok(()),
    }
}

fn resolve_members(
    members: &[String],
    targets_by_tag: &HashMap<String, TargetId>,
) -> Result<Box<[TargetId]>, EngineError> {
    let mut resolved = Vec::with_capacity(members.len());
    for member in members {
        resolved.push(target_id_by_tag(targets_by_tag, member)?);
    }

    Ok(resolved.into_boxed_slice())
}

fn target_id_by_tag(
    targets_by_tag: &HashMap<String, TargetId>,
    tag: &str,
) -> Result<TargetId, EngineError> {
    targets_by_tag
        .get(tag)
        .copied()
        .ok_or_else(|| EngineError::InvalidPlan {
            message: format!("target tag `{tag}` is missing from engine plan"),
        })
}

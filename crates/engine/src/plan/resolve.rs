//! Resolution of target plans into executable outbound selections.

use rand::Rng;
use zero_config::OutboundRuntimeKind;

use crate::groups::OutboundGroupStateStore;

use super::{EnginePlan, OutboundTarget, TargetId, TargetKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OutboundIdentity(usize);

impl OutboundIdentity {
    pub const fn from_config_index(index: usize) -> Self {
        Self(index)
    }

    pub const fn config_index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedLeafOutbound<'a> {
    Direct { tag: Option<&'a str> },
    Block { tag: Option<&'a str> },
    Proxy { identity: OutboundIdentity },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedOutbound<'a> {
    Single(ResolvedLeafOutbound<'a>),
    Fallback {
        candidates: Vec<ResolvedLeafOutbound<'a>>,
    },
    /// Chain of proxies connected sequentially.  Each hop's connection
    /// is established through the previous hop's TCP stream.
    Relay {
        chain: Vec<ResolvedLeafOutbound<'a>>,
    },
}

pub(crate) fn resolve_target_id<'a>(
    plan: &'a EnginePlan,
    outbound_group_state: &OutboundGroupStateStore,
    target_id: TargetId,
) -> Option<ResolvedOutbound<'a>> {
    let mut stack = Vec::new();
    resolve_target_inner(plan, outbound_group_state, target_id, &mut stack, &mut None)
}

pub(crate) fn resolve_target_id_with_urltest_selector<'a>(
    plan: &'a EnginePlan,
    outbound_group_state: &OutboundGroupStateStore,
    target_id: TargetId,
    selector: &mut dyn FnMut(TargetId, TargetId) -> TargetId,
) -> Option<ResolvedOutbound<'a>> {
    let mut stack = Vec::new();
    resolve_target_inner(
        plan,
        outbound_group_state,
        target_id,
        &mut stack,
        &mut Some(selector),
    )
}

pub(crate) fn resolve_target_chains(
    plan: &EnginePlan,
    outbound_group_state: &OutboundGroupStateStore,
    target_id: TargetId,
) -> Vec<Vec<TargetId>> {
    let mut stack = Vec::new();
    resolve_target_chains_inner(plan, outbound_group_state, target_id, &mut stack)
}

fn resolve_target_inner<'a>(
    plan: &'a EnginePlan,
    outbound_group_state: &OutboundGroupStateStore,
    target_id: TargetId,
    stack: &mut Vec<TargetId>,
    urltest_selector: &mut Option<&mut dyn FnMut(TargetId, TargetId) -> TargetId>,
) -> Option<ResolvedOutbound<'a>> {
    if stack.contains(&target_id) {
        return None;
    }
    stack.push(target_id);

    let target = plan.target(target_id)?;
    let resolved = match target.kind() {
        TargetKind::Outbound(outbound) => Some(ResolvedOutbound::Single(resolve_leaf_outbound(
            target.tag(),
            outbound,
        ))),
        TargetKind::Selector(selector) => {
            let selected = outbound_group_state
                .selector_selected_target(target_id)
                .unwrap_or_else(|| selector.initial_member());
            resolve_target_inner(
                plan,
                outbound_group_state,
                selected,
                stack,
                urltest_selector,
            )
        }
        TargetKind::Relay(relay) => {
            let mut chain = Vec::with_capacity(relay.chain().len());
            for &member_id in relay.chain() {
                let resolved = resolve_target_inner(
                    plan,
                    outbound_group_state,
                    member_id,
                    stack,
                    urltest_selector,
                )?;
                match resolved {
                    ResolvedOutbound::Single(leaf) => chain.push(leaf),
                    _ => return None,
                }
            }
            Some(ResolvedOutbound::Relay { chain })
        }
        TargetKind::Fallback(fallback) => {
            let mut candidates = Vec::new();
            for &member_id in fallback.members() {
                let resolved = resolve_target_inner(
                    plan,
                    outbound_group_state,
                    member_id,
                    stack,
                    urltest_selector,
                )?;
                append_candidates(&mut candidates, resolved);
            }

            Some(ResolvedOutbound::Fallback { candidates })
        }
        TargetKind::UrlTest(urltest) => {
            let default_selected = outbound_group_state
                .selected_target(target_id)
                .unwrap_or_else(|| urltest.initial_member());
            let selected = urltest_selector
                .as_deref_mut()
                .map(|selector| selector(target_id, default_selected))
                .unwrap_or(default_selected);
            resolve_target_inner(
                plan,
                outbound_group_state,
                selected,
                stack,
                urltest_selector,
            )
        }
        TargetKind::LoadBalance(lb) => {
            let member_count = lb.members().len();
            let index = match lb.strategy() {
                zero_config::LoadBalanceStrategy::RoundRobin => {
                    outbound_group_state.loadbalance_next_pick(target_id, member_count)
                }
                zero_config::LoadBalanceStrategy::Random => {
                    rand::rng().random_range(0..member_count)
                }
            };

            // Picked member first, remaining members follow in original order.
            let mut ordered = Vec::with_capacity(member_count);
            ordered.push(lb.members()[index]);
            ordered.extend(
                lb.members()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != index)
                    .map(|(_, &member_id)| member_id),
            );

            let mut candidates = Vec::new();
            for &member_id in &ordered {
                let resolved = resolve_target_inner(
                    plan,
                    outbound_group_state,
                    member_id,
                    stack,
                    urltest_selector,
                )?;
                append_candidates(&mut candidates, resolved);
            }

            Some(ResolvedOutbound::Fallback { candidates })
        }
    };

    stack.pop();
    resolved
}

fn resolve_leaf_outbound<'a>(
    tag: &'a str,
    outbound: &'a OutboundTarget,
) -> ResolvedLeafOutbound<'a> {
    match outbound.runtime_kind() {
        OutboundRuntimeKind::Direct => ResolvedLeafOutbound::Direct { tag: Some(tag) },
        OutboundRuntimeKind::Block => ResolvedLeafOutbound::Block { tag: Some(tag) },
        OutboundRuntimeKind::Proxy => ResolvedLeafOutbound::Proxy {
            identity: OutboundIdentity::from_config_index(outbound.outbound_index()),
        },
    }
}

fn append_candidates<'a>(
    candidates: &mut Vec<ResolvedLeafOutbound<'a>>,
    resolved: ResolvedOutbound<'a>,
) {
    match resolved {
        ResolvedOutbound::Single(candidate) => candidates.push(candidate),
        ResolvedOutbound::Fallback { candidates: nested } => candidates.extend(nested),
        ResolvedOutbound::Relay { .. } => {} // relay inside fallback is not meaningful
    }
}

fn resolve_target_chains_inner(
    plan: &EnginePlan,
    outbound_group_state: &OutboundGroupStateStore,
    target_id: TargetId,
    stack: &mut Vec<TargetId>,
) -> Vec<Vec<TargetId>> {
    let Some(target) = plan.target(target_id) else {
        return Vec::new();
    };

    if stack.contains(&target_id) {
        return Vec::new();
    }

    stack.push(target_id);
    let chains = match target.kind() {
        TargetKind::Outbound(_) => vec![stack.clone()],
        TargetKind::Selector(selector) => {
            let selected = outbound_group_state
                .selector_selected_target(target_id)
                .unwrap_or_else(|| selector.initial_member());
            resolve_target_chains_inner(plan, outbound_group_state, selected, stack)
        }
        TargetKind::Fallback(fallback) => fallback
            .members()
            .iter()
            .flat_map(|member_id| {
                resolve_target_chains_inner(plan, outbound_group_state, *member_id, stack)
            })
            .collect(),
        TargetKind::Relay(relay) => {
            // Relay chains go through all proxies in order.
            let mut full_chain = stack.clone();
            for &member_id in relay.chain() {
                full_chain.push(member_id);
            }
            vec![full_chain]
        }
        TargetKind::UrlTest(urltest) => {
            let selected = outbound_group_state
                .selected_target(target_id)
                .unwrap_or_else(|| urltest.initial_member());
            resolve_target_chains_inner(plan, outbound_group_state, selected, stack)
        }
        TargetKind::LoadBalance(lb) => lb
            .members()
            .iter()
            .flat_map(|member_id| {
                resolve_target_chains_inner(plan, outbound_group_state, *member_id, stack)
            })
            .collect(),
    };
    stack.pop();
    chains
}

use std::sync::atomic::{AtomicU64, Ordering};

use zero_config::OutboundRuntimeKind;

use super::{EnginePlan, TargetKind, TargetNode};

impl EnginePlan {
    /// Assign stable socket-policy identities before publishing this plan.
    /// The engine-owned allocator outlives staged plans and rollback, so a
    /// reverted policy can never reuse the identity of a discarded candidate.
    pub(crate) fn reconcile_direct_dial_generations(
        &mut self,
        previous: Option<&Self>,
        next: &AtomicU64,
    ) {
        for target in self.targets.iter_mut() {
            let TargetKind::Outbound(outbound) = &mut target.kind else {
                continue;
            };
            if outbound.runtime_kind != OutboundRuntimeKind::Direct {
                continue;
            }
            let retained = previous
                .and_then(|plan| plan.target_id(&target.tag).and_then(|id| plan.target(id)))
                .and_then(TargetNode::as_outbound)
                .filter(|old| {
                    old.runtime_kind == OutboundRuntimeKind::Direct
                        && old.dial_policy == outbound.dial_policy
                })
                .map(|old| old.dial_generation);
            outbound.dial_generation = retained.unwrap_or_else(|| allocate_generation(next));
        }
    }
}

fn allocate_generation(next: &AtomicU64) -> u64 {
    let mut generation = next.load(Ordering::Relaxed);
    loop {
        let incremented = generation
            .checked_add(1)
            .expect("direct dial generation exhausted");
        match next.compare_exchange_weak(
            generation,
            incremented,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(allocated) => return allocated,
            Err(current) => generation = current,
        }
    }
}

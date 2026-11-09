//! Existing test failures only; no admission/IO/cleanup authority or new waits.
use super::*;
pub(crate) fn source(label: &str, root: &(dyn Error + 'static)) {
    let mut next = Some(root);
    let mut seen: [Option<&(dyn Error + 'static)>; 32] = [None; 32];
    for depth in 0..32 {
        let Some(actual) = next else { break };
        if seen[..depth]
            .iter()
            .flatten()
            .any(|prior| std::ptr::eq(*prior, actual))
        {
            break;
        }
        seen[depth] = Some(actual);
        // ChannelError sources are redacted by their owning producer per its
        // contract. Display never emits an arbitrary Debug request/body dump.
        eprintln!("actual QUIC capacity source label={label} depth={depth} error={actual}");
        if let Some(channel) = actual.downcast_ref::<ChannelError>() {
            eprintln!("actual QUIC capacity fields component={:?} stage={:?} reason={:?} impact={:?} delivery={:?}",channel.component,channel.stage,channel.reason,channel.impact,channel.delivery);
            next = channel
                .retained_diagnostic()
                .map(|retained| AsRef::<ChannelError>::as_ref(retained) as &(dyn Error + 'static))
                .or_else(|| actual.source());
        } else if let Some(shared) = actual.downcast_ref::<Arc<ChannelError>>() {
            next = Some(shared.as_ref());
        } else if let Some(io) = actual.downcast_ref::<io::Error>() {
            eprintln!(
                "actual QUIC capacity IO kind={:?} os={:?}",
                io.kind(),
                io.raw_os_error()
            );
            next = io
                .get_ref()
                .map(|inner| inner as &(dyn Error + 'static))
                .or_else(|| actual.source());
        } else {
            next = actual.source();
        }
    }
}
pub(crate) fn books(proxy: &crate::Proxy) {
    let host = proxy.protocols.runtime_host();
    let originals = host.original_budgets();
    let carrier = originals.carrier.snapshot();
    let buffers = originals.carrier.buffer_budget();
    let metadata = originals.packet.driver_metadata_view().snapshot();
    eprintln!("actual QUIC capacity Root originals={}/{} queue_bytes={}/{} queue_packets_limit={} metadata_allocations={}/{} metadata_bytes={}/{}",carrier.charged,carrier.limit,buffers.used_bytes(),buffers.limits().total_max_bytes,buffers.limits().max_queue_packets,metadata.occupied.allocations(),metadata.limit.allocations(),metadata.occupied.bytes(),metadata.limit.bytes());
    let Ok(plan) = proxy.tcp_runtime_services().components.clone() else {
        return;
    };
    for tag in ["node", "prefix"] {
        let Some(slot) = plan.configured(tag) else {
            continue;
        };
        eprintln!(
            "actual QUIC capacity Slot tag={tag} instance={:?} declared_tasks={:?}",
            slot.instance_id(),
            slot.recipe.spec().task_limit
        );
        for ordinal in 0..slot.recipe.spec().resources.len() {
            if let Some(owner) = slot.original_carrier_owner(ordinal) {
                if let Some(observed) = owner.observe() {
                    eprintln!("actual QUIC capacity Carrier tag={tag} ordinal={ordinal} retained={}/{} live={} stopped={}",observed.retained(),owner.limit(),observed.is_live(),observed.is_stopped());
                    for row in observed.slots() {
                        eprintln!("actual QUIC capacity row={} opening={} spawning={} view={} borrowed={} native={} driver={:?} aux={:?}",row.ordinal(),row.is_opening(),row.is_spawning(),row.physical_view_live(),row.borrowed_origin().is_some(),row.native().is_some(),row.driver().map(|(id,_)|*id),row.auxiliary().and_then(|task|task.task_id()));
                        if let Some(task) = row.auxiliary() {
                            if let Some(joined) = task.try_joined() {
                                eprintln!(
                                    "actual QUIC capacity Aux id={:?} joined={} HostACK={}",
                                    task.task_id(),
                                    task.matches_joined(&joined),
                                    joined.registration_released()
                                );
                                if let Err(error) = joined.result() {
                                    source("actual Aux original", error.as_ref());
                                }
                            }
                        }
                        if let Some(error) = row.acquisition_unknown() {
                            source("actual acquisition unknown", error.as_ref());
                        }
                    }
                }
            }
            if let Some(owner) = slot.original_metadata_owner(ordinal) {
                eprintln!(
                    "actual QUIC capacity Metadata tag={tag} ordinal={ordinal} limit={} current={}",
                    owner.limit(),
                    owner.current_original().is_some()
                );
            }
        }
    }
    for error in originals.carrier.failures() {
        source("actual Root retained original", error.as_ref());
    }
}

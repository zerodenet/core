use super::{HostResources, Owner};
use std::sync::Mutex;
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, Unfinished},
    ChannelError, CloseReason,
};

impl Owner {
    pub(super) async fn shutdown(
        &mut self,
        instance_id: zero_core::component::ComponentInstanceId,
        deadline: Instant,
        resources: &HostResources,
        tasks: &super::TaskMonitor,
        users: Option<&super::users::Users>,
    ) -> Result<CloseReport, ChannelError> {
        let component = instance_id.component;
        if !self.protocol_closed {
            self.instance.lifecycle.revoke(CloseReason::Shutdown);
        }
        tasks.begin_shutdown();
        let Self {
            instance,
            pending_close,
            protocol_closed,
            resource_failures,
        } = self;
        let pending = pending_close.get_or_insert_with(Box::default);
        pending.report.unfinished.clear();
        pending.io.unfinished.clear();
        // Each branch writes directly into the retained owner before it can
        // return to join!. Cancellation while another branch is still pending
        // cannot discard the protocol, task or real blocking-job receipt.
        {
            let super::PendingClose { report, io } = pending.as_mut();
            let pending = Mutex::new(report);
            let persist = |report| {
                pending
                    .lock()
                    .expect("component close report poisoned")
                    .append(report);
            };
            let collect_protocol = async {
                if *protocol_closed {
                    return Ok(());
                }
                match timeout_at(deadline, instance.lifecycle.shutdown(deadline)).await {
                    Ok(Ok(report)) => {
                        let complete = report.is_complete();
                        persist(report);
                        *protocol_closed = complete;
                        Ok(())
                    }
                    Ok(Err(error)) => retain_resource_failure(
                        &resource_failures[0], instance_id,
                        zero_platform_tokio::contract::lifecycle::ResourceFailurePhase::ProtocolLifecycle,
                        error,
                    ),
                    Err(_) => {
                        persist(CloseReport {
                            unfinished: vec![Unfinished::Resource(format!(
                                "component {} cleanup deadline elapsed",
                                component.0
                            ))],
                            ..Default::default()
                        });
                        Ok(())
                    }
                }
            };
            let collect_tasks = async {
                // The same elected host pump joins actual native exits. This
                // branch only receives/persists outcomes before scope ACKs.
                tasks.collect(deadline, persist).await;
            };
            let collect_io = async {
                match resources.shutdown_io(deadline).await {
                    Ok(report) => {
                        // Keep the exact provider's early observation separate
                        // from protocol/task/user obligations.
                        io.append(report);
                        Ok(())
                    }
                    Err(error) => retain_resource_failure(
                        &resource_failures[1], instance_id,
                        zero_platform_tokio::contract::lifecycle::ResourceFailurePhase::DeclaredResources,
                        error,
                    ),
                }
            };
            let collect_users = async {
                if let Some(users) = users {
                    persist(users.wait_idle(deadline).await);
                }
            };
            let (protocol, (), io, ()) =
                tokio::join!(collect_protocol, collect_tasks, collect_io, collect_users);
            protocol?;
            io?;
        }
        // Protocol cleanup can return the same Bound view after the concurrent
        // provider branch observed the real wire exit. Retain its late receipt
        // before asking that exact provider for final declaration evidence.
        pending.io.append(resources.take_returned_bound_io());
        let evidence = resources.io_evidence();
        evidence.retain_carrier_task_failures(&mut pending.report);
        if let Some(source) = evidence.sticky_faults.first() {
            return Err(super::retained(source.clone()));
        }
        let mut pending = pending_close
            .take()
            .map(|report| *report)
            .unwrap_or_default();
        let physically_closed = evidence.admission_closed && evidence.physical_confirmed();
        if physically_closed {
            // Only a fresh, exact-original declaration proof supersedes the
            // earlier provider snapshot. Never discard task/protocol/user
            // unfinished states or any original completed receipt/failure.
            pending.io.unfinished.clear();
        }
        pending.report.append(pending.io);
        let mut report = pending.report;
        for fault in resource_failures
            .iter()
            .filter_map(std::sync::OnceLock::get)
        {
            if !report
                .resource_failures
                .iter()
                .any(|same| std::sync::Arc::ptr_eq(same, fault))
            {
                report.resource_failures.push(fault.clone());
            }
        }
        if !*protocol_closed {
            report.unfinished.push(Unfinished::Resource(format!(
                "component {} original protocol controller remains unfinished",
                component.0
            )));
        }
        if !physically_closed {
            #[cfg(test)]
            eprintln!(
                "actual component {} incomplete original declaration evidence: {:?}; retained report: {:?}",
                component.0, evidence, report
            );
            report.unfinished.push(Unfinished::Resource(format!(
                "component {} declaration coverage remains unconfirmed",
                component.0
            )));
        }
        Ok(report)
    }
}

// Persist each actual branch error before join! can suspend in another branch.
// Receipt acceptance belongs to the existing Plan archive, not this observer.
fn retain_resource_failure(
    slot: &std::sync::OnceLock<
        std::sync::Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>,
    >,
    instance: zero_core::component::ComponentInstanceId,
    phase: zero_platform_tokio::contract::lifecycle::ResourceFailurePhase,
    mut error: ChannelError,
) -> Result<(), ChannelError> {
    error.component.get_or_insert(instance.component);
    let source = std::sync::Arc::new(error);
    let actual = slot.get_or_init(|| {
        std::sync::Arc::new(
            zero_platform_tokio::contract::lifecycle::ResourceFailure::new(
                instance,
                phase,
                source.clone(),
            ),
        )
    });
    if let Some(receipt) = source.cleanup_failure_receipt() {
        if !actual
            .source
            .cleanup_failure_receipt()
            .is_some_and(|same| same.same_receipt(receipt))
        {
            return Err(super::retained(source));
        }
    }
    Ok(())
}

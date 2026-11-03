use super::{HostResources, Owner};
use std::sync::Mutex;
use tokio::time::{timeout_at, Instant};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, Unfinished},
    ChannelError, CloseReason,
};

impl Owner {
    pub(super) async fn shutdown(
        &mut self,
        component: ComponentId,
        deadline: Instant,
        resources: &HostResources,
        tasks: &super::TaskMonitor,
        users: Option<&super::users::Users>,
    ) -> Result<CloseReport, ChannelError> {
        if !self.protocol_closed {
            self.instance.lifecycle.revoke(CloseReason::Shutdown);
        }
        tasks.begin_shutdown();
        let Self {
            instance,
            pending_close,
            protocol_closed,
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
                    Ok(Err(mut error)) => {
                        error.component.get_or_insert(component);
                        Err(error)
                    }
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
                let report = resources.shutdown_io(deadline).await?;
                // Keep the provider's early observation separate from actual
                // protocol/task/user obligations. It can precede their close
                // receipts in this same structured shutdown.
                io.append(report);
                Ok::<(), ChannelError>(())
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
        let evidence = resources.io_evidence();
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
        if !physically_closed {
            report.unfinished.push(Unfinished::Resource(format!(
                "component {} declaration coverage remains unconfirmed",
                component.0
            )));
        }
        Ok(report)
    }
}

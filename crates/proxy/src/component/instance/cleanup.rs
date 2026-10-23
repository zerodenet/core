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
        pending_close
            .get_or_insert_with(Box::default)
            .unfinished
            .clear();
        // Each branch writes directly into the retained owner before it can
        // return to join!. Cancellation while another branch is still pending
        // cannot discard the protocol, task or real blocking-job receipt.
        {
            let pending = Mutex::new(&mut *pending_close);
            let persist = |report| {
                pending
                    .lock()
                    .expect("component close report poisoned")
                    .get_or_insert_with(Box::default)
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
                persist(report);
                Ok::<(), ChannelError>(())
            };
            let (protocol, (), io) = tokio::join!(collect_protocol, collect_tasks, collect_io);
            protocol?;
            io?;
        }
        let evidence = resources.io_evidence();
        if let Some(source) = evidence.sticky_faults.first() {
            return Err(super::retained(source.clone()));
        }
        let mut report = pending_close
            .take()
            .map(|report| *report)
            .unwrap_or_default();
        if !evidence.admission_closed || !evidence.physical_confirmed() {
            report.unfinished.push(Unfinished::Resource(format!(
                "component {} declaration coverage remains unconfirmed",
                component.0
            )));
        }
        Ok(report)
    }
}

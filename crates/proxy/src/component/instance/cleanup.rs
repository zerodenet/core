use super::Owner;
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
    ) -> Result<CloseReport, ChannelError> {
        self.instance.lifecycle.revoke(CloseReason::Shutdown);
        self.tasks.begin_shutdown();
        let Self {
            instance,
            tasks,
            pending_close,
        } = self;
        // Persist a task report as soon as that branch completes. A protocol
        // hook may still be waiting, fail, or have its caller interrupted.
        let collect_tasks = async {
            let report = tasks.shutdown(deadline).await;
            let cache = pending_close.get_or_insert_with(Box::default);
            cache.unfinished.clear();
            cache.append(report);
        };
        let (protocol, ()) = tokio::join!(
            timeout_at(deadline, instance.lifecycle.shutdown(deadline)),
            collect_tasks,
        );
        let mut report = match protocol {
            Ok(Ok(report)) => report,
            Ok(Err(mut error)) => {
                error.component.get_or_insert(component);
                return Err(error);
            }
            Err(_) => CloseReport {
                unfinished: vec![Unfinished::Resource(format!(
                    "component {} cleanup deadline elapsed",
                    component.0
                ))],
                ..Default::default()
            },
        };
        if let Some(tasks) = pending_close.take() {
            report.append(*tasks);
        }
        Ok(report)
    }
}

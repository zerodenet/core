//! Explicit fixture driver for the same HostedScope consumer as production.
use super::*;

pub(in crate::component) struct TestContext<'a> {
    slots: Vec<&'a ComponentSlot>,
    admission: Arc<crate::component::admission::HostAdmission>,
    driver: InlineSupervisor,
}
impl<'a> TestContext<'a> {
    pub(in crate::component) fn new(slots: impl IntoIterator<Item = &'a ComponentSlot>) -> Self {
        let slots: Vec<_> = slots.into_iter().collect();
        let admission = slots
            .iter()
            .find_map(|slot| slot.host_admission())
            .unwrap_or_default();
        for slot in &slots {
            slot.bind_admission(admission.clone());
        }
        admission.stage(
            slots
                .iter()
                .filter(|slot| slot.can_reuse())
                .map(|slot| slot.admission_grant()),
        );
        Self {
            slots,
            admission,
            driver: Default::default(),
        }
    }
    pub(in crate::component) async fn drive<T>(&self, work: impl Future<Output = T>) -> T {
        self.driver
            .drive(
                self.admission.signal(),
                |cx| {
                    for slot in &self.slots {
                        slot.poll_tasks(cx);
                    }
                },
                work,
            )
            .await
    }
}

//! Only returned logical views and actual controller completion remove users.
use super::*;

impl Managed {
    pub(in crate::component::udp) fn logical_returned(&self) {
        let returned = {
            let mut users = self.logical.lock().unwrap();
            let mut returned = Vec::new();
            let mut index = 0;
            while index < users.len() {
                if users[index].settled() {
                    returned.push(users.remove(index));
                } else {
                    index += 1;
                }
            }
            returned
        };
        let changed = !returned.is_empty();
        drop(returned);
        if changed {
            self.changed.notify_waiters();
        }
    }
    pub(in crate::component::udp) fn origin(
        &self,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, ChannelError> {
        let current = self
            .current
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::StalePlan))?;
        let witness = current
            .driver
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::StalePlan))?;
        current.adoption.origin(&witness)
    }
}

impl PoolRoot {
    pub(in crate::component::udp) fn managed(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Managed>, ChannelError> {
        match &self.state.lock().unwrap().ownership {
            Ownership::Native(actual)
                if actual.instance == instance
                    && actual.ready.load(std::sync::atomic::Ordering::Acquire)
                    && !actual.cancel.is_cancelled() =>
            {
                Ok(actual.clone())
            }
            _ => Err(error(instance, Stage::Prepare, Reason::Revoked)),
        }
    }
}
impl Managed {
    pub(in crate::component::udp) fn register_logical(
        self: &Arc<Self>,
        flow: ShadowsocksUdpSocketFlow,
        original: zero_platform_tokio::controlled_carrier::CarrierOrigin,
        admission: Arc<zero_platform_tokio::contract::admission::SessionAdmission>,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<Arc<super::logical::Logical>, ChannelError> {
        self.logical_returned();
        let record = super::logical::Logical::new(self, flow, original, admission, context);
        let mut users = self.logical.lock().unwrap();
        if self.cancel.is_cancelled() || users.len() >= 4096 {
            return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
        }
        users.push(record.clone());
        Ok(record)
    }
}

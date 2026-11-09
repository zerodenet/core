use super::*;
impl AcceptedTaskFailures {
    pub(in crate::controlled_listener::accepted) async fn wait_capacity(
        &self,
    ) -> Result<(), ChannelError> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.reserve() {
                Ok(reserved) => {
                    drop(reserved);
                    return Ok(());
                }
                Err(error) if error.reason == Reason::Capacity => {}
                Err(error) => return Err(error),
            }
            let layout = super::plan::required(&mut self.0.rows.lock().unwrap())?.total;
            let snapshot = self.0.memory.snapshot();
            let credit = tokio::select! {
                _ = &mut changed => continue,
                result = self.0.memory.reserve_owned(layout) => match result {
                    Ok(credit) => credit,
                    Err(PacketMemoryError::Capacity) => {
                        tokio::select! {
                            _ = &mut changed => {},
                            _ = self.0.memory.changed_since(snapshot) => {},
                        }
                        continue;
                    }
                    Err(source) => return Err(capacity(source)),
                }
            };
            match self.reserve_prepaid(Some(credit)) {
                Ok(reserved) => {
                    drop(reserved);
                    return Ok(());
                }
                Err(error) if error.reason == Reason::Capacity => {}
                Err(error) => return Err(error),
            }
        }
    }
}

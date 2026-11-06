//! The original cache owns preparation before its first suspend or native IO.
use super::*;
use std::sync::atomic::Ordering;
struct Pending(Arc<Record>, bool);
impl Drop for Pending {
    fn drop(&mut self) {
        if self.1 {
            self.0.stop();
        }
    }
}
impl Managed {
    pub(crate) async fn open(
        &self,
        end: Option<Instant>,
    ) -> Result<ShadowsocksUdpSocketFlow, ChannelError> {
        let _opening = self.opening.lock().await;
        if !self.ready.load(Ordering::Acquire)
            || self.cancel.is_cancelled()
            || self.tasks.cancellation().is_cancelled()
        {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        let old = self.current.lock().unwrap().clone();
        if let Some(record) = old {
            if let Some(flow) = record
                .flow
                .lock()
                .unwrap()
                .as_ref()
                .filter(|flow| !flow.native_unavailable())
                .cloned()
            {
                return Ok(flow);
            }
            if !record.confirmed() {
                let _ = record.adoption.try_retire()?;
            }
            if !record.confirmed() {
                return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
            }
            let prior = self.current.lock().unwrap().take();
            drop(prior);
        }
        let mut grant = self.grants.reserve()?;
        let adoption = grant.adoption();
        let view = grant.claim_driver_view()?;
        let record = Arc::new(Record {
            adoption: adoption.clone(),
            flow: Mutex::new(None),
            driver: Mutex::new(None),
        });
        *self.current.lock().unwrap() = Some(record.clone());
        let mut pending = Pending(record.clone(), true);
        let stop = grant.cancellation();
        let call = CarrierContext {
            session: None,
            deadline: end,
            cancel: stop.clone(),
        };
        let upstream = self
            .upstream
            .select_background_carrier(Some(self.options.binding.clone()), call.clone())
            .await?;
        let (io, plugin) = if let Some((process, startup)) = self.process {
            let io = upstream
                .acquire_process_endpoint_datagram(
                    zero_platform_tokio::contract::network::ProcessEndpointDatagramRequest {
                        process,
                        process_ordinal: 0,
                        endpoint: self.options.endpoint.clone(),
                        context: call,
                        grant,
                        startup_deadline: end.map_or_else(
                            || Instant::now() + startup,
                            |end| end.min(Instant::now() + startup),
                        ),
                    },
                )
                .await?;
            (io, None)
        } else {
            let candidates = upstream
                .resolve_controlled_carrier(self.options.endpoint.clone(), call)
                .await?;
            (candidates.prepare_datagram(0, grant)?, None)
        };
        let codec = self
            .options
            .resume
            .clone()
            .into_shared_managed_socket_flow_codec();
        let (flow, prepared) = ShadowsocksUdpSocketFlow::prepare_native(
            io,
            codec,
            plugin,
            self.options.pending.limits.per_cache,
            self.pending_budget.clone(),
            stop.clone(),
        );
        // The same persistent record holds the sender before spawn can fail or
        // its driver can complete. Sender aliases retain only a revocable cell.
        *record.flow.lock().unwrap() = Some(flow.clone());
        let witness = crate::transport::udp_socket::native::start(
            prepared,
            &adoption,
            &self.tasks,
            view,
            stop,
        )?;
        *record.driver.lock().unwrap() = Some(witness);
        pending.1 = false;
        Ok(flow)
    }
}

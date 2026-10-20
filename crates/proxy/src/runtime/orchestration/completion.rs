//! One completion path for normal shutdown and task/startup failures.
use std::time::Duration;

use zero_api::{EndpointDirections, EndpointGetQuery, EndpointRuntimeState};
use zero_engine::EngineError;

use super::state::OrchestrationState;
use crate::runtime::Proxy;

impl OrchestrationState {
    pub(super) async fn finish(
        &mut self,
        proxy: &Proxy,
        mut result: Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let mut cleanup_errors = Vec::new();
        // Error/startup paths reach this method without the normal drain arm.
        // Stop admission, retain and join the accepted transaction while its
        // real reconciliation/rollback notifications can still be serviced.
        if let Err(error) = self.drain_configuration(proxy).await {
            cleanup_errors.push(error.to_string());
            if result.is_ok() {
                result = Err(error);
            }
        }
        self.publish_stopping();
        self.propagate_shutdown();
        if let Err(error) = proxy.stop_tun_if_running().await {
            cleanup_errors.push(format!("TUN shutdown failed: {error}"));
            if result.is_ok() {
                result = Err(error);
            }
        }
        let bindings = self.applied_snapshot.config().endpoint_bindings();
        let directions = EndpointDirections {
            inbound: true,
            outbound: true,
        };
        for binding in &bindings {
            self.engine.cancel_endpoint_flows(binding, directions);
        }
        // Signal first, then drain all owners within one bounded grace period.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            self.services.shutdown().await;
            while let Some(task) = self.listeners.join_next().await {
                task.map_err(|error| EngineError::Io(std::io::Error::other(error)))??;
            }
            while let Some(task) = self.urltests.join_next().await {
                task.map_err(|error| EngineError::Io(std::io::Error::other(error)))??;
            }
            Ok::<(), EngineError>(())
        })
        .await;
        let cleanup_error = match drained {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some(
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "orchestration task shutdown timed out",
                )
                .into(),
            ),
        };
        if let Some(error) = cleanup_error {
            cleanup_errors.push(error.to_string());
            if result.is_ok() {
                result = Err(error);
            }
        }
        // Joining after abort confirms resource guards have actually dropped.
        self.listeners.shutdown().await;
        self.urltests.shutdown().await;
        self.services.shutdown().await;
        if let Err(error) = proxy
            .inbound_owners
            .shutdown_all(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
        {
            cleanup_errors.push(format!("inbound owner shutdown failed: {error}"));
            if result.is_ok() {
                result = Err(error);
            }
        }
        // Accepted configuration compensation and its consumers have drained.
        // Confirm the actual DNS query/wire workers before retiring components;
        // this does not claim resolver pools or FakeIP persistence are closed.
        if let Err(error) = proxy
            .resolver
            .shutdown_queries(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
        {
            cleanup_errors.push(format!("DNS query worker shutdown failed: {error}"));
            if result.is_ok() {
                result = Err(EngineError::Io(error));
            }
        }
        // This interface confirms only the actual DoH pools/drivers/users.
        // DoQ and other transports require their own precise owner receipts.
        if let Err(error) = proxy
            .resolver
            .shutdown_doh_pools(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
        {
            cleanup_errors.push(format!("DoH pool shutdown failed: {error}"));
            if result.is_ok() {
                result = Err(EngineError::Io(error));
            }
        }
        match proxy
            .retired_datagrams
            .wait_closed(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
        {
            Ok(report) if report.is_complete() && report.failed_tasks.is_empty() => {}
            Ok(mut report) => {
                if let Some(failure) = report.failed_tasks.pop() {
                    cleanup_errors.push(format!(
                        "datagram retirement task failed: {}",
                        failure.error
                    ));
                    if result.is_ok() {
                        result = Err(EngineError::Io(std::io::Error::other(failure.error)));
                    }
                    // The persistent owner retains any unfinished controllers.
                }
                let message = format!(
                    "datagram retirement remains unconfirmed: {:?}",
                    report.unfinished
                );
                cleanup_errors.push(message.clone());
                if result.is_ok() {
                    result = Err(std::io::Error::new(std::io::ErrorKind::TimedOut, message).into());
                }
            }
            Err(error) => {
                cleanup_errors.push(format!("datagram retirement failed: {error}"));
                if result.is_ok() {
                    result = Err(EngineError::Io(std::io::Error::other(error)));
                }
            }
        }
        if let Err(error) = proxy
            .protocols
            .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
        {
            cleanup_errors.push(format!("component shutdown failed: {error}"));
            if result.is_ok() {
                result = Err(error);
            }
        }
        self.listener_stops.clear();
        self.active_inbounds.clear();
        #[cfg(feature = "raw-ip-runtime")]
        {
            let receipts = self.outbound_devices.shutdown_outbound_devices();
            let completion = tokio::time::timeout(Duration::from_secs(5), async {
                for receipt in receipts {
                    receipt.await;
                }
            })
            .await;
            if completion.is_err() {
                cleanup_errors.push("outbound device shutdown was not confirmed".to_owned());
                if result.is_ok() {
                    result = Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "outbound device shutdown was not confirmed",
                    )
                    .into());
                }
            }
        }
        // TUN, ingress and outbound-device owners have received stop and been
        // joined above. Their real route receipts can now complete without an
        // apply-lock/device dependency cycle.
        if let Err(message) = proxy
            .packet_close_owner
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
        {
            cleanup_errors.push(message.clone());
            if result.is_ok() {
                result = Err(std::io::Error::other(message).into());
            }
        }
        let mut failure = result.as_ref().err().map(ToString::to_string);
        if !cleanup_errors.is_empty() {
            tracing::warn!(errors = ?cleanup_errors, reason = "orchestration_cleanup_error", "orchestration cleanup encountered errors");
            if let Some(message) = &mut failure {
                message.push_str(&format!("; cleanup: {}", cleanup_errors.join("; ")));
            }
        }
        self.publish_terminal(failure.as_deref());
        self.finished = true;
        result
    }

    fn publish_stopping(&self) {
        for binding in self.applied_snapshot.config().endpoint_bindings() {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id,
            };
            if let Ok(mut endpoint) = self
                .engine
                .endpoint_snapshot_in(&self.applied_snapshot, &query)
            {
                if matches!(
                    endpoint.state,
                    EndpointRuntimeState::Running | EndpointRuntimeState::Starting
                ) {
                    endpoint.state = EndpointRuntimeState::Stopping;
                    self.engine.record_endpoint_runtime_state(&endpoint);
                }
            }
        }
    }

    fn publish_terminal(&self, failure: Option<&str>) {
        let current = self.engine.runtime_snapshot();
        let mut seen = std::collections::HashSet::new();
        for snapshot in [&current, &self.applied_snapshot] {
            for binding in snapshot.config().endpoint_bindings() {
                if !seen.insert(binding.endpoint_id.clone()) {
                    continue;
                }
                let query = EndpointGetQuery {
                    endpoint_id: binding.endpoint_id,
                };
                if let Ok(mut endpoint) = self.engine.endpoint_snapshot_in(snapshot, &query) {
                    if failure.is_some() && endpoint.state == EndpointRuntimeState::Failed {
                        continue;
                    }
                    let affected = matches!(
                        endpoint.state,
                        EndpointRuntimeState::Running
                            | EndpointRuntimeState::Starting
                            | EndpointRuntimeState::Stopping
                    );
                    if let Some(message) = failure.filter(|_| affected) {
                        self.engine.record_endpoint_runtime_error(
                            &endpoint.endpoint_id,
                            message,
                            true,
                        );
                    } else {
                        endpoint.state = EndpointRuntimeState::Stopped;
                        self.engine.record_endpoint_runtime_state(&endpoint);
                    }
                }
            }
        }
    }
}

impl Drop for OrchestrationState {
    fn drop(&mut self) {
        // Panic/cancellation cannot await cleanup. Report failed, never a
        // confirmed stopped state; normal completion disarms this guard.
        if !self.finished {
            self.propagate_shutdown();
            self.publish_terminal(Some(
                "orchestration interrupted before shutdown confirmation",
            ));
        }
        #[cfg(feature = "raw-ip-runtime")]
        drop(self.outbound_devices.shutdown_outbound_devices());
    }
}

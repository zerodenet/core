//! Acknowledged endpoint control over the existing serialized reload executor.

use super::super::ProxyHandle;
use std::{sync::Arc, time::Duration};
use zero_api::{
    ApiError, ApiErrorCode, CommandRequest, CommandResponse, EndpointDirections, EndpointGetQuery,
    EndpointPersistence, EndpointRuntimeState, QueryRequest, QueryResponse, QueryService,
};
use zero_engine::{EndpointChange, EngineRuntimeSnapshot};

mod transition;

const TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn is_endpoint_command(command: &CommandRequest) -> bool {
    matches!(
        command,
        CommandRequest::EndpointSetState(_)
            | CommandRequest::EndpointSetDirections(_)
            | CommandRequest::EndpointRestart(_)
            | CommandRequest::EndpointClearOverrides(_)
    )
}

impl ProxyHandle {
    pub(super) async fn execute_endpoint(
        &self,
        command: CommandRequest,
    ) -> zero_api::ApiResult<CommandResponse> {
        let (id, change, persist, expected, expected_core) = match command {
            CommandRequest::EndpointSetState(request) => (
                request.endpoint_id,
                EndpointChange::Enabled(request.enabled),
                request.persistence == EndpointPersistence::SourceFile,
                request.expected_intent_revision,
                request.expected_core_instance_id,
            ),
            CommandRequest::EndpointSetDirections(request) => (
                request.endpoint_id,
                EndpointChange::Directions(request.directions),
                request.persistence == EndpointPersistence::SourceFile,
                request.expected_intent_revision,
                request.expected_core_instance_id,
            ),
            CommandRequest::EndpointRestart(request) => (
                request.endpoint_id,
                EndpointChange::Restart,
                false,
                request.expected_intent_revision,
                request.expected_core_instance_id,
            ),
            CommandRequest::EndpointClearOverrides(request) => (
                request.endpoint_id,
                EndpointChange::ClearOverrides,
                false,
                request.expected_intent_revision,
                request.expected_core_instance_id,
            ),
            _ => {
                return Err(ApiError::new(
                    ApiErrorCode::InvalidArgument,
                    "not an endpoint command",
                ))
            }
        };
        // The same lock serializes config.apply, runtime overlays, restart and
        // resource control. Admission and revision checks happen under it.
        // The persistent apply owner already holds the shared serialization
        // guard; waiting HTTP/IPC futures do not own this transaction.
        let previous = self.proxy.engine.runtime_snapshot();
        self.proxy
            .engine
            .configuration_source()
            .ensure_available()
            .map_err(|error| ApiError::new(ApiErrorCode::Internal, error.to_string()))?;
        self.proxy.engine.check_endpoint_preconditions(
            &previous,
            &id,
            expected_core.as_deref(),
            expected,
        )?;
        let binding = previous
            .config()
            .endpoint_bindings()
            .into_iter()
            .find(|binding| binding.endpoint_id == id)
            .ok_or_else(|| {
                ApiError::new(
                    ApiErrorCode::NotFound,
                    format!("endpoint `{id}` was not found"),
                )
            })?;
        if !self.proxy.protocols.endpoint_control_supported(&binding) {
            return Err(ApiError::new(
                ApiErrorCode::Unsupported,
                "endpoint has no registered control capability",
            ));
        }
        let mut candidate = self
            .proxy
            .engine
            .prepare_endpoint_change(&previous, &id, change, persist, expected)?;
        let old = self.observed_endpoint(&id)?;
        let next = self.proxy.engine.endpoint_snapshot_in(
            &candidate,
            &EndpointGetQuery {
                endpoint_id: id.clone(),
            },
        )?;
        if Arc::ptr_eq(&candidate, &previous)
            && ((next.enabled && old.state == EndpointRuntimeState::Running)
                || (!next.enabled && old.state == EndpointRuntimeState::Stopped))
        {
            return self.endpoint_command_response(&id, persist);
        }
        // A registered capability must confirm independent role cancellation.
        if old.state == EndpointRuntimeState::Running
            && old.enabled
            && next.enabled
            && old.allowed.outbound
            && !next.allowed.outbound
            && !self
                .proxy
                .protocols
                .endpoint_live_direction_contraction(&binding)
                .outbound
        {
            return Err(ApiError::new(ApiErrorCode::Unsupported,
                "live outbound direction contraction requires packet-path cancellation; disable the endpoint before changing directions"));
        }
        let revoked = EndpointDirections {
            inbound: old.enabled && old.allowed.inbound && !next.allowed.inbound,
            outbound: old.enabled && old.allowed.outbound && !next.allowed.outbound,
        };
        if let Some(reconciler) = &self.config_reconciler {
            reconciler
                .validate(previous.config(), candidate.config())
                .map_err(|message| ApiError::new(ApiErrorCode::InvalidArgument, message))?;
        }
        let result = async {
            if matches!(change, EndpointChange::Restart) {
                let stopped = self.proxy.engine.prepare_endpoint_change(
                    &previous,
                    &id,
                    EndpointChange::Enabled(false),
                    false,
                    None,
                )?;
                self.apply_endpoint_snapshot(stopped, false).await?;
                self.wait_endpoint_flows_ended(
                    &binding,
                    EndpointDirections {
                        inbound: true,
                        outbound: true,
                    },
                )
                .await?;
                // The temporary stop was Ready, not a second configuration
                // publication. Retire its actual guard before the final pass.
                #[cfg(feature = "raw-ip-runtime")]
                self.proxy.protocols.discard_pending_device_candidates(
                    tokio::time::Instant::now() + TIMEOUT,
                ).await.map_err(|error| ApiError::new(ApiErrorCode::Internal, error.to_string()))?;
                self.proxy.resolver.discard_prepared_reload();
                // Allocate after the temporary stop so the final published
                // intent revision cannot precede a visible stop revision.
                candidate = self.proxy.engine.prepare_endpoint_change(
                    &previous,
                    &id,
                    EndpointChange::Restart,
                    false,
                    None,
                )?;
            }
            self.apply_endpoint_snapshot(candidate.clone(), false)
                .await?;
            let dns = self.retire_prepared_dns(TIMEOUT).await
                .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
            let component_config = candidate.config().clone();
            self.proxy
                .protocols
                .scan_components(&component_config, tokio::time::Instant::now() + TIMEOUT)
                .await
                .map_err(|error| ApiError::new(ApiErrorCode::Internal, error.to_string()))?;
            self.proxy
                .host
                .apply()
                .commit(|| {
                    if persist {
                        self.proxy
                            .engine
                            .configuration_source()
                            .persist_ready(
                                &component_config,
                                &format!(
                                    "{}:endpoint:{}",
                                    self.proxy.engine.core_instance_id(),
                                    candidate.config_revision()
                                ),
                            )
                            .map_err(|error| {
                                format!("endpoint source persistence failed: {error}")
                            })?;
                    }
                    self.proxy
                        .protocols
                        .commit_components(&component_config, || {
                            self.proxy.resolver.commit_reload_resources(&dns)
                                .map_err(|error| format!("endpoint DNS commit failed: {error}"))?;
                            self.proxy
                                .engine
                                .publish_prepared_snapshot(candidate.clone())
                                .map_err(|error| error.to_string())?;
                            if persist {
                                self.proxy.engine.commit_config_change();
                            }
                            Ok(())
                        })?;
                    self.proxy
                        .inbound_owners
                        .finish_recovery_transition(&component_config);
                    Ok(())
                })
                .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
            #[cfg(feature = "raw-ip-runtime")]
            self.proxy.protocols.finish_device_publication(&component_config).await
                .map_err(|error| ApiError::new(ApiErrorCode::Internal, format!("endpoint committed; device completion remains owned: {error}")))?;
            self.proxy.publish_endpoint_facts(&candidate);
            // Revocation is irreversible. Failed application readiness or
            // source persistence must leave the old business sessions intact.
            if revoked.inbound || revoked.outbound {
                self.wait_endpoint_flows_ended(&binding, revoked).await?;
            }
            if let Some(reconciler) = &self.config_reconciler {
                reconciler.committed(candidate.config().clone()).await.map_err(|message| ApiError::new(ApiErrorCode::Internal, format!("endpoint committed; application finalization failed: {message}")))?;
            }
            self.proxy.engine.configuration_source().finalize_committed().map_err(|error| ApiError::new(ApiErrorCode::Internal, format!("endpoint committed; source finalization failed: {error}; query current state")))?;
            Ok::<(), ApiError>(())
        }
        .await;
        if let Err(error) = result {
            // The candidate resolver is staged until application reconciliation
            // succeeds. Release that stage before preparing the old config.
            self.proxy.resolver.discard_prepared_reload();
            let recovery = if let Some(reconciler) = &self.config_reconciler {
                self.retire_application_for_recovery(
                    reconciler.as_ref(),
                    &candidate,
                    candidate.config().clone(),
                    &error.message,
                )
                .await
            } else {
                self.confirm_source_compensation(&candidate)
            }
            .map_err(|message| ApiError::new(error.code, message))?;
            let rollback = self.apply_endpoint_snapshot(recovery, true).await;
            self.proxy.resolver.discard_prepared_reload();
            self.proxy.engine.record_endpoint_runtime_error(
                &id,
                if rollback.is_ok() {
                    "endpoint operation failed; previous resources restored"
                } else {
                    "endpoint operation and rollback failed"
                },
                rollback.is_err(),
            );
            return Err(ApiError::new(
                error.code,
                match rollback {
                    Ok(()) => format!(
                        "{}; restored previous endpoint intent and resources",
                        error.message
                    ),
                    Err(rollback) => format!(
                        "{}; endpoint rollback failed: {}",
                        error.message, rollback.message
                    ),
                },
            ));
        }
        self.endpoint_command_response(&id, persist)
    }

    async fn apply_endpoint_snapshot(
        &self,
        snapshot: Arc<EngineRuntimeSnapshot>,
        compensating: bool,
    ) -> zero_api::ApiResult<()> {
        self.publish_endpoint_control_transitions(&snapshot);
        self.apply_proxy_config_under_guard(
            (**snapshot.config()).clone(),
            TIMEOUT,
            Some(snapshot.clone()),
            compensating,
        )
        .await
        .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
        if let Some(reconciler) = &self.config_reconciler {
            reconciler
                .reconcile(snapshot.config().clone())
                .await
                .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
        }
        if !compensating {
            // The accepted operation still owes persistence and application
            // commit. Publishing here would discard last-good immutable recipes
            // before the real outer commit can fail and request compensation.
            return Ok(());
        }
        let token = self
            .retire_prepared_dns(TIMEOUT)
            .await
            .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
        self.proxy
            .protocols
            .scan_components(snapshot.config(), tokio::time::Instant::now() + TIMEOUT)
            .await
            .map_err(|error| ApiError::new(ApiErrorCode::Internal, error.to_string()))?;
        let publish = || {
            self.proxy
                .protocols
                .commit_components(snapshot.config(), || {
                    self.proxy
                        .resolver
                        .commit_reload_resources(&token)
                        .map_err(|error| format!("endpoint DNS commit failed: {error}"))
                })?;
            self.proxy
                .inbound_owners
                .finish_recovery_transition(snapshot.config());
            Ok(())
        };
        let result = if compensating {
            self.proxy.host.apply().compensate(publish)
        } else {
            self.proxy.host.apply().commit(publish)
        };
        result.map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
        #[cfg(feature = "raw-ip-runtime")]
        self.proxy
            .protocols
            .finish_device_publication(snapshot.config())
            .await
            .map_err(|error| {
                ApiError::new(
                    ApiErrorCode::Internal,
                    format!("endpoint committed; device completion remains owned: {error}"),
                )
            })?;
        self.proxy.publish_endpoint_facts(&snapshot);
        if compensating {
            if let Some(reconciler) = &self.config_reconciler {
                reconciler
                    .committed(snapshot.config().clone())
                    .await
                    .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
            }
        }
        Ok(())
    }

    async fn wait_endpoint_flows_ended(
        &self,
        binding: &zero_config::EndpointBindingConfig,
        directions: EndpointDirections,
    ) -> zero_api::ApiResult<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.proxy.engine.cancel_endpoint_flows(binding, directions);
                if self
                    .proxy
                    .engine
                    .endpoint_flow_ids(binding, directions)
                    .is_empty()
                    && !self.endpoint_packet_roles_active(binding, directions)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            ApiError::new(
                ApiErrorCode::Internal,
                "endpoint business termination was not confirmed",
            )
        })
    }

    fn observed_endpoint(&self, id: &str) -> zero_api::ApiResult<zero_api::EndpointSnapshot> {
        match self.query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: id.into(),
        }))? {
            QueryResponse::Endpoint(endpoint) => Ok(endpoint),
            _ => Err(ApiError::new(
                ApiErrorCode::Internal,
                "endpoint query returned the wrong response",
            )),
        }
    }

    fn endpoint_command_response(
        &self,
        id: &str,
        persist: bool,
    ) -> zero_api::ApiResult<CommandResponse> {
        Ok(CommandResponse {
            accepted: true,
            result: Some(serde_json::json!({
                "applied": true, "reconciled": true,
                "persistence": if persist { "source_file" } else { "runtime_only" },
                "endpoint": self.observed_endpoint(id)?,
            })),
        })
    }
}

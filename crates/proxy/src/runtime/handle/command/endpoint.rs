//! Acknowledged endpoint control over the existing serialized reload executor.

use super::super::ProxyHandle;
use std::{sync::Arc, time::Duration};
use zero_api::{
    ApiError, ApiErrorCode, CommandRequest, CommandResponse, EndpointDirections, EndpointGetQuery,
    EndpointPersistence, EndpointRuntimeState, QueryRequest, QueryResponse, QueryService,
};
use zero_engine::{EndpointChange, EngineRuntimeSnapshot};

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
        let (id, change, persist, expected) = match command {
            CommandRequest::EndpointSetState(request) => (
                request.endpoint_id,
                EndpointChange::Enabled(request.enabled),
                request.persistence == EndpointPersistence::SourceFile,
                request.expected_intent_revision,
            ),
            CommandRequest::EndpointSetDirections(request) => (
                request.endpoint_id,
                EndpointChange::Directions(request.directions),
                request.persistence == EndpointPersistence::SourceFile,
                request.expected_intent_revision,
            ),
            CommandRequest::EndpointRestart(request) => (
                request.endpoint_id,
                EndpointChange::Restart,
                false,
                request.expected_intent_revision,
            ),
            CommandRequest::EndpointClearOverrides(request) => (
                request.endpoint_id,
                EndpointChange::ClearOverrides,
                false,
                request.expected_intent_revision,
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
        let _guard = self.proxy.reload_apply_lock.lock().await;
        let previous = self.proxy.engine.runtime_snapshot();
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
        // Existing inbound sessions can be cancelled by their inbound tag;
        // the raw-IP listener admits each new business packet through the
        // current engine snapshot while preserving correlated outbound replies.
        // Outbound Packet returns and client stacks still lack an independent
        // cancellation scope, so keep that contraction explicit.
        if old.state == EndpointRuntimeState::Running
            && old.enabled
            && next.enabled
            && old.allowed.outbound
            && !next.allowed.outbound
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
            self.apply_endpoint_snapshot(candidate, persist).await?;
            if revoked.inbound || revoked.outbound {
                self.wait_endpoint_flows_ended(&binding, revoked).await?;
            }
            Ok::<(), ApiError>(())
        }
        .await;
        if let Err(error) = result {
            let rollback = self.apply_endpoint_snapshot(previous, persist).await;
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
        if persist {
            self.proxy.engine.commit_config_change();
        }
        self.endpoint_command_response(&id, persist)
    }

    async fn apply_endpoint_snapshot(
        &self,
        snapshot: Arc<EngineRuntimeSnapshot>,
        persist: bool,
    ) -> zero_api::ApiResult<()> {
        self.apply_proxy_config_under_guard(
            (**snapshot.config()).clone(),
            TIMEOUT,
            persist,
            Some(snapshot.clone()),
        )
        .await
        .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
        if let Some(reconciler) = &self.config_reconciler {
            reconciler
                .reconcile(snapshot.config().clone())
                .await
                .map_err(|message| ApiError::new(ApiErrorCode::Internal, message))?;
        }
        self.proxy
            .resolver
            .commit_prepared_reload()
            .map_err(|error| {
                ApiError::new(
                    ApiErrorCode::Internal,
                    format!("endpoint DNS commit failed: {error}"),
                )
            })?;
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

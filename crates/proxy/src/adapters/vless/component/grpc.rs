//! Ordinary cold recipes use the leaf's actual shared pool and runtime totals.
mod production;
use super::super::VlessAdapter;
use crate::component::{
    background::{grpc, pool},
    RegistrationError, ResourcePreparation,
};
pub(crate) use production::{has_nodes, selected, whole_selected};
use std::{num::NonZeroUsize, sync::Arc};
use zero_config::{OutboundConfig, OutboundProtocolConfig};
use zero_transport::grpc::{PoolBudgets, TotalsFactory};
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if !matches!(
        &input.protocol,
        OutboundProtocolConfig::Vless {
            flow: None,
            reality: None,
            mux_concurrency: None,
            xudp_concurrency: None,
            grpc: Some(_),
            ..
        }
    ) {
        return false;
    }
    let mut plain = input.clone();
    let OutboundProtocolConfig::Vless { grpc, tls, .. } = &mut plain.protocol else {
        return false;
    };
    if grpc.take().is_none() {
        return false;
    }
    if tls.as_ref().is_some_and(|tls| {
        zero_transport::tls::OwnedPreparedClient::validate_profile(tls.as_ref()).is_err()
    }) {
        return false;
    }
    // Eligibility borrows the actual CA File owner; the original profile/path
    // remains intact in the real prepared recipe and File declaration.
    if let Some(tls) = tls.as_mut() {
        tls.ca_cert_path = None;
    }
    super::native_input(&plain)
}
impl VlessAdapter {
    pub(crate) fn grpc_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: &Arc<pool::Factory>,
    ) -> Result<super::mux::Options, RegistrationError> {
        let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
            unreachable!("owning VLESS projection")
        };
        let runtime = resources
            .candidate()
            .map(|config| config.runtime.clone())
            .unwrap_or_default();
        let nonzero = |limit| {
            NonZeroUsize::new(limit).ok_or_else(|| {
                zero_config::ConfigError::InvalidRuntime("zero gRPC owner budget".into())
            })
        };
        let budgets = PoolBudgets {
            pool_carriers: nonzero(runtime.effective_grpc_pool_max_carriers())?,
            pool_streams: nonzero(runtime.effective_grpc_pool_max_streams())?,
            total_carriers: nonzero(runtime.effective_grpc_total_max_carriers())?,
            total_streams: nonzero(runtime.effective_grpc_total_max_streams())?,
        };
        let root = Arc::new(
            self.runtime
                .grpc_budget_options(budgets)
                .map_err(|source| {
                    zero_config::ConfigError::InvalidOutbound(format!(
                        "outbound `{}`: {source}",
                        input.tag
                    ))
                })?,
        );
        let root_id = resources.share(
            "vless.grpc.actual_registry_budget",
            Arc::new(TotalsFactory),
            root.clone(),
        )?;
        let input_refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning VLESS projection");
        let mut prepared = ::vless::component::grpc::PreparedGrpc::for_background_options_refs(
            input_refs,
            &self.runtime,
            resources.candidate().and_then(|config| config.source_dir()),
            input.dial.to_policy(),
            budgets.pool_carriers,
            budgets.pool_streams,
        )
        .and_then(|prepared| prepared.with_root_budget(root_id, root.proposal()))
        .and_then(|prepared| {
            prepared.with_file_owner(
                resources.candidate().and_then(|config| config.source_dir()),
                nonzero(runtime.effective_file_read_max_bytes())
                    .map_err(|source| std::io::Error::other(source))?,
            )
        })
        .map_err(|source| {
            zero_config::ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        if zero_core::address::parse_ip_literal(server).is_none() {
            let node_dns =
                crate::component::native_dns::prepare_node(resources, factory.network.clone())?;
            prepared = prepared.with_node_dns(node_dns);
        }
        let pool_id = resources.share(
            "vless.grpc.actual_pool",
            Arc::new(grpc::Factory {
                network: factory.network.clone(),
                generation: factory.generation,
            }),
            Arc::new(grpc::Options {
                pool: Arc::new(prepared.pool_options().clone()),
                tag: input.tag.clone(),
            }),
        )?;
        if matches!(&input.protocol,OutboundProtocolConfig::Vless{tls:Some(tls),..} if tls.ca_cert_path.is_some())
        {
            let inherited = if resources.same_source_as_previous() {
                resources
                    .previous_resource_instance(pool_id)
                    .map(|previous| prepared.reuse_file_material_from(previous))
                    .transpose()
                    .map_err(|source| {
                        zero_config::ConfigError::InvalidOutbound(format!(
                            "outbound `{}`: {source}",
                            input.tag
                        ))
                    })?
                    .unwrap_or(false)
            } else {
                false
            };
            if !inherited {
                resources.require_fresh_resource(pool_id)?;
            }
        }
        Ok(super::mux::Options::Grpc(Arc::new(prepared.bind(pool_id))))
    }
}

use ::shadowsocks::transport::{
    ShadowsocksInboundUserRef, ShadowsocksOutboundOptionsRef, ShadowsocksTransportLeaf,
};
#[cfg(feature = "shadowsocks")]
use ::shadowsocks::ShadowsocksInboundProfileStore;
use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig};
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput};
use crate::runtime::path::TcpPathCategory;

pub(crate) mod component;
#[cfg(feature = "shadowsocks")]
mod inbound_component;
#[cfg(feature = "shadowsocks")]
mod tcp;
#[cfg(feature = "shadowsocks")]
mod tcp_owned;
#[cfg(feature = "shadowsocks")]
pub(crate) mod udp;

#[cfg(feature = "shadowsocks")]
#[derive(Debug)]
pub(crate) struct ShadowsocksAdapter {
    inbound_profiles: ShadowsocksInboundProfileStore,
    outbound_state: ::shadowsocks::transport::ShadowsocksTransportState,
}

#[cfg(feature = "shadowsocks")]
impl Default for ShadowsocksAdapter {
    fn default() -> Self {
        // One actual adapter's TCP2022 salt entries, allocated on demand. This
        // is neither a per-profile cap nor a cap for UDP/Bloom security state.
        let budget = ::shadowsocks::ReplayBudget::new(Some(500_000))
            .expect("positive SS TCP2022 default budget");
        let udp = ::shadowsocks::udp::UdpReplayBudget::with_limits(Default::default())
            .expect("positive SS UDP receiver-window default budget");
        let bloom = ::shadowsocks::shared::LegacyReplayBudget::new(Some(64 * 1024 * 1024))
            .expect("positive legacy Bloom bitmap byte policy");
        Self {
            inbound_profiles: ShadowsocksInboundProfileStore::with_security_budgets(
                budget.clone(),
                udp.clone(),
                bloom.clone(),
            ),
            outbound_state:
                ::shadowsocks::transport::ShadowsocksTransportState::with_security_budgets(
                    budget, udp, bloom,
                ),
        }
    }
}

#[cfg(feature = "shadowsocks")]
pub(crate) fn udp_budget_limits(
    runtime: &zero_config::RuntimeOptionsConfig,
) -> ::shadowsocks::udp::UdpBudgetLimits {
    ::shadowsocks::udp::UdpBudgetLimits {
        receiver_windows: runtime.effective_ss_udp_replay_max_windows(),
        senders: runtime.effective_ss_udp_max_senders(),
        associations: runtime.effective_ss_udp_max_associations(),
        return_bindings: runtime.effective_ss_udp_max_return_bindings(),
        authenticated_codecs: runtime.effective_ss_udp_max_authenticated_codecs(),
    }
}

#[cfg(feature = "shadowsocks")]
fn inbound_user_refs<'a>(
    cipher: &str,
    password: &'a str,
    users: &'a [zero_config::ShadowsocksUserConfig],
) -> Vec<ShadowsocksInboundUserRef<'a>> {
    if users.is_empty() {
        return (!password.is_empty() || ::shadowsocks::validation::is_plain(cipher))
            .then_some(ShadowsocksInboundUserRef {
                password,
                principal_key: None,
                up_bps: None,
                down_bps: None,
                device_limit: None,
                quota_remaining_bytes: None,
                policy_revision: None,
            })
            .into_iter()
            .collect();
    }
    users
        .iter()
        .map(|user| ShadowsocksInboundUserRef {
            password: user.password.as_str(),
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

#[cfg(feature = "shadowsocks")]
fn transport_leaf(
    state: &::shadowsocks::transport::ShadowsocksTransportState,
    tag: &str,
    protocol: &OutboundProtocolConfig,
) -> Option<ShadowsocksTransportLeaf> {
    let OutboundProtocolConfig::Shadowsocks {
        server,
        port,
        password,
        cipher,
        replay_attack,
        plugin,
        state_limits,
    } = protocol
    else {
        return None;
    };
    Some(
        state.with_plugin(
            state.apply(
                ShadowsocksTransportLeaf::from_options_refs(
                    tag,
                    server,
                    *port,
                    ShadowsocksOutboundOptionsRef { cipher, password },
                )
                .with_state_limits(*state_limits),
                *replay_attack,
            ),
            plugin.clone(),
        ),
    )
}

#[cfg(feature = "shadowsocks")]
impl NamedProtocolAdapter for ShadowsocksAdapter {
    const PROTOCOL_NAME: &'static str = "shadowsocks";
    const FEATURE_NAME: &'static str = "shadowsocks";

    fn validate_runtime_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), zero_engine::EngineError> {
        self.outbound_state
            .validate_tcp_replay_budget(config.runtime.effective_ss_tcp_replay_max_salts())
            .and_then(|()| {
                self.outbound_state
                    .validate_udp_budget(udp_budget_limits(&config.runtime))
            })
            .and_then(|()| {
                self.outbound_state.validate_legacy_replay_budget(
                    config.runtime.effective_ss_legacy_replay_max_bytes(),
                )
            })
            .map_err(|source| std::io::Error::other(source).into())
    }
    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.outbound_state.clear_plugins();
        self.outbound_state.compact_udp();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Shadowsocks {
                password,
                identity_password,
                users,
                cipher,
                ..
            } = &inbound.protocol
            else {
                continue;
            };
            let users = inbound_user_refs(cipher, password, users);
            let _ = self.inbound_profiles.replace_with_identity(
                &inbound.tag,
                cipher,
                identity_password.as_deref(),
                &users,
            );
        }
    }
}

#[cfg(feature = "shadowsocks")]
impl ShadowsocksAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy { outbound, .. } = input else {
            return None;
        };
        let leaf = transport_leaf(&self.outbound_state, outbound.tag(), &outbound.protocol)?;
        let tcp = std::sync::Arc::new(
            crate::runtime::tcp_dispatch::operation::SocketTcpOperation {
                handshake: leaf.clone(),
            },
        );
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Session,
            tcp: Some(Ok(if component::native_input(outbound) {
                std::sync::Arc::new(tcp_owned::Connect {
                    tag: leaf.tag().into(),
                    server: leaf.server().into(),
                    port: leaf.port(),
                })
                    as std::sync::Arc<
                        dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation,
                    >
            } else {
                // Targets lacking a concrete whole-process provider retain the
                // existing caller; a failed migrated role never falls back here.
                tcp.clone()
            })),
            tcp_relay: Some(Ok(tcp)),
            udp: Some(self.udp_flow_operation(leaf.clone())),
            udp_relay: None,
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: self.udp_packet_path_operation(leaf),
        })
    }
}

#[cfg(feature = "shadowsocks")]
impl ProtocolMetadata for ShadowsocksAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::shadowsocks::ShadowsocksProtocol.descriptor()
    }
}

#[cfg(all(test, feature = "raw-ip-runtime"))]
#[path = "shadowsocks/tests/replay_budget.rs"]
mod replay_budget_tests;

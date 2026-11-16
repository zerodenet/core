//! Fixed-peer Packet source grammar over actual declared Stream/Datagram shapes.
//! This selects implemented roles; native custody is checked at actual execution.
use std::collections::{BTreeMap, BTreeSet};
use zero_config::{OutboundConfig, OutboundGroupKind, OutboundProtocolConfig as P, RuntimeConfig};

#[derive(Clone, Copy)]
struct Shape {
    stream_loan: bool,
    datagram_loan: bool,
    packet_carrier: bool,
    packet_formatter: bool,
    fixed_packet: bool,
    root_only: bool,
    packet_underlay: bool,
    pooled: bool,
    reentrant: bool,
}
impl Shape {
    fn stream(datagram_loan: bool) -> Self {
        Self {
            stream_loan: true,
            datagram_loan,
            packet_carrier: false,
            packet_formatter: false,
            fixed_packet: datagram_loan,
            root_only: false,
            packet_underlay: false,
            pooled: false,
            reentrant: true,
        }
    }
    fn pool(datagram_loan: bool) -> Self {
        Self {
            pooled: true,
            ..Self::stream(datagram_loan)
        }
    }
}

fn selection(config: &RuntimeConfig) -> BTreeMap<&str, Shape> {
    config
        .outbounds
        .iter()
        .filter_map(|input| shape(config, input).map(|shape| (input.tag(), shape)))
        .collect()
}

pub(super) fn packet_source_selected(config: &RuntimeConfig, consumer: &OutboundConfig) -> bool {
    match &consumer.protocol {
        P::Wireguard {
            outer_udp_proxy: Some(outer),
            ..
        } => {
            #[cfg(feature = "wireguard")]
            if !super::super::wireguard::lifecycle::outer_graph_acyclic(config, consumer) {
                return false;
            }
            outer_path_selected(config, &selection(config), outer)
        }
        P::Wireguard {
            outer_udp_proxy: None,
            ..
        } => true,
        _ => false,
    }
}

fn outer_path_selected(
    config: &RuntimeConfig,
    selected: &BTreeMap<&str, Shape>,
    outer: &str,
) -> bool {
    // Packet paths and Stream relays have different grammars. A Stream-only
    // donor may precede an Associated VLESS root; trailing SS codecs do
    // not open a Stream or a second Datagram session. Do not require a packet
    // role from those donors, or TCP/plugin ownership from a pure codec.
    let leaf = |name: &str| {
        config
            .outbounds
            .iter()
            .find(|input| input.tag() == name)
            .filter(|input| input.udp.enabled)
            .and_then(|_| selected.get(name))
    };
    if config.outbounds.iter().any(|input| input.tag() == outer) {
        return leaf(outer).is_some_and(|shape| shape.packet_carrier && shape.fixed_packet);
    }
    let Some(group) = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == outer)
    else {
        return false;
    };
    let OutboundGroupKind::Relay { proxies } = &group.group else {
        return false;
    };
    let Some(shapes) = proxies
        .iter()
        .map(|name| leaf(name))
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let Some(root) = shapes
        .iter()
        .rposition(|shape| !shape.packet_formatter)
        .or_else(|| (!shapes.is_empty()).then_some(0))
    else {
        return false;
    };
    if !shapes[root].packet_carrier || !shapes[root].fixed_packet {
        return false;
    }
    if root == 0 {
        // Includes a one-member Browser root. It gains no Associated grammar.
        return true;
    }
    let mut tail = None;
    for name in &proxies[..=root] {
        let Some(next) = path(config, selected, name, tail, &mut Vec::new()) else {
            return false;
        };
        tail = Some(next);
    }
    tail.is_some_and(|tail| tail.datagram)
}

#[derive(Clone)]
struct Tail {
    stream: bool,
    datagram: bool,
    serialized_pools: BTreeSet<String>,
}
fn path(
    config: &RuntimeConfig,
    selected: &BTreeMap<&str, Shape>,
    name: &str,
    previous: Option<Tail>,
    active: &mut Vec<String>,
) -> Option<Tail> {
    if let Some(input) = config.outbounds.iter().find(|input| input.tag() == name) {
        let shape = *selected.get(name)?;
        if previous.as_ref().is_some_and(|donor| {
            !donor.stream
                || shape.root_only
                || !input.dial.is_default()
                || (shape.packet_underlay && !donor.datagram)
                || donor.serialized_pools.contains(name)
        }) {
            return None;
        }
        let mut serialized_pools = previous
            .as_ref()
            .map(|tail| tail.serialized_pools.clone())
            .unwrap_or_default();
        // Re-entry uses strictly shorter binding lanes and the actual Host
        // lifecycle group. Owners without both remain non-reentrant here.
        if shape.pooled && !shape.reentrant {
            serialized_pools.insert(name.into());
        }
        return Some(Tail {
            serialized_pools,
            stream: shape.stream_loan,
            // A non-root SOCKS association has no existing UDP relay grammar.
            datagram: shape.datagram_loan
                && (!matches!(input.protocol, P::Socks5 { .. }) || previous.is_none()),
        });
    }
    if name == "direct" && previous.is_none() {
        return Some(Tail {
            stream: true,
            datagram: true,
            serialized_pools: BTreeSet::new(),
        });
    }
    if active.iter().any(|current| current == name) {
        return None;
    }
    let group = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == name)?;
    active.push(name.into());
    let result = (|| match &group.group {
        OutboundGroupKind::Relay { proxies } => {
            let mut tail = previous;
            for child in proxies {
                tail = Some(path(config, selected, child, tail, active)?);
            }
            tail
        }
        _ => {
            let mut tail = Tail {
                stream: true,
                datagram: true,
                serialized_pools: BTreeSet::new(),
            };
            for child in group.group.members() {
                let branch = path(config, selected, child, previous.clone(), active)?;
                tail.stream &= branch.stream;
                tail.datagram &= branch.datagram;
                tail.serialized_pools.extend(branch.serialized_pools);
            }
            Some(tail)
        }
    })();
    active.pop();
    result
}

fn shape(config: &RuntimeConfig, input: &OutboundConfig) -> Option<Shape> {
    let needs_dns = match &input.protocol {
        #[cfg(feature = "vless")]
        P::Vless { .. } => super::super::vless::component::mux::needs_ip_backend(input),
        #[cfg(feature = "vmess")]
        P::Vmess { .. } => super::super::vmess::component::mux::needs_ip_backend(input),
        _ => input
            .protocol
            .endpoint()
            .is_some_and(|(host, _)| zero_core::address::parse_ip_literal(host).is_none()),
    };
    if needs_dns && crate::adapters::node_dns::validate(config).is_err() {
        return None;
    }
    match &input.protocol {
        P::Socks5 { .. } => Some(Shape {
            packet_carrier: true,
            ..Shape::stream(true)
        }),
        #[cfg(feature = "vless")]
        P::VlessReverse => Some(Shape {
            root_only: true,
            ..Shape::stream(true)
        }),
        P::Direct => Some(Shape {
            root_only: true,
            ..Shape::stream(true)
        }),
        #[cfg(feature = "mieru")]
        P::Mieru { transport, .. } => Some(Shape {
            packet_underlay: *transport == zero_config::MieruTransport::Udp,
            ..Shape::pool(true)
        }),
        #[cfg(feature = "vless")]
        P::Vless {
            hysteria,
            quic,
            mkcp,
            split_http,
            ws,
            ..
        } => super::super::vless::component::relay_shape(input).map(|(datagram, pooled, _grpc)| {
            Shape {
                pooled,
                packet_carrier: true,
                fixed_packet: datagram
                    || super::super::vless::component::browser_source_datagram(input),
                // The actual cold catalog binds the upper pools, private
                // carriers and every cyclic relay participant into one retained
                // lifecycle. Per-binding preparation still uses its real owner.
                reentrant: true,
                packet_underlay: quic.is_some()
                    || mkcp.is_some()
                    || split_http
                        .as_ref()
                        .and_then(|http| http.download_settings.as_ref())
                        .is_some_and(|download| download.quic.is_some()),
                root_only: hysteria
                    .as_ref()
                    .is_some_and(|profile| profile.quic_parameters.udp_hop.is_some())
                    || ws.as_ref().is_some_and(|ws| ws.browser_dialer.is_some())
                    || split_http.as_ref().is_some_and(|http| {
                        http.browser_dialer.is_some()
                            || http.download_settings.as_ref().is_some_and(|download| {
                                download.split_http.browser_dialer.is_some()
                            })
                    }),
                ..Shape::stream(datagram)
            }
        }),
        #[cfg(feature = "vmess")]
        P::Vmess {
            mux_concurrency,
            grpc,
            ..
        } if super::super::vmess::component::mux::eligible(input) => Some(Shape {
            pooled: mux_concurrency.is_some() || grpc.is_some(),
            // Inline H2 belongs to the original MUX driver; the Node gRPC
            // role installs its record under epoch admission before borrowing.
            reentrant: true,
            ..Shape::stream(true)
        }),
        #[cfg(feature = "trojan")]
        P::Trojan {
            mux_concurrency, ..
        } if super::super::trojan::component::mux::eligible(input) => Some(Shape {
            pooled: mux_concurrency.is_some(),
            ..Shape::stream(true)
        }),
        #[cfg(feature = "hysteria2")]
        P::Hysteria2 { .. } => Some(Shape {
            reentrant: false,
            // One protocol anchor retains the real current wire/endpoint
            // handoff through port rotation. HY2 has no Associated grammar.
            packet_carrier: true,
            root_only: true,
            packet_underlay: true,
            ..Shape::pool(true)
        }),
        #[cfg(feature = "shadowsocks")]
        P::Shadowsocks { plugin, .. }
            if super::super::shadowsocks::component::registered_input(input) =>
        {
            Some(Shape {
                packet_carrier: plugin.as_ref().is_none_or(|plugin| !plugin.mode.udp()),
                packet_formatter: plugin.as_ref().is_none_or(|plugin| !plugin.mode.udp()),
                stream_loan: super::super::shadowsocks::component::stream_input(input),
                root_only: plugin.as_ref().is_some_and(|plugin| plugin.mode.tcp()),
                ..Shape::stream(
                    super::super::shadowsocks::component::native_input(input)
                        || super::super::shadowsocks::component::packet_input(input),
                )
            })
        }
        #[cfg(feature = "wireguard")]
        P::Wireguard { .. } => Some(Shape {
            root_only: true,
            ..Shape::stream(true)
        }),
        _ => None,
    }
}

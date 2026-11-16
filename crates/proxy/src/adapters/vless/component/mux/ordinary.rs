//! Compose only the existing ordinary role omitted by TCP MUX or XUDP.
//! The callable component retains its real carrier originals in the same Scope.
use super::*;
pub(super) fn compose(
    adapter: &VlessAdapter,
    input: &OutboundConfig,
    options: ::vless::component::mux::Options,
    grpc: Option<Arc<::vless::component::grpc::StreamOptions>>,
    resources: &mut ResourcePreparation<'_>,
    factory: Arc<background_pool::Factory>,
) -> Result<Options, RegistrationError> {
    let OutboundProtocolConfig::Vless {
        mux_concurrency,
        xudp_concurrency,
        reverse_tag,
        ..
    } = &input.protocol
    else {
        unreachable!("owning VLESS projection")
    };
    if mux_concurrency.is_some() && xudp_concurrency.is_some() && reverse_tag.is_none() {
        return Ok(Options::Mux(Arc::new(options)));
    }
    if let Some(grpc) = grpc {
        return Ok(Options::Mux(Arc::new(options.with_grpc_ordinary(grpc))));
    }
    // Removing only framing leaves the exact remote, security and dial facts.
    // Each ordinary connection gets its own original, never another MUX pool.
    let mut plain = input.clone();
    let OutboundProtocolConfig::Vless {
        mux_concurrency: tcp,
        xudp_concurrency: udp,
        ..
    } = &mut plain.protocol
    else {
        unreachable!("owning VLESS projection")
    };
    *tcp = None;
    *udp = None;
    let capacity = NonZeroUsize::new(
        resources
            .candidate()
            .map(|config| config.runtime.effective_native_stream_max_originals())
            .unwrap_or(4),
    )
    .filter(|limit| limit.get() <= 4096)
    .ok_or_else(|| ConfigError::InvalidRuntime("invalid ordinary original limit".into()))?;
    let ordinary =
        adapter.ordinary_options(&plain, resources, factory.network.clone(), capacity)?;
    let combined = ordinary.map_inner(|ordinary| options.with_ordinary(ordinary));
    let combined = if mux_concurrency.is_some() && xudp_concurrency.is_some() {
        combined.for_control_only()
    } else if mux_concurrency.is_none() {
        combined.for_stream_only()
    } else {
        combined.for_datagram_only()
    };
    let identity = stateless::Factory(super::super::material::MuxFactory {
        network: factory.network.clone(),
    })
    .describe(&combined)
    .static_identity;
    combined
        .inner_options()
        .remember_ordinary_material_identity(&identity)
        .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
    Ok(Options::Mixed(Arc::new(combined), factory.network.clone()))
}

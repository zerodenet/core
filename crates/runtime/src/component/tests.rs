mod bindings;
mod borrowing;
mod build_gate;
mod catalog;
mod dependencies;
mod generations;
mod host_admission;
mod host_cleanup;
mod native_process;
mod ownership;
mod preparation;
mod scoped;

mod shadowsocks_stream;

mod mieru_stream;

mod vless_grpc;

/// Follow the actual typed payloads, including std::io's boxed error payload.
pub(in crate::component) fn source<'a, T: std::error::Error + 'static>(
    error: &'a (dyn std::error::Error + 'static),
) -> &'a T {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(value) = error.downcast_ref::<T>() {
            return value;
        }
        if let Some(inner) = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            current = Some(inner);
        } else {
            current = error.source();
        }
    }
    panic!(
        "missing typed source {} in {error:?}",
        std::any::type_name::<T>()
    );
}

pub(super) fn default_factories() -> super::ComponentRegistry<zero_config::OutboundConfig> {
    let mut factories = super::ComponentRegistry::default();
    factories
        .register(
            zero_proxy::component::direct_descriptor(),
            zero_proxy::component::DirectFactory,
            zero_proxy::component::direct_options,
        )
        .unwrap();
    factories
}

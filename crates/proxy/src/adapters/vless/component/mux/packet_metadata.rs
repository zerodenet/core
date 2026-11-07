//! Facts from these already-prepared VLESS options, never another whole leaf.
use super::*;
pub(crate) fn declare_packet_path(
    input: &OutboundConfig,
    options: &Options,
    resources: &mut ResourcePreparation<'_>,
) -> Result<(), RegistrationError> {
    if resources.named_component(input.tag()).is_none() {
        // Explicit independent library preparation has no Kernel named Source.
        return Ok(());
    }
    // Read only the actual prepared owning options. No second claim/leaf,
    // private configuration reparse, CA/File read or new association occurs.
    let (endpoint, associated) = match options {
        Options::Ordinary(options, _) => options.inner_options().packet_path_metadata(),
        Options::Mux(options) => options.packet_path_metadata(),
        Options::Grpc(options) => options.packet_path_metadata(),
    };
    let description = Factory.describe(options);
    // The old whole VLESS packet path supported Base and Associated. A missing
    // standard Datagram role is still an actual producer gap, not permission
    // to execute a legacy path or create that role from these data facts.
    resources.declare_named_packet_path(
        input.tag(),
        description.static_identity.clone(),
        zero_runtime::component::PacketPathFacts::new(endpoint, associated),
    )?;
    Ok(())
}

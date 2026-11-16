//! Actual raw Browser Source and the existing MUX physical pool dependency.
use super::*;
pub(crate) mod lender;
pub(crate) mod pool;
pub(crate) fn prepare(
    input: &OutboundConfig,
    resources: &mut ResourcePreparation<'_>,
    registry: &registry::Registry,
    network: Arc<BackgroundNetwork>,
    originals: NonZeroUsize,
) -> Result<
    (
        zero_transport::browser_dialer::component::raw::RawCarrier,
        zero_core::component::ComponentId,
    ),
    RegistrationError,
> {
    let mut browser = super::prepare(input, resources, registry)?;
    Arc::get_mut(&mut browser)
        .expect("new cold Browser projection")
        .origins = originals;
    let mut identity = b"neutral.browser.raw-mux-source.v1".to_vec();
    browser.extend_identity(&mut identity);
    identity.extend_from_slice(&(input.tag.len() as u64).to_le_bytes());
    identity.extend_from_slice(input.tag.as_bytes());
    let carrier = registry.raw_carrier(&identity, browser.endpoint.clone());
    let raw = carrier::raw::RawOptions {
        browser: browser.clone(),
        carrier: carrier.clone(),
        identity: identity.into(),
    };
    let options = Arc::new(Options::new(raw, browser, input.tag.clone(), network));
    let dependency = resources.share(
        "transport.browser.actual-carrier.v1",
        Arc::new(Factory(carrier::raw::RawFactory)),
        options,
    )?;
    Ok((carrier, dependency))
}

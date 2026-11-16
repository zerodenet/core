//! Inline TCP transforms retain the actual Host's buffer/material credits.
//! The raw recipe stays cold; no carrier, task, cache or controller is invented.
use super::*;
pub(in crate::component) fn prepare(
    options: &Options,
    resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    hosted: bool,
    prepared: Option<zero_transport::finalmask::Profile>,
) -> Result<zero_transport::finalmask::Profile, ChannelError> {
    if let Some(profile) = prepared {
        let expected = options
            .masks
            .as_ref()
            .map(zero_transport::finalmask::Profile::settings_identity)
            .unwrap_or_else(|| zero_transport::finalmask::Profile::default().identity());
        let matches = profile.identity() == expected
            && (!hosted
                || profile.tcp().is_empty()
                || resources.carrier_buffer_budget().is_some_and(|book| {
                    resources
                        .packet_driver_metadata()
                        .is_ok_and(|memory| profile.same_owned_budget(&book, &memory))
                }));
        if !matches {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        return Ok(profile);
    }
    let Some(settings) = options
        .masks
        .as_ref()
        .filter(|settings| !settings.tcp.is_empty())
    else {
        return Ok(Default::default());
    };
    let fail = |source| {
        ChannelError::new(
            Stage::Prepare,
            Reason::InvalidConfig,
            zero_platform_tokio::contract::Impact::Component,
        )
        .with_source(source)
    };
    if !settings.udp.is_empty() {
        return Err(fail(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "ordinary TCP masks cannot acquire a UDP driver",
        )));
    }
    if hosted {
        let book = resources.carrier_buffer_budget().ok_or_else(|| {
            fail(std::io::Error::other(
                "actual TCP mask buffer book is absent",
            ))
        })?;
        zero_transport::finalmask::Profile::new_owned_from_ref(
            settings,
            book,
            resources.packet_driver_metadata()?,
        )
        .map_err(fail)
    } else {
        zero_transport::finalmask::Profile::new(settings.clone()).map_err(fail)
    }
}

//! Cold assembly of an already-supported carrier and its existing raw borrower.
use super::*;
pub(super) fn eligible(input: &OutboundConfig) -> bool {
    super::super::browser::carrier_eligible(input)
        || super::super::xhttp::carrier_eligible(input)
        || super::super::quic::carrier_eligible(input)
}
impl VlessAdapter {
    pub(in crate::adapters::vless::component) fn carrier_pool_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<crate::component::background::BackgroundNetwork>,
        originals: NonZeroUsize,
    ) -> Result<
        (
            ::vless::component::quic::TransportCarrier,
            Arc<::vless::component::grpc::StreamOptions>,
            zero_core::component::ComponentId,
        ),
        RegistrationError,
    > {
        let mut carrier = input.clone();
        let OutboundProtocolConfig::Vless {
            testpre,
            mux_concurrency,
            xudp_concurrency,
            ..
        } = &mut carrier.protocol
        else {
            unreachable!("owning VLESS projection")
        };
        *testpre = 0;
        *mux_concurrency = None;
        *xudp_concurrency = None;
        if super::super::browser::carrier_eligible(&carrier) {
            let (options, owner) =
                self.browser_pool_options(&carrier, resources, network, originals)?;
            return Ok((
                options.carrier().expect("actual Browser owner"),
                Arc::new(options.bind(owner)),
                owner,
            ));
        }
        if super::super::xhttp::carrier_eligible(&carrier) {
            let (options, owner) =
                self.xhttp_pool_options(&carrier, resources, network, originals)?;
            return Ok((
                options.carrier().expect("actual XHTTP owner"),
                Arc::new(options.bind(owner)),
                owner,
            ));
        }
        let (options, owner) = self.packet_pool_options(&carrier, resources, network, originals)?;
        Ok((
            options.carrier().expect("actual packet owner"),
            Arc::new(options.bind(owner)),
            owner,
        ))
    }
}

impl VlessAdapter {
    pub(super) fn mux_lower_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: &Arc<background_pool::Factory>,
        mut prepared: ::vless::component::mux::PreparedMux,
        limit: NonZeroUsize,
    ) -> Result<
        (
            ::vless::component::mux::PreparedMux,
            Option<(zero_core::component::ComponentId, lender::Source)>,
            Option<Arc<::vless::component::grpc::StreamOptions>>,
        ),
        RegistrationError,
    > {
        let mut ordinary_grpc = None;
        let lower = if matches!(&input.protocol,OutboundProtocolConfig::Vless{testpre,..} if *testpre!=0)
        {
            let mut carrier = input.clone();
            let OutboundProtocolConfig::Vless {
                mux_concurrency,
                xudp_concurrency,
                ..
            } = &mut carrier.protocol
            else {
                unreachable!("owning VLESS projection");
            };
            *mux_concurrency = None;
            *xudp_concurrency = None;
            let (preconnect, dependency) =
                self.preconnect_pool_options(&carrier, resources, factory.clone())?;
            if matches!(
                &input.protocol,
                OutboundProtocolConfig::Vless {
                    reverse_tag: Some(_),
                    ..
                } | OutboundProtocolConfig::Vless {
                    mux_concurrency: None,
                    ..
                } | OutboundProtocolConfig::Vless {
                    xudp_concurrency: None,
                    ..
                }
            ) {
                ordinary_grpc = Some(Arc::new(preconnect.bind(dependency)));
            }
            prepared = prepared.with_carrier_lender(dependency);
            Some((
                dependency,
                lender::Source::Preconnect {
                    endpoint: preconnect.pool_options().endpoint().clone(),
                    pool: preconnect.pool_handle(),
                },
            ))
        } else if matches!(
            &input.protocol,
            OutboundProtocolConfig::Vless { grpc: Some(_), .. }
        ) {
            let mut carrier = input.clone();
            let OutboundProtocolConfig::Vless {
                mux_concurrency,
                xudp_concurrency,
                ..
            } = &mut carrier.protocol
            else {
                unreachable!("owning protocol")
            };
            *mux_concurrency = None;
            *xudp_concurrency = None;
            let (grpc, dependency) = self.grpc_pool_options(&carrier, resources, factory)?;
            let options = Arc::new(grpc.pool_options().clone());
            if matches!(
                &input.protocol,
                OutboundProtocolConfig::Vless {
                    reverse_tag: Some(_),
                    ..
                } | OutboundProtocolConfig::Vless {
                    mux_concurrency: None,
                    ..
                } | OutboundProtocolConfig::Vless {
                    xudp_concurrency: None,
                    ..
                }
            ) {
                ordinary_grpc = Some(Arc::new(grpc.bind(dependency)));
            }
            prepared = prepared.with_carrier_lender(dependency);
            Some((dependency, lender::Source::Grpc(options)))
        } else if lower::eligible(input) {
            let mixed = matches!(
                &input.protocol,
                OutboundProtocolConfig::Vless {
                    reverse_tag: Some(_),
                    ..
                } | OutboundProtocolConfig::Vless {
                    mux_concurrency: None,
                    ..
                } | OutboundProtocolConfig::Vless {
                    xudp_concurrency: None,
                    ..
                }
            );
            let ordinary = if mixed {
                resources
                    .candidate()
                    .map(|config| config.runtime.effective_native_stream_max_originals())
                    .unwrap_or(4)
            } else {
                0
            };
            let originals = limit
                .get()
                .checked_add(ordinary)
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| {
                    ConfigError::InvalidRuntime("combined carrier original limit overflow".into())
                })?;
            let (carrier, ordinary, dependency) =
                self.carrier_pool_options(input, resources, factory.network.clone(), originals)?;
            if matches!(
                &input.protocol,
                OutboundProtocolConfig::Vless {
                    reverse_tag: Some(_),
                    ..
                } | OutboundProtocolConfig::Vless {
                    mux_concurrency: None,
                    ..
                } | OutboundProtocolConfig::Vless {
                    xudp_concurrency: None,
                    ..
                }
            ) {
                ordinary_grpc = Some(ordinary);
            }
            prepared = prepared.with_carrier_lender(dependency);
            Some((dependency, lender::Source::Carrier(carrier)))
        } else {
            None
        };
        Ok((prepared, lower, ordinary_grpc))
    }
}

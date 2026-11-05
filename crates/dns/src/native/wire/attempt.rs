//! Retry authority comes from the original controller, never an error kind.
use super::*;
use zero_platform_tokio::controlled_carrier::{loan::LoanClosed, CarrierOriginClosed};

pub(crate) struct Attempt {
    result: io::Result<Vec<u8>>,
    closed: Option<Proof>,
}
enum Proof {
    // The selected host consumed this attempt's real original ledger and
    // nested Scope joins. No native acquisition is asserted for an empty open.
    Service,
    #[cfg(feature = "doh")]
    Pool(crate::backends::doh::NativeCompletion),
    System(zero_platform_tokio::contract::scope::TaskJoined),
    FailedOpen(zero_platform_tokio::controlled_carrier::CarrierClosed),
    Carrier(LoanClosed, CarrierOriginClosed),
    #[cfg(feature = "doq")]
    Metadata(
        zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolClosed,
    ),
}
impl Attempt {
    #[cfg(feature = "doh")]
    pub(crate) fn pooled(
        result: io::Result<Vec<u8>>,
        settled: Option<crate::backends::doh::NativeCompletion>,
    ) -> Self {
        Self {
            result,
            closed: settled.map(Proof::Pool),
        }
    }

    pub(super) fn system(
        mut lookup: crate::NativeSystemLookup,
        query: &[u8],
        query_type: u16,
        family: zero_traits::AddressFamily,
    ) -> Self {
        lookup.result = lookup.result.and_then(|addresses| {
            let question = crate::message::parse_question(query)?;
            crate::family::filter_addresses(&question.domain, addresses, family)
        });
        let result = if !matches!(
            query_type,
            crate::message::TYPE_A | crate::message::TYPE_AAAA
        ) {
            Ok(crate::message::build_error_response(
                query,
                crate::message::RCODE_NOTIMP,
                false,
            ))
        } else {
            match lookup.result {
                Ok(addresses) => {
                    let addresses = addresses
                        .into_iter()
                        .filter(|address| {
                            matches!(
                                (query_type, address),
                                (crate::message::TYPE_A, zero_traits::IpAddress::V4(_))
                                    | (crate::message::TYPE_AAAA, zero_traits::IpAddress::V6(_))
                            )
                        })
                        .collect::<Vec<_>>();
                    Ok(crate::message::build_address_response(
                        query,
                        &addresses,
                        crate::message::DEFAULT_NEGATIVE_TTL_SECONDS,
                    ))
                }
                Err(source) if source.kind() == io::ErrorKind::NotFound => {
                    Ok(crate::message::build_error_response(
                        query,
                        crate::message::RCODE_NXDOMAIN,
                        false,
                    ))
                }
                Err(source) => Err(source),
            }
        };
        Self {
            result,
            closed: Some(Proof::System(lookup.joined)),
        }
    }
    pub(super) fn open_failed(source: io::Error) -> Self {
        let closed = source
            .get_ref()
            .and_then(|source| source.downcast_ref::<crate::native::NativeDnsOpenFailure>())
            .map(|failure| Proof::FailedOpen(failure.original_close().clone()));
        #[cfg(feature = "doq")]
        let closed = closed.or_else(|| {
            source
                .get_ref()
                .and_then(|source| source.downcast_ref::<crate::native::NativeDnsQuicOpenFailure>())
                .map(|failure| {
                    Proof::Metadata(failure.original().clone(), failure.original_close().clone())
                })
        });
        Self {
            result: Err(source),
            closed,
        }
    }
    pub(crate) fn blocked(source: io::Error) -> Self {
        Self {
            result: Err(source),
            closed: None,
        }
    }
    pub(crate) fn cleanup_failed(query: io::Error, close: io::Error) -> Self {
        Self::blocked(io::Error::new(
            close.kind(),
            CloseFailure {
                query: Some(query),
                close,
            },
        ))
    }
    pub(super) fn can_retry(&self) -> bool {
        self.closed.as_ref().is_some_and(|proof| match proof {
            Proof::Service => true,
            #[cfg(feature = "doh")]
            Proof::Pool(completion) => completion.is_settled(),
            Proof::System(joined) => joined.result().is_ok(),
            Proof::FailedOpen(closed) => closed.had_native_acquisition(),
            Proof::Carrier(logical, native) => {
                native.matches(logical.original()) && native.had_native_acquisition()
            }
            #[cfg(feature = "doq")]
            Proof::Metadata(original, native) => {
                native.matches(original) && native.had_native_acquisition()
            }
        })
    }
    pub(super) fn failed(&self) -> bool {
        self.result.is_err()
    }
    pub(super) fn is_truncated(&self, query: &[u8]) -> bool {
        self.can_retry()
            && self.result.as_ref().is_ok_and(|response| {
                crate::message::parse_response(query, response).is_ok_and(|parsed| parsed.truncated)
            })
    }
    pub(crate) fn into_parts(self) -> (io::Result<Vec<u8>>, bool) {
        let settled = self.can_retry();
        (self.result, settled)
    }
}
pub(super) async fn failed_service(
    provider: &dyn NativeDnsProvider,
    caller: TaskJoinWitness,
    end: Instant,
    opening: io::Error,
) -> Attempt {
    match provider.close_failed_stream_originals(caller, end).await {
        Ok(()) => Attempt {
            result: Err(opening),
            closed: Some(Proof::Service),
        },
        Err(close) => Attempt::cleanup_failed(opening, close),
    }
}
pub(super) async fn settle(
    owner: &mut LoanOwner,
    end: Instant,
    result: io::Result<Vec<u8>>,
) -> Attempt {
    let confirmation = async {
        let report = owner.close(end).await.map_err(io::Error::other)?;
        let logical = owner.closed().filter(|closed| closed.matches(owner));
        let native = owner
            .original()
            .closed()
            .filter(|closed| closed.matches(owner.original()) && closed.had_native_acquisition());
        match (report.is_complete(), logical, native) {
            (true, Some(logical), Some(native)) => Ok((logical, native)),
            _ => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "DNS original close remains unconfirmed",
            )),
        }
    }
    .await;
    match confirmation {
        Ok(closed) => Attempt {
            result,
            closed: Some(Proof::Carrier(closed.0, closed.1)),
        },
        Err(close) => Attempt::blocked(io::Error::new(
            close.kind(),
            CloseFailure {
                query: result.err(),
                close,
            },
        )),
    }
}
#[cfg(feature = "doq")]
pub(super) async fn settle_quic(
    owner: &mut dyn crate::native::NativeDnsQuicControl,
    end: Instant,
    result: io::Result<Vec<u8>>,
) -> Attempt {
    let original = owner.original().clone();
    match owner.close(end).await {
        Ok(closed) if closed.matches(&original) && closed.had_native_acquisition() => Attempt {
            result,
            closed: Some(Proof::Metadata(original, closed)),
        },
        closed => {
            let close = closed.err().unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DoQ original close remains unconfirmed",
                )
            });
            Attempt::blocked(io::Error::new(
                close.kind(),
                CloseFailure {
                    query: result.err(),
                    close,
                },
            ))
        }
    }
}
#[derive(Debug)]
struct CloseFailure {
    query: Option<io::Error>,
    close: io::Error,
}
impl std::fmt::Display for CloseFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(query) = &self.query {
            write!(formatter, "query failed: {query}; ")?;
        }
        write!(formatter, "original DNS close failed: {}", self.close)
    }
}
impl std::error::Error for CloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.close)
    }
}

#[cfg(test)]
#[path = "../tests/open_failure.rs"]
mod tests;

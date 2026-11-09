//! Immutable carrier masks materialized once per transport plan.
use super::*;
use std::sync::Arc;
#[derive(Clone, Default)]
pub struct PreparedMasks(
    pub(super) Arc<[Prepared]>,
    pub(super) Option<zero_traits::packet_metadata::PacketMetadataArc<MaterialCustody>>,
);
pub(super) struct MaterialCustody {
    pub(super) book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    pub(super) metadata: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    _bytes: zero_platform_tokio::controlled_carrier::CarrierBufferCredit,
}
pub(super) enum Prepared {
    Custom(Custom),
    Fragment(Fragment),
    Sudoku(super::super::sudoku::Profile),
}
impl PreparedMasks {
    pub fn new(masks: &[Mask]) -> io::Result<Self> {
        if masks.len() > 64 {
            return Err(invalid("too many TCP masks"));
        }
        let masks = masks
            .iter()
            .map(|mask| {
                if let Mask::Sudoku(settings) = mask {
                    return super::super::sudoku::Profile::new(settings).map(Prepared::Sudoku);
                }
                mask.validate()?;
                Ok(match mask {
                    Mask::Custom(config) => Prepared::Custom(config.clone()),
                    Mask::Fragment(config) => Prepared::Fragment(config.clone()),
                    Mask::Sudoku(_) => unreachable!(),
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self(masks.into(), None))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
pub async fn wrap_prepared(
    mut stream: TcpRelayStream,
    masks: &PreparedMasks,
    server: bool,
) -> io::Result<TcpRelayStream> {
    if let Some(actual) = &masks.1 {
        return masks
            .prepare_owned(actual.book.clone(), actual.metadata.clone())?
            .wrap(stream, server)
            .await;
    }
    for mask in masks.0.iter() {
        stream = match mask {
            Prepared::Custom(config) => {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    custom::handshake(&mut stream, config, server),
                )
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
                stream
            }
            Prepared::Fragment(config) => TcpRelayStream::from_client(stream::Stream::new(
                stream,
                stream::Transform::Fragment(config.clone()),
            )),
            Prepared::Sudoku(profile) => TcpRelayStream::from_client(stream::Stream::new(
                stream,
                stream::Transform::Sudoku(profile.clone(), server),
            )),
        };
    }
    Ok(stream)
}

impl PreparedMasks {
    /// Same immutable material, paid BEFORE the reference builder allocates it.
    /// The private plan does not expose raw table/recipe Arcs or Weak handles.
    pub fn new_owned(
        masks: &[Mask],
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        metadata: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> io::Result<Self> {
        use zero_traits::packet_metadata::PacketMetadataArc;
        if masks.len() > 64 {
            return Err(invalid("too many TCP masks"));
        }
        let header = metadata
            .reserve(PacketMetadataArc::<MaterialCustody>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let normalize = masks
            .iter()
            .try_fold(0usize, |sum, mask| {
                sum.checked_add(match mask {
                    Mask::Sudoku(settings) => {
                        super::super::sudoku::normalization_bound(settings).ok()?
                    }
                    _ => 0,
                })
            })
            .ok_or(io::ErrorKind::InvalidInput)?;
        let scratch = book.try_reserve(normalize)?;
        let mut bytes = masks
            .len()
            .checked_mul(3)
            .and_then(|v| v.checked_add(4))
            .and_then(|v| v.checked_mul(std::mem::size_of::<Prepared>()))
            .and_then(|v| v.checked_add(64))
            .ok_or(io::ErrorKind::InvalidInput)?;
        for mask in masks {
            let extra = match mask {
                Mask::Fragment(_) => 0,
                Mask::Sudoku(settings) => super::super::sudoku::owned_material_bound(settings)?,
                Mask::Custom(config) => {
                    let mut n = 0usize;
                    for group in [&config.clients, &config.servers, &config.errors] {
                        n = n
                            .checked_add(
                                group
                                    .len()
                                    .checked_mul(std::mem::size_of::<Vec<Item>>())
                                    .ok_or(io::ErrorKind::InvalidInput)?,
                            )
                            .ok_or(io::ErrorKind::InvalidInput)?;
                        for sequence in group {
                            n = n
                                .checked_add(
                                    sequence
                                        .len()
                                        .checked_mul(std::mem::size_of::<Item>())
                                        .ok_or(io::ErrorKind::InvalidInput)?,
                                )
                                .ok_or(io::ErrorKind::InvalidInput)?;
                            for item in sequence {
                                if let super::super::udp::Item::Bytes(value) = &item.content {
                                    n = n
                                        .checked_add(value.len())
                                        .ok_or(io::ErrorKind::InvalidInput)?;
                                }
                            }
                        }
                    }
                    n
                }
            };
            bytes = bytes
                .checked_add(extra)
                .ok_or(io::ErrorKind::InvalidInput)?;
        }
        let paid = book.try_reserve(bytes)?;
        let mut actual = Self::new(masks)?;
        let original = PacketMetadataArc::from_prepaid(
            MaterialCustody {
                book,
                metadata,
                _bytes: paid,
            },
            header,
        )
        .unwrap_or_else(|_| unreachable!("same paid immutable mask material header"));
        actual.1 = Some(original);
        drop(scratch);
        Ok(actual)
    }
}

impl PreparedMasks {
    pub(in crate::finalmask) fn same_owned_budget(
        &self,
        book: &zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        metadata: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> bool {
        self.1.as_ref().is_some_and(|actual| {
            actual.book.same_budget(book) && actual.metadata.same_root(metadata)
        })
    }
}

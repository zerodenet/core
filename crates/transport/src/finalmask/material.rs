//! Retained accepted UDP recipe data, distinct from every mutable codec copy.
use super::*;
use std::{io, mem::size_of, ops::Deref, sync::Arc};
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{CarrierBufferBudget, CarrierBufferCredit},
};
use zero_traits::packet_metadata::PacketMetadataArc;
#[derive(Clone)]
pub(super) enum UdpRecipe {
    Library(Arc<[udp::Mask]>),
    Owned(PacketMetadataArc<Original>),
}
pub(super) struct Original {
    values: Arc<[udp::Mask]>,
    _credit: CarrierBufferCredit,
}
impl From<Vec<udp::Mask>> for UdpRecipe {
    fn from(values: Vec<udp::Mask>) -> Self {
        Self::Library(values.into())
    }
}
impl Deref for UdpRecipe {
    type Target = [udp::Mask];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Library(values) => values,
            Self::Owned(original) => &original.values,
        }
    }
}
fn add(bytes: &mut usize, n: usize) -> io::Result<()> {
    *bytes = bytes.checked_add(n).ok_or(io::ErrorKind::InvalidInput)?;
    Ok(())
}
fn items_bytes(items: &Vec<udp::Item>) -> io::Result<usize> {
    let mut bytes = items
        .capacity()
        .checked_mul(size_of::<udp::Item>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    for item in items {
        if let udp::Item::Bytes(value) = item {
            add(&mut bytes, value.capacity())?;
        }
    }
    Ok(bytes)
}
impl UdpRecipe {
    pub(super) fn new_owned(
        values: Vec<udp::Mask>,
        book: &CarrierBufferBudget,
        metadata: &PacketDriverMetadataView,
    ) -> io::Result<Self> {
        let header = metadata
            .reserve(PacketMetadataArc::<Original>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let mut bytes = values
            .capacity()
            .checked_add(values.len())
            .and_then(|v| v.checked_mul(size_of::<udp::Mask>()))
            .and_then(|v| v.checked_add(2 * size_of::<usize>()))
            .ok_or(io::ErrorKind::InvalidInput)?;
        for value in &values {
            add(
                &mut bytes,
                match value {
                    udp::Mask::Xdns { domain } | udp::Mask::Dns { domain } => domain.capacity(),
                    udp::Mask::Xicmp { ip, .. } => ip.capacity(),
                    udp::Mask::Salamander { password } | udp::Mask::MkcpAes128Gcm { password } => {
                        password.capacity()
                    }
                    udp::Mask::Custom { client, server } => items_bytes(client)?
                        .checked_add(items_bytes(server)?)
                        .ok_or(io::ErrorKind::InvalidInput)?,
                    udp::Mask::Noise(settings) => {
                        let mut n = settings
                            .items
                            .capacity()
                            .checked_mul(size_of::<noise::Item>())
                            .ok_or(io::ErrorKind::InvalidInput)?;
                        for item in &settings.items {
                            add(&mut n, item.packet.capacity())?;
                        }
                        n
                    }
                    udp::Mask::Sudoku(settings) => {
                        let mut n = settings
                            .password
                            .capacity()
                            .checked_add(settings.ascii.capacity())
                            .and_then(|v| {
                                v.checked_add(
                                    settings.custom_tables.capacity() * size_of::<String>(),
                                )
                            })
                            .ok_or(io::ErrorKind::InvalidInput)?;
                        for value in &settings.custom_tables {
                            add(&mut n, value.capacity())?;
                        }
                        n
                    }
                    _ => 0,
                },
            )?;
        }
        let credit = book.try_reserve(bytes)?;
        PacketMetadataArc::from_prepaid(
            Original {
                values: values.into(),
                _credit: credit,
            },
            header,
        )
        .map(Self::Owned)
        .map_err(|(error, _, _)| io::Error::other(error))
    }
}

// Bound the concrete deep Clone of accepted raw settings before invoking it.
// Capacities of the SOURCE do not force new allocations: Vec/String Clone
// copies their initialized values. These are not ciphertext/profile tables.
pub(super) fn clone_bound(settings: &Settings) -> io::Result<usize> {
    fn items(items: &[udp::Item]) -> io::Result<usize> {
        let mut n = items
            .len()
            .checked_mul(size_of::<udp::Item>())
            .ok_or(io::ErrorKind::InvalidInput)?;
        for item in items {
            if let udp::Item::Bytes(bytes) = item {
                add(&mut n, bytes.len())?;
            }
        }
        Ok(n)
    }
    fn sudoku(settings: &sudoku::Settings) -> io::Result<usize> {
        let mut n = settings
            .password
            .len()
            .checked_add(settings.ascii.len())
            .and_then(|v| {
                v.checked_add(
                    settings
                        .custom_tables
                        .len()
                        .checked_mul(size_of::<String>())?,
                )
            })
            .ok_or(io::ErrorKind::InvalidInput)?;
        for text in &settings.custom_tables {
            add(&mut n, text.len())?;
        }
        Ok(n)
    }
    let mut n = settings
        .udp
        .len()
        .checked_mul(size_of::<udp::Mask>())
        .and_then(|v| v.checked_add(settings.tcp.len().checked_mul(size_of::<tcp::Mask>())?))
        .ok_or(io::ErrorKind::InvalidInput)?;
    for mask in &settings.udp {
        add(
            &mut n,
            match mask {
                udp::Mask::Xdns { domain } | udp::Mask::Dns { domain } => domain.len(),
                udp::Mask::Xicmp { ip, .. } => ip.len(),
                udp::Mask::MkcpAes128Gcm { password } | udp::Mask::Salamander { password } => {
                    password.len()
                }
                udp::Mask::Custom { client, server } => items(client)?
                    .checked_add(items(server)?)
                    .ok_or(io::ErrorKind::InvalidInput)?,
                udp::Mask::Sudoku(settings) => sudoku(settings)?,
                udp::Mask::Noise(settings) => {
                    let mut count = settings
                        .items
                        .len()
                        .checked_mul(size_of::<noise::Item>())
                        .ok_or(io::ErrorKind::InvalidInput)?;
                    for item in &settings.items {
                        add(&mut count, item.packet.len())?;
                    }
                    count
                }
                _ => 0,
            },
        )?;
    }
    for mask in &settings.tcp {
        add(
            &mut n,
            match mask {
                tcp::Mask::Fragment(_) => 0,
                tcp::Mask::Sudoku(settings) => sudoku(settings)?,
                tcp::Mask::Custom(config) => {
                    let mut count = 0usize;
                    for group in [&config.clients, &config.servers, &config.errors] {
                        add(
                            &mut count,
                            group
                                .len()
                                .checked_mul(size_of::<Vec<tcp::Item>>())
                                .ok_or(io::ErrorKind::InvalidInput)?,
                        )?;
                        for sequence in group {
                            add(
                                &mut count,
                                sequence
                                    .len()
                                    .checked_mul(size_of::<tcp::Item>())
                                    .ok_or(io::ErrorKind::InvalidInput)?,
                            )?;
                            for item in sequence {
                                if let udp::Item::Bytes(bytes) = &item.content {
                                    add(&mut count, bytes.len())?;
                                }
                            }
                        }
                    }
                    count
                }
            },
        )?;
    }
    Ok(n)
}

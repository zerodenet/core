//! Fill caller-owned, already admitted packet storage. These values are only
//! IP/TCP/UDP format facts; they grant no task, route, native IO or ownership.
use super::*;
use zero_traits::PacketDeriveError;
mod tcp;
mod udp;
pub use tcp::TcpPacketSpec;
pub use udp::UdpPacketSpec;
fn ip_header(source: IpAddr, destination: IpAddr) -> Result<usize, PacketDeriveError> {
    match (source, destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => Ok(20),
        (IpAddr::V6(_), IpAddr::V6(_)) => Ok(40),
        _ => Err(PacketDeriveError::LayoutMismatch),
    }
}
fn fill_ip(output: &mut [u8], source: IpAddr, destination: IpAddr, protocol: u8) {
    match (source, destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            output[0] = 0x45;
            let total = output.len() as u16;
            output[2..4].copy_from_slice(&total.to_be_bytes());
            output[8] = 64;
            output[9] = protocol;
            output[12..16].copy_from_slice(&source.octets());
            output[16..20].copy_from_slice(&destination.octets());
            let sum = checksum(&output[..20]);
            output[10..12].copy_from_slice(&sum.to_be_bytes());
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            output[0] = 0x60;
            let payload = (output.len() - 40) as u16;
            output[4..6].copy_from_slice(&payload.to_be_bytes());
            output[6] = protocol;
            output[7] = 64;
            output[8..24].copy_from_slice(&source.octets());
            output[24..40].copy_from_slice(&destination.octets());
        }
        _ => unreachable!("same validated IP family"),
    }
}

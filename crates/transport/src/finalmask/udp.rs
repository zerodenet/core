//! Datagram masks in configured wire order, with invalid packets rejected.
use rand::Rng;
use std::io;
mod crypto;
mod header;
#[derive(Debug, Clone)]
pub enum Mask {
    Xicmp {
        ip: String,
        id: u16,
    },
    Xdns {
        domain: String,
    },
    Noise(super::noise::Settings),
    Sudoku(super::sudoku::Settings),
    Dns {
        domain: String,
    },
    Dtls,
    Srtp,
    Utp,
    Wechat,
    Wireguard,
    Custom {
        client: Vec<Item>,
        server: Vec<Item>,
    },
    MkcpOriginal,
    MkcpAes128Gcm {
        password: String,
    },
    Salamander {
        password: String,
    },
}
#[derive(Debug, Clone)]
pub enum Item {
    Bytes(Vec<u8>),
    Random {
        length: usize,
        minimum: u8,
        maximum: u8,
    },
}
impl Item {
    pub(super) fn length(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Random { length, .. } => *length,
        }
    }
    pub(in crate::finalmask) fn append(&self, out: &mut Vec<u8>) {
        match self {
            Self::Bytes(bytes) => out.extend(bytes),
            Self::Random {
                length,
                minimum,
                maximum,
            } => {
                let mut rng = rand::rng();
                out.extend((0..*length).map(|_| rng.random_range(*minimum..=*maximum)));
            }
        }
    }
}
pub struct Codec {
    stages: Vec<Stage>,
}
enum Stage {
    Noise(super::noise::State),
    Header(header::Header),
    Crypto(crypto::Crypto),
    Sudoku(super::sudoku::Profile),
}
impl Codec {
    pub fn new(masks: &[Mask], server: bool) -> io::Result<Self> {
        if masks.len() > 64 {
            return Err(invalid("too many FinalMask stages"));
        }
        if masks
            .iter()
            .enumerate()
            .any(|(i, mask)| matches!(mask, Mask::Sudoku(_)) && i + 1 != masks.len())
        {
            return Err(invalid("Sudoku must be the innermost UDP mask"));
        }
        let stages = masks
            .iter()
            .map(|mask| match mask {
                Mask::Xdns { .. } | Mask::Xicmp { .. } => {
                    Err(invalid("datagram tunnel requires a stateful packet socket"))
                }
                Mask::Noise(settings) => super::noise::State::new(settings).map(Stage::Noise),
                Mask::Sudoku(settings) => super::sudoku::Profile::new(settings).map(Stage::Sudoku),
                Mask::MkcpOriginal | Mask::MkcpAes128Gcm { .. } | Mask::Salamander { .. } => {
                    crypto::Crypto::new(mask).map(Stage::Crypto)
                }
                _ => header::Header::new(mask, server).map(Stage::Header),
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self { stages })
    }
    pub fn encode(&mut self, packet: &[u8]) -> io::Result<Vec<u8>> {
        let mut out = packet.to_vec();
        for stage in self.stages.iter_mut().rev() {
            out = match stage {
                Stage::Noise(_) => out,
                Stage::Header(header) => header.encode(&out),
                Stage::Crypto(crypto) => crypto.encode(&out)?,
                Stage::Sudoku(profile) => profile.encode_datagram(&out),
            };
            // Xray's header manager has a fixed 4096-byte datagram buffer.
            if out.len()
                > if matches!(stage, Stage::Sudoku(_)) {
                    65507
                } else {
                    4096
                }
            {
                return Err(invalid("FinalMask datagram exceeds its wire buffer"));
            }
        }
        Ok(out)
    }
    pub fn encode_for(
        &mut self,
        peer: std::net::SocketAddr,
        packet: &[u8],
    ) -> io::Result<Vec<super::noise::Emission>> {
        use super::noise::Emission;
        let mut packets = vec![Emission {
            packet: packet.to_vec(),
            delay: std::time::Duration::ZERO,
            payload: true,
        }];
        for stage in self.stages.iter_mut().rev() {
            if let Stage::Noise(noise) = stage {
                let mut prelude = noise.before(peer)?;
                prelude.append(&mut packets);
                packets = prelude;
                continue;
            }
            let mut encoded = Vec::with_capacity(packets.len());
            for mut emission in packets {
                let wire = match stage {
                    Stage::Header(header) => Ok(header.encode(&emission.packet)),
                    Stage::Crypto(crypto) => crypto.encode(&emission.packet),
                    Stage::Sudoku(profile) => Ok(profile.encode_datagram(&emission.packet)),
                    Stage::Noise(_) => unreachable!(),
                };
                let limit = if matches!(stage, Stage::Sudoku(_)) {
                    65507
                } else {
                    4096
                };
                match wire {
                    Ok(wire) if wire.len() <= limit => {
                        emission.packet = wire;
                        encoded.push(emission);
                    }
                    Err(error) if emission.payload => return Err(error),
                    _ if emission.payload => {
                        return Err(invalid("FinalMask payload exceeds wire buffer"))
                    }
                    _ => {}
                }
            }
            packets = encoded;
        }
        Ok(packets)
    }
    pub fn finish_noise(&mut self, peer: std::net::SocketAddr) {
        for stage in &mut self.stages {
            if let Stage::Noise(noise) = stage {
                noise.finish(peer);
            }
        }
    }
    pub fn decode(&mut self, packet: &[u8]) -> io::Result<Vec<u8>> {
        let mut out = packet.to_vec();
        for stage in &mut self.stages {
            out = match stage {
                Stage::Noise(_) => out,
                Stage::Header(header) => header.decode(&out)?,
                Stage::Crypto(crypto) => crypto.decode(&out)?,
                Stage::Sudoku(profile) => profile.decode_datagram(&out)?,
            };
        }
        Ok(out)
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
#[cfg(test)]
#[path = "../../tests/finalmask/udp.rs"]
mod tests;

pub fn validate(masks: &[Mask]) -> io::Result<()> {
    if masks.len() > 64 {
        return Err(invalid("too many FinalMask stages"));
    }
    for (i, mask) in masks.iter().enumerate() {
        if matches!(mask, Mask::Sudoku(_)) && i + 1 != masks.len() {
            return Err(invalid("Sudoku must be the innermost UDP mask"));
        }
        if let Mask::Xicmp { ip, .. } = mask {
            if i != 0 {
                return Err(invalid("XICMP must be the outermost UDP mask"));
            }
            super::xicmp::address(ip)?;
        }
        if let Mask::Xdns { domain } = mask {
            super::xdns::validate(domain)?;
        }
    }
    for ordinary in masks.split(|mask| matches!(mask, Mask::Xdns { .. } | Mask::Xicmp { .. })) {
        Codec::new(ordinary, false)?;
        Codec::new(ordinary, true)?;
    }
    Ok(())
}

//! The complete wire recipe and its actual Host promises precede acquisition.
use super::*;
use crate::finalmask::{memory::Memory, udp::Codec};
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView, controlled_carrier::CarrierBufferBudget,
};
pub struct PreparedMasks {
    book: CarrierBufferBudget,
    stages: Vec<Stage>,
    masked: bool,
    server: bool,
}
enum Stage {
    Ordinary {
        codec: Codec,
        noise: Option<usize>,
        ordinal: usize,
        memory: Memory,
    },
    Dns {
        domain: String,
        ordinal: u8,
        memory: Memory,
    },
    Icmp {
        id: u16,
        ordinal: u8,
        memory: Memory,
    },
}
impl PreparedMasks {
    pub fn new(
        masks: &[Mask],
        server: bool,
        book: CarrierBufferBudget,
        metadata: PacketDriverMetadataView,
    ) -> io::Result<Self> {
        validate(masks)?;
        let mut stages = Vec::new();
        let mut start = 0;
        while start < masks.len() {
            let ordinal = start;
            let stage = match &masks[start] {
                Mask::Xdns { domain } => {
                    start += 1;
                    Stage::Dns {
                        domain: domain.clone(),
                        ordinal: ordinal as u8,
                        memory: Memory::prepare(
                            book.clone(),
                            metadata.clone(),
                            16 * (934 + 4096),
                            512 * 1024,
                        )?,
                    }
                }
                Mask::Xicmp { id, .. } => {
                    start += 1;
                    Stage::Icmp {
                        id: *id,
                        ordinal: ordinal as u8,
                        memory: Memory::prepare(
                            book.clone(),
                            metadata.clone(),
                            16 * (65536 + 4096),
                            3 * 65536,
                        )?,
                    }
                }
                _ => {
                    let end = masks[start..]
                        .iter()
                        .position(|m| matches!(m, Mask::Xdns { .. } | Mask::Xicmp { .. }))
                        .map_or(masks.len(), |count| start + count);
                    let mut codec = Codec::new(&masks[start..end], server)?;
                    let peak = codec.working_peak(65536)?;
                    let memory = Memory::prepare(book.clone(), metadata.clone(), peak, peak)?;
                    codec.install_memory(memory.clone())?;
                    let noise = masks[start..end]
                        .iter()
                        .position(|m| matches!(m, Mask::Noise(_)));
                    start = end;
                    Stage::Ordinary {
                        codec,
                        noise,
                        ordinal,
                        memory,
                    }
                }
            };
            stages.push(stage);
        }
        Ok(Self {
            book,
            stages,
            masked: !masks.is_empty(),
            server,
        })
    }
    pub(crate) fn matches_book(&self, actual: &CarrierBufferBudget) -> bool {
        self.book.same_budget(actual)
    }
    pub(crate) fn masked(&self) -> bool {
        self.masked
    }
    pub(crate) fn prepare(
        self,
        mut inner: Arc<dyn AsyncUdpSocket>,
        mut icmp: Option<ControlledIcmpSocket>,
    ) -> io::Result<Prepared> {
        let mut drivers = Vec::new();
        for stage in self.stages {
            inner = match stage {
                Stage::Ordinary {
                    codec,
                    noise,
                    ordinal,
                    memory,
                } => Socket::prepare_codec(inner, codec, noise, memory, &mut drivers, ordinal)?,
                Stage::Dns {
                    domain,
                    ordinal,
                    memory,
                } => {
                    let (socket, work) =
                        super::super::xdns::prepare_owned(inner, &domain, self.server, memory)?;
                    drivers.push(Driver {
                        kind: DriverKind::Xdns,
                        ordinal,
                        work,
                    });
                    socket
                }
                Stage::Icmp {
                    id,
                    ordinal,
                    memory,
                } => {
                    let raw = icmp
                        .take()
                        .ok_or_else(|| io::Error::other("original ICMP grant missing"))?;
                    let (socket, work) =
                        super::super::xicmp::prepare_owned(inner, raw, id, self.server, memory)?;
                    drivers.push(Driver {
                        kind: DriverKind::Xicmp,
                        ordinal,
                        work,
                    });
                    socket
                }
            };
        }
        if icmp.is_some() {
            return Err(io::Error::other("unexpected original ICMP grant"));
        }
        Ok(Prepared {
            socket: inner,
            drivers,
        })
    }
}

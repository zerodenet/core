//! Each admitted return carries its own concrete fragment storage reservation.
use super::*;
use zero_traits::PacketDeriveError;
#[derive(Debug)]
pub(in crate::runtime::raw_ip::device::returns) struct ReturnAdmissionFailure {
    pub(in crate::runtime::raw_ip::device::returns) original: PacketBuffer,
    pub(in crate::runtime::raw_ip::device::returns) cause: Option<PacketDeriveError>,
}
pub(super) struct Prepared {
    lengths: Vec<usize>,
    credits: Vec<Credit>,
}
impl ReturnBudget {
    pub(in crate::runtime::raw_ip::device::returns) fn retain_for_return(
        &self,
        packet: PacketBuffer,
        mtu: u16,
    ) -> Result<PacketBuffer, ReturnAdmissionFailure> {
        let lengths = match zero_stack::packet::fragment_packet_lengths(&packet, usize::from(mtu)) {
            Ok(lengths) => lengths,
            Err(cause) => {
                return Err(ReturnAdmissionFailure {
                    original: packet,
                    cause: Some(cause),
                })
            }
        };
        self.retain_planned(packet, lengths)
    }
    fn retain_planned(
        &self,
        packet: PacketBuffer,
        lengths: Vec<usize>,
    ) -> Result<PacketBuffer, ReturnAdmissionFailure> {
        let Some(frames) = lengths.len().checked_add(1) else {
            return Err(ReturnAdmissionFailure {
                original: packet,
                cause: Some(PacketDeriveError::Overflow),
            });
        };
        let Some(bytes) = lengths
            .iter()
            .try_fold(packet.allocation_bytes(), |sum, length| {
                sum.checked_add(*length)
            })
        else {
            return Err(ReturnAdmissionFailure {
                original: packet,
                cause: Some(PacketDeriveError::Overflow),
            });
        };
        let mut occupied = self.0.occupied.lock().unwrap_or_else(|e| e.into_inner());
        if frames > self.0.limits.0 || bytes > self.0.limits.1 {
            return Err(ReturnAdmissionFailure {
                original: packet,
                cause: Some(PacketDeriveError::Capacity),
            });
        }
        if frames > self.0.limits.0.saturating_sub(occupied.0)
            || bytes > self.0.limits.1.saturating_sub(occupied.1)
        {
            return Err(ReturnAdmissionFailure {
                original: packet,
                cause: None,
            });
        }
        let allocations = std::iter::once(packet.allocation_bytes())
            .chain(lengths.iter().copied())
            .collect::<Vec<_>>();
        let mut totals = match self.0.totals.reserve_returns(&allocations) {
            Ok(credits) => credits.into_iter(),
            Err(source) => {
                return Err(ReturnAdmissionFailure {
                    original: packet,
                    cause: match source {
                        zero_tun::packet_memory::PacketMemoryError::Busy
                        | zero_tun::packet_memory::PacketMemoryError::Paused => None,
                        zero_tun::packet_memory::PacketMemoryError::Overflow => {
                            Some(PacketDeriveError::Overflow)
                        }
                        _ => Some(PacketDeriveError::Capacity),
                    },
                });
            }
        };
        occupied.0 += frames;
        occupied.1 += bytes;
        drop(occupied);
        let original = Credit {
            budget: self.clone(),
            bytes: packet.allocation_bytes(),
            _total: totals.next().expect("same original allocation credit"),
        };
        let credits = lengths
            .iter()
            .map(|length| Credit {
                budget: self.clone(),
                bytes: *length,
                _total: totals
                    .next()
                    .expect("same prepaid derived allocation credit"),
            })
            .collect();
        Ok(PacketBuffer::from_owner(Charged {
            packet,
            credit: original,
            derived: Mutex::new(Some(Prepared { lengths, credits })),
            derivation_reserved: true,
        }))
    }
    /// The original producing driver owns this pending admission. Once this
    /// succeeds, its queue entry never waits for additional transformation credit.
    pub(in crate::runtime::raw_ip::device::returns) async fn retain_for_return_wait(
        &self,
        mut packet: PacketBuffer,
        mtu: u16,
    ) -> Result<PacketBuffer, ReturnAdmissionFailure> {
        loop {
            let local = self.0.changed.notified();
            let total = self.0.totals.notified();
            tokio::pin!(local);
            tokio::pin!(total);
            local.as_mut().enable();
            total.as_mut().enable();
            match self.retain_for_return(packet, mtu) {
                Ok(packet) => return Ok(packet),
                Err(ReturnAdmissionFailure {
                    original,
                    cause: Some(cause),
                }) => {
                    return Err(ReturnAdmissionFailure {
                        original,
                        cause: Some(cause),
                    })
                }
                Err(ReturnAdmissionFailure {
                    original,
                    cause: None,
                }) => packet = original,
            }
            tokio::select! {_=local=>{},_=total=>{}}
        }
    }
}
pub(super) fn claim(
    owner: &Charged,
    lengths: &[usize],
) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
    if !owner.derivation_reserved {
        return Err(PacketDeriveError::Unreserved);
    }
    let prepared = {
        let mut prepared = owner.derived.lock().unwrap_or_else(|e| e.into_inner());
        let plan = prepared.as_ref().ok_or(PacketDeriveError::AlreadyDerived)?;
        if plan.lengths != lengths {
            return Err(PacketDeriveError::LayoutMismatch);
        }
        prepared.take().expect("same original reservation")
    };
    Ok(prepared
        .lengths
        .into_iter()
        .zip(prepared.credits)
        .map(|(length, credit)| {
            PacketBuffer::from_owner(Storage {
                bytes: vec![0; length],
                _credit: credit,
            })
        })
        .collect())
}
struct Storage {
    bytes: Vec<u8>,
    _credit: Credit,
}
impl AsRef<[u8]> for Storage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Storage {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl PacketStorage for Storage {
    fn allocation_bytes(&self) -> usize {
        self.bytes.capacity()
    }
}

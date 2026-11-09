//! Unique per-carrier preparation over its original Host books. No worker,
//! detached task, Host, pool or native/close/ACK authority is created here.
use super::*;
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{
        CarrierBufferBudget, CarrierBufferCredit, CarrierBufferFrameCredit,
        CarrierBufferWorkingLane, CarrierBufferWorkingShape,
    },
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
mod driver;
mod memory;
mod queue;
mod stream;
mod wake;
pub struct OwnedMasks {
    plan: PreparedMasks,
    stages: Vec<Stage>,
    _headers: PacketMetadataCredit,
}
enum Stage {
    Custom(CarrierBufferCredit),
    Transform(memory::Prepared),
}
impl PreparedMasks {
    /// Called before acquiring the real carrier. Prepares the same immutable
    /// mask recipe and complete mutable working peak on the actual Host books.
    pub fn prepare_owned(
        &self,
        book: CarrierBufferBudget,
        metadata: PacketDriverMetadataView,
    ) -> io::Result<OwnedMasks> {
        let layout =
            PacketMetadataLayout::array::<Stage>(self.0.len()).map_err(io::Error::other)?;
        let headers = metadata.reserve(layout).map_err(io::Error::other)?;
        let slots = Box::<[Stage]>::new_uninit_slice(self.0.len());
        let pointer = Box::into_raw(slots) as *mut Stage;
        // The exact paid array begins empty; only initialized stages are live.
        let mut stages = unsafe { Vec::from_raw_parts(pointer, 0, self.0.len()) };
        for mask in self.0.iter() {
            let stage = match mask {
                prepared::Prepared::Custom(config) => {
                    let peak = config
                        .clients
                        .iter()
                        .chain(config.servers.iter())
                        .chain(config.errors.iter())
                        .map(|sequence| {
                            sequence
                                .iter()
                                .map(|item| item.content.length())
                                .sum::<usize>()
                        })
                        .max()
                        .unwrap_or(0);
                    Stage::Custom(
                        book.try_reserve(
                            peak.max(8)
                                .checked_mul(4)
                                .ok_or(io::ErrorKind::InvalidInput)?,
                        )?,
                    )
                }
                prepared::Prepared::Fragment(_) => Stage::Transform(memory::Prepared::new(
                    &book,
                    &metadata,
                    40 * memory::WRITE + 4096,
                )?),
                prepared::Prepared::Sudoku(_) => Stage::Transform(memory::Prepared::new(
                    &book,
                    &metadata,
                    18 * memory::WRITE + 4096,
                )?),
            };
            stages.push(stage);
        }
        Ok(OwnedMasks {
            plan: self.clone(),
            stages,
            _headers: headers,
        })
    }
}
impl OwnedMasks {
    pub async fn wrap(self, mut raw: TcpRelayStream, server: bool) -> io::Result<TcpRelayStream> {
        let plan = self.plan;
        let mut stages = StageIter {
            values: self.stages.into_iter(),
            _headers: self._headers,
        };
        // Keep the paid Vec allocation and its unused stages through the whole
        // wrap. An exhausted IntoIter does not mean the real array was freed.
        for mask in plan.0.iter() {
            let stage = stages.values.next().expect("same immutable mask recipe");
            raw = match (mask, stage) {
                (prepared::Prepared::Custom(config), Stage::Custom(_credit)) => {
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        custom::handshake(&mut raw, config, server),
                    )
                    .await
                    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
                    raw
                }
                (prepared::Prepared::Fragment(config), Stage::Transform(memory)) => {
                    stream::Stream::wrap(
                        raw,
                        super::stream::Transform::Fragment(config.clone()),
                        memory,
                        plan.clone(),
                    )?
                }
                (prepared::Prepared::Sudoku(profile), Stage::Transform(memory)) => {
                    stream::Stream::wrap(
                        raw,
                        super::stream::Transform::Sudoku(profile.clone(), server),
                        memory,
                        plan.clone(),
                    )?
                }
                _ => unreachable!("same prepared immutable mask shape"),
            };
        }
        drop(stages);
        Ok(raw)
    }
}

struct StageIter {
    values: std::vec::IntoIter<Stage>,
    _headers: PacketMetadataCredit,
}

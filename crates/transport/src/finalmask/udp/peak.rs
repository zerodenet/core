//! Conservative codec backing peaks, including simultaneous old/new Vecs.
use super::*;
pub(super) enum StagePeak {
    Header(usize),
    Crypto,
    Sudoku,
    Noise { bytes: usize, count: usize },
}
pub(super) fn prepare(masks: &[Mask]) -> io::Result<Vec<StagePeak>> {
    masks
        .iter()
        .map(|mask| {
            Ok(match mask {
                Mask::Noise(settings) => StagePeak::Noise {
                    bytes: settings
                        .items
                        .iter()
                        .try_fold(0usize, |sum, item| {
                            sum.checked_add(
                                item.packet
                                    .len()
                                    .max(usize::try_from(item.random_length.maximum).ok()?),
                            )
                        })
                        .ok_or(io::ErrorKind::InvalidInput)?,
                    count: settings.items.len(),
                },
                Mask::Sudoku(_) => StagePeak::Sudoku,
                Mask::MkcpOriginal | Mask::MkcpAes128Gcm { .. } | Mask::Salamander { .. } => {
                    StagePeak::Crypto
                }
                Mask::Custom { client, server } => StagePeak::Header(
                    client
                        .iter()
                        .try_fold(0usize, |sum, item| sum.checked_add(item.length()))
                        .ok_or(io::ErrorKind::InvalidInput)?
                        .max(
                            server
                                .iter()
                                .try_fold(0usize, |sum, item| sum.checked_add(item.length()))
                                .ok_or(io::ErrorKind::InvalidInput)?,
                        ),
                ),
                Mask::Dns { domain } => StagePeak::Header(
                    domain
                        .len()
                        .max(13)
                        .checked_add(20)
                        .ok_or(io::ErrorKind::InvalidInput)?,
                ),
                Mask::Wireguard | Mask::Utp | Mask::Srtp => StagePeak::Header(4),
                Mask::Wechat | Mask::Dtls => StagePeak::Header(13),
                Mask::Xicmp { .. } | Mask::Xdns { .. } => {
                    return Err(invalid("stateful mask has its own original codec"))
                }
            })
        })
        .collect()
}
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
pub(super) fn bytes(stages: &[StagePeak], input: usize) -> io::Result<usize> {
    let (mut live, mut count, mut peak) = (input, 1usize, mul(input, 2)?);
    for stage in stages.iter().rev() {
        let next = match stage {
            StagePeak::Noise { bytes, count: n } => {
                count = add(count, *n)?;
                add(live, *bytes)?
            }
            StagePeak::Header(header) => {
                let output = add(live, mul(count, *header)?)?;
                peak = peak.max(add(input, mul(output, 4)?)?);
                mul(output.min(mul(count, 4096)?), 2)?
            }
            StagePeak::Crypto => {
                let output = add(live, mul(count, 64)?)?;
                peak = peak.max(add(input, mul(output, 8)?)?);
                mul(output.min(mul(count, 4096)?), 2)?
            }
            StagePeak::Sudoku => {
                peak = peak.max(add(input, add(mul(live, 20)?, 64)?)?);
                mul(add(mul(live, 12)?, count)?.min(mul(count, 65507)?), 3)?
            }
        };
        let descriptors = mul(
            mul(count, 8)?,
            std::mem::size_of::<super::super::noise::Emission>(),
        )?;
        peak = peak.max(add(add(input, add(live, next)?)?, descriptors)?);
        live = next;
    }
    // Empty codec still copies the original and its queued caller backing.
    Ok(peak.max(mul(input, 2)?).max(1))
}

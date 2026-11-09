//! Actual private codec/material backing bounds; no IO or key state is issued.
use super::*;
use std::mem::size_of;
use zero_platform_tokio::controlled_carrier::CarrierBufferBudget;
fn add(total: &mut usize, n: usize) -> io::Result<()> {
    *total = total.checked_add(n).ok_or(io::ErrorKind::InvalidInput)?;
    Ok(())
}
fn item_bytes(items: &[Item]) -> io::Result<usize> {
    let mut n = items
        .len()
        .checked_mul(size_of::<Item>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    for item in items {
        if let Item::Bytes(bytes) = item {
            add(&mut n, bytes.len())?;
        }
    }
    Ok(n)
}
pub(super) fn normalization_bound(masks: &[Mask]) -> io::Result<usize> {
    masks
        .iter()
        .try_fold(0usize, |sum, mask| {
            sum.checked_add(match mask {
                Mask::Sudoku(settings) => {
                    super::super::sudoku::normalization_bound(settings).ok()?
                }
                _ => 0,
            })
        })
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
pub(super) fn bound(masks: &[Mask]) -> io::Result<usize> {
    let mut bytes = masks
        .len()
        .checked_mul(3 * size_of::<Stage>() + 3 * size_of::<peak::StagePeak>())
        .and_then(|v| v.checked_add(4 * (size_of::<Stage>() + size_of::<peak::StagePeak>())))
        .ok_or(io::ErrorKind::InvalidInput)?;
    for mask in masks {
        add(
            &mut bytes,
            match mask {
                Mask::Sudoku(settings) => super::super::sudoku::owned_material_bound(settings)?,
                Mask::Noise(settings) => {
                    let mut n = settings
                        .items
                        .len()
                        .checked_mul(size_of::<super::super::noise::Item>())
                        .ok_or(io::ErrorKind::InvalidInput)?;
                    for item in &settings.items {
                        add(&mut n, item.packet.len())?;
                    }
                    n
                }
                Mask::Custom { client, server } => item_bytes(client)?
                    .checked_add(item_bytes(server)?)
                    .ok_or(io::ErrorKind::InvalidInput)?,
                Mask::MkcpAes128Gcm { .. } => size_of::<ring::aead::LessSafeKey>(),
                Mask::Salamander { password } => password.len(),
                Mask::Dns { domain } => domain
                    .len()
                    .max(13)
                    .checked_add(272)
                    .and_then(|v| v.checked_mul(3))
                    .ok_or(io::ErrorKind::InvalidInput)?,
                Mask::Wireguard | Mask::Utp => 16,
                _ => 0,
            },
        )?;
    }
    Ok(bytes)
}
impl Codec {
    pub(in crate::finalmask) fn new_owned(
        masks: &[Mask],
        server: bool,
        book: &CarrierBufferBudget,
    ) -> io::Result<Self> {
        if masks.len() > 64 {
            return Err(invalid("too many FinalMask stages"));
        }
        let scratch = book.try_reserve(normalization_bound(masks)?)?;
        let credit = book.try_reserve(bound(masks)?)?;
        let mut actual = Self::new(masks, server)?;
        actual._material_credit = Some(credit);
        drop(scratch);
        Ok(actual)
    }
}
pub(crate) fn validate_owned(masks: &[Mask], book: &CarrierBufferBudget) -> io::Result<()> {
    let scratch = book.try_reserve(normalization_bound(masks)?)?;
    let credit = book.try_reserve(bound(masks)?)?;
    let result = validate(masks);
    drop(credit);
    drop(scratch);
    result
}

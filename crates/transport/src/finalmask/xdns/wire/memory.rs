//! Measure the actual DNS tree before materialising any compressed name.
//! Vector peaks include old and new arrays; compression keys borrow names.
use super::*;
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(invalid)
}
fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b).ok_or_else(invalid)
}
fn array<T>(n: usize) -> io::Result<usize> {
    if n == 0 {
        return Ok(0);
    }
    // Vec::push's minimum four rows, geometric growth and simultaneous old/new.
    mul(
        mul(
            n.max(4).checked_next_power_of_two().ok_or_else(invalid)?,
            std::mem::size_of::<T>(),
        )?,
        3,
    )
}
pub(in crate::finalmask::xdns) fn storage_peak(bytes: &[u8]) -> io::Result<usize> {
    let mut p = Parser { bytes, at: 0 };
    p.take(4)?;
    let counts = [p.u16()?, p.u16()?, p.u16()?, p.u16()?];
    if counts.iter().map(|n| usize::from(*n)).sum::<usize>() > 4096 {
        return Err(invalid());
    }
    let mut total = array::<Question>(usize::from(counts[0]))?;
    let mut suffixes = 0usize;
    for (group, count) in counts.into_iter().enumerate() {
        if group != 0 {
            total = add(total, array::<Record>(usize::from(count))?)?;
        }
        for _ in 0..count {
            let (labels, label_bytes) = name(&mut p)?;
            total = add(total, add(array::<Vec<u8>>(labels)?, label_bytes)?)?;
            suffixes = add(suffixes, labels)?;
            p.take(4)?;
            if group != 0 {
                p.take(4)?;
                let size = usize::from(p.u16()?);
                total = add(total, p.take(size)?.len())?;
            }
        }
    }
    if p.at != bytes.len() {
        return Err(invalid());
    }
    // Original tree, retained response and answer-name clone can coexist with
    // encoding and TXT/base32 temporary arrays. This follows concrete tree
    // rows/labels; compressed wire size alone is never a decoded-tree charge.
    total = mul(total, 4)?;
    total = add(total, array::<(&[Vec<u8>], u16)>(mul(suffixes, 2)?)?)?;
    add(total, add(mul(bytes.len(), 8)?, 4 * 65536 + 64)?)
}
fn name(p: &mut Parser<'_>) -> io::Result<(usize, usize)> {
    let (mut labels, mut size, mut pointers) = (0usize, 0usize, 0usize);
    let mut resume = None;
    loop {
        let length = p.take(1)?[0];
        match length & 0xc0 {
            0 => {
                if length == 0 {
                    break;
                }
                labels = add(labels, 1)?;
                size = add(size, p.take(usize::from(length))?.len())?;
                if labels > 127 {
                    return Err(invalid());
                }
            }
            0xc0 => {
                let offset = (usize::from(length & 0x3f) << 8) | usize::from(p.take(1)?[0]);
                pointers += 1;
                if pointers > 10 {
                    return Err(invalid());
                }
                resume.get_or_insert(p.at);
                p.at = offset;
            }
            _ => return Err(invalid()),
        }
    }
    if let Some(at) = resume {
        p.at = at;
    }
    if add(add(size, labels)?, 1)? > 255 {
        return Err(invalid());
    }
    Ok((labels, size))
}

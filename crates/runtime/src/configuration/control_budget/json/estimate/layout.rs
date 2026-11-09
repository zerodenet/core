use super::*;
pub(crate) fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(capacity)
}
pub(crate) fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b).ok_or_else(capacity)
}
pub(crate) fn array(n: usize, item: usize) -> io::Result<usize> {
    if n == 0 {
        return Ok(0);
    }
    // Also covers the overlap of the old/new geometrically grown allocation.
    let slots = n.checked_next_power_of_two().ok_or_else(capacity)?.max(4);
    mul(mul(slots, 2)?, item)
}
pub(crate) fn typed_array(n: usize, item: usize) -> io::Result<usize> {
    if n == 0 {
        Ok(0)
    } else {
        mul(add(mul(n, 4)?, 8)?, item)
    }
}
pub(crate) fn map(n: usize) -> io::Result<usize> {
    if n == 0 {
        return Ok(0);
    }
    let nodes = if n <= 11 {
        1
    } else {
        let height = usize::BITS as usize - n.leading_zeros() as usize;
        add(add(1, n / 5)?, add(height, 2)?)?
    };
    let cell = add(
        mul(
            11,
            add(
                std::mem::size_of::<String>(),
                std::mem::size_of::<serde_json::Value>(),
            )?,
        )?,
        mul(32, std::mem::size_of::<usize>())?,
    )?;
    mul(nodes, cell)
}
pub(crate) fn scratch(bytes: &[u8]) -> io::Result<usize> {
    let (mut quoted, mut escaped, mut start, mut span, mut depth, mut high, mut number) =
        (false, false, 0, 0usize, 0usize, 0usize, 0usize);
    let mut has_escape = false;
    for (at, b) in bytes.iter().enumerate() {
        if quoted {
            if has_escape {
                span = span.max(at - start);
            }
            if escaped {
                escaped = false;
                continue;
            }
            if *b == b'\\' {
                escaped = true;
                has_escape = true;
            } else if *b == b'"' {
                quoted = false;
                if has_escape {
                    span = span.max(at - start);
                }
            }
        } else {
            match b {
                b'"' => {
                    quoted = true;
                    start = at;
                    has_escape = false;
                }
                b'[' | b'{' => {
                    depth = add(depth, 1)?;
                    high = high.max(depth);
                }
                b']' | b'}' => depth = depth.saturating_sub(1),
                b'0'..=b'9' | b'e' | b'E' | b'+' | b'-' | b'.' => {
                    number = add(number, 1)?;
                    span = span.max(number);
                    continue;
                }
                _ => {}
            }
            number = 0;
        }
    }
    mul(span.max(high), 2).map(|n| if n == 0 { 0 } else { n.max(8) })
}

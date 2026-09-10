use alloc::vec;
use alloc::vec::Vec;
use rand::{Rng, RngCore};

use super::{fixed_int, host_version_fixed_int, OFFICIAL_VERSION};

const MAX_PADDING: usize = u8::MAX as usize;
const TARGET_PROBABILITY: f64 = 0.325;

/// Builds the suffix padding used by session control segments. `random_data_len`
/// is the official strategy's proxy for the encoded segment size: 64 + payload
/// on TCP and 88 + payload on UDP.
pub fn session_padding(max_padding: usize, random_data_len: usize, username: &str) -> Vec<u8> {
    let max_padding = max_padding.min(MAX_PADDING);
    let strategy = fixed_int(2, &format!("{username} {OFFICIAL_VERSION}"));
    if strategy == 0 {
        ascii_session_padding(max_padding)
    } else {
        entropy_session_padding(max_padding, random_data_len)
    }
}

/// Selects the prefix and suffix padding lengths for data and ACK segments.
/// The caller supplies the already computed padding budget, excluding IP and
/// UDP headers exactly once.
pub fn data_padding_lengths(max_padding: usize) -> (u8, u8) {
    let max_padding = max_padding.min(MAX_PADDING);
    let prefix = scaled_random(max_padding + 1).min(max_padding);
    let suffix_budget = max_padding - prefix;
    let suffix = scaled_random(suffix_budget + 1).min(suffix_budget);
    (prefix as u8, suffix as u8)
}

fn ascii_session_padding(max_padding: usize) -> Vec<u8> {
    let consecutive = max_padding.min(24 + host_version_fixed_int(17));
    let len = (scaled_random(max_padding - consecutive + 1) + consecutive).min(max_padding);
    let mut padding = random_bytes(len);
    let begin = if len > consecutive {
        rand::thread_rng().gen_range(0..len - consecutive)
    } else {
        0
    };
    for byte in &mut padding[begin..begin + consecutive] {
        make_printable(byte);
    }
    padding
}

fn entropy_session_padding(max_padding: usize, random_data_len: usize) -> Vec<u8> {
    let existing = random_bytes(random_data_len);
    let one_count = existing
        .iter()
        .map(|byte| byte.count_ones() as usize)
        .sum::<usize>();
    let bit_count = existing.len() * 8;
    let zero_count = bit_count - one_count;
    let (lower_is_zero, lower_count) = if one_count < zero_count {
        (false, one_count)
    } else {
        (true, zero_count)
    };
    let min_bits = ((lower_count as f64 / TARGET_PROBABILITY) - bit_count as f64).max(0.0) as usize;
    let min_bytes = min_bits.div_ceil(8);
    let len = if min_bytes >= max_padding {
        max_padding
    } else {
        scaled_random(max_padding - min_bytes + 1) + min_bytes
    };
    let mut padding = vec![if lower_is_zero { 0xff } else { 0 }; len];
    let flips = (((existing.len() + len) * 8) as f64 * TARGET_PROBABILITY) as usize;
    let flips = flips.saturating_sub(lower_count).min(len * 8);
    for _ in 0..flips {
        if len == 0 {
            break;
        }
        let bit = rand::thread_rng().gen_range(0..len * 8);
        let mask = 1 << (bit % 8);
        if lower_is_zero {
            padding[bit / 8] &= !mask;
        } else {
            padding[bit / 8] |= mask;
        }
    }
    padding
}

fn scaled_random(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let mut rng = rand::thread_rng();
    let integer = rng.gen_range(0..=n);
    let base: f64 = rng.gen();
    (integer as f64 * (base * base * base).sqrt()) as usize
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes
}

fn make_printable(byte: &mut u8) {
    if (0x20..=0x7e).contains(byte) {
        return;
    }
    if *byte & 0x80 != 0 {
        let low = *byte & 0x7f;
        if (0x20..=0x7e).contains(&low) {
            *byte = low;
            return;
        }
    }
    *byte = rand::thread_rng().gen_range(0x20..=0x7e);
}

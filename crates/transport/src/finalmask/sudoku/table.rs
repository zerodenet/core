use super::*;
use std::{collections::HashMap, sync::OnceLock};
pub(super) struct Table {
    pub groups: [u8; 64],
    pub group_for: [Option<u8>; 256],
    pub marker: u8,
    pub encode: Vec<Vec<[u8; 4]>>,
    pub decode: HashMap<[u8; 4], u8>,
    pub padding: Vec<u8>,
}
impl Table {
    pub(super) fn new(password: &str, layout: layout::Layout) -> Self {
        static BASE: OnceLock<Vec<Vec<[u8; 4]>>> = OnceLock::new();
        let base = BASE.get_or_init(patterns);
        let hash = ring::digest::digest(&ring::digest::SHA256, password.as_bytes());
        let seed = i64::from_be_bytes(hash.as_ref()[..8].try_into().unwrap());
        let mut order: Vec<_> = (0..base.len()).collect();
        crate::gorand::GoRandom::new(seed).shuffle(&mut order);
        let mut encode = Vec::with_capacity(256);
        let mut decode = HashMap::new();
        for (byte, index) in order.iter().take(256).enumerate() {
            let mut choices = Vec::new();
            for groups in &base[*index] {
                let hints = groups.map(|group| layout.encode(group));
                let mut key = hints;
                key.sort_unstable();
                assert!(decode.insert(key, byte as u8).is_none());
                choices.push(hints);
            }
            encode.push(choices);
        }
        let padding = layout.padding();
        let marker = layout.marker();
        let groups = std::array::from_fn(|i| layout.encode(i as u8));
        let group_for = std::array::from_fn(|i| layout.decode(i as u8));
        Self {
            groups,
            group_for,
            marker,
            encode,
            decode,
            padding,
        }
    }
}
fn grids() -> Vec<[u8; 16]> {
    fn visit(index: usize, grid: &mut [u8; 16], out: &mut Vec<[u8; 16]>) {
        if index == 16 {
            out.push(*grid);
            return;
        }
        let row = index / 4;
        let col = index % 4;
        for value in 1..=4 {
            if (0..4).any(|i| grid[row * 4 + i] == value || grid[i * 4 + col] == value) {
                continue;
            }
            if (0..2)
                .any(|r| (0..2).any(|c| grid[((row / 2) * 2 + r) * 4 + (col / 2) * 2 + c] == value))
            {
                continue;
            }
            grid[index] = value;
            visit(index + 1, grid, out);
            grid[index] = 0;
        }
    }
    let mut out = Vec::new();
    visit(0, &mut [0; 16], &mut out);
    out
}
fn patterns() -> Vec<Vec<[u8; 4]>> {
    let grids = grids();
    assert_eq!(grids.len(), 288);
    let mut patterns = vec![Vec::new(); grids.len()];
    for a in 0..13 {
        for b in a + 1..14 {
            for c in b + 1..15 {
                for d in c + 1..16 {
                    let mut counts = HashMap::<[u8; 4], u16>::new();
                    let keys: Vec<_> = grids
                        .iter()
                        .map(|grid| {
                            let mut groups =
                                [a, b, c, d].map(|pos| ((grid[pos] - 1) << 4) | pos as u8);
                            groups.sort_unstable();
                            *counts.entry(groups).or_default() += 1;
                            groups
                        })
                        .collect();
                    for (i, key) in keys.into_iter().enumerate() {
                        if counts[&key] == 1 {
                            patterns[i].push(key);
                        }
                    }
                }
            }
        }
    }
    assert!(patterns.iter().all(|choices| !choices.is_empty()));
    patterns
}

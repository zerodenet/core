//! Exact fixed-reference replay window, with a bounded 128-block bitmap.
pub struct ReplayWindow {
    storage: Storage,
    width: u64,
}
enum Storage {
    Reference { blocks: Box<[u64; 128]>, last: u64 },
    Custom(std::collections::BTreeSet<u64>),
}
impl ReplayWindow {
    pub const DEFAULT_WINDOW: u64 = 8128;
    pub fn new() -> Self {
        Self::new_with_window(Self::DEFAULT_WINDOW)
    }
    pub fn new_with_window(width: u64) -> Self {
        Self {
            width,
            storage: if width == Self::DEFAULT_WINDOW {
                Storage::Reference {
                    blocks: Box::new([0; 128]),
                    last: 0,
                }
            } else {
                Storage::Custom(Default::default())
            },
        }
    }
    pub fn check_and_update(&mut self, packet: u64) -> bool {
        if packet == u64::MAX {
            return false;
        }
        match &mut self.storage {
            Storage::Reference { blocks, last } => {
                if packet < *last && *last - packet > self.width {
                    return false;
                }
                if packet > *last {
                    let old_block = *last / 64;
                    let new_block = packet / 64;
                    if new_block - old_block >= 128 {
                        blocks.fill(0);
                    } else {
                        for block in old_block + 1..=new_block {
                            blocks[(block % 128) as usize] = 0;
                        }
                    }
                    *last = packet;
                }
                let block = &mut blocks[((packet / 64) % 128) as usize];
                let bit = 1u64 << (packet % 64);
                if *block & bit != 0 {
                    return false;
                }
                *block |= bit;
                true
            }
            Storage::Custom(seen) => {
                let last = seen.last().copied().unwrap_or(packet).max(packet);
                if last - packet > self.width || !seen.insert(packet) {
                    return false;
                }
                let left = last.saturating_sub(self.width);
                seen.retain(|id| *id >= left);
                true
            }
        }
    }
}
impl Default for ReplayWindow {
    fn default() -> Self {
        Self::new()
    }
}

use rand::Rng;

pub(super) fn vision_padding_len(
    content_len: usize,
    long_padding: bool,
    max_content_len: usize,
    testseed: [u32; 4],
) -> usize {
    let mut rng = rand::rng();
    let [long_threshold, long_random, long_base, short_random] = testseed.map(u64::from);
    let content_len = content_len as u64;
    let proposed = if content_len < long_threshold && long_padding {
        rng.random_range(0..long_random) + long_base - content_len
    } else {
        rng.random_range(0..short_random)
    };
    proposed.min(max_content_len.saturating_sub(content_len as usize) as u64) as usize
}

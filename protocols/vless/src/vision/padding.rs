use rand::Rng;

pub(super) fn vision_padding_len(
    content_len: usize,
    long_padding: bool,
    max_content_len: usize,
) -> usize {
    let mut rng = rand::rng();
    let proposed = if content_len < 900 && long_padding {
        rng.random_range(0..500) + 900 - content_len
    } else {
        rng.random_range(0..256)
    };
    proposed.min(max_content_len.saturating_sub(content_len))
}

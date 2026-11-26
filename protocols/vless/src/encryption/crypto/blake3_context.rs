// SPDX-License-Identifier: MPL-2.0
//! Byte-exact BLAKE3 `DERIVE_KEY_CONTEXT` compression (BLAKE3 spec §2.1 & §5.1)
//! feeding upstream `blake3::hazmat::HasherExt::new_from_context_key` so binary
//! non-UTF-8 contexts work with standard crates.io `blake3` without vendoring.

use blake3::hazmat::{ContextKey, HasherExt};

const OUT_LEN: usize = 32;
const BLOCK_LEN: usize = 64;
const CHUNK_LEN: usize = 1024;

const CHUNK_START: u32 = 1 << 0;
const CHUNK_END: u32 = 1 << 1;
const PARENT: u32 = 1 << 2;
const ROOT: u32 = 1 << 3;
const DERIVE_KEY_CONTEXT: u32 = 1 << 5;

const IV: [u32; 8] = [
    0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
];

const MSG_PERMUTATION: [usize; 16] = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8];

#[inline]
fn g(state: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize, mx: u32, my: u32) {
    state[a] = state[a].wrapping_add(state[b]).wrapping_add(mx);
    state[d] = (state[d] ^ state[a]).rotate_right(16);
    state[c] = state[c].wrapping_add(state[d]);
    state[b] = (state[b] ^ state[c]).rotate_right(12);
    state[a] = state[a].wrapping_add(state[b]).wrapping_add(my);
    state[d] = (state[d] ^ state[a]).rotate_right(8);
    state[c] = state[c].wrapping_add(state[d]);
    state[b] = (state[b] ^ state[c]).rotate_right(7);
}

fn round(state: &mut [u32; 16], m: &[u32; 16]) {
    g(state, 0, 4, 8, 12, m[0], m[1]);
    g(state, 1, 5, 9, 13, m[2], m[3]);
    g(state, 2, 6, 10, 14, m[4], m[5]);
    g(state, 3, 7, 11, 15, m[6], m[7]);
    g(state, 0, 5, 10, 15, m[8], m[9]);
    g(state, 1, 6, 11, 12, m[10], m[11]);
    g(state, 2, 7, 8, 13, m[12], m[13]);
    g(state, 3, 4, 9, 14, m[14], m[15]);
}

fn permute(m: &mut [u32; 16]) {
    let mut permuted = [0u32; 16];
    for i in 0..16 {
        permuted[i] = m[MSG_PERMUTATION[i]];
    }
    *m = permuted;
}

fn compress(
    chaining_value: &[u32; 8],
    block_words: &[u32; 16],
    counter: u64,
    block_len: u32,
    flags: u32,
) -> [u32; 16] {
    let mut state = [
        chaining_value[0],
        chaining_value[1],
        chaining_value[2],
        chaining_value[3],
        chaining_value[4],
        chaining_value[5],
        chaining_value[6],
        chaining_value[7],
        IV[0],
        IV[1],
        IV[2],
        IV[3],
        counter as u32,
        (counter >> 32) as u32,
        block_len,
        flags,
    ];
    let mut block = *block_words;
    for r in 0..7 {
        round(&mut state, &block);
        if r < 6 {
            permute(&mut block);
        }
    }
    for i in 0..8 {
        state[i] ^= state[i + 8];
        state[i + 8] ^= chaining_value[i];
    }
    state
}

fn words_from_le_bytes(bytes: &[u8; BLOCK_LEN]) -> [u32; 16] {
    let mut words = [0u32; 16];
    for (chunk, word) in bytes.as_chunks::<4>().0.iter().zip(&mut words) {
        *word = u32::from_le_bytes(*chunk);
    }
    words
}

struct Output {
    input_cv: [u32; 8],
    block_words: [u32; 16],
    counter: u64,
    block_len: u32,
    flags: u32,
}

impl Output {
    fn chaining_value(&self) -> [u32; 8] {
        let out = compress(
            &self.input_cv,
            &self.block_words,
            self.counter,
            self.block_len,
            self.flags,
        );
        out[..8].try_into().expect("8-word chaining value")
    }

    fn root_context_key(&self) -> ContextKey {
        let words = compress(
            &self.input_cv,
            &self.block_words,
            0,
            self.block_len,
            self.flags | ROOT,
        );
        let mut out = [0u8; OUT_LEN];
        for (word, slot) in words[..8].iter().zip(out.as_chunks_mut::<4>().0) {
            *slot = word.to_le_bytes();
        }
        out
    }
}

fn chunk_output(chunk: &[u8], chunk_counter: u64) -> Output {
    let mut cv = IV;
    let mut blocks_compressed = 0u8;
    let mut offset = 0usize;
    while chunk.len() - offset > BLOCK_LEN {
        let mut block = [0u8; BLOCK_LEN];
        block.copy_from_slice(&chunk[offset..offset + BLOCK_LEN]);
        let start = if blocks_compressed == 0 {
            CHUNK_START
        } else {
            0
        };
        let out = compress(
            &cv,
            &words_from_le_bytes(&block),
            chunk_counter,
            BLOCK_LEN as u32,
            DERIVE_KEY_CONTEXT | start,
        );
        cv = out[..8].try_into().expect("8-word cv");
        blocks_compressed += 1;
        offset += BLOCK_LEN;
    }
    let tail = &chunk[offset..];
    let mut block = [0u8; BLOCK_LEN];
    block[..tail.len()].copy_from_slice(tail);
    let start = if blocks_compressed == 0 {
        CHUNK_START
    } else {
        0
    };
    Output {
        input_cv: cv,
        block_words: words_from_le_bytes(&block),
        counter: chunk_counter,
        block_len: tail.len() as u32,
        flags: DERIVE_KEY_CONTEXT | start | CHUNK_END,
    }
}

fn parent_output(left_cv: [u32; 8], right_cv: [u32; 8]) -> Output {
    let mut block_words = [0u32; 16];
    block_words[..8].copy_from_slice(&left_cv);
    block_words[8..].copy_from_slice(&right_cv);
    Output {
        input_cv: IV,
        block_words,
        counter: 0,
        block_len: BLOCK_LEN as u32,
        flags: DERIVE_KEY_CONTEXT | PARENT,
    }
}

fn hash_derive_key_context_bytes(context: &[u8]) -> ContextKey {
    if context.len() <= CHUNK_LEN {
        return chunk_output(context, 0).root_context_key();
    }
    let mut cv_stack = [[0u32; 8]; 54];
    let mut cv_len = 0usize;
    let mut chunk_counter = 0u64;
    let mut rest = context;
    while rest.len() > CHUNK_LEN {
        let mut new_cv = chunk_output(&rest[..CHUNK_LEN], chunk_counter).chaining_value();
        chunk_counter += 1;
        let mut total = chunk_counter;
        while total & 1 == 0 {
            cv_len -= 1;
            new_cv = parent_output(cv_stack[cv_len], new_cv).chaining_value();
            total >>= 1;
        }
        cv_stack[cv_len] = new_cv;
        cv_len += 1;
        rest = &rest[CHUNK_LEN..];
    }
    let mut output = chunk_output(rest, chunk_counter);
    while cv_len > 0 {
        cv_len -= 1;
        output = parent_output(cv_stack[cv_len], output.chaining_value());
    }
    output.root_context_key()
}

pub(super) fn derive_key_bytes(context: &[u8], key: &[u8]) -> [u8; 32] {
    if let Ok(utf8) = core::str::from_utf8(context) {
        return blake3::derive_key(utf8, key);
    }
    let context_key = hash_derive_key_context_bytes(context);
    let mut hasher = blake3::Hasher::new_from_context_key(&context_key);
    hasher.update(key);
    *hasher.finalize().as_bytes()
}

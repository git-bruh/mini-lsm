// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // + 10 for offset, key, prefix and value lengths, and final number of elements
        if self.first_key.key_len() > 0
            && ((self.data.len() + (self.offsets.len() * 2) + key.raw_len() + value.len() + 10)
                > self.block_size)
        {
            return false;
        }

        let prefix_len = 'blk: {
            for (idx, (a, b)) in key
                .into_inner()
                .iter()
                .zip(self.first_key.as_key_slice().into_inner().iter())
                .enumerate()
            {
                if *a != *b {
                    break 'blk idx;
                }
            }

            0
        };

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(prefix_len as _);
        self.data.put_u16((key.key_len() - prefix_len) as _);
        self.data.put(&key.into_inner()[prefix_len..]);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as _);
        self.data.put(value);

        if self.first_key.key_len() == 0 {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.key_len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

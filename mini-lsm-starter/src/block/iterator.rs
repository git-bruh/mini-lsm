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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// Prefix length to be taken from first_key
    prefix_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            prefix_size: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.key.len() > 0
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.next()
    }

    /// Seeks to the last key in the block
    pub fn seek_to_last(&mut self) {
        self.idx = self.block.offsets.len() - 1;
        self.next()
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() {
            self.key.clear();
            self.prefix_size = 0;
            return;
        }

        let mut offset = self.block.offsets[self.idx] as usize;
        self.idx += 1;

        let prefix_size =
            u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        offset += 2;
        let key_size =
            u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        offset += 2;
        let mut prefixed_key = self.first_key.as_key_slice().into_inner()[0..prefix_size].to_vec();
        prefixed_key.extend(self.block.data[offset..offset + key_size].to_vec());
        let key = KeyVec::from_vec(prefixed_key);
        offset += key_size;
        let value_size =
            u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        offset += 2;

        if self.first_key.len() == 0 {
            self.first_key = key.clone();
        }
        self.key = key;
        self.value_range = (offset, offset + value_size);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.idx = 0;
        loop {
            self.next();
            if self.key.as_key_slice() >= key || !self.is_valid() {
                return;
            }
        }
    }
}

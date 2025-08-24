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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{BlockBuilder, BlockIterator},
    key::KeySlice,
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            hashes: Vec::new(),
        }
    }

    fn flush_block(&mut self) {
        let block = Arc::new(
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)).build(),
        );
        let mut iter = BlockIterator::create_and_seek_to_first(block.clone());

        let first_key = iter.key().to_key_vec();
        iter.seek_to_last();
        let last_key = iter.key().to_key_vec();

        if self.first_key.len() == 0 {
            self.first_key = first_key.clone().into_inner();
        }
        self.last_key = last_key.clone().into_inner();

        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: first_key.into_key_bytes(),
            last_key: last_key.into_key_bytes(),
        });
        self.data.extend(Vec::from(block.encode()));
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.hashes.push(farmhash::fingerprint32(key.into_inner()));
        if !self.builder.add(key, value) {
            self.flush_block();
            if !self.builder.add(key, value) {
                panic!("new BlockBuilder returned false");
            }
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.flush_block();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data, &self.hashes);
        let file = FileObject::create(path.as_ref(), self.data)?;
        SsTable::open(id, block_cache, file)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}

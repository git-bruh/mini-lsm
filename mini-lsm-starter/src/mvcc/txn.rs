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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let mut local_iter = TxnLocalIterator::new(
            self.local_storage.clone(),
            |skip_map| {
                skip_map.range((
                    lower.map(|x| Bytes::copy_from_slice(x)),
                    upper.map(|x| Bytes::copy_from_slice(x)),
                ))
            },
            (Bytes::new(), Bytes::new()),
        );
        local_iter.next()?;

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.local_storage.remove(key);
    }

    pub fn commit(&self) -> Result<()> {
        self.inner.write_batch(
            &self
                .local_storage
                .iter()
                .map(|entry| WriteBatchRecord::Put(entry.key().clone(), entry.value().clone()))
                .collect::<Vec<_>>(),
        )
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {}
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.iter().as_slice()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.iter().as_slice()
    }

    fn is_valid(&self) -> bool {
        self.borrow_item().0.len() > 0
    }

    fn next(&mut self) -> Result<()> {
        let (key, value) = self.with_iter_mut(|iter| {
            let pair = iter.next();
            if let Some(pair) = pair {
                (pair.key().clone(), pair.value().clone())
            } else {
                (Bytes::new(), Bytes::new())
            }
        });

        self.with_item_mut(|item| {
            item.0 = key;
            item.1 = value;
        });

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if self.is_valid() { 1 } else { 0 }
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { _txn: txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

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

use anyhow::Result;
use bytes::Bytes;
use std::ops::Bound;

use crate::{
    iterators::{
        StorageIterator, merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    is_valid: bool,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            is_valid: true,
            end_bound,
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        match &self.end_bound {
            Bound::Included(x) => self.is_valid = self.inner.key().raw_ref() <= x,
            Bound::Excluded(x) => self.is_valid = self.inner.key().raw_ref() < x,
            Bound::Unbounded => {}
        };

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_to_non_delete()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            anyhow::bail!("iterator has errored");
        }

        if let Err(e) = self.iter.next() {
            self.has_errored = true;
            Err(e)
        } else {
            Ok(())
        }
    }
}

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

use std::cmp::{self};
use std::collections::{BinaryHeap, binary_heap::PeekMut};

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(mut iters: Vec<Box<I>>) -> Self {
        let mut iterator = Self {
            iters: BinaryHeap::new(),
            current: None,
        };

        iters.drain(0..).enumerate().for_each(|(idx, vec)| {
            if !vec.is_valid() {
                return;
            }

            iterator.iters.push(HeapWrapper(idx, vec))
        });
        iterator.current = iterator.iters.pop();

        iterator
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(iter) = &self.current {
            iter.1.key()
        } else {
            KeySlice::from_slice(&[])
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(iter) = &self.current {
            iter.1.value()
        } else {
            &[]
        }
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.current {
            iter.1.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        let current = std::mem::replace(&mut self.current, None);
        if current.is_none() {
            return Ok(());
        }

        let mut current = current.expect("already checked");
        while let Some(mut iter) = self.iters.peek_mut() {
            if iter.1.key() == current.1.key() {
                if let Err(e) = iter.1.next() {
                    PeekMut::pop(iter);
                    return Err(e);
                }

                if !iter.1.is_valid() {
                    PeekMut::pop(iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                self.current = Some(iter);
            } else {
                self.current = Some(current);
            }
        } else {
            if let Some(iter) = self.iters.peek_mut() {
                if iter.1.key() < current.1.key()
                    || (iter.0 < current.0 && iter.1.key() == current.1.key())
                {
                    self.current = Some(PeekMut::pop(iter));
                }
            }

            if self.current.is_some() {
                self.iters.push(current);
            } else {
                self.current = Some(current);
            }
        }

        Ok(())
    }
}

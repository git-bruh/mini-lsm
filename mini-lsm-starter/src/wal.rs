// REMOVE THIS LINE after fully implementing this functionality
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
use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let mut remaining_len = file.metadata()?.len() as usize;
        while remaining_len > 0 {
            let mut key_len = vec![0; 2];
            file.read_exact(&mut key_len)?;
            let mut key = vec![0; u16::from_be_bytes([key_len[0], key_len[1]]) as usize];
            file.read_exact(&mut key)?;
            let mut key_ts = vec![0; std::mem::size_of::<u64>()];
            file.read_exact(&mut key_ts)?;
            let mut value_len = vec![0; 2];
            file.read_exact(&mut value_len)?;
            let mut value = vec![0; u16::from_be_bytes([value_len[0], value_len[1]]) as usize];
            file.read_exact(&mut value)?;

            remaining_len -=
                key_len.len() + key.len() + key_ts.len() + value_len.len() + value.len();

            skiplist.insert(
                KeyBytes::from_bytes_with_ts(
                    key.into(),
                    u64::from_be_bytes(key_ts.try_into().expect("invalid size of u64")),
                ),
                value.into(),
            );
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut entry = Vec::<u8>::new();
        for (key, value) in data {
            entry.put_u16(key.key_len() as _);
            entry.put(key.into_inner());
            entry.put_u64(key.ts());
            entry.put_u16(value.len() as _);
            entry.put(*value);
        }
        self.file.lock().write_all(&entry)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().get_mut().sync_all()?;
        Ok(())
    }
}

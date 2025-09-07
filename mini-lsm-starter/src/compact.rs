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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;
use std::{collections::BTreeSet, iter::FromIterator};

use crate::key::KeySlice;
use crate::manifest::ManifestRecord;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::{
    StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
    two_merge_iterator::TwoMergeIterator,
};
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut ssts = Vec::new();
        while iter.is_valid() {
            if !compact_to_bottom_level || iter.value().len() > 0 {
                builder.add(iter.key(), iter.value());
                if builder.estimated_size() > self.options.target_sst_size {
                    let id = self.next_sst_id();
                    let builder = std::mem::replace(
                        &mut builder,
                        SsTableBuilder::new(self.options.block_size),
                    );
                    ssts.push(Arc::new(builder.build(id, None, self.path_of_sst(id))?))
                }
            }
            iter.next()?;
        }
        // TODO this leads to an extra SST under some cases
        let id = self.next_sst_id();
        ssts.push(Arc::new(builder.build(id, None, self.path_of_sst(id))?));
        Ok(ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = Arc::clone(&self.state.read());
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut sstable_iters: Vec<Box<SsTableIterator>> = Vec::new();
                for id in l0_sstables {
                    let sstable = state
                        .sstables
                        .get(id)
                        .expect("id in l0_sstables but not sstables")
                        .clone();
                    sstable_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        sstable,
                    )?));
                }
                let concat_iter = SstConcatIterator::create_and_seek_to_first(
                    l1_sstables
                        .iter()
                        .map(|id| {
                            state
                                .sstables
                                .get(id)
                                .expect("id in l1_sstables but not sstables")
                                .clone()
                        })
                        .collect(),
                )?;
                let iter =
                    TwoMergeIterator::create(MergeIterator::create(sstable_iters), concat_iter)?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => {
                let mut sstable_iters: Vec<Box<SsTableIterator>> = Vec::new();
                let mut upper_sstables: Vec<Arc<SsTable>> = Vec::new();
                let mut lower_sstables: Vec<Arc<SsTable>> = Vec::new();
                if upper_level.is_none() {
                    for id in upper_level_sst_ids {
                        sstable_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            state
                                .sstables
                                .get(id)
                                .expect("id in upper_level_sst_ids but not sstables")
                                .clone(),
                        )?));
                    }
                } else {
                    upper_sstables.extend(
                        upper_level_sst_ids
                            .iter()
                            .map(|id| {
                                state
                                    .sstables
                                    .get(id)
                                    .expect("id in upper_level_sst_ids but not sstables")
                                    .clone()
                            })
                            .collect::<Vec<_>>(),
                    );
                };
                lower_sstables.extend(
                    lower_level_sst_ids
                        .iter()
                        .map(|id| {
                            state
                                .sstables
                                .get(id)
                                .expect("id in lower_level_sst_ids but not sstables")
                                .clone()
                        })
                        .collect::<Vec<_>>(),
                );

                if sstable_iters.len() > 0 {
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(
                            MergeIterator::create(sstable_iters),
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?,
                        )?,
                        task.compact_to_bottom_level(),
                    )
                } else {
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(
                            SstConcatIterator::create_and_seek_to_first(upper_sstables)?,
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?,
                        )?,
                        task.compact_to_bottom_level(),
                    )
                }
            }
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut concat_iters: Vec<Box<SstConcatIterator>> = Vec::new();
                for (_, ssts) in tiers {
                    concat_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        ssts.iter()
                            .map(|id| state.sstables.get(id).expect("id not in sstables").clone())
                            .collect(),
                    )?));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(concat_iters),
                    task.compact_to_bottom_level(),
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let sstables = self.compact(&task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = LsmStorageState {
                memtable: state.memtable.clone(),
                imm_memtables: state.imm_memtables.clone(),
                l0_sstables: state.l0_sstables.clone(),
                levels: state.levels.clone(),
                sstables: state.sstables.clone(),
            };
            if let Some(latest) = l0_sstables.first() {
                let old_ids_begin = new_state
                    .l0_sstables
                    .iter()
                    .position(|id| id == latest)
                    .expect("latest sst from old snapshot not found");
                new_state
                    .l0_sstables
                    .drain(old_ids_begin..new_state.l0_sstables.len());
            }
            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                new_state.sstables.remove(id);
            }
            sstables.iter().for_each(|sstable| {
                new_state.sstables.insert(sstable.sst_id(), sstable.clone());
            });
            new_state.levels[0] = (0, sstables.iter().map(|sstable| sstable.sst_id()).collect());
            let _ = std::mem::replace(&mut *state, Arc::new(new_state));

            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(task, state.levels[0].1.clone()),
                )?;
            }
        }

        for id in l0_sstables {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let state = Arc::clone(&self.state.read());
        if let Some(task) = self.compaction_controller.generate_compaction_task(&state) {
            if !state.l0_sstables.is_empty() {
                println!("L0 ({}): {:?}", state.l0_sstables.len(), state.l0_sstables,);
            }
            for (level, files) in &state.levels {
                println!("L{level} ({}): {:?}", files.len(), files);
            }

            let mut ssts = self.compact(&task)?;
            let deleted = {
                let state_lock = self.state_lock.lock();
                let mut state = self.state.write();
                let mut new_state = state.as_ref().clone();
                let sst_ids = ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
                new_state
                    .sstables
                    .extend(ssts.drain(0..).map(|sst| (sst.sst_id(), sst)));
                let (mut new_state, deleted) = self
                    .compaction_controller
                    .apply_compaction_result(&new_state, &task, &sst_ids, false);
                let deleted = BTreeSet::from_iter(deleted);
                new_state.sstables.retain(|k, _| !deleted.contains(k));
                println!("compaction finished: {deleted:?} removed, {sst_ids:?} added",);
                let _ = std::mem::replace(&mut *state, Arc::new(new_state));
                if let Some(manifest) = &self.manifest {
                    manifest.add_record(&state_lock, ManifestRecord::Compaction(task, sst_ids))?;
                }
                deleted
            };
            for id in deleted {
                std::fs::remove_file(self.path_of_sst(id))?;
            }
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if (self.state.read().imm_memtables.len() + 1) > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}

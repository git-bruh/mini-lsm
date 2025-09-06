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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;
use std::{collections::BTreeSet, iter::FromIterator};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let mut ssts = Vec::new();
        let sstables = sst_ids
            .iter()
            .map(|sst| snapshot.sstables.get(sst).expect("sst not found"))
            .collect::<Vec<_>>();
        let first_key = sstables
            .iter()
            .map(|sstable| sstable.first_key())
            .min()
            .expect("empty sst_ids")
            .as_key_slice();
        let last_key = sstables
            .iter()
            .map(|sstable| sstable.last_key())
            .max()
            .expect("empty sst_ids")
            .as_key_slice();
        for sst in &snapshot.levels[in_level].1 {
            let sstable = snapshot.sstables.get(sst).expect("sst not found");
            if !(sstable.last_key().as_key_slice() < first_key
                || sstable.first_key().as_key_slice() > last_key)
            {
                ssts.push(*sst);
            }
        }
        ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_size = |idx: usize| {
            snapshot.levels[idx]
                .1
                .iter()
                .map(|sst| {
                    snapshot
                        .sstables
                        .get(sst)
                        .expect("sstable not found")
                        .table_size() as usize
                        / (1024 * 1024)
                })
                .sum::<usize>()
        };

        let mut target_sizes = snapshot.levels.iter().map(|_| 0).collect::<Vec<_>>();
        target_sizes[snapshot.levels.len() - 1] =
            level_size(snapshot.levels.len() - 1).max(self.options.base_level_size_mb);

        for level in (0..snapshot.levels.len() - 1).rev() {
            if target_sizes[level + 1] > self.options.base_level_size_mb {
                target_sizes[level] = target_sizes[level + 1] / self.options.level_size_multiplier;
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let level_idx = target_sizes
                .iter()
                .enumerate()
                .filter(|(_, size)| **size > 0)
                .nth(0)
                .expect("all target sizes 0")
                .0;
            println!("flushing L0 SSTs to level {level_idx}");

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: level_idx,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    level_idx,
                ),
                is_lower_level_bottom_level: (level_idx + 1) == snapshot.levels.len(),
            });
        }

        let mut sorted_ratios = target_sizes
            .iter()
            .enumerate()
            .map(|(idx, target_size)| (idx, level_size(idx) as f64 / *target_size as f64))
            .filter(|(_, ratio)| *ratio > 1.0)
            .collect::<Vec<_>>();
        sorted_ratios.sort_by(|a, b| a.1.total_cmp(&b.1));

        if let Some((upper, _)) = sorted_ratios.first() {
            let upper = *upper;
            let upper_sst = *snapshot.levels[upper]
                .1
                .iter()
                .min()
                .expect("upper level empty");
            let lower = upper + 1;
            return Some(LeveledCompactionTask {
                upper_level: Some(upper),
                upper_level_sst_ids: vec![upper_sst],
                lower_level: lower,
                lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[upper_sst], lower),
                is_lower_level_bottom_level: (lower + 1) == snapshot.levels.len(),
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut deleted = Vec::new();

        deleted.extend_from_slice(&task.upper_level_sst_ids);
        deleted.extend_from_slice(&task.lower_level_sst_ids);
        let to_remove = BTreeSet::from_iter(&deleted);

        if let Some(upper_level) = task.upper_level {
            snapshot.levels[upper_level]
                .1
                .retain(|e| !to_remove.contains(e));
        } else {
            snapshot.l0_sstables.retain(|e| !to_remove.contains(e));
        }
        snapshot.levels[task.lower_level]
            .1
            .retain(|e| !to_remove.contains(e));
        snapshot.levels[task.lower_level]
            .1
            .extend_from_slice(output);

        snapshot.levels[task.lower_level].1.sort_by(|a, b| {
            snapshot
                .sstables
                .get(a)
                .expect("sst not found")
                .first_key()
                .cmp(snapshot.sstables.get(b).expect("sst not found").first_key())
        });
        (snapshot, deleted)
    }
}

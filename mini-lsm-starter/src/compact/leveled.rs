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
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let target_sizes = snapshot.levels.iter().enumerate().map(|(idx, (_, ssts))| {
            if ssts.len() == 0 {
                (idx, 0)
            } else {
                (
                    idx,
                    self.options.base_level_size_mb
                        / (self.options.level_size_multiplier
                            * (self.options.max_levels - idx - 1))
                            .min(1),
                )
            }
        });

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let level_idx = target_sizes
                .filter(|(_, target_size)| *target_size > 0)
                .nth(0)
                .map_or(snapshot.levels.len() - 1, |(idx, _)| idx);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: snapshot.levels[level_idx].0,
                lower_level_sst_ids: snapshot.levels[level_idx].1.clone(),
                is_lower_level_bottom_level: (level_idx + 1) == snapshot.levels.len(),
            });
        }

        let mut sorted_ratios = target_sizes
            .filter(|(idx, target_size)| {
                snapshot
                    .sstables
                    .get(idx)
                    .expect("sstable not found")
                    .file
                    .size() as f64
                    / *target_size as f64
                    > 1.0
            })
            .collect::<Vec<_>>();
        sorted_ratios.sort_by(|(_, ratio_a), (_, ratio_b)| ratio_a.cmp(ratio_b));
        if sorted_ratios.len() >= 2 {
            let (upper, lower) = (sorted_ratios[0].0, sorted_ratios[1].0);
            return Some(LeveledCompactionTask {
                upper_level: Some(snapshot.levels[upper].0),
                upper_level_sst_ids: snapshot.levels[upper].1.clone(),
                lower_level: snapshot.levels[lower].0,
                lower_level_sst_ids: snapshot.levels[lower].1.clone(),
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

        if let Some(upper_level) = task.upper_level {
            deleted.extend_from_slice(&task.upper_level_sst_ids);
            deleted.extend_from_slice(&task.lower_level_sst_ids);
            let to_remove = BTreeSet::from_iter(&deleted);
            snapshot.levels[upper_level]
                .1
                .retain(|e| !to_remove.contains(e));
            snapshot.levels[task.lower_level]
                .1
                .retain(|e| !to_remove.contains(e));
        } else {
            deleted.extend_from_slice(&task.upper_level_sst_ids);
            let to_remove = BTreeSet::from_iter(&deleted);
            snapshot.l0_sstables.retain(|e| !to_remove.contains(e));
        }

        snapshot.levels[task.lower_level]
            .1
            .extend_from_slice(output);

        (snapshot, deleted)
    }
}

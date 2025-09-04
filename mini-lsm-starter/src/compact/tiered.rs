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
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let sum = snapshot
            .levels
            .iter()
            .map(|level| level.1.len())
            .sum::<usize>();
        let last = snapshot.levels.last().unwrap();
        if (sum - last.1.len()) / last.1.len().max(1) * 100
            >= self.options.max_size_amplification_percent
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let mut prev_tiers_sum = 0;
        for (idx, (tier, ssts)) in snapshot.levels.iter().enumerate() {
            if (ssts.len() as f64 / prev_tiers_sum.max(1) as f64)
                > ((100f64 + self.options.size_ratio as f64) / 100f64)
            {
                if idx >= self.options.min_merge_width {
                    return Some(TieredCompactionTask {
                        tiers: snapshot.levels[0..idx].to_vec(),
                        bottom_tier_included: false,
                    });
                }
            }
            prev_tiers_sum += ssts.len();
        }

        let num_tiers_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot.levels[0..num_tiers_to_take].to_vec(),
            bottom_tier_included: true,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        // full compaction
        if task.bottom_tier_included {
            let mut deleted = Vec::new();
            for (_, ssts) in snapshot.levels {
                deleted.extend(ssts);
            }
            snapshot.levels = vec![(output[0], output.to_vec())];
            return (snapshot, deleted);
        }

        let deleted = BTreeSet::from_iter(task.tiers.iter().map(|(id, _)| id));
        snapshot.levels.retain(|(id, _)| !deleted.contains(id));
        snapshot.levels.insert(0, (output[0], output.to_vec()));

        (snapshot, deleted.iter().map(|id| **id).collect::<Vec<_>>())
    }
}

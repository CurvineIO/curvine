// Copyright 2025 OPPO.
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

use super::types::{AggregationStats, DirAgg};
use curvine_core_error::{err_box, CommonResult};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct AggregationOutput {
    pub root: DirAgg,
    pub entries: HashMap<i64, DirAgg>,
    pub stats: AggregationStats,
}

pub struct SubtreeAggregator {
    root_id: i64,
    max_dirs: u64,
    max_depth: u32,
    dir_parent: HashMap<i64, i64>,
    children: HashMap<i64, Vec<i64>>,
    direct: HashMap<i64, DirAgg>,
    stats: AggregationStats,
}

impl SubtreeAggregator {
    pub fn new(root_id: i64, max_dirs: u64, max_depth: u32) -> Self {
        let mut direct = HashMap::new();
        direct.insert(root_id, DirAgg::empty_dir());
        Self {
            root_id,
            max_dirs,
            max_depth,
            dir_parent: HashMap::new(),
            children: HashMap::new(),
            direct,
            stats: AggregationStats::default(),
        }
    }

    pub fn add_dir(&mut self, dir_id: i64, parent_id: i64) -> CommonResult<()> {
        if dir_id == parent_id {
            self.stats.record_anomaly(1);
            return Ok(());
        }
        if self.dir_parent.contains_key(&dir_id) {
            self.stats.record_anomaly(1);
            return Ok(());
        }
        self.dir_parent.insert(dir_id, parent_id);
        self.children.entry(parent_id).or_default().push(dir_id);
        self.direct.entry(dir_id).or_insert_with(DirAgg::empty_dir);
        self.direct
            .entry(parent_id)
            .or_insert_with(DirAgg::empty_dir);
        Ok(())
    }

    pub fn add_file(&mut self, parent_id: i64, len: i64) -> CommonResult<()> {
        let entry = self
            .direct
            .entry(parent_id)
            .or_insert_with(DirAgg::empty_dir);
        entry.add_file(len)?;
        Ok(())
    }

    pub fn build(mut self) -> CommonResult<AggregationOutput> {
        let reachable = self.mark_reachable_from_root()?;
        self.validate_limits(&reachable)?;

        let mut subtree: HashMap<i64, DirAgg> = HashMap::new();
        for (&dir_id, direct) in &self.direct {
            if reachable.contains(&dir_id) {
                subtree.insert(dir_id, *direct);
            }
        }

        let order = self.postorder(&reachable);
        for dir_id in order {
            let Some(children) = self.children.get(&dir_id) else {
                continue;
            };
            for &child_id in children {
                if !reachable.contains(&child_id) {
                    continue;
                }
                let Some(child_agg) = subtree.get(&child_id).copied() else {
                    return err_box!("missing child subtree aggregate");
                };
                let Some(parent) = subtree.get_mut(&dir_id) else {
                    return err_box!("missing dir subtree aggregate");
                };
                parent.merge(&child_agg)?;
            }
        }

        let Some(root) = subtree.remove(&self.root_id) else {
            return err_box!("root dir missing from aggregation");
        };

        Ok(AggregationOutput {
            root,
            entries: subtree,
            stats: self.stats,
        })
    }

    fn mark_reachable_from_root(&mut self) -> CommonResult<HashSet<i64>> {
        let mut reachable = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back((self.root_id, 0u32));
        reachable.insert(self.root_id);

        while let Some((dir_id, depth)) = queue.pop_front() {
            if depth >= self.max_depth {
                return err_box!(
                    "dir-space MaxDepth limit {} exceeded (observed depth {})",
                    self.max_depth,
                    depth.saturating_add(1)
                );
            }
            if let Some(kids) = self.children.get(&dir_id) {
                for &child in kids {
                    if reachable.insert(child) {
                        queue.push_back((child, depth.saturating_add(1)));
                    } else {
                        // cycle: already on path from root via another branch — skip extra edge
                    }
                }
            }
        }

        // Orphans: dirs/files registered but not reachable from root
        for &dir_id in self.direct.keys() {
            if !reachable.contains(&dir_id) {
                self.stats.record_anomaly(1);
            }
        }

        Ok(reachable)
    }

    fn validate_limits(&self, reachable: &HashSet<i64>) -> CommonResult<()> {
        let dir_count = reachable.len() as u64;
        if dir_count > self.max_dirs {
            return err_box!(
                "dir-space MaxDirs limit {} exceeded (observed {})",
                self.max_dirs,
                dir_count
            );
        }
        Ok(())
    }

    fn postorder(&self, reachable: &HashSet<i64>) -> Vec<i64> {
        let mut order = Vec::new();
        let mut stack = vec![(self.root_id, false)];
        while let Some((dir_id, visited)) = stack.pop() {
            if !reachable.contains(&dir_id) {
                continue;
            }
            if visited {
                order.push(dir_id);
            } else {
                stack.push((dir_id, true));
                if let Some(kids) = self.children.get(&dir_id) {
                    for &child in kids.iter().rev() {
                        if reachable.contains(&child) {
                            stack.push((child, false));
                        }
                    }
                }
            }
        }
        order
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ROOT_INODE_ID;

    fn baseline_recursive(
        root: i64,
        children: &HashMap<i64, Vec<i64>>,
        direct: &HashMap<i64, DirAgg>,
    ) -> (DirAgg, HashMap<i64, DirAgg>) {
        fn dfs(
            id: i64,
            children: &HashMap<i64, Vec<i64>>,
            direct: &HashMap<i64, DirAgg>,
            memo: &mut HashMap<i64, DirAgg>,
        ) -> DirAgg {
            if let Some(v) = memo.get(&id) {
                return *v;
            }
            let mut agg = direct.get(&id).copied().unwrap_or(DirAgg::empty_dir());
            if let Some(kids) = children.get(&id) {
                for &child in kids {
                    let child_agg = dfs(child, children, direct, memo);
                    agg.merge(&child_agg).unwrap();
                }
            }
            memo.insert(id, agg);
            agg
        }
        let mut memo = HashMap::new();
        let root_agg = dfs(root, children, direct, &mut memo);
        memo.remove(&root);
        (root_agg, memo)
    }

    fn build_from_edges(
        root: i64,
        max_dirs: u64,
        max_depth: u32,
        dirs: &[(i64, i64)],
        files: &[(i64, i64)],
    ) -> CommonResult<AggregationOutput> {
        let mut agg = SubtreeAggregator::new(root, max_dirs, max_depth);
        for &(d, p) in dirs {
            agg.add_dir(d, p)?;
        }
        for &(p, len) in files {
            agg.add_file(p, len)?;
        }
        agg.build()
    }

    #[test]
    fn empty_root() {
        let out = build_from_edges(ROOT_INODE_ID, 1_000_000, 1024, &[], &[]).unwrap();
        assert_eq!(out.root.logical_bytes, 0);
        assert_eq!(out.root.file_count, 0);
        assert_eq!(out.root.directory_count, 1);
        assert!(out.entries.is_empty());
    }

    #[test]
    fn wide_tree_and_deep_tree() {
        let mut dirs = Vec::new();
        for i in 0..50 {
            dirs.push((10_000 + i, ROOT_INODE_ID));
        }
        let mut files = vec![(ROOT_INODE_ID, 10_i64)];
        for i in 0..50 {
            files.push((10_000 + i, 100));
        }
        let out = build_from_edges(ROOT_INODE_ID, 1_000_000, 1024, &dirs, &files).unwrap();
        assert_eq!(out.root.file_count, 51);
        assert_eq!(out.root.directory_count, 51);

        let mut chain_dirs = Vec::new();
        let mut chain_files = Vec::new();
        let mut parent = ROOT_INODE_ID;
        for depth in 0..500 {
            let id = 20_000 + depth;
            chain_dirs.push((id, parent));
            chain_files.push((id, 1));
            parent = id;
        }
        let deep =
            build_from_edges(ROOT_INODE_ID, 1_000_000, 1024, &chain_dirs, &chain_files).unwrap();
        assert_eq!(deep.root.directory_count, 501);
        assert_eq!(deep.root.file_count, 500);
    }

    #[test]
    fn hard_link_double_count() {
        let dirs = [(2001, ROOT_INODE_ID), (2002, ROOT_INODE_ID)];
        let files = [(2001, 50), (2002, 50)];
        let out = build_from_edges(ROOT_INODE_ID, 1_000_000, 1024, &dirs, &files).unwrap();
        assert_eq!(out.root.file_count, 2);
        assert_eq!(out.root.logical_bytes, 100);
    }

    #[test]
    fn orphan_self_edge_and_cycle() {
        let mut agg = SubtreeAggregator::new(ROOT_INODE_ID, 1_000_000, 1024);
        agg.add_dir(5, 5).unwrap();
        agg.add_dir(6, 99).unwrap();
        agg.add_file(99, 10).unwrap();
        agg.add_dir(7, 8).unwrap();
        agg.add_dir(8, 7).unwrap();
        let out = agg.build().unwrap();
        assert!(out.stats.degraded);
        assert!(out.stats.anomaly_count >= 2);
        assert_eq!(out.root.directory_count, 1);
    }

    #[test]
    fn max_dirs_limit() {
        let dirs = [(2, ROOT_INODE_ID), (3, ROOT_INODE_ID)];
        let err = build_from_edges(ROOT_INODE_ID, 1, 1024, &dirs, &[]).unwrap_err();
        assert!(err.to_string().contains("MaxDirs"));
    }

    #[test]
    fn max_depth_limit() {
        let dirs = [(2, ROOT_INODE_ID), (3, 2)];
        let err = build_from_edges(ROOT_INODE_ID, 1_000_000, 1, &dirs, &[]).unwrap_err();
        assert!(err.to_string().contains("MaxDepth"));
    }

    #[test]
    fn matches_recursive_baseline() {
        let dirs = [(2, ROOT_INODE_ID), (3, 2), (4, 2), (5, ROOT_INODE_ID)];
        let files = [(2, 7), (3, 11), (4, 13), (5, 17), (ROOT_INODE_ID, 3)];
        let out = build_from_edges(ROOT_INODE_ID, 1_000_000, 1024, &dirs, &files).unwrap();

        let mut children = HashMap::new();
        for &(d, p) in &dirs {
            children.entry(p).or_insert_with(Vec::new).push(d);
        }
        let mut direct = HashMap::new();
        direct.insert(ROOT_INODE_ID, DirAgg::empty_dir());
        for &(d, p) in &dirs {
            direct.entry(d).or_insert_with(DirAgg::empty_dir);
            let _ = p;
        }
        for &(p, len) in &files {
            direct
                .entry(p)
                .or_insert_with(DirAgg::empty_dir)
                .add_file(len)
                .unwrap();
        }
        let (root_base, entries_base) = baseline_recursive(ROOT_INODE_ID, &children, &direct);
        assert_eq!(out.root, root_base);
        for (id, agg) in entries_base {
            assert_eq!(out.entries.get(&id), Some(&agg));
        }
    }

    #[test]
    fn overflow_on_merge() {
        let mut agg = DirAgg::empty_dir();
        agg.logical_bytes = u64::MAX;
        let err = agg.merge(&DirAgg {
            logical_bytes: 1,
            file_count: 0,
            directory_count: 0,
        });
        assert!(err.is_err());
    }
}

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

use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;

#[test]
fn glob_listing_preserves_hard_link_parent_paths() {
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("glob-links-{}", Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!("glob-links-j-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.create("/a/p1/file", true).unwrap();
    fs.create("/a/p2/other", true).unwrap();
    fs.link("/a/p1/file", "/a/p2/file").unwrap();
    fs.link("/a/p1/file", "/a/p1/file-alias").unwrap();

    for (pattern, expected) in [
        ("/a/p*/file", vec!["/a/p1/file", "/a/p2/file"]),
        (
            "/a/p*/file*",
            vec!["/a/p1/file", "/a/p1/file-alias", "/a/p2/file"],
        ),
    ] {
        let mut paths: Vec<_> = fs
            .list_status(pattern)
            .unwrap()
            .into_iter()
            .map(|status| status.path)
            .collect();
        paths.sort();
        assert_eq!(paths, expected, "{pattern}");
    }
}

#[test]
fn glob_listing_keeps_owned_walk_results_alive() {
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("glob-listing-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("glob-listing-j-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.create("/a/dir1/nested/first", true).unwrap();
    fs.create("/a/dir2/nested/second", true).unwrap();
    fs.create("/a/other/nested/excluded", true).unwrap();

    for _ in 0..20 {
        let mut directories: Vec<String> = fs
            .list_status("/a/dir*")
            .unwrap()
            .into_iter()
            .map(|status| status.path)
            .collect();
        directories.sort();
        assert_eq!(directories, ["/a/dir1/nested", "/a/dir2/nested"]);
        let mut paths: Vec<String> = fs
            .list_status("/a/dir*/nested/*")
            .unwrap()
            .into_iter()
            .map(|status| status.path)
            .collect();
        paths.sort();
        assert_eq!(paths, ["/a/dir1/nested/first", "/a/dir2/nested/second"]);
        let mut children: Vec<String> = fs
            .list_status("/a/dir*/nested")
            .unwrap()
            .into_iter()
            .map(|status| status.path)
            .collect();
        children.sort();
        assert_eq!(children, paths);
        assert!(fs.list_status("/a/missing*/nested").unwrap().is_empty());
    }

    let start = std::sync::Arc::new(std::sync::Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let fs = fs.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..100 {
                    assert!(fs.exists("/a/dir1/nested/first").unwrap());
                    assert_eq!(
                        fs.file_status("/a/dir2/nested/second").unwrap().path,
                        "/a/dir2/nested/second"
                    );
                    assert_eq!(fs.list_status("/a/dir*/nested/*").unwrap().len(), 2);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
}

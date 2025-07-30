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

use crate::proto::*;
use crate::state::*;
use orpc::{try_err, CommonResult};
use prost::bytes::BytesMut;
use prost::Message;
use std::fmt::Debug;

pub struct ProtoUtils;

impl ProtoUtils {
    pub fn storage_info_to_pb(item: StorageInfo) -> StorageInfoProto {
        StorageInfoProto {
            dir_id: item.dir_id,
            storage_id: item.storage_id,
            failed: false,
            capacity: item.capacity,
            fs_used: item.fs_used,
            non_fs_used: item.non_fs_used,
            available: item.available,
            storage_type: item.storage_type.into(),
        }
    }

    pub fn storage_info_from_pb(item: StorageInfoProto) -> StorageInfo {
        StorageInfo {
            dir_id: item.dir_id,
            storage_id: item.storage_id,
            failed: false,
            capacity: item.capacity,
            fs_used: item.fs_used,
            non_fs_used: item.non_fs_used,
            available: item.available,
            storage_type: StorageType::from(item.storage_type),
        }
    }

    pub fn worker_address_to_pb(addr: &WorkerAddress) -> WorkerAddressProto {
        WorkerAddressProto {
            worker_id: addr.worker_id.to_owned(),
            hostname: addr.hostname.to_owned(),
            ip_addr: addr.ip_addr.to_owned(),
            rpc_port: addr.rpc_port,
            web_port: addr.web_port,
        }
    }

    pub fn worker_address_from_pb(addr: &WorkerAddressProto) -> WorkerAddress {
        WorkerAddress {
            worker_id: addr.worker_id.to_owned(),
            hostname: addr.hostname.to_owned(),
            ip_addr: addr.ip_addr.to_owned(),
            rpc_port: addr.rpc_port,
            web_port: addr.web_port,
        }
    }

    pub fn storage_info_list_from_pb(list: Vec<StorageInfoProto>) -> Vec<StorageInfo> {
        let mut res = Vec::with_capacity(list.len());
        for item in list {
            let info = Self::storage_info_from_pb(item);
            res.push(info)
        }

        res
    }

    pub fn client_address_from_pb(addr: ClientAddressProto) -> ClientAddress {
        ClientAddress {
            client_name: addr.client_name,
            hostname: addr.hostname,
            ip_addr: addr.ip_addr,
            port: addr.port,
        }
    }

    pub fn extend_block_from_pb(block: ExtendedBlockProto) -> ExtendedBlock {
        ExtendedBlock {
            id: block.id,
            len: block.block_size,
            storage_type: StorageType::from(block.storage_type),
            file_type: FileType::from(block.file_type),
        }
    }

    pub fn extend_block_to_pb(block: ExtendedBlock) -> ExtendedBlockProto {
        ExtendedBlockProto {
            id: block.id,
            block_size: block.len,
            storage_type: block.storage_type.into(),
            file_type: block.file_type.into(),
        }
    }

    pub fn block_location_from_pb(locations: BlockLocationProto) -> BlockLocation {
        BlockLocation {
            worker_id: locations.worker_id,
            storage_type: StorageType::from(locations.storage_type),
        }
    }

    pub fn block_location_to_pb(locations: BlockLocation) -> BlockLocationProto {
        BlockLocationProto {
            worker_id: locations.worker_id,
            storage_type: locations.storage_type.into(),
        }
    }

    pub fn commit_block_from_pb(block: CommitBlockProto) -> CommitBlock {
        let mut locations = vec![];
        for item in block.locations {
            locations.push(Self::block_location_from_pb(item))
        }

        CommitBlock {
            block_id: block.block_id,
            block_len: block.block_len,
            locations,
        }
    }

    pub fn commit_block_to_pb(block: CommitBlock) -> CommitBlockProto {
        let mut locations = vec![];
        for item in block.locations {
            locations.push(Self::block_location_to_pb(item))
        }

        CommitBlockProto {
            block_id: block.block_id,
            block_len: block.block_len,
            locations,
        }
    }

    pub fn located_block_to_pb(block: LocatedBlock) -> LocatedBlockProto {
        let b = Self::extend_block_to_pb(block.block);

        let locs: Vec<WorkerAddressProto> =
            block.locs.iter().map(Self::worker_address_to_pb).collect();

        LocatedBlockProto {
            block: b,
            offset: 0,
            locs,
        }
    }

    pub fn located_block_from_pb(block: LocatedBlockProto) -> LocatedBlock {
        let locs = block
            .locs
            .iter()
            .map(Self::worker_address_from_pb)
            .collect();

        LocatedBlock {
            block: Self::extend_block_from_pb(block.block),
            locs,
        }
    }

    pub fn encode<T: Message + Debug>(proto: T) -> CommonResult<BytesMut> {
        let mut bytes = BytesMut::with_capacity(proto.encoded_len());
        try_err!(proto.encode(&mut bytes));
        Ok(bytes)
    }

    pub fn storage_policy_to_pb(policy: StoragePolicy) -> StoragePolicyProto {
        StoragePolicyProto {
            storage_type: policy.storage_type.into(),
            ttl_ms: policy.ttl_ms,
            ttl_action: policy.ttl_action.into(),
            ufs_mtime: policy.ufs_mtime,
        }
    }

    pub fn storage_policy_from_pb(policy: StoragePolicyProto) -> StoragePolicy {
        StoragePolicy {
            storage_type: StorageType::from(policy.storage_type),
            ttl_ms: policy.ttl_ms,
            ttl_action: TtlAction::from(policy.ttl_action),
            ufs_mtime: policy.ufs_mtime,
        }
    }

    pub fn file_status_to_pb(status: FileStatus) -> FileStatusProto {
        FileStatusProto {
            id: status.id,
            path: status.path,
            name: status.name,
            is_dir: status.is_dir,
            mtime: status.mtime,
            atime: status.atime,
            children_num: status.children_num,
            is_complete: status.is_complete,
            len: status.len,
            replicas: status.replicas,
            block_size: status.block_size,
            file_type: status.file_type.into(),
            x_attr: status.x_attr,
            storage_policy: Self::storage_policy_to_pb(status.storage_policy),

            owner: status.owner,
            group: status.group,
            mode: status.mode,
        }
    }

    pub fn file_status_from_pb(status: FileStatusProto) -> FileStatus {
        FileStatus {
            id: status.id,
            path: status.path,
            name: status.name,
            is_dir: status.is_dir,
            mtime: status.mtime,
            atime: status.atime,
            children_num: status.children_num,
            is_complete: status.is_complete,
            len: status.len,
            replicas: status.replicas,
            block_size: status.block_size,
            file_type: FileType::from(status.file_type),
            x_attr: status.x_attr,
            storage_policy: Self::storage_policy_from_pb(status.storage_policy),
            owner: status.owner,
            group: status.group,
            mode: status.mode,
        }
    }

    pub fn file_blocks_to_pb(src: FileBlocks) -> FileBlocksProto {
        let block_locs: Vec<LocatedBlockProto> = src
            .block_locs
            .into_iter()
            .map(Self::located_block_to_pb)
            .collect();

        FileBlocksProto {
            status: Self::file_status_to_pb(src.status),
            block_locs,
        }
    }

    pub fn file_blocks_from_pb(src: FileBlocksProto) -> FileBlocks {
        let block_locs: Vec<LocatedBlock> = src
            .block_locs
            .into_iter()
            .map(Self::located_block_from_pb)
            .collect();

        FileBlocks {
            status: Self::file_status_from_pb(src.status),
            block_locs,
        }
    }

    pub fn master_info_to_pb(src: MasterInfo) -> GetMasterInfoResponse {
        let mut pb = GetMasterInfoResponse {
            active_master: src.active_master,
            journal_nodes: src.journal_nodes,
            inode_num: src.inode_num,
            block_num: src.block_num,
            capacity: src.capacity,
            available: src.available,
            fs_used: src.fs_used,
            non_fs_used: src.non_fs_used,
            ..Default::default()
        };

        for item in src.live_workers {
            pb.live_workers.push(Self::worker_info_to_pb(item));
        }

        for item in src.blacklist_workers {
            pb.blacklist_workers.push(Self::worker_info_to_pb(item));
        }

        for item in src.decommission_workers {
            pb.decommission_workers.push(Self::worker_info_to_pb(item));
        }

        for item in src.lost_workers {
            pb.lost_workers.push(Self::worker_info_to_pb(item));
        }

        pb
    }

    pub fn worker_info_to_pb(src: WorkerInfo) -> WorkerInfoProto {
        let mut pb = WorkerInfoProto {
            address: ProtoUtils::worker_address_to_pb(&src.address),
            capacity: src.capacity,
            available: src.available,
            fs_used: src.fs_used,
            non_fs_used: src.non_fs_used,
            last_update: src.last_update,
            storage_map: Default::default(),
        };

        for item in src.storage_map {
            pb.storage_map
                .insert(item.0, Self::storage_info_to_pb(item.1));
        }

        pb
    }

    pub fn master_info_from_pb(src: GetMasterInfoResponse) -> MasterInfo {
        MasterInfo {
            active_master: src.active_master,
            journal_nodes: src.journal_nodes,
            inode_num: src.inode_num,
            block_num: src.block_num,
            capacity: src.capacity,
            available: src.available,
            fs_used: src.fs_used,
            non_fs_used: src.non_fs_used,
            live_workers: Self::worker_info_from_pb(src.live_workers),
            blacklist_workers: Self::worker_info_from_pb(src.blacklist_workers),
            decommission_workers: Self::worker_info_from_pb(src.decommission_workers),
            lost_workers: Self::worker_info_from_pb(src.lost_workers),
        }
    }

    pub fn worker_info_from_pb(workers: Vec<WorkerInfoProto>) -> Vec<WorkerInfo> {
        let mut vec = vec![];
        for info in workers {
            let mut worker_info = WorkerInfo {
                address: Self::worker_address_from_pb(&info.address),
                capacity: info.capacity,
                available: info.available,
                fs_used: info.fs_used,
                non_fs_used: info.non_fs_used,
                ..Default::default()
            };
            for (k, v) in info.storage_map {
                worker_info
                    .storage_map
                    .insert(k, Self::storage_info_from_pb(v));
            }
            vec.push(worker_info);
        }

        vec
    }

    pub fn worker_cmd_to_pb(cmds: Vec<WorkerCommand>) -> Vec<WorkerCommandProto> {
        let mut vec = vec![];
        for cmd in cmds {
            match cmd {
                WorkerCommand::DeleteBlock(cmd) => {
                    let pb_cmd = WorkerCommandProto {
                        delete_block: Some(DeleteBlockCmdProto { blocks: cmd.blocks }),
                    };
                    vec.push(pb_cmd)
                }
            }
        }

        vec
    }

    pub fn worker_cmd_from_pb(cmds: Vec<WorkerCommandProto>) -> Vec<WorkerCommand> {
        let mut vec = vec![];
        for cmd in cmds {
            if let Some(c) = cmd.delete_block {
                let my_cmd = WorkerCommand::DeleteBlock(DeleteBlockCmd { blocks: c.blocks });
                vec.push(my_cmd);
            }
        }
        vec
    }

    pub fn block_report_list_from_pb(list: BlockReportListRequest) -> BlockReportList {
        let mut dst = BlockReportList {
            cluster_id: list.cluster_id,
            worker_id: list.worker_id,
            full_report: list.full_report,
            total_len: list.total_len,
            blocks: vec![],
        };
        for block in list.blocks {
            let dst_blocks = BlockReportInfo {
                id: block.id,
                status: BlockReportStatus::from(block.status),
                storage_type: StorageType::from(block.storage_type),
                block_size: block.block_size,
            };
            dst.blocks.push(dst_blocks);
        }
        dst
    }

    pub fn set_attr_opts_to_pb(opts: SetAttrOpts) -> SetAttrOptsProto {
        SetAttrOptsProto {
            recursive: opts.recursive,
            replicas: opts.replicas,
            owner: opts.owner,
            group: opts.group,
            mode: opts.mode,
            ttl_ms: opts.ttl_ms,
            ttl_action: opts.ttl_action.map(|v| v as i32),
            add_x_attr: opts.add_x_attr,
            remove_x_attr: opts.remove_x_attr,
        }
    }

    pub fn set_attr_opts_from_pb(opts: SetAttrOptsProto) -> SetAttrOpts {
        SetAttrOpts {
            recursive: opts.recursive,
            replicas: opts.replicas,
            owner: opts.owner,
            group: opts.group,
            mode: opts.mode,
            ttl_ms: opts.ttl_ms,
            ttl_action: opts.ttl_action.map(TtlAction::from),
            add_x_attr: opts.add_x_attr,
            remove_x_attr: opts.remove_x_attr,
        }
    }
}

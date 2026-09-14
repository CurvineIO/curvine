use curvine_client::file::CurvineFileSystem;
use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Writer};
use curvine_model::StorageType;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::master::fs::MasterFilesystem;
use curvine_tests::Testing;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn single_file_writers_persist_actual_storage_type() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let local_fs = testing.get_fs(Some(rt.clone()), Some(conf.clone()))?;

    let mut remote_conf = conf;
    remote_conf.client.short_circuit = false;
    let remote_fs = testing.get_fs(Some(rt.clone()), Some(remote_conf))?;
    let master_fs = cluster.get_active_master_fs();

    rt.block_on(async move {
        assert_actual_disk_location(&local_fs, &master_fs, "/actual-storage/local.data").await?;
        assert_actual_disk_location(&remote_fs, &master_fs, "/actual-storage/remote.data").await?;
        Ok(())
    })
}

#[test]
fn replicas_on_different_workers_persist_each_actual_storage_type() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(2)
        .mutate_worker_conf(|index, conf| {
            let storage_type = if index == 0 { "MEM" } else { "DISK" };
            conf.worker.data_dir = conf
                .worker
                .data_dir
                .iter()
                .map(|path| format!("[{storage_type}:256MB]{path}"))
                .collect();
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let mem_worker_port = cluster.worker_conf[0].worker.rpc_port as u32;
    let disk_worker_port = cluster.worker_conf[1].worker.rpc_port as u32;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.replicas = 2;
    conf.client.short_circuit = false;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let master_fs = cluster.get_active_master_fs();

    rt.block_on(async move {
        let path = Path::from_str("/actual-storage/mixed-workers.data")?;
        let opts = fs
            .create_opts_builder()
            .create_parent(true)
            .replicas(2)
            .storage_type(StorageType::Mem)
            .build();
        let mut writer = fs.create_with_opts(&path, opts, false).await?;
        writer.write(b"mixed-worker-storage-types").await?;
        writer.complete().await?;

        let blocks = fs.get_block_locations(&path).await?;
        assert_eq!(blocks.block_locs.len(), 1);
        assert_eq!(blocks.block_locs[0].locs.len(), 2);

        let locations = master_fs
            .fs_dir
            .read()
            .get_block_locations(blocks.block_locs[0].block.id)?;
        assert_eq!(locations.len(), 2);

        let actual_types = locations
            .iter()
            .map(|location| (location.worker_id, location.storage_type))
            .collect::<HashMap<_, _>>();
        let expected_types = blocks.block_locs[0]
            .locs
            .iter()
            .map(|address| {
                let storage_type = if address.rpc_port == mem_worker_port {
                    StorageType::Mem
                } else if address.rpc_port == disk_worker_port {
                    StorageType::Disk
                } else {
                    panic!("unexpected worker RPC port: {}", address.rpc_port);
                };
                (address.worker_id, storage_type)
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(actual_types, expected_types);
        Ok(())
    })
}

async fn assert_actual_disk_location(
    fs: &CurvineFileSystem,
    master_fs: &MasterFilesystem,
    path: &str,
) -> CommonResult<()> {
    let path = Path::from_str(path)?;
    let opts = fs
        .create_opts_builder()
        .create_parent(true)
        .storage_type(StorageType::Mem)
        .build();
    let mut writer = fs.create_with_opts(&path, opts, false).await?;
    writer.write(b"actual-storage-type").await?;
    writer.complete().await?;

    let blocks = fs.get_block_locations(&path).await?;
    assert_eq!(blocks.status.storage_policy.storage_type, StorageType::Mem);
    assert_eq!(blocks.block_locs.len(), 1);
    assert_eq!(blocks.block_locs[0].block.storage_type, StorageType::Mem);

    let locations = master_fs
        .fs_dir
        .read()
        .get_block_locations(blocks.block_locs[0].block.id)?;
    assert_eq!(locations.len(), 1);
    assert_eq!(
        locations[0].worker_id,
        blocks.block_locs[0].locs[0].worker_id
    );
    assert_eq!(locations[0].storage_type, StorageType::Disk);
    Ok(())
}

use super::super::inode::{InodeDir, InodeView};
use super::super::{FsDir, MetadataReplicaReader};
use curvine_core_error::CommonResult;

#[test]
fn topology_reader_rejects_stale_directory_and_root_generations() -> CommonResult<()> {
    let root = FsDir::create_root();
    let reader = MetadataReplicaReader::new(&root)?;

    let missing = reader.resolve("/dir")?;
    assert!(!missing.is_full());
    assert!(reader.validate(&missing));

    root.as_dir_ref()?
        .children_handle()
        .add_child(InodeView::new_dir(
            "dir".to_string(),
            InodeDir::new(2001, 0),
        ))?;
    assert!(!reader.validate(&missing));

    let resolved = reader.resolve("/dir")?;
    assert!(resolved.is_full());
    assert!(reader.validate(&resolved));

    reader.replace_root(&FsDir::create_root())?;
    assert!(!reader.validate(&resolved));
    Ok(())
}

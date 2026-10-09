use super::test_fs;
use curvine_error::FsError;
use curvine_fs_api::GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS;

#[test]
fn batch_preserves_request_order_and_individual_errors() {
    let fs = test_fs("get-block-locations-batch");
    fs.create("/first", true).unwrap();
    fs.create("/second", true).unwrap();

    let results = fs
        .get_block_locations_batch(&[
            "/second".to_string(),
            "/missing".to_string(),
            "/first".to_string(),
        ])
        .unwrap()
        .collect::<Vec<_>>();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().status.path, "/second");
    assert!(matches!(&results[1], Err(FsError::FileNotFound(_))));
    assert_eq!(results[2].as_ref().unwrap().status.path, "/first");
}

#[test]
fn batch_rejects_more_than_the_protocol_limit() {
    let fs = test_fs("get-block-locations-batch-limit");
    let paths = vec!["/missing".to_string(); GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS + 1];
    assert!(matches!(
        fs.get_block_locations_batch(&paths),
        Err(FsError::InvalidArgument(_))
    ));
}

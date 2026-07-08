use curvine_common::error::{ErrorKind, FsError};
use orpc::error::ErrorExt;

#[test]
fn block_not_found_round_trips_with_structured_kind() {
    let error = FsError::block_not_found(42);
    assert!(matches!(error.kind(), ErrorKind::BlockNotFound));

    let decoded = FsError::decode(error.encode());
    assert!(matches!(decoded.kind(), ErrorKind::BlockNotFound));

    match decoded {
        FsError::BlockNotFound(error) => {
            assert_eq!(error.source.to_string(), "Block 42 not found");
        }
        other => panic!("expected BlockNotFound, got {:?}", other),
    }
}

#[test]
fn resource_exhausted_round_trips_with_structured_kind() {
    let error = FsError::resource_exhausted("master load job queue is full; retry later");
    assert!(matches!(error.kind(), ErrorKind::ResourceExhausted));

    let decoded = FsError::decode(error.encode());
    assert!(matches!(decoded.kind(), ErrorKind::ResourceExhausted));

    match decoded {
        FsError::ResourceExhausted(error) => {
            assert_eq!(
                error.source.to_string(),
                "master load job queue is full; retry later"
            );
        }
        other => panic!("expected ResourceExhausted, got {:?}", other),
    }
}

use curvine_vector_gateway::sig_v4::{
    get_v4_signature, CanonicalQueryPair, EMPTY_PAYLOAD_HASH, SIGV4_SERVICE_S3VECTORS,
};

use super::harness::StaticHeaders;

#[test]
fn query_parameter_order_does_not_change_signature() {
    let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    let region = "us-east-1";

    let h = StaticHeaders(vec![
        ("host".into(), "vectors.example.com".into()),
        ("x-amz-date".into(), "20260101T000000Z".into()),
        ("x-amz-content-sha256".into(), EMPTY_PAYLOAD_HASH.into()),
    ]);

    let signed_headers = vec!["host", "x-amz-date"];

    let q_fwd = vec![
        CanonicalQueryPair {
            key: "b".into(),
            val: "2".into(),
        },
        CanonicalQueryPair {
            key: "a".into(),
            val: "1".into(),
        },
        CanonicalQueryPair {
            key: "a".into(),
            val: "0".into(),
        },
    ];
    let mut q_rev = q_fwd.clone();
    q_rev.reverse();

    let (sig_fwd, _) = get_v4_signature(
        &h,
        "POST",
        region,
        SIGV4_SERVICE_S3VECTORS,
        "/CreateIndex",
        secret,
        EMPTY_PAYLOAD_HASH,
        &signed_headers,
        q_fwd,
    )
    .unwrap();

    let (sig_rev, _) = get_v4_signature(
        &h,
        "POST",
        region,
        SIGV4_SERVICE_S3VECTORS,
        "/CreateIndex",
        secret,
        EMPTY_PAYLOAD_HASH,
        &signed_headers,
        q_rev,
    )
    .unwrap();

    assert_eq!(sig_fwd, sig_rev);
}

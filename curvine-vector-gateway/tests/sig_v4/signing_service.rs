use curvine_vector_gateway::sig_v4::{get_v4_signature, EMPTY_PAYLOAD_HASH};
use curvine_vector_gateway::sig_v4::{SIGV4_SERVICE_S3, SIGV4_SERVICE_S3VECTORS};

use super::harness::StaticHeaders;

#[test]
fn signing_differs_when_credential_scope_service_differs() {
    let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    let region = "us-east-1";

    let h = StaticHeaders(vec![
        ("host".into(), "vectors.example.com".into()),
        ("x-amz-date".into(), "20260101T000000Z".into()),
        ("x-amz-content-sha256".into(), EMPTY_PAYLOAD_HASH.into()),
    ]);

    let signed_headers = vec!["host", "x-amz-date"];
    let (sig_s3vectors, _) = get_v4_signature(
        &h,
        "POST",
        region,
        SIGV4_SERVICE_S3VECTORS,
        "/CreateIndex",
        secret,
        EMPTY_PAYLOAD_HASH,
        &signed_headers,
        vec![],
    )
    .unwrap();

    let (sig_s3, _) = get_v4_signature(
        &h,
        "POST",
        region,
        SIGV4_SERVICE_S3,
        "/CreateIndex",
        secret,
        EMPTY_PAYLOAD_HASH,
        &signed_headers,
        vec![],
    )
    .unwrap();

    assert_eq!(sig_s3vectors.len(), 64);
    assert_eq!(sig_s3.len(), 64);
    assert_ne!(sig_s3vectors, sig_s3);
}

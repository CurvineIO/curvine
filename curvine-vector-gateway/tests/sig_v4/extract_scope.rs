use curvine_vector_gateway::sig_v4::{extract_args, EMPTY_PAYLOAD_HASH};

use super::harness::StaticHeaders;

#[test]
fn extract_args_reads_service_from_credential_scope() {
    let h = StaticHeaders(vec![
        (
            "authorization".into(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3vectors/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".into(),
        ),
        ("x-amz-date".into(), "20260101T000000Z".into()),
        ("x-amz-content-sha256".into(), EMPTY_PAYLOAD_HASH.into()),
    ]);
    let args = extract_args(&h).unwrap();
    assert_eq!(args.service.as_str(), "s3vectors");
    assert_eq!(args.region.as_str(), "us-east-1");
}

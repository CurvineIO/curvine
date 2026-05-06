use curvine_vector_gateway::sig_v4::{extract_args, EMPTY_PAYLOAD_HASH};

use super::harness::StaticHeaders;

fn auth_hdr(credential_date_path: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/{credential_date_path}/us-east-1/s3vectors/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    )
}

#[test]
fn extract_rejects_missing_x_amz_date() {
    let h = StaticHeaders(vec![
        ("authorization".into(), auth_hdr("20260101")),
        ("x-amz-content-sha256".into(), EMPTY_PAYLOAD_HASH.into()),
    ]);
    assert!(extract_args(&h).is_err());
}

#[test]
fn extract_rejects_malformed_x_amz_date() {
    let h = StaticHeaders(vec![
        ("authorization".into(), auth_hdr("20260101")),
        ("x-amz-date".into(), "abcdefgh".into()),
        ("x-amz-content-sha256".into(), EMPTY_PAYLOAD_HASH.into()),
    ]);
    assert!(extract_args(&h).is_err());
}

#[test]
fn extract_rejects_date_mismatch_with_credential_scope() {
    let h = StaticHeaders(vec![
        ("authorization".into(), auth_hdr("20260102")),
        ("x-amz-date".into(), "20260101T000000Z".into()),
        ("x-amz-content-sha256".into(), EMPTY_PAYLOAD_HASH.into()),
    ]);
    assert!(extract_args(&h).is_err());
}

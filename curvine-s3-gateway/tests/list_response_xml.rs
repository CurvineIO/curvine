// Regression coverage for the XML shape of a listing entry. quick-xml
// serializes a bare `Option` without `skip_serializing_if` as an empty
// `<Owner/>` element instead of dropping it, and botocore reads that empty
// element as a present-but-incomplete Owner - exactly what aws-chunked and the
// s3tests listing suites saw (`test_object_list_fetchowner_defaultempty`).

use curvine_s3_gateway::s3::dto::ListObjectContent;
use curvine_s3_gateway::s3::dto::Owner;

fn list_entry(owner: Option<Owner>) -> ListObjectContent {
    ListObjectContent {
        key: "test/foo".to_string(),
        last_modified: None,
        etag: Some("\"etag\"".to_string()),
        size: 1,
        storage_class: None,
        owner,
    }
}

#[test]
fn a_missing_owner_is_omitted_not_serialised_empty() {
    let xml = quick_xml::se::to_string(&list_entry(None)).unwrap();
    assert!(!xml.contains("Owner"), "owner-less entry emitted {xml}");
    assert!(xml.contains("<Key>test/foo</Key>"));
}

#[test]
fn a_present_owner_is_serialised() {
    let owner = Owner {
        id: "id".to_string(),
        display_name: "name".to_string(),
    };
    let xml = quick_xml::se::to_string(&list_entry(Some(owner))).unwrap();
    assert!(xml.contains("<Owner>"), "{xml}");
    assert!(xml.contains("<ID>id</ID>"), "{xml}");
}

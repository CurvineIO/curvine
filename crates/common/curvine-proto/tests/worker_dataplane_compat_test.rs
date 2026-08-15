use curvine_proto::{
    BlockReadRequest, BlockWriteRequest, BlocksBatchCommitRequest, BlocksBatchWriteRequest,
    ComponentInfoProto, ExtendedBlockProto, FileTypeProto, FileWriteData, FilesBatchWriteRequest,
    StorageTypeProto,
};
use prost::Message;

fn sample_component_info() -> ComponentInfoProto {
    ComponentInfoProto {
        component: Some("client".to_string()),
        release_version: Some("0.4.0-alpha".to_string()),
        git_commit: Some("359fce7d982a15f09c3b4e0b2e62fee4229609dd".to_string()),
        git_tag: Some("v0.4.0-alpha".to_string()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: vec!["short-circuit".to_string(), "batch-write".to_string()],
    }
}

fn sample_block() -> ExtendedBlockProto {
    ExtendedBlockProto {
        id: 42,
        block_size: 4 * 1024 * 1024,
        storage_type: StorageTypeProto::Mem as i32,
        file_type: FileTypeProto::File as i32,
        alloc_opts: None,
    }
}

/// Append a raw varint field (field number, wire type 0) to a wire buffer.
fn push_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// Append a length-delimited field (wire type 2) to a wire buffer.
fn push_len_delimited(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    push_varint(buf, ((field as u64) << 3) | 2);
    push_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

// ---------------------------------------------------------------------------
// Legacy wire views: the exact messages a pre-change client actually sent,
// with no component_info on the reserved 1000+ range. Decoding these through
// the new types must leave component_info = None (old client + new worker).
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyBlockWriteRequest {
    #[prost(message, required, tag = "1")]
    block: ExtendedBlockProto,
    #[prost(int64, required, tag = "2")]
    off: i64,
    #[prost(int64, required, tag = "3")]
    block_size: i64,
    #[prost(bool, required, tag = "4")]
    short_circuit: bool,
    #[prost(string, required, tag = "5")]
    client_name: String,
    #[prost(int32, required, tag = "6")]
    chunk_size: i32,
    #[prost(message, repeated, tag = "7")]
    pipeline_stream: Vec<curvine_proto::WorkerAddressProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyBlockReadRequest {
    #[prost(int64, required, tag = "1")]
    id: i64,
    #[prost(int64, required, tag = "2")]
    off: i64,
    #[prost(int64, required, tag = "3")]
    len: i64,
    #[prost(int32, required, tag = "4")]
    chunk_size: i32,
    #[prost(bool, required, tag = "5")]
    short_circuit: bool,
    #[prost(bool, required, tag = "8")]
    enable_read_ahead: bool,
    #[prost(int64, required, tag = "9")]
    read_ahead_len: i64,
    #[prost(int64, required, tag = "10")]
    drop_cache_len: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyBlocksBatchWriteRequest {
    #[prost(message, repeated, tag = "1")]
    blocks: Vec<ExtendedBlockProto>,
    #[prost(int64, required, tag = "2")]
    off: i64,
    #[prost(int64, required, tag = "3")]
    block_size: i64,
    #[prost(int64, required, tag = "4")]
    req_id: i64,
    #[prost(int32, required, tag = "5")]
    seq_id: i32,
    #[prost(int32, required, tag = "6")]
    chunk_size: i32,
    #[prost(bool, required, tag = "7")]
    short_circuit: bool,
    #[prost(string, required, tag = "8")]
    client_name: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyBlocksBatchCommitRequest {
    #[prost(message, repeated, tag = "1")]
    blocks: Vec<ExtendedBlockProto>,
    #[prost(int64, required, tag = "2")]
    off: i64,
    #[prost(int64, required, tag = "3")]
    block_size: i64,
    #[prost(int64, required, tag = "4")]
    req_id: i64,
    #[prost(int32, required, tag = "5")]
    seq_id: i32,
    #[prost(bool, required, tag = "6")]
    cancel: bool,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyFilesBatchWriteRequest {
    #[prost(message, repeated, tag = "1")]
    files: Vec<FileWriteData>,
    #[prost(int64, required, tag = "2")]
    req_id: i64,
    #[prost(int32, required, tag = "3")]
    seq_id: i32,
}

#[test]
fn test_block_write_component_info_round_trip() {
    // New client -> new worker: component info survives the wire on the
    // reserved 1000+ range of BlockWriteRequest.
    let req = BlockWriteRequest {
        block: sample_block(),
        off: 0,
        block_size: 4 * 1024 * 1024,
        short_circuit: true,
        client_name: "client-1".to_string(),
        chunk_size: 1 << 20,
        pipeline_stream: vec![],
        component_info: Some(sample_component_info()),
    };

    let encoded = req.encode_to_vec();
    let decoded = BlockWriteRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("client".to_string()));
    assert_eq!(info.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(info.protocol_version, Some(1));
    assert_eq!(decoded.client_name, "client-1");
    assert_eq!(decoded.block.id, 42);
    assert_eq!(decoded.off, 0);
    assert_eq!(decoded.block_size, 4 * 1024 * 1024);
}

#[test]
fn test_block_read_component_info_round_trip() {
    // New client -> new worker: component info survives the wire on the
    // reserved 1000+ range of BlockReadRequest.
    let req = BlockReadRequest {
        id: 42,
        off: 0,
        len: 4096,
        chunk_size: 1 << 20,
        short_circuit: false,
        enable_read_ahead: true,
        read_ahead_len: 4 * 1024 * 1024,
        drop_cache_len: 1 << 20,
        component_info: Some(sample_component_info()),
    };

    let encoded = req.encode_to_vec();
    let decoded = BlockReadRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("client".to_string()));
    assert_eq!(info.protocol_version, Some(1));
    assert_eq!(decoded.id, 42);
    assert_eq!(decoded.len, 4096);
    assert!(decoded.enable_read_ahead);
    assert_eq!(decoded.read_ahead_len, 4 * 1024 * 1024);
}

#[test]
fn test_blocks_batch_write_component_info_round_trip() {
    // New client -> new worker: component info survives on BlocksBatchWriteRequest.
    let req = BlocksBatchWriteRequest {
        blocks: vec![sample_block(), sample_block()],
        off: 0,
        block_size: 4 * 1024 * 1024,
        req_id: 7,
        seq_id: 1,
        chunk_size: 1 << 20,
        short_circuit: true,
        client_name: "client-1".to_string(),
        component_info: Some(sample_component_info()),
    };

    let encoded = req.encode_to_vec();
    let decoded = BlocksBatchWriteRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("client".to_string()));
    assert_eq!(info.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(decoded.blocks.len(), 2);
    assert_eq!(decoded.req_id, 7);
    assert_eq!(decoded.client_name, "client-1");
}

#[test]
fn test_blocks_batch_commit_component_info_round_trip() {
    // New client -> new worker: component info survives on BlocksBatchCommitRequest.
    let req = BlocksBatchCommitRequest {
        blocks: vec![sample_block()],
        off: 0,
        block_size: 4 * 1024 * 1024,
        req_id: 7,
        seq_id: 1,
        cancel: false,
        component_info: Some(sample_component_info()),
    };

    let encoded = req.encode_to_vec();
    let decoded = BlocksBatchCommitRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("client".to_string()));
    assert_eq!(info.min_protocol_version, Some(1));
    assert_eq!(decoded.blocks.len(), 1);
    assert_eq!(decoded.req_id, 7);
    assert!(!decoded.cancel);
}

#[test]
fn test_files_batch_write_component_info_round_trip() {
    // New client -> new worker: component info survives on FilesBatchWriteRequest.
    let req = FilesBatchWriteRequest {
        files: vec![FileWriteData {
            path: "/dir/a".to_string(),
            content: b"hello".to_vec(),
        }],
        req_id: 7,
        seq_id: 2,
        component_info: Some(sample_component_info()),
    };

    let encoded = req.encode_to_vec();
    let decoded = FilesBatchWriteRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("client".to_string()));
    assert_eq!(info.capabilities.len(), 2);
    assert_eq!(decoded.files.len(), 1);
    assert_eq!(decoded.files[0].path, "/dir/a");
    assert_eq!(decoded.files[0].content, b"hello".to_vec());
    assert_eq!(decoded.req_id, 7);
    assert_eq!(decoded.seq_id, 2);
}

#[test]
fn test_block_write_legacy_client_decodes() {
    // Old client + new worker: a legacy BlockWriteRequest without
    // component_info must decode; the worker treats absence as a
    // legacy/unknown peer.
    let legacy = LegacyBlockWriteRequest {
        block: sample_block(),
        off: 1024,
        block_size: 4 * 1024 * 1024,
        short_circuit: true,
        client_name: "legacy-client".to_string(),
        chunk_size: 1 << 20,
        pipeline_stream: vec![],
    };

    let encoded = legacy.encode_to_vec();
    let decoded = BlockWriteRequest::decode(encoded.as_slice()).unwrap();

    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.block.id, 42);
    assert_eq!(decoded.off, 1024);
    assert_eq!(decoded.block_size, 4 * 1024 * 1024);
    assert!(decoded.short_circuit);
    assert_eq!(decoded.client_name, "legacy-client");
    assert_eq!(decoded.chunk_size, 1 << 20);
    assert!(decoded.pipeline_stream.is_empty());
}

#[test]
fn test_block_read_legacy_client_decodes() {
    // Old client + new worker: a legacy BlockReadRequest without
    // component_info must decode.
    let legacy = LegacyBlockReadRequest {
        id: 42,
        off: 0,
        len: 4096,
        chunk_size: 1 << 20,
        short_circuit: false,
        enable_read_ahead: true,
        read_ahead_len: 4 * 1024 * 1024,
        drop_cache_len: 1 << 20,
    };

    let encoded = legacy.encode_to_vec();
    let decoded = BlockReadRequest::decode(encoded.as_slice()).unwrap();

    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.id, 42);
    assert_eq!(decoded.off, 0);
    assert_eq!(decoded.len, 4096);
    assert_eq!(decoded.chunk_size, 1 << 20);
    assert!(!decoded.short_circuit);
    assert!(decoded.enable_read_ahead);
    assert_eq!(decoded.read_ahead_len, 4 * 1024 * 1024);
    assert_eq!(decoded.drop_cache_len, 1 << 20);
}

#[test]
fn test_blocks_batch_write_legacy_client_decodes() {
    // Old client + new worker: a legacy BlocksBatchWriteRequest without
    // component_info must decode.
    let legacy = LegacyBlocksBatchWriteRequest {
        blocks: vec![sample_block()],
        off: 0,
        block_size: 4 * 1024 * 1024,
        req_id: 7,
        seq_id: 1,
        chunk_size: 1 << 20,
        short_circuit: true,
        client_name: "legacy-client".to_string(),
    };

    let encoded = legacy.encode_to_vec();
    let decoded = BlocksBatchWriteRequest::decode(encoded.as_slice()).unwrap();

    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.blocks.len(), 1);
    assert_eq!(decoded.blocks[0].id, 42);
    assert_eq!(decoded.req_id, 7);
    assert_eq!(decoded.seq_id, 1);
    assert!(decoded.short_circuit);
    assert_eq!(decoded.client_name, "legacy-client");
}

#[test]
fn test_blocks_batch_commit_legacy_client_decodes() {
    // Old client + new worker: a legacy BlocksBatchCommitRequest without
    // component_info must decode.
    let legacy = LegacyBlocksBatchCommitRequest {
        blocks: vec![sample_block()],
        off: 0,
        block_size: 4 * 1024 * 1024,
        req_id: 7,
        seq_id: 1,
        cancel: false,
    };

    let encoded = legacy.encode_to_vec();
    let decoded = BlocksBatchCommitRequest::decode(encoded.as_slice()).unwrap();

    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.blocks.len(), 1);
    assert_eq!(decoded.req_id, 7);
    assert_eq!(decoded.seq_id, 1);
    assert!(!decoded.cancel);
}

#[test]
fn test_files_batch_write_legacy_client_decodes() {
    // Old client + new worker: a legacy FilesBatchWriteRequest without
    // component_info must decode.
    let legacy = LegacyFilesBatchWriteRequest {
        files: vec![FileWriteData {
            path: "/dir/a".to_string(),
            content: b"hello".to_vec(),
        }],
        req_id: 7,
        seq_id: 2,
    };

    let encoded = legacy.encode_to_vec();
    let decoded = FilesBatchWriteRequest::decode(encoded.as_slice()).unwrap();

    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.files.len(), 1);
    assert_eq!(decoded.files[0].path, "/dir/a");
    assert_eq!(decoded.files[0].content, b"hello".to_vec());
    assert_eq!(decoded.req_id, 7);
    assert_eq!(decoded.seq_id, 2);
}

#[test]
fn test_dataplane_unknown_high_fields_are_skipped() {
    // Forward compatibility: a peer that sends fields this build does not
    // know about (e.g. a later reserved-range field beyond 1000) must be
    // decoded and those fields silently skipped — the same mechanism legacy
    // components rely on to ignore our new component_info field.
    let req = BlockWriteRequest {
        block: sample_block(),
        off: 0,
        block_size: 4 * 1024 * 1024,
        short_circuit: true,
        client_name: "client-1".to_string(),
        chunk_size: 1 << 20,
        pipeline_stream: vec![],
        component_info: Some(sample_component_info()),
    };

    let mut encoded = req.encode_to_vec();
    // Append an unknown length-delimited field 1001 (wire type 2).
    push_len_delimited(&mut encoded, 1001, b"future-metadata");

    let decoded = BlockWriteRequest::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.component_info, req.component_info);
    assert_eq!(decoded.client_name, "client-1");
}

#[cfg(test)]
mod tests {
    use curvine_common::proto::{
        BlockWriteRequest, BlockWriteResponse, ExtendedBlockProto, PipelineStatus,
        StorageTypeProto, WorkerAddressProto,
    };
    use prost::Message;

    fn create_test_block() -> ExtendedBlockProto {
        ExtendedBlockProto {
            id: 12345,
            block_size: 1024 * 1024,
            storage_type: StorageTypeProto::Disk as i32,
            file_type: 1,
            alloc_opts: None,
        }
    }

    fn create_test_worker_address(worker_id: u32) -> WorkerAddressProto {
        WorkerAddressProto {
            worker_id,
            hostname: format!("worker-{}", worker_id),
            ip_addr: format!("192.168.1.{}", worker_id),
            rpc_port: 9000 + worker_id,
            web_port: 8000 + worker_id,
        }
    }

    #[test]
    fn test_block_write_request_round_trip_without_pipeline_stream() {
        let request = BlockWriteRequest {
            block: create_test_block(),
            off: 0,
            block_size: 64 * 1024 * 1024,
            short_circuit: false,
            client_name: "test-client".to_string(),
            chunk_size: 1024 * 1024,
            pipeline_stream: vec![],
        };

        let encoded = request.encode_to_vec();
        let decoded = BlockWriteRequest::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.block.id, request.block.id);
        assert_eq!(decoded.off, request.off);
        assert_eq!(decoded.block_size, request.block_size);
        assert_eq!(decoded.short_circuit, request.short_circuit);
        assert_eq!(decoded.client_name, request.client_name);
        assert_eq!(decoded.chunk_size, request.chunk_size);
        assert!(decoded.pipeline_stream.is_empty());
    }

    #[test]
    fn test_block_write_request_round_trip_with_pipeline_stream() {
        let pipeline_stream = vec![create_test_worker_address(2), create_test_worker_address(3)];

        let request = BlockWriteRequest {
            block: create_test_block(),
            off: 0,
            block_size: 64 * 1024 * 1024,
            short_circuit: false,
            client_name: "test-client".to_string(),
            chunk_size: 1024 * 1024,
            pipeline_stream: pipeline_stream.clone(),
        };

        let encoded = request.encode_to_vec();
        let decoded = BlockWriteRequest::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.pipeline_stream.len(), 2);
        assert_eq!(decoded.pipeline_stream[0].worker_id, 2);
        assert_eq!(decoded.pipeline_stream[1].worker_id, 3);
        assert_eq!(decoded.pipeline_stream[0].ip_addr, "192.168.1.2");
        assert_eq!(decoded.pipeline_stream[1].ip_addr, "192.168.1.3");
    }

    #[test]
    fn test_pipeline_status_round_trip_success() {
        let status = PipelineStatus {
            success: true,
            established_count: 3,
            failed_worker: None,
            error_message: None,
        };

        let encoded = status.encode_to_vec();
        let decoded = PipelineStatus::decode(encoded.as_slice()).unwrap();

        assert!(decoded.success);
        assert_eq!(decoded.established_count, 3);
        assert!(decoded.failed_worker.is_none());
        assert!(decoded.error_message.is_none());
    }

    #[test]
    fn test_pipeline_status_round_trip_failure() {
        let failed_worker = create_test_worker_address(2);
        let status = PipelineStatus {
            success: false,
            established_count: 1,
            failed_worker: Some(failed_worker),
            error_message: Some("Connection refused".to_string()),
        };

        let encoded = status.encode_to_vec();
        let decoded = PipelineStatus::decode(encoded.as_slice()).unwrap();

        assert!(!decoded.success);
        assert_eq!(decoded.established_count, 1);
        assert!(decoded.failed_worker.is_some());
        assert_eq!(decoded.failed_worker.as_ref().unwrap().worker_id, 2);
        assert_eq!(
            decoded.error_message.as_ref().unwrap(),
            "Connection refused"
        );
    }

    #[test]
    fn test_block_write_response_round_trip_without_pipeline_status() {
        let response = BlockWriteResponse {
            id: 12345,
            path: Some("/data/block/12345".to_string()),
            off: 0,
            block_size: 64 * 1024 * 1024,
            storage_type: StorageTypeProto::Disk as i32,
            pipeline_status: None,
        };

        let encoded = response.encode_to_vec();
        let decoded = BlockWriteResponse::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.id, response.id);
        assert_eq!(decoded.path, response.path);
        assert_eq!(decoded.off, response.off);
        assert_eq!(decoded.block_size, response.block_size);
        assert_eq!(decoded.storage_type, response.storage_type);
        assert!(decoded.pipeline_status.is_none());
    }

    #[test]
    fn test_block_write_response_round_trip_with_pipeline_status() {
        let status = PipelineStatus {
            success: true,
            established_count: 3,
            failed_worker: None,
            error_message: None,
        };

        let response = BlockWriteResponse {
            id: 12345,
            path: None,
            off: 0,
            block_size: 64 * 1024 * 1024,
            storage_type: StorageTypeProto::Disk as i32,
            pipeline_status: Some(status),
        };

        let encoded = response.encode_to_vec();
        let decoded = BlockWriteResponse::decode(encoded.as_slice()).unwrap();

        assert!(decoded.pipeline_status.is_some());
        let decoded_status = decoded.pipeline_status.unwrap();
        assert!(decoded_status.success);
        assert_eq!(decoded_status.established_count, 3);
    }

    #[test]
    fn test_default_values() {
        let request = BlockWriteRequest {
            block: create_test_block(),
            off: 0,
            block_size: 64 * 1024 * 1024,
            short_circuit: false,
            client_name: String::new(),
            chunk_size: 0,
            pipeline_stream: vec![],
        };

        let encoded = request.encode_to_vec();
        let decoded = BlockWriteRequest::decode(encoded.as_slice()).unwrap();

        assert!(!decoded.short_circuit);
        assert!(decoded.client_name.is_empty());
        assert!(decoded.pipeline_stream.is_empty());
    }
}

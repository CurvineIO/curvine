# Curvine S3 Object Gateway - Technical Implementation Guide

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Core Components](#core-components)
4. [Request Processing Flow](#request-processing-flow)
5. [S3 API Implementation](#s3-api-implementation)
6. [Authentication & Authorization](#authentication--authorization)
7. [Error Handling Strategy](#error-handling-strategy)
8. [Performance Considerations](#performance-considerations)
9. [Development Guide](#development-guide)
10. [Testing & Debugging](#testing--debugging)

## Overview

The Curvine S3 Object Gateway is a high-performance, S3-compatible object storage gateway built with Rust and Axum. It provides a complete S3 API implementation that seamlessly integrates with the Curvine distributed file system, offering enterprise-grade object storage capabilities with full AWS S3 compatibility.

### Key Features

- **Full S3 API Compatibility**: Supports all major S3 operations including object CRUD, bucket management, and multipart uploads
- **High Performance**: Built on Axum and Tokio for asynchronous, high-throughput request processing
- **Enterprise Security**: AWS Signature V4 authentication with comprehensive access control
- **Scalable Architecture**: Modular design supporting horizontal scaling and microservice deployment
- **Range Request Support**: HTTP 206 partial content for efficient large file streaming
- **Multipart Upload**: Complete implementation of S3 multipart upload protocol for large objects

### Technology Stack

- **Web Framework**: Axum 0.8.4 with Tokio async runtime
- **HTTP Processing**: Custom request/response abstractions for S3 compatibility
- **Authentication**: AWS Signature V4 implementation with HMAC-SHA256
- **Storage Backend**: Curvine unified file system with pluggable storage adapters
- **Serialization**: Serde with custom XML handling for S3 API responses
- **Logging**: Structured logging with tracing for observability

## System Architecture

### High-Level Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        Client[S3 Clients<br/>AWS CLI, SDKs, etc.]
    end
    
    subgraph "Gateway Layer"
        LB[Load Balancer]
        Gateway[Curvine S3 Gateway<br/>Axum + Tokio]
    end
    
    subgraph "Processing Layer"
        Auth[Authentication<br/>AWS SigV4]
        Router[S3 Router<br/>Request Routing]
        Handlers[S3 Handlers<br/>Operation Logic]
    end
    
    subgraph "Storage Layer"
        FS[Curvine FileSystem<br/>Unified Interface]
        Backend[Storage Backend<br/>Distributed Storage]
    end
    
    Client --> LB
    LB --> Gateway
    Gateway --> Auth
    Auth --> Router
    Router --> Handlers
    Handlers --> FS
    FS --> Backend
```

### Component Interaction Flow

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant G as Gateway
    participant A as Auth Middleware
    participant R as S3 Router
    participant H as S3 Handler
    participant F as FileSystem
    
    C->>G: HTTP Request
    G->>A: Authenticate Request
    A->>A: Verify SigV4 Signature
    A->>R: Route Authenticated Request
    R->>R: Parse HTTP Method & Path
    R->>H: Delegate to Handler
    H->>F: File System Operation
    F->>H: Operation Result
    H->>R: Response
    R->>G: HTTP Response
    G->>C: S3-Compatible Response
```

## Core Components

### 1. HTTP Layer (`src/http/`)

#### Axum Integration (`axum.rs`)

The Axum integration layer provides custom request/response abstractions that bridge the gap between Axum's native HTTP handling and S3's specific requirements.

**Key Components:**

- **Request Wrapper**: Custom `Request` struct that implements S3-specific traits (`VRequest`, `VRequestPlus`, `BodyReader`)
- **Response Wrapper**: Custom `Response` struct with S3 header management and body streaming
- **Authentication Middleware**: `handle_authorization_middleware` for AWS SigV4 verification
- **Main Handler**: `handle_fn` that delegates all requests to the S3Router

**Design Decisions:**

- **Why Custom Wrappers?**: S3 API has specific requirements for header handling, query parameter parsing, and body streaming that don't map directly to Axum's interfaces
- **Async Body Handling**: Custom `BodyReader` and `PollRead` traits enable efficient streaming for large objects without loading entire files into memory
- **Middleware Chain**: Authentication happens before routing to ensure all requests are properly verified

#### S3 Router (`router.rs`)

The S3 Router implements a hierarchical routing strategy that follows S3 API conventions:

**Routing Hierarchy:**
1. **HTTP Method Level**: PUT, GET, DELETE, HEAD, POST
2. **Path Analysis**: Bucket vs Object operations based on URL segments
3. **Query Parameters**: Special operations like multipart uploads, listing
4. **Operation Delegation**: Specific handler methods for each operation type

**Key Methods:**

```rust
// Main routing entry point
pub async fn route(req: Request) -> Response

// Method-specific routing
async fn handle_put_request(req: Request) -> Response     // Upload operations
async fn handle_get_request(req: Request) -> Response     // Download/listing
async fn handle_delete_request(req: Request) -> Response  // Deletion operations
async fn handle_head_request(req: Request) -> Response    // Metadata operations
async fn handle_post_request(req: Request) -> Response    // Multipart management
```

**Routing Logic Examples:**

```
PUT /bucket           → Create bucket
PUT /bucket/object    → Upload object
PUT /bucket/obj?uploadId=123&partNumber=1 → Multipart part

GET /                 → List buckets
GET /?list-type=2     → List buckets (S3v2)
GET /bucket/?list-type=2 → List objects
GET /bucket/object    → Download object
GET /bucket?location  → Get bucket location

HEAD /bucket          → Check bucket existence
HEAD /bucket/object   → Get object metadata

DELETE /bucket        → Delete bucket
DELETE /bucket/object → Delete object

POST /bucket/obj?uploads → Create multipart session
POST /bucket/obj?uploadId=123 → Complete multipart upload
```

### 2. S3 API Layer (`src/s3/`)

#### S3 API Definitions (`s3_api.rs`)

This module defines all S3 API structures, traits, and handler functions. It provides a complete abstraction layer that isolates S3-specific logic from the underlying storage implementation.

**Core Traits:**

```rust
// Object metadata operations
pub trait HeadHandler {
    fn lookup(&self, bucket: &str, object: &str) -> Future<HeadObjectResult>
}

// Object content operations  
pub trait GetObjectHandler: HeadHandler {
    fn handle(&self, bucket: &str, object: &str, opt: GetObjectOption, out: Writer) -> Future<()>
}

// Object upload operations
pub trait PutObjectHandler {
    fn handle(&self, opt: &PutObjectOption, bucket: &str, object: &str, body: Reader) -> Future<()>
}

// Bucket management
pub trait CreateBucketHandler {
    fn handle(&self, opt: &CreateBucketOption, bucket: &str) -> Future<()>
}

// Object/bucket listing
pub trait ListObjectHandler {
    fn handle(&self, opt: &ListObjectOption, bucket: &str) -> Future<Vec<ListObjectContent>>
}

// Multipart upload operations
pub trait MultiUploadObjectHandler {
    fn handle_create_session(&self, bucket: &str, key: &str) -> Future<String>
    fn handle_upload_part(&self, bucket: &str, key: &str, upload_id: &str, part_number: u32, body: Reader) -> Future<String>
    fn handle_complete(&self, bucket: &str, key: &str, upload_id: &str, parts: &[(ETag, PartNumber)]) -> Future<String>
}
```

**S3 Data Structures:**

The module defines comprehensive S3-compatible data structures with proper XML serialization:

```rust
#[derive(Serialize)]
pub struct HeadObjectResult {
    pub content_length: Option<usize>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub storage_class: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    // ... 30+ additional S3-compatible fields
}

#[derive(Serialize)]
pub struct ListObjectContent {
    pub key: String,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub storage_class: Option<String>,
    pub owner: Option<Owner>,
}
```

#### S3 Handlers Implementation (`handlers.rs`)

The handlers module provides concrete implementations of all S3 API traits, bridging S3 operations to the Curvine file system.

**Core Handler Structure:**

```rust
#[derive(Clone)]
pub struct S3Handlers {
    pub fs: UnifiedFileSystem,      // File system interface
    pub region: String,             // S3 region identifier
    pub rt: Arc<AsyncRuntime>,      // Shared async runtime
}
```

**Key Implementation Features:**

1. **Path Mapping**: Converts S3 paths (`bucket/object`) to file system paths (`/bucket/object`)
2. **Metadata Conversion**: Maps file system metadata to S3-compatible attributes
3. **Error Translation**: Converts file system errors to appropriate HTTP status codes
4. **Performance Optimization**: Uses `tokio::task::block_in_place` for CPU-intensive operations

**Example Implementation - Object Upload:**

```rust
async fn handle(&self, opt: &PutObjectOption, bucket: &str, object: &str, 
               body: &mut dyn PollRead) -> Result<(), String> {
    // Convert S3 path to file system path
    let path = self.cv_object_path(bucket, object)?;
    
    // Create file writer
    let mut writer = self.fs.create(&path, true).await?;
    
    // Stream data from client to storage
    while let Some(chunk) = body.poll_read().await? {
        writer.write(&chunk).await?;
    }
    
    // Complete the write operation
    writer.complete().await?;
    Ok(())
}
```

#### Types & Utilities (`types.rs`)

Provides modular components for complex operations like PUT uploads:

```rust
// Upload operation context
pub struct PutContext {
    pub fs: UnifiedFileSystem,
    pub rt: Arc<AsyncRuntime>,
    pub bucket: String,
    pub object: String,
    pub path: FsResult<Path>,
}

// Main orchestrator for PUT operations
pub struct PutOperation;
impl PutOperation {
    pub async fn execute(context: PutContext, body: &mut dyn PollRead) -> Result<(), String> {
        // Comprehensive upload logic with error handling
    }
}
```

### 3. Authentication Layer (`src/auth/`)

#### AWS Signature V4 Implementation (`sig_v4.rs`)

Complete implementation of AWS Signature Version 4 authentication protocol:

**Signature Verification Process:**

1. **Extract Authorization Header**: Parse AWS4-HMAC-SHA256 credential string
2. **Canonical Request Construction**: Build canonical HTTP request representation
3. **String-to-Sign Creation**: Generate AWS SigV4 string-to-sign
4. **Signature Calculation**: Compute HMAC-SHA256 signature with derived key
5. **Signature Comparison**: Verify client signature matches computed signature

**Key Features:**

- **Request Canonicalization**: Proper handling of headers, query parameters, and payload hashing
- **Streaming Support**: Chunked payload signature verification for large uploads
- **Error Handling**: Comprehensive error reporting for authentication failures
- **Performance**: Optimized signature computation with minimal allocations

**Example Signature Verification:**

```rust
pub fn get_v4_signature<T: VHeader>(
    req: &T,
    method: &str,
    region: &str,
    service: &str,
    url_path: &str,
    secret_key: &str,
    content_hash: &str,
    signed_headers: &[String],
    query: Vec<BaseKv<String, String>>,
) -> Result<(String, HmacSha256CircleHasher), String> {
    // Build canonical request
    let canonical_request = build_canonical_request(
        method, url_path, &query, req, signed_headers, content_hash
    );
    
    // Create string-to-sign
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
        xamz_date, date, region, service, sha256_hex(canonical_request)
    );
    
    // Calculate signature
    let signing_key = derive_signing_key(secret_key, date, region, service);
    let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes());
    
    Ok((hex::encode(signature), circle_hasher))
}
```

#### Access Key Management (`mod.rs`)

Provides pluggable access key storage with built-in implementations:

```rust
pub trait AccesskeyStore: Send + Sync {
    fn get(&self, access_key: &str) -> Future<Option<String>>
}

// Static credential store for development/testing
pub struct StaticAccessKeyStore {
    credentials: HashMap<String, String>,
}

// Environment-based credential loading
impl StaticAccessKeyStore {
    pub fn from_env() -> Result<Self, String> {
        let access_key = env::var("AWS_ACCESS_KEY_ID")?;
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY")?;
        Ok(Self::with_single_key(access_key, secret_key))
    }
}
```

### 4. Storage Integration Layer

#### File System Interface

The gateway integrates with Curvine's unified file system through a clean abstraction layer:

**Core Operations:**

```rust
// File operations
fs.create(&path, overwrite) -> Writer      // Create file for writing
fs.open(&path) -> Reader                   // Open file for reading  
fs.delete(&path, recursive) -> Result      // Delete file/directory
fs.get_status(&path) -> FileStatus         // Get file metadata

// Directory operations  
fs.mkdir(&path, recursive) -> Result       // Create directory
fs.list_status(&path) -> Vec<FileStatus>   // List directory contents
```

**Metadata Mapping:**

The system maps between file system metadata and S3 attributes:

```rust
pub struct FileStatus {
    pub id: String,              // → S3 object identifier
    pub path: String,            // → S3 object key
    pub name: String,            // → Object name component
    pub is_dir: bool,            // → Determines if object or prefix
    pub mtime: i64,              // → S3 LastModified timestamp
    pub len: u64,                // → S3 ContentLength
    pub storage_policy: StoragePolicy,  // → S3 StorageClass
    pub owner: String,           // → S3 Owner ID
    pub mode: u32,               // → Custom metadata
    // ... additional fields
}

// Conversion to S3 metadata
pub fn file_status_to_head_object_result(fs: &FileStatus, object_name: &str) -> HeadObjectResult {
    HeadObjectResult {
        content_length: Some(fs.len as usize),
        last_modified: format_s3_timestamp(fs.mtime),
        etag: generate_etag(fs),
        content_type: infer_content_type(object_name),
        storage_class: map_storage_class(&fs.storage_policy.storage_type),
        metadata: create_metadata_map(fs),
        // ... additional mappings
    }
}
```

## Request Processing Flow

### Typical Request Lifecycle

1. **HTTP Request Reception**: Axum receives incoming HTTP request
2. **Authentication Middleware**: Verify AWS SigV4 signature and extract credentials
3. **Request Conversion**: Convert Axum request to internal Request wrapper
4. **S3 Router Analysis**: Determine operation type from HTTP method and path
5. **Handler Delegation**: Route to appropriate S3 operation handler
6. **File System Operation**: Execute storage operation through unified file system
7. **Response Generation**: Convert results to S3-compatible HTTP response
8. **Response Transmission**: Send response back to client

### Detailed Flow Examples

#### Object Upload (PUT /bucket/object)

```mermaid
sequenceDiagram
    participant C as Client
    participant A as Auth Middleware
    participant R as S3 Router
    participant H as PutObjectHandler
    participant F as FileSystem
    
    C->>A: PUT /bucket/object + Body
    A->>A: Verify SigV4 Signature
    A->>R: Authenticated Request
    R->>R: Parse as Object Upload
    R->>H: handle_put_object_request()
    H->>F: create(/bucket/object)
    F->>H: Writer Stream
    H->>H: Stream Body → Writer
    H->>F: writer.complete()
    F->>H: Success
    H->>R: HTTP 200 OK
    R->>C: S3 Response
```

#### Object Download with Range (GET /bucket/object)

```mermaid
sequenceDiagram
    participant C as Client
    participant R as S3 Router
    participant H as GetObjectHandler
    participant F as FileSystem
    
    C->>R: GET /bucket/object<br/>Range: bytes=0-1023
    R->>H: handle_get_object_request()
    H->>F: get_status(/bucket/object)
    F->>H: FileStatus (size: 10MB)
    H->>H: Validate Range (0-1023)
    H->>F: open(/bucket/object)
    F->>H: Reader Stream
    H->>F: seek(0), read(1024)
    F->>H: Data Chunk
    H->>R: HTTP 206 Partial Content<br/>Content-Range: bytes 0-1023/10485760
    R->>C: Partial Object Data
```

#### Multipart Upload Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant R as S3 Router
    participant M as MultipartHandler
    participant FS as FileSystem
    
    Note over C,FS: 1. Initiate Multipart Upload
    C->>R: POST /bucket/object?uploads
    R->>M: handle_multipart_create_session()
    M->>M: Generate Upload ID
    M->>R: UploadId: abc123
    R->>C: XML Response with UploadId
    
    Note over C,FS: 2. Upload Parts (parallel)
    C->>R: PUT /bucket/object?uploadId=abc123&partNumber=1
    R->>M: handle_multipart_upload_part()
    M->>FS: Write Part to Temp Storage
    FS->>M: Success
    M->>R: ETag: "etag1"
    R->>C: HTTP 200 + ETag
    
    Note over C,FS: 3. Complete Upload
    C->>R: POST /bucket/object?uploadId=abc123<br/>Body: Part List XML
    R->>M: handle_multipart_complete_session()
    M->>FS: Combine Parts → Final Object
    FS->>M: Success
    M->>R: Final ETag
    R->>C: XML Completion Response
```

## S3 API Implementation

### Supported Operations

#### Object Operations

| Operation | HTTP Method | Path | Status | Notes |
|-----------|-------------|------|--------|-------|
| PutObject | PUT | `/bucket/object` | ✅ Complete | Supports metadata, checksums |
| GetObject | GET | `/bucket/object` | ✅ Complete | Range requests, streaming |
| HeadObject | HEAD | `/bucket/object` | ✅ Complete | All metadata headers |
| DeleteObject | DELETE | `/bucket/object` | ✅ Complete | Idempotent deletion |
| CopyObject | PUT | `/bucket/object` | ❌ Not Implemented | Future enhancement |

#### Bucket Operations

| Operation | HTTP Method | Path | Status | Notes |
|-----------|-------------|------|--------|-------|
| CreateBucket | PUT | `/bucket` | ✅ Complete | Regional support |
| HeadBucket | HEAD | `/bucket` | ✅ Complete | Existence check |
| DeleteBucket | DELETE | `/bucket` | ✅ Complete | Empty bucket only |
| ListBuckets | GET | `/` | ✅ Complete | Owner-scoped listing |
| GetBucketLocation | GET | `/bucket?location` | ✅ Complete | Region information |

#### Object Listing

| Operation | HTTP Method | Path | Status | Notes |
|-----------|-------------|------|--------|-------|
| ListObjectsV2 | GET | `/bucket?list-type=2` | ✅ Complete | Pagination, prefixes |
| ListObjects | GET | `/bucket` | ✅ Complete | Legacy API support |

#### Multipart Upload

| Operation | HTTP Method | Path | Status | Notes |
|-----------|-------------|------|--------|-------|
| InitiateMultipartUpload | POST | `/bucket/object?uploads` | ✅ Complete | Returns upload ID |
| UploadPart | PUT | `/bucket/object?uploadId=X&partNumber=N` | ✅ Complete | ETag generation |
| CompleteMultipartUpload | POST | `/bucket/object?uploadId=X` | ✅ Complete | Part combination |
| AbortMultipartUpload | DELETE | `/bucket/object?uploadId=X` | ✅ Complete | Cleanup temp files |

### S3 Compatibility Features

#### HTTP Headers

The implementation supports all standard S3 headers:

**Request Headers:**
- `Authorization`: AWS4-HMAC-SHA256 signature
- `Content-Type`: Object MIME type
- `Content-Length`: Request body size  
- `Content-MD5`: Request body checksum
- `Range`: Partial content requests
- `x-amz-*`: AWS-specific headers

**Response Headers:**
- `ETag`: Object entity tag for caching
- `Last-Modified`: Object modification time
- `Content-Length`: Response body size
- `Content-Type`: Object MIME type
- `Content-Range`: Partial content range (HTTP 206)
- `x-amz-storage-class`: Storage tier information

#### Status Codes

Proper HTTP status code handling:

- `200 OK`: Successful operations
- `204 No Content`: Successful deletion
- `206 Partial Content`: Range requests
- `400 Bad Request`: Invalid parameters
- `403 Forbidden`: Authentication failure
- `404 Not Found`: Object/bucket not found
- `409 Conflict`: Bucket not empty (delete)
- `416 Range Not Satisfiable`: Invalid range
- `500 Internal Server Error`: System errors

#### XML Response Format

All list operations return properly formatted XML:

```xml
<!-- ListBuckets Response -->
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>ffffffffffffffff</ID>
    <DisplayName>curvine-user</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>my-bucket</Name>
      <CreationDate>2023-01-01T00:00:00.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>

<!-- ListObjectsV2 Response -->
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>my-bucket</Name>
  <Prefix>photos/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/image1.jpg</Key>
    <LastModified>2023-01-01T12:00:00.000Z</LastModified>
    <ETag>"abc123"</ETag>
    <Size>1048576</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>
```

## Authentication & Authorization

### AWS Signature Version 4

Complete implementation of AWS SigV4 with all security features:

#### Signature Calculation

```rust
// 1. Create canonical request
let canonical_request = format!(
    "{}\n{}\n{}\n{}\n\n{}\n{}",
    http_method,
    canonical_uri,
    canonical_query_string,
    canonical_headers,
    signed_headers,
    payload_hash
);

// 2. Create string to sign  
let string_to_sign = format!(
    "AWS4-HMAC-SHA256\n{}\n{}\n{}",
    amz_date,
    credential_scope,
    sha256_hex(canonical_request)
);

// 3. Calculate signature
let signing_key = derive_signing_key(secret_key, date, region, "s3");
let signature = hmac_sha256(signing_key, string_to_sign);
```

#### Security Features

- **Timestamp Validation**: Prevents replay attacks with 15-minute window
- **Content Hash Verification**: Ensures request body integrity
- **Header Canonicalization**: Prevents header manipulation attacks
- **Signature Verification**: Cryptographic proof of authorization

#### Credential Management

```rust
// Environment-based credentials (production)
let store = StaticAccessKeyStore::from_env()?;

// Development credentials (testing)
let store = StaticAccessKeyStore::with_single_key(
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
);

// Future: IAM role-based credentials
let store = IamRoleCredentialStore::new(role_arn)?;
```

### Access Control

Currently implements credential-based access control with plans for advanced authorization:

**Current Implementation:**
- Static access key/secret key pairs
- Credential validation per request
- Basic bucket/object access control

**Future Enhancements:**
- IAM policy evaluation
- Resource-based bucket policies  
- Role-based access control (RBAC)
- Temporary security tokens (STS)

## Error Handling Strategy

### Error Classification

The system implements a comprehensive error handling strategy with proper error classification:

#### S3 API Errors

```rust
// Client errors (4xx)
pub enum ClientError {
    InvalidRequest,        // 400 Bad Request
    AccessDenied,         // 403 Forbidden  
    NoSuchBucket,         // 404 Not Found
    NoSuchKey,            // 404 Not Found
    BucketNotEmpty,       // 409 Conflict
    InvalidRange,         // 416 Range Not Satisfiable
}

// Server errors (5xx)  
pub enum ServerError {
    InternalError,        // 500 Internal Server Error
    ServiceUnavailable,   // 503 Service Unavailable
    SlowDown,            // 503 Slow Down
}
```

#### Error Response Format

All errors return properly formatted S3 XML error responses:

```xml
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>non-existent-bucket</BucketName>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>
```

#### Error Propagation

```rust
// File system errors → HTTP status codes
match fs_error {
    FsError::NotFound => StatusCode::NOT_FOUND,
    FsError::PermissionDenied => StatusCode::FORBIDDEN,
    FsError::AlreadyExists => StatusCode::CONFLICT,
    FsError::InvalidPath => StatusCode::BAD_REQUEST,
    _ => StatusCode::INTERNAL_SERVER_ERROR,
}
```

### Logging & Observability

Comprehensive structured logging for debugging and monitoring:

```rust
// Request-level logging
tracing::info!(
    "S3 Request: {} {} from {}",
    method, path, client_ip
);

// Operation-level logging  
tracing::debug!(
    "PUT object s3://{}/{}, size: {} bytes",
    bucket, object, content_length
);

// Error logging with context
tracing::error!(
    "Failed to upload object s3://{}/{}: {}",
    bucket, object, error
);
```

## Performance Considerations

### Async Architecture

The entire system is built on Tokio's async runtime for maximum concurrency:

- **Non-blocking I/O**: All file system operations are async
- **Request Parallelism**: Thousands of concurrent requests
- **Memory Efficiency**: Streaming uploads/downloads without buffering
- **CPU Efficiency**: Work-stealing scheduler for optimal CPU utilization

### Memory Management

Careful memory management for handling large objects:

```rust
// Streaming uploads - no memory buffering
while let Some(chunk) = body.poll_read().await? {
    writer.write(&chunk).await?;  // Immediate write to storage
}

// Streaming downloads - chunked reading
let mut buffer = vec![0u8; 64 * 1024];  // 64KB chunks
while let Ok(n) = reader.read(&mut buffer).await {
    if n == 0 { break; }
    response_writer.write(&buffer[..n]).await?;
}
```

### Optimization Strategies

1. **Zero-Copy Operations**: Direct memory mapping where possible
2. **Chunk Size Optimization**: 64KB chunks for optimal throughput
3. **Connection Reuse**: HTTP/1.1 keep-alive and HTTP/2 multiplexing
4. **Async Batching**: Batch multiple small operations
5. **Background Processing**: Async cleanup and maintenance tasks

### Scalability Features

- **Horizontal Scaling**: Stateless design supports load balancing
- **Resource Pooling**: Connection pools for database/storage backends
- **Caching**: In-memory metadata caching for frequently accessed objects
- **Rate Limiting**: Built-in rate limiting to prevent abuse

## Development Guide

### Project Structure

```
curvine-object/
├── src/
│   ├── auth/              # Authentication & authorization
│   │   ├── mod.rs         # Access key management
│   │   ├── sig_v2.rs      # AWS Signature V2 (legacy)
│   │   └── sig_v4.rs      # AWS Signature V4 (primary)
│   ├── http/              # HTTP layer & routing
│   │   ├── mod.rs         # Module exports
│   │   ├── axum.rs        # Axum integration
│   │   └── router.rs      # S3 API routing
│   ├── s3/                # S3 API implementation
│   │   ├── mod.rs         # Module exports
│   │   ├── error.rs       # S3 error types
│   │   ├── handlers.rs    # S3 operation handlers
│   │   ├── s3_api.rs      # S3 API definitions
│   │   └── types.rs       # S3 operation types
│   ├── utils/             # Utility functions
│   │   ├── mod.rs         # Module exports
│   │   └── utils.rs       # File conversion utilities
│   ├── config.rs          # Configuration management
│   └── lib.rs             # Library entry point
├── examples/              # Usage examples
├── tests/                 # Integration tests
└── Cargo.toml            # Dependencies and metadata
```

### Building & Running

```bash
# Development build
cargo build --bin curvine-object

# Release build
cargo build --release --bin curvine-object

# Run with default configuration
cargo run --bin curvine-object

# Run with custom configuration
cargo run --bin curvine-object -- --conf /path/to/config.toml --listen 0.0.0.0:9000

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run --bin curvine-object
```

### Configuration

The gateway supports flexible configuration through TOML files and environment variables:

```toml
# curvine-cluster.toml
[object]
listen = "0.0.0.0:9900"
region = "us-east-1"
multipart_temp = "/tmp/curvine-multipart"

[client]
io_threads = 4
worker_threads = 8

[master]
log = { level = "info", format = "json" }
```

Environment variables take precedence over configuration files:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export RUST_LOG="curvine_object=debug"
```

### Adding New S3 Operations

To add a new S3 operation:

1. **Define the API**: Add handler trait to `s3_api.rs`
2. **Implement Handler**: Add implementation to `handlers.rs`
3. **Add Routing**: Update router logic in `router.rs`
4. **Register Handler**: Add to handler registration in `lib.rs`
5. **Add Tests**: Create integration tests

Example - Adding ListObjectVersions:

```rust
// 1. Define trait in s3_api.rs
pub trait ListObjectVersionsHandler {
    fn handle(&self, bucket: &str, opt: &ListVersionsOption) 
        -> Future<Vec<ObjectVersion>>;
}

// 2. Implement in handlers.rs
impl ListObjectVersionsHandler for S3Handlers {
    async fn handle(&self, bucket: &str, opt: &ListVersionsOption) 
        -> Result<Vec<ObjectVersion>, String> {
        // Implementation
    }
}

// 3. Add routing in router.rs
if req.get_query("versions").is_some() {
    return Self::handle_list_versions_request(req, handler).await;
}

// 4. Register in lib.rs
.layer(axum::Extension(
    handlers.clone() as Arc<dyn ListObjectVersionsHandler + Send + Sync>
))
```

## Testing & Debugging

### Testing Strategy

The project implements comprehensive testing at multiple levels:

#### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_object_success() {
        let handlers = create_test_handlers().await;
        let result = handlers.handle_put_object(
            &PutObjectOption::default(),
            "test-bucket",
            "test-object",
            &mut test_body_reader()
        ).await;
        assert!(result.is_ok());
    }
}
```

#### Integration Tests

```rust
#[tokio::test]
async fn test_s3_api_compatibility() {
    let app = create_test_app().await;
    
    // Test bucket creation
    let response = app
        .oneshot(Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .header("Authorization", "AWS4-HMAC-SHA256 ...")
            .body(Body::empty())
            .unwrap())
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
}
```

#### AWS CLI Testing

Complete test suite using real AWS CLI:

```bash
#!/bin/bash
# test_s3_complete.sh

export AWS_ACCESS_KEY_ID="AqU4axe4feDyIielarPI"
export AWS_SECRET_ACCESS_KEY="0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt"
export AWS_DEFAULT_REGION="us-east-1"

ENDPOINT="http://localhost:9900"

# Test bucket operations
aws s3 mb s3://test-bucket --endpoint-url=$ENDPOINT
aws s3 ls --endpoint-url=$ENDPOINT

# Test object operations  
echo "Hello World" > test.txt
aws s3 cp test.txt s3://test-bucket/ --endpoint-url=$ENDPOINT
aws s3 ls s3://test-bucket/ --endpoint-url=$ENDPOINT
aws s3 cp s3://test-bucket/test.txt download.txt --endpoint-url=$ENDPOINT

# Test multipart upload
aws s3 cp large-file.bin s3://test-bucket/ --endpoint-url=$ENDPOINT

# Cleanup
aws s3 rm s3://test-bucket/test.txt --endpoint-url=$ENDPOINT
aws s3 rb s3://test-bucket --endpoint-url=$ENDPOINT
```

### Debugging Tools

#### Logging Configuration

```rust
// Enable comprehensive logging
RUST_LOG=debug,curvine_object=trace cargo run

// Component-specific logging
RUST_LOG=curvine_object::s3::handlers=debug cargo run

// Request tracing
RUST_LOG=curvine_object::http=trace cargo run
```

#### Development Tools

1. **Health Check Endpoint**: `GET /probe-bsign` for monitoring
2. **Request Logging**: Detailed request/response logging
3. **Memory Profiling**: Built-in memory usage monitoring
4. **Performance Metrics**: Request latency and throughput tracking

#### Common Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Authentication Failure | 403 Forbidden responses | Check AWS credentials and signature calculation |
| Large Upload Timeout | Connection reset during upload | Increase client timeout or use multipart upload |
| Memory Usage | High memory consumption | Enable streaming mode for large objects |
| Permission Denied | 403 on valid requests | Verify file system permissions |

### Performance Testing

Load testing with realistic S3 workloads:

```bash
# Install testing tools
cargo install criterion

# Run benchmarks
cargo bench

# Load testing with Apache Bench
ab -n 1000 -c 50 -H "Authorization: AWS4-HMAC-SHA256 ..." \
   http://localhost:9900/test-bucket/

# Multipart upload performance test
python3 test_multipart_performance.py --size 1GB --parts 100
```

## Conclusion

The Curvine S3 Object Gateway represents a production-ready, enterprise-grade S3-compatible object storage solution. Its modular architecture, comprehensive S3 API support, and high-performance async design make it suitable for a wide range of use cases from development environments to large-scale production deployments.

The implementation demonstrates best practices in modern Rust development:

- **Type Safety**: Leveraging Rust's type system for correctness
- **Performance**: Zero-cost abstractions and efficient async I/O
- **Maintainability**: Clean separation of concerns and comprehensive documentation
- **Reliability**: Extensive error handling and testing
- **Compatibility**: Full S3 API compliance for seamless integration

For questions, contributions, or support, please refer to the project documentation or contact the development team. 
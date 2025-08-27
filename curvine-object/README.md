# Curvine S3 Object Gateway

Curvine S3 Object Gateway 是一个兼容 Amazon S3 API 的对象存储网关，基于 `bws-rs` 框架构建，为 Curvine 分布式文件系统提供 S3 兼容的访问接口。

## 功能特性

- **完整的 S3 API 支持**：PutObject、GetObject、HeadObject、DeleteObject、CreateBucket、HeadBucket、ListBucket、DeleteBucket、GetBucketLocation
- **Range Get 支持**：支持 HTTP Range 请求，返回 206 Partial Content
- **Multipart Upload**：支持大文件分片上传
- **S3 V4 签名验证**：完整的 AWS S3 V4 签名验证
- **灵活的启动方式**：支持独立启动或与 Master 集成启动

## 快速开始

### 1. 独立启动

```bash
# 使用默认配置文件启动（etc/curvine-cluster.toml）
./build/bin/curvine-object.sh

# 使用指定配置文件启动
./build/bin/curvine-object.sh --conf /path/to/curvine.conf

# 覆盖配置文件中的监听地址和区域
./build/bin/curvine-object.sh \
    --conf /path/to/curvine.conf \
    --listen 0.0.0.0:9000 \
    --region us-west-2

# 只覆盖监听地址，区域使用配置文件中的值
./build/bin/curvine-object.sh \
    --conf /path/to/curvine.conf \
    --listen 0.0.0.0:9000
```

### 2. 与 Master 集成启动

```bash
# 启动 Master 并启用 S3 网关
cargo run --bin curvine-server -- \
    --service master \
    --conf /path/to/curvine.conf \
    --enable-object-gateway \
    --object-listen 0.0.0.0:9900 \
    --object-region us-east-1
```

## 配置说明

### 环境变量

- `S3_ACCESS_KEY`：S3 访问密钥（默认：minioadmin）
- `S3_SECRET_KEY`：S3 秘密密钥（默认：minioadmin）

### 配置文件

S3 网关支持从 Curvine 集群配置文件中读取配置：

```toml
[object]
# 监听地址
listen = "0.0.0.0:9900"
# S3 区域标识
region = "us-east-1"
# 分片上传临时目录
multipart_temp = "/tmp/curvine-multipart"
```

### 命令行参数

- `--conf`：Curvine 集群配置文件路径（可选，默认：etc/curvine-cluster.toml）
- `--listen`：监听地址，格式：host:port（可选，会覆盖配置文件中的值）
- `--region`：S3 区域标识（可选，会覆盖配置文件中的值）

### 配置优先级

1. 命令行参数（最高优先级）
2. 配置文件中的值
3. 默认值（最低优先级）

## 使用示例

### AWS CLI

```bash
# 配置 AWS CLI 使用 Curvine S3 网关
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1

# 创建存储桶
aws s3 mb s3://mybucket --endpoint-url http://localhost:9900 --no-verify-ssl

# 上传文件
aws s3 cp /path/to/file.txt s3://mybucket/ --endpoint-url http://localhost:9900 --no-verify-ssl

# 下载文件
aws s3 cp s3://mybucket/file.txt /tmp/ --endpoint-url http://localhost:9900 --no-verify-ssl

# 列出存储桶内容
aws s3 ls s3://mybucket/ --endpoint-url http://localhost:9900 --no-verify-ssl

# 删除文件
aws s3 rm s3://mybucket/file.txt --endpoint-url http://localhost:9900 --no-verify-ssl

# 删除存储桶
aws s3 rb s3://mybucket --endpoint-url http://localhost:9900 --no-verify-ssl
```

### MinIO Client (mc)

```bash
# 配置 MinIO 客户端
mc alias set curvine http://localhost:9900 minioadmin minioadmin

# 创建存储桶
mc mb curvine/mybucket

# 上传文件
mc cp /path/to/file.txt curvine/mybucket/

# 下载文件
mc cp curvine/mybucket/file.txt /tmp/

# 列出内容
mc ls curvine/mybucket/

# 删除文件
mc rm curvine/mybucket/file.txt

# 删除存储桶
mc rb curvine/mybucket
```

### 范围查询 (Range Get)

```bash
# 获取文件的前 1000 字节
curl -H "Range: bytes=0-999" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     http://localhost:9900/mybucket/file.txt

# 获取文件的 1000-1999 字节
curl -H "Range: bytes=1000-1999" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     http://localhost:9900/mybucket/file.txt
```

### 分片上传 (Multipart Upload)

```bash
# 创建分片上传会话
aws s3api create-multipart-upload \
    --bucket mybucket \
    --key large-file.zip \
    --endpoint-url http://localhost:9900

# 上传分片
aws s3api upload-part \
    --bucket mybucket \
    --key large-file.zip \
    --part-number 1 \
    --upload-id <upload-id> \
    --body part1.bin \
    --endpoint-url http://localhost:9900

# 完成分片上传
aws s3api complete-multipart-upload \
    --bucket mybucket \
    --key large-file.zip \
    --upload-id <upload-id> \
    --multipart-upload file://parts.json \
    --endpoint-url http://localhost:9900
```

## API 端点

- `GET /healthz`：健康检查端点
- 所有 S3 操作通过标准 S3 REST API 端点提供

## 路径映射

Curvine S3 网关将 S3 路径映射到 Curvine 文件系统：

- **存储桶**：`/buckets/{bucket-name}`
- **对象**：`/buckets/{bucket-name}/{object-path}`

## 开发

### 构建

```bash
# 构建 curvine-object
cargo build --bin curvine-object

# 构建并运行测试
cargo test

# 构建发布版本
cargo build --release --bin curvine-object
```

### 测试

```bash
# 运行单元测试
cargo test

# 运行特定测试
cargo test test_cv_object_path
```

## 故障排除

### 常见问题

1. **端口被占用**：修改 `--listen` 参数使用其他端口
2. **认证失败**：检查 `S3_ACCESS_KEY` 和 `S3_SECRET_KEY` 环境变量
3. **配置文件错误**：确保 Curvine 配置文件路径正确且格式有效

### 日志

启用详细日志：

```bash
export RUST_LOG=debug
./build/bin/curvine-object.sh --conf /path/to/curvine.conf
```

## 许可证

Apache License 2.0 
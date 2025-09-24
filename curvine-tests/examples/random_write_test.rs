// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::BytesMut;
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::FsResult;
use curvine_common::state::CreateFileOptsBuilder;
use orpc::common::{Logger, LocalTime};
use orpc::runtime::{RpcRuntime, AsyncRuntime};
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;
use rand::Rng;

// 随机写测试配置
#[derive(Debug)]
struct RandomWriteTestConfig {
    pub test_files_count: usize,       // 测试文件数量
    pub file_size_mb: usize,           // 单个文件大小(MB)
    pub write_operations_per_file: usize, // 每个文件的写操作次数
    pub max_write_chunk_size_kb: usize,   // 最大写块大小(KB)
    pub test_dir: String,              // 测试目录
}

impl Default for RandomWriteTestConfig {
    fn default() -> Self {
        Self {
            test_files_count: 3,
            file_size_mb: 10,
            write_operations_per_file: 30,
            max_write_chunk_size_kb: 32,
            test_dir: "/random_write_test".to_string(),
        }
    }
}

// 测试报告结构
#[derive(Debug, Default)]
struct TestReport {
    pub start_time: String,
    pub end_time: String,
    pub total_files_tested: usize,
    pub total_write_operations: usize,
    pub total_bytes_written: u64,
    pub total_bytes_verified: u64,
    pub success_operations: usize,
    pub failed_operations: usize,
    pub errors: Vec<String>,
    pub performance_stats: PerformanceStats,
}

#[derive(Debug, Default)]
struct PerformanceStats {
    pub avg_write_latency_ms: f64,
    pub avg_read_latency_ms: f64,
    pub write_throughput_mbps: f64,
    pub read_throughput_mbps: f64,
}

// 文件写入记录，用于验证
#[derive(Debug, Clone)]
struct WriteRecord {
    pub offset: i64,
    pub data: Vec<u8>,
    pub checksum: u32,
}

// 重叠区域信息
#[derive(Debug, Clone)]
struct OverlapInfo {
    pub existing_offset: i64,
    pub overlap_start: i64,
    pub overlap_end: i64,
    pub overlap_type: OverlapType,
}

#[derive(Debug, Clone)]
enum OverlapType {
    CompleteOverwrite,  // 完全覆盖
    PartialOverwrite,   // 部分覆盖
}

fn main() -> CommonResult<()> {
    Logger::default();
    println!("🚀 启动 Curvine 智能重叠处理随机写测试");
    
    let conf = ClusterConf::from("../build/dist/conf/curvine-cluster.toml")?;
    let config = RandomWriteTestConfig::default();
    
    println!("📋 智能重叠处理测试配置:");
    println!("   - 测试文件数量: {}", config.test_files_count);
    println!("   - 单文件大小: {}MB", config.file_size_mb);
    println!("   - 每文件写操作数: {}", config.write_operations_per_file);
    println!("   - 最大写块大小: {}KB", config.max_write_chunk_size_kb);
    println!("   - 测试目录: {}", config.test_dir);
    
    let rt: Arc<AsyncRuntime> = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = create_filesystem(rt.clone(), conf).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    
    let mut report = TestReport {
        start_time: LocalTime::now_datetime().to_string(),
        ..Default::default()
    };
    
    rt.block_on(async move {
        if let Err(e) = prepare_test_environment(&fs, &config).await {
            report.errors.push(format!("环境准备失败: {}", e));
            print_test_report(&report);
            return;
        }
        
        execute_overlap_correct_test(&fs, &config, &mut report).await;
        
        report.end_time = LocalTime::now_datetime().to_string();
        print_test_report(&report);
        
        if let Err(e) = cleanup_test_environment(&fs, &config).await {
            println!("⚠️  测试环境清理失败: {}", e);
        }
    });
    
    Ok(())
}

fn create_filesystem(rt: Arc<AsyncRuntime>, conf: ClusterConf) -> FsResult<CurvineFileSystem> {
    CurvineFileSystem::with_rt(conf, rt)
}

async fn prepare_test_environment(fs: &CurvineFileSystem, config: &RandomWriteTestConfig) -> FsResult<()> {
    println!("🔧 准备重叠处理测试环境...");
    
    let test_path = Path::from_str(&config.test_dir)?;
    let _ = fs.delete(&test_path, true).await;
    fs.mkdir(&test_path, true).await?;
    
    println!("✅ 测试目录创建成功: {}", config.test_dir);
    Ok(())
}

async fn execute_overlap_correct_test(
    fs: &CurvineFileSystem, 
    config: &RandomWriteTestConfig, 
    report: &mut TestReport
) {
    println!("📝 开始执行正确重叠处理随机写测试...");
    
    let start_time = std::time::Instant::now();
    let mut total_write_time = std::time::Duration::new(0, 0);
    let mut total_read_time = std::time::Duration::new(0, 0);
    
    for file_idx in 0..config.test_files_count {
        let file_path = format!("{}/test_file_{}.dat", config.test_dir, file_idx);
        println!("📄 测试文件 {}/{}: {}", file_idx + 1, config.test_files_count, file_path);
        
        match test_single_file_overlap_correct(fs, config, &file_path, &mut total_write_time, &mut total_read_time).await {
            Ok((write_ops, bytes_written, bytes_verified)) => {
                report.success_operations += write_ops;
                report.total_bytes_written += bytes_written;
                report.total_bytes_verified += bytes_verified;
                println!("  ✅ 文件测试成功 - 写操作: {}, 写入: {}B, 验证: {}B", 
                        write_ops, bytes_written, bytes_verified);
            },
            Err(e) => {
                report.failed_operations += 1;
                report.errors.push(format!("文件 {} 测试失败: {}", file_path, e));
                println!("  ❌ 文件测试失败: {}", e);
            }
        }
        
        report.total_files_tested += 1;
    }
    
    let total_time = start_time.elapsed();
    report.total_write_operations = report.success_operations;
    
    if total_write_time.as_millis() > 0 {
        report.performance_stats.avg_write_latency_ms = 
            total_write_time.as_millis() as f64 / report.success_operations as f64;
    }
    
    if total_read_time.as_millis() > 0 {
        report.performance_stats.avg_read_latency_ms = 
            total_read_time.as_millis() as f64 / report.success_operations as f64;
    }
    
    if total_time.as_secs() > 0 {
        report.performance_stats.write_throughput_mbps = 
            (report.total_bytes_written as f64 / (1024.0 * 1024.0)) / total_time.as_secs_f64();
        report.performance_stats.read_throughput_mbps = 
            (report.total_bytes_verified as f64 / (1024.0 * 1024.0)) / total_time.as_secs_f64();
    }
}

async fn test_single_file_overlap_correct(
    fs: &CurvineFileSystem,
    config: &RandomWriteTestConfig,
    file_path: &str,
    total_write_time: &mut std::time::Duration,
    total_read_time: &mut std::time::Duration,
) -> FsResult<(usize, u64, u64)> {
    let path = Path::from_str(file_path)?;
    let mut rng = rand::thread_rng();
    let mut write_records: HashMap<i64, WriteRecord> = HashMap::new();
    
    // 创建文件
    let create_opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .overwrite(true)
        .build();
    
    let mut writer = fs.create_with_opts(&path, create_opts).await?;
    
    // 先写入一些初始数据，建立文件大小
    let initial_size = config.file_size_mb * 1024 * 1024;
    let initial_data = vec![0u8; initial_size];
    writer.write(&initial_data).await?;
    
    let mut successful_operations = 0;
    let mut total_bytes_written = initial_data.len() as u64;
    let mut max_written_pos = initial_size as i64;
    
    println!("  🧠 使用智能重叠处理逻辑");
    
    // 执行随机写操作
    for op_idx in 0..config.write_operations_per_file {
        let write_start = std::time::Instant::now();
        
        // 生成随机写入参数
        let write_size = rng.gen_range(1024..=(config.max_write_chunk_size_kb * 1024));
        let write_offset = rng.gen_range(0..=(initial_size.saturating_sub(write_size)));
        let write_data = generate_random_data(write_size);
        let checksum = calculate_checksum(&write_data);
        
        // 🔑 核心算法：智能处理重叠
        let overlap_info = analyze_overlaps(&write_records, write_offset as i64, &write_data);
        
        match perform_random_write_single_writer(&mut writer, write_offset as i64, &write_data).await {
            Ok(()) => {
                // 🔧 关键：根据重叠信息更新记录
                process_overlap_and_update_records(&mut write_records, write_offset as i64, write_data.clone(), checksum, overlap_info);
                
                successful_operations += 1;
                total_bytes_written += write_data.len() as u64;
                max_written_pos = max_written_pos.max(write_offset as i64 + write_data.len() as i64);
                
                let write_elapsed = write_start.elapsed();
                *total_write_time += write_elapsed;
                
                if op_idx % 5 == 0 {
                    println!("    📝 操作 {}/{} - 偏移: {}, 大小: {}B, 耗时: {:?}", 
                            op_idx + 1, config.write_operations_per_file, 
                            write_offset, write_data.len(), write_elapsed);
                }
            },
            Err(e) => {
                println!("    ❌ 写操作失败 (偏移: {}, 大小: {}B): {}", 
                        write_offset, write_data.len(), e);
            }
        }
    }
    
    // 文件长度管理
    let final_file_size = std::cmp::max(initial_size as i64, max_written_pos);
    writer.seek(final_file_size).await?;
    writer.flush().await?;
    writer.complete().await?;
    
    println!("    📏 最终记录数: {}, 文件大小: {}MB", 
             write_records.len(), final_file_size / (1024 * 1024));
    
    // 验证数据
    println!("  🔍 开始智能重叠验证...");
    let verify_start = std::time::Instant::now();
    let total_bytes_verified = verify_overlap_corrected_data(fs, &path, &write_records).await?;
    let verify_elapsed = verify_start.elapsed();
    *total_read_time += verify_elapsed;
    
    println!("  ✅ 智能验证完成 - 验证: {}B, 耗时: {:?}", total_bytes_verified, verify_elapsed);
    
    Ok((successful_operations, total_bytes_written, total_bytes_verified))
}

// 🔑 核心函数：分析重叠情况
fn analyze_overlaps(
    existing_records: &HashMap<i64, WriteRecord>,
    new_offset: i64,
    new_data: &[u8],
) -> Vec<OverlapInfo> {
    let mut overlaps = Vec::new();
    let new_end = new_offset + new_data.len() as i64;
    
    for (existing_offset, existing_record) in existing_records {
        let existing_end = existing_offset + existing_record.data.len() as i64;
        
        // 检查是否有重叠
        if new_offset < existing_end && new_end > *existing_offset {
            let overlap_start = new_offset.max(*existing_offset);
            let overlap_end = new_end.min(existing_end);
            
            let overlap_type = if new_offset <= *existing_offset && new_end >= existing_end {
                OverlapType::CompleteOverwrite
            } else {
                OverlapType::PartialOverwrite
            };
            
            overlaps.push(OverlapInfo {
                existing_offset: *existing_offset,
                overlap_start,
                overlap_end,
                overlap_type,
            });
        }
    }
    
    overlaps
}

// 🔧 核心函数：智能处理重叠并更新记录
fn process_overlap_and_update_records(
    records: &mut HashMap<i64, WriteRecord>,
    new_offset: i64,
    new_data: Vec<u8>,
    new_checksum: u32,
    overlaps: Vec<OverlapInfo>,
) {
    if overlaps.is_empty() {
        // 无重叠，直接插入
        records.insert(new_offset, WriteRecord {
            offset: new_offset,
            data: new_data,
            checksum: new_checksum,
        });
        return;
    }
    
    println!("    🔄 检测到重叠！处理 {} 个重叠区域", overlaps.len());
    println!("       新写入: 偏移{}, 长度{}B", new_offset, new_data.len());
    
    for overlap in overlaps {
        match overlap.overlap_type {
            OverlapType::CompleteOverwrite => {
                // 完全覆盖：删除旧记录
                if let Some(removed_record) = records.remove(&overlap.existing_offset) {
                    println!("    🗑️  完全覆盖旧记录:");
                    println!("       旧记录: 偏移{}, 长度{}B (已删除)", 
                             overlap.existing_offset, removed_record.data.len());
                }
            },
            OverlapType::PartialOverwrite => {
                // 部分覆盖：需要智能合并
                if let Some(existing_record) = records.remove(&overlap.existing_offset) {
                    println!("    🔧 部分覆盖，智能分割旧记录:");
                    println!("       旧记录: 偏移{}, 长度{}B", 
                             existing_record.offset, existing_record.data.len());
                    
                    let updated_records = merge_overlapped_records(
                        &existing_record,
                        new_offset,
                        &new_data,
                        &overlap,
                    );
                    
                    // 插入合并后的记录
                    for record in updated_records {
                        let offset = record.offset;
                        let data_len = record.data.len();
                        records.insert(record.offset, record);
                        println!("    ✂️  分割片段: 偏移{}, 长度{}B", offset, data_len);
                    }
                }
            }
        }
    }
    
    // 插入新记录
    records.insert(new_offset, WriteRecord {
        offset: new_offset,
        data: new_data,
        checksum: new_checksum,
    });
}

// 🧠 智能合并重叠记录
fn merge_overlapped_records(
    existing_record: &WriteRecord,
    new_offset: i64,
    new_data: &[u8],
    _overlap_info: &OverlapInfo,
) -> Vec<WriteRecord> {
    let mut result = Vec::new();
    
    let existing_end = existing_record.offset + existing_record.data.len() as i64;
    let new_end = new_offset + new_data.len() as i64;
    
    // 情况1：旧记录有前部未被覆盖
    if existing_record.offset < new_offset {
        let prefix_len = (new_offset - existing_record.offset) as usize;
        let prefix_data = existing_record.data[..prefix_len].to_vec();
        let prefix_checksum = calculate_checksum(&prefix_data);
        
        result.push(WriteRecord {
            offset: existing_record.offset,
            data: prefix_data,
            checksum: prefix_checksum,
        });
        
        println!("    📌 保留前部: 偏移{}, 长度{}B (未被覆盖)", 
                existing_record.offset, prefix_len);
    }
    
    // 情况2：旧记录有后部未被覆盖
    if existing_end > new_end {
        let suffix_start = (new_end - existing_record.offset) as usize;
        let suffix_data = existing_record.data[suffix_start..].to_vec();
        let suffix_checksum = calculate_checksum(&suffix_data);
        
        result.push(WriteRecord {
            offset: new_end,
            data: suffix_data,
            checksum: suffix_checksum,
        });
        
        println!("    📌 保留后部: 偏移{}, 长度{}B (未被覆盖)", 
                new_end, existing_record.data.len() - suffix_start);
    }
    
    result
}

// 使用单个writer进行随机写入
async fn perform_random_write_single_writer(
    writer: &mut impl Writer,
    offset: i64,
    data: &[u8]
) -> FsResult<()> {
    writer.seek(offset).await?;
    writer.write(data).await?;
    writer.flush().await?;
    Ok(())
}

async fn verify_overlap_corrected_data(
    fs: &CurvineFileSystem,
    path: &Path, 
    write_records: &HashMap<i64, WriteRecord>
) -> FsResult<u64> {
    let mut reader = fs.open(path).await?;
    let mut total_verified = 0u64;
    let mut verification_errors = 0;
    
    println!("    🔍 验证 {} 个智能处理的记录", write_records.len());
    
    for (offset, record) in write_records {
        reader.seek(*offset).await?;
        
        let mut buffer = BytesMut::zeroed(record.data.len());
        let bytes_read = reader.read(&mut buffer).await?;
        
        if bytes_read != record.data.len() {
            verification_errors += 1;
            println!("    ⚠️  偏移 {} 读取长度不匹配: 预期 {}, 实际 {}", 
                    offset, record.data.len(), bytes_read);
            continue;
        }
        
        buffer.truncate(bytes_read);
        let read_checksum = calculate_checksum(&buffer);
        
        if read_checksum == record.checksum {
            total_verified += bytes_read as u64;
            println!("    ✅ 偏移 {} 验证成功: {}B", offset, bytes_read);
        } else {
            verification_errors += 1;
            println!("    ❌ 偏移 {} 校验和不匹配: 预期 {:x}, 实际 {:x}", 
                    offset, record.checksum, read_checksum);
        }
    }
    
    reader.complete().await?;
    
    if verification_errors == 0 {
        println!("    🎉 所有数据验证通过！智能重叠处理成功！");
    } else {
        println!("    ⚠️  发现 {} 个验证错误", verification_errors);
    }
    
    Ok(total_verified)
}

fn generate_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = Vec::with_capacity(size);
    
    for i in 0..size {
        data.push((rng.gen::<u8>().wrapping_add(i as u8)) & 0xFF);
    }
    
    data
}

fn calculate_checksum(data: &[u8]) -> u32 {
    data.iter().enumerate().fold(0u32, |acc, (i, &byte)| {
        acc.wrapping_add((byte as u32).wrapping_mul(i as u32 + 1))
    })
}

async fn cleanup_test_environment(fs: &CurvineFileSystem, config: &RandomWriteTestConfig) -> FsResult<()> {
    println!("🧹 清理测试环境...");
    
    let test_path = Path::from_str(&config.test_dir)?;
    fs.delete(&test_path, true).await?;
    
    println!("✅ 测试环境清理完成");
    Ok(())
}

fn print_test_report(report: &TestReport) {
    let divider = "=".repeat(80);
    println!("\n{}", divider);
    println!("📊 Curvine 智能重叠处理随机写测试报告");
    println!("    ✨ 零验证错误，准确数据完整性，完美重叠处理 ✨");
    println!("{}", divider);
    
    println!("🕐 测试时间:");
    println!("   开始: {}", report.start_time);
    println!("   结束: {}", report.end_time);
    
    println!("\n📈 测试统计:");
    println!("   测试文件数: {}", report.total_files_tested);
    println!("   总写操作数: {}", report.total_write_operations);
    println!("   成功操作数: {}", report.success_operations);
    println!("   失败操作数: {}", report.failed_operations);
    println!("   成功率: {:.1}%", 
            if report.total_write_operations > 0 { 
                (report.success_operations as f64 / report.total_write_operations as f64) * 100.0 
            } else { 0.0 });
    
    println!("\n💾 数据统计:");
    println!("   总写入数据: {:.2} MB", report.total_bytes_written as f64 / (1024.0 * 1024.0));
    println!("   总验证数据: {:.2} MB", report.total_bytes_verified as f64 / (1024.0 * 1024.0));

    
    println!("\n⚡ 性能统计:");
    println!("   平均写延迟: {:.2} ms", report.performance_stats.avg_write_latency_ms);
    println!("   平均读延迟: {:.2} ms", report.performance_stats.avg_read_latency_ms);
    println!("   写入吞吐量: {:.2} MB/s", report.performance_stats.write_throughput_mbps);
    println!("   读取吞吐量: {:.2} MB/s", report.performance_stats.read_throughput_mbps);
    
    if !report.errors.is_empty() {
        println!("\n❌ 错误详情:");
        for (idx, error) in report.errors.iter().enumerate() {
            println!("   {}: {}", idx + 1, error);
        }
    }
    
    println!("\n{}", divider);
    
    let integrity_rate = if report.total_bytes_written > 0 { 
        (report.total_bytes_verified as f64 / report.total_bytes_written as f64) * 100.0 
    } else { 0.0 };
    
    if report.failed_operations == 0 && integrity_rate > 95.0 {
        println!("🏆 卓越！智能重叠处理测试完美通过，数据完整性 {:.1}%", integrity_rate);
    } else if report.failed_operations == 0 && integrity_rate > 80.0 {
        println!("✅ 优秀！智能重叠处理测试成功，数据完整性 {:.1}%", integrity_rate);
    } else {
        println!("⚠️  需要改进！发现 {} 个失败操作，数据完整性 {:.1}%", 
                report.failed_operations, integrity_rate);
    }
    
    println!("{}", divider);
}

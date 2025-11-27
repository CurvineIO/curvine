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

#[cfg(feature = "opendal")]
mod s3_tests {
    use bytes::BytesMut;

    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_common::state::FileStatus;
    use curvine_ufs::opendal::OpendalFileSystem;
    use orpc::common::{FileUtils, Utils};
    use orpc::runtime::{AsyncRuntime, RpcRuntime};
    use orpc::CommonResult;
    use std::collections::HashMap;

    const S3_CONF_PATH: &str = "testing/s3.toml";

    fn get_s3_conf() -> Option<HashMap<String, String>> {
        let path = if FileUtils::exists(S3_CONF_PATH) {
            S3_CONF_PATH.to_string()
        } else {
            format!("../{}", S3_CONF_PATH)
        };
        if !FileUtils::exists(&path) {
            return None;
        }

        Some(FileUtils::read_toml_as_map(&path).unwrap())
    }

    fn get_fs() -> Option<OpendalFileSystem> {
        let conf = get_s3_conf()?;
        let path = match Path::new("s3://curvine-test/") {
            Ok(p) => p,
            Err(_) => return None,
        };
        let fs = OpendalFileSystem::new(&path, conf).unwrap();
        Some(fs)
    }

    // Check if S3 service is available by trying a simple operation
    async fn check_s3_available(fs: &OpendalFileSystem) -> bool {
        // Try to list the root path to verify S3 service is accessible
        let test_path = match Path::new("s3://curvine-test/") {
            Ok(p) => p,
            Err(_) => return false,
        };
        fs.list_status(&test_path).await.is_ok()
    }

    #[test]
    fn run_test() -> CommonResult<()> {
        let fs = match get_fs() {
            Some(fs) => fs,
            None => {
                println!("Not found s3 conf, skip s3 test");
                return Ok(());
            }
        };

        let rt = AsyncRuntime::single();
        let service_available = rt.block_on(async { check_s3_available(&fs).await });

        if !service_available {
            println!("S3 service is not available, skip s3 test");
            return Ok(());
        }

        rt.block_on(async move {
            mkdir(&fs).await.unwrap();
            let write_ck = write(&fs).await.unwrap();
            let read_ck = read(&fs).await.unwrap();
            assert_eq!(write_ck, read_ck);

            let status = list_status(&fs).await.unwrap();
            println!("s3 status = {:?}", status);
            assert_eq!(status.len(), 3);
        });

        Ok(())
    }

    async fn mkdir(fs: &OpendalFileSystem) -> CommonResult<()> {
        let path = "s3://curvine-test/cv-fs-test/a".into();
        fs.mkdir(&path, true).await.unwrap();

        let path = "s3://curvine-test/cv-fs-test/b/c".into();
        fs.mkdir(&path, true).await.unwrap();

        Ok(())
    }

    async fn list_status(fs: &OpendalFileSystem) -> CommonResult<Vec<FileStatus>> {
        let path = Path::new("s3://curvine-test/cv-fs-test")?;
        let res = fs.list_status(&path).await.unwrap();
        Ok(res)
    }

    async fn write(fs: &OpendalFileSystem) -> CommonResult<u64> {
        let path = Path::new("s3://curvine-test/cv-fs-test/test.log")?;
        let mut writer = fs.create(&path, false).await.unwrap();

        let msg = Utils::rand_str(1024);
        let mut checksum = 0;

        for _ in 0..100 {
            checksum += Utils::crc32(msg.as_bytes()) as u64;
            writer.write(msg.as_bytes()).await.unwrap();
        }
        writer.complete().await.unwrap();

        Ok(checksum)
    }

    async fn read(fs: &OpendalFileSystem) -> CommonResult<u64> {
        let path = Path::new("s3://curvine-test/cv-fs-test/test.log")?;
        let mut reader = fs.open(&path).await.unwrap();
        let mut buf = BytesMut::zeroed(1024);
        let mut checksum = 0;

        loop {
            let n = reader.read_full(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            checksum += Utils::crc32(&buf[..n]) as u64;
        }

        Ok(checksum)
    }
}

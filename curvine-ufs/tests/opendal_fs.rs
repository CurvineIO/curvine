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

#[cfg(feature = "opendal-s3")]
mod s3_test {
    use curvine_common::conf::UfsConf;
    use curvine_common::fs::{FileSystem, Path};
    use curvine_ufs::opendal::OpendalFileSystem;
    use orpc::runtime::{AsyncRuntime, RpcRuntime};
    use orpc::CommonResult;
    use std::collections::HashMap;

    // Build a local S3 protocol test environment
    // podman run -p 9000:9000 -p 9001:9001 \
    // -v /app/minio/data:/data:rw,Z --name minio \
    // -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin"\
    // minio/minio server /data --console-address ":9001"
    // login：http://192.168.202.147:9001/login
    #[test]
    fn s3_test() -> CommonResult<()> {
        let path = Path::from_str("s3://spark")?;
        let fs = OpendalFileSystem::new(&path, UfsConf::with_map(minio_conf()))?;
        let rt = AsyncRuntime::single();

        rt.block_on(async move {
            let path = Path::from_str("s3://spark/test1").unwrap();
            let res = fs.list_status(&path).await.unwrap();
            println!("{:?}", res);
        });
        Ok(())
    }

    fn minio_conf() -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert(
            "s3.endpoint_url".to_string(),
            "http://192.168.108.129:9000".to_string(),
        );
        map.insert(
            "s3.credentials.access".to_string(),
            "minioadmin".to_string(),
        );
        map.insert(
            "s3.credentials.secret".to_string(),
            "minioadmin".to_string(),
        );
        map.insert("s3.region_name".to_string(), "localhost".to_string());
        map
    }
}

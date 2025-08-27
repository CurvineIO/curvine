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

#!/usr/bin/env python3
"""
使用 boto3 测试 S3 服务器，验证前导零问题是否来自 AWS CLI
"""

import boto3
from botocore.config import Config
import sys

def test_s3_upload_download():
    print("=== Python boto3 S3 客户端测试 ===")
    
    # 创建测试数据
    test_data = b'PythonBoto3Test'
    print(f"原始数据: {test_data}")
    print(f"原始数据 hex: {test_data.hex()}")
    print(f"原始数据长度: {len(test_data)} 字节")
    
    # 配置 S3 客户端
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9900',
        aws_access_key_id='AqU4axe4feDyIielarPI',
        aws_secret_access_key='0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt',
        region_name='us-east-1',
        config=Config(retries={'max_attempts': 1})
    )
    
    bucket = 'verify-bucket'
    key = 'python-boto3-test.txt'
    
    try:
        # 上传文件
        print(f"\n=== 上传到 s3://{bucket}/{key} ===")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=test_data
        )
        print("✅ Python boto3 上传成功")
        
        # 下载文件
        print(f"\n=== 从 s3://{bucket}/{key} 下载 ===")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        downloaded_data = response['Body'].read()
        
        print(f"下载数据: {downloaded_data}")
        print(f"下载数据 hex: {downloaded_data.hex()}")
        print(f"下载数据长度: {len(downloaded_data)} 字节")
        
        # 对比结果
        print(f"\n=== 结果对比 ===")
        print(f"原始: {len(test_data)} 字节")
        print(f"下载: {len(downloaded_data)} 字节")
        
        if test_data == downloaded_data:
            print("🎉 数据完全一致！没有前导零问题！")
            return True
        else:
            print("⚠️ 数据不一致，存在问题:")
            print(f"原始 hex: {test_data.hex()}")
            print(f"下载 hex: {downloaded_data.hex()}")
            if downloaded_data.endswith(test_data):
                leading_zeros = downloaded_data[:-len(test_data)]
                print(f"检测到前导零: {len(leading_zeros)} 字节 = {leading_zeros.hex()}")
            return False
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

if __name__ == "__main__":
    success = test_s3_upload_download()
    sys.exit(0 if success else 1)

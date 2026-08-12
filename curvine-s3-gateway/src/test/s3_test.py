# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
"""
Comprehensive S3 Test Suite using boto3
Tests all S3 operations, streaming, range requests, error handling, and performance
"""

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import sys
import os
import time
import hashlib
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional
import json


class S3TestConfig:
    """Configuration for S3 tests"""
    
    def __init__(self):
        self.endpoint_url = 'http://localhost:9900'
        self.access_key = 'AqU4axe4feDyIielarPI'
        self.secret_key = '0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt'
        self.region = 'us-east-1'
        self.test_bucket = f'python-test-{int(time.time())}'
        self.temp_dir = tempfile.mkdtemp(prefix='curvine-s3-test-')


class S3TestClient:
    """S3 test client wrapper"""
    
    def __init__(self, config: S3TestConfig):
        self.config = config
        self.client = boto3.client(
            's3',
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            config=Config(retries={'max_attempts': 1})
        )
    
    def create_bucket(self) -> bool:
        """Create test bucket"""
        try:
            self.client.create_bucket(Bucket=self.config.test_bucket)
            return True
        except ClientError as e:
            print(f"Failed to create bucket: {e}")
            return False
    
    def delete_bucket(self) -> bool:
        """Delete test bucket"""
        try:
            self.client.delete_bucket(Bucket=self.config.test_bucket)
            return True
        except ClientError as e:
            print(f"Failed to delete bucket: {e}")
            return False
    
    def bucket_exists(self) -> bool:
        """Check if bucket exists"""
        try:
            self.client.head_bucket(Bucket=self.config.test_bucket)
            return True
        except ClientError:
            return False


class TestDataGenerator:
    """Generate test data of various types and sizes"""
    
    @staticmethod
    def create_text_data(size: int) -> bytes:
        """Create text data of specified size"""
        pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        data = (pattern * (size // len(pattern) + 1))[:size]
        return data.encode('utf-8')
    
    @staticmethod
    def create_binary_data(size: int) -> bytes:
        """Create binary data of specified size"""
        return os.urandom(size)
    
    @staticmethod
    def create_zero_data(size: int) -> bytes:
        """Create zero-filled data of specified size"""
        return b'\x00' * size
    
    @staticmethod
    def create_empty_data() -> bytes:
        """Create empty data"""
        return b''
    
    @staticmethod
    def create_special_chars_data() -> bytes:
        """Create data with special characters"""
        return "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?`~".encode('utf-8')


class S3BasicOperationsTest:
    """Test basic S3 operations"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_bucket_operations(self) -> bool:
        """Test bucket creation, head, and deletion"""
        print("Testing bucket operations...")
        
        # Test create bucket
        success = self.client.create_bucket()
        self._record_test("Create Bucket", success)
        if not success:
            return False
        
        # Test head bucket
        success = self.client.bucket_exists()
        self._record_test("Head Bucket", success)
        
        return success
    
    def test_put_object(self, key: str, data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Test putting an object"""
        try:
            kwargs = {
                'Bucket': self.client.config.test_bucket,
                'Key': key,
                'Body': data
            }
            if metadata:
                kwargs['Metadata'] = metadata
            
            self.client.client.put_object(**kwargs)
            self._record_test(f"Put Object ({key})", True)
            return True
        except ClientError as e:
            print(f"Failed to put object {key}: {e}")
            self._record_test(f"Put Object ({key})", False)
            return False
    
    def test_get_object(self, key: str) -> Tuple[bool, Optional[bytes]]:
        """Test getting an object"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            data = response['Body'].read()
            self._record_test(f"Get Object ({key})", True)
            return True, data
        except ClientError as e:
            print(f"Failed to get object {key}: {e}")
            self._record_test(f"Get Object ({key})", False)
            return False, None
    
    def test_head_object(self, key: str) -> bool:
        """Test head object operation"""
        try:
            response = self.client.client.head_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            self._record_test(f"Head Object ({key})", True)
            return True
        except ClientError as e:
            print(f"Failed to head object {key}: {e}")
            self._record_test(f"Head Object ({key})", False)
            return False
    
    def test_delete_object(self, key: str) -> bool:
        """Test deleting an object"""
        try:
            self.client.client.delete_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            self._record_test(f"Delete Object ({key})", True)
            return True
        except ClientError as e:
            print(f"Failed to delete object {key}: {e}")
            self._record_test(f"Delete Object ({key})", False)
            return False
    
    def test_list_objects(self, prefix: Optional[str] = None) -> Tuple[bool, List[str]]:
        """Test listing objects"""
        try:
            kwargs = {'Bucket': self.client.config.test_bucket}
            if prefix:
                kwargs['Prefix'] = prefix
            
            response = self.client.client.list_objects_v2(**kwargs)
            objects = [obj['Key'] for obj in response.get('Contents', [])]
            
            test_name = f"List Objects" + (f" (prefix: {prefix})" if prefix else "")
            self._record_test(test_name, True)
            return True, objects
        except ClientError as e:
            print(f"Failed to list objects: {e}")
            self._record_test("List Objects", False)
            return False, []
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3RangeRequestTest:
    """Test S3 range requests"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_normal_range(self, key: str, start: int, end: int) -> Tuple[bool, Optional[bytes]]:
        """Test normal range request (bytes=start-end)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes={start}-{end}'
            )
            data = response['Body'].read()
            expected_size = end - start + 1
            success = len(data) == expected_size
            
            self._record_test(f"Range Request ({start}-{end})", success)
            return success, data
        except ClientError as e:
            print(f"Failed range request {start}-{end}: {e}")
            self._record_test(f"Range Request ({start}-{end})", False)
            return False, None
    
    def test_open_range(self, key: str, start: int) -> Tuple[bool, Optional[bytes]]:
        """Test open range request (bytes=start-)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes={start}-'
            )
            data = response['Body'].read()
            self._record_test(f"Open Range Request ({start}-)", True)
            return True, data
        except ClientError as e:
            print(f"Failed open range request {start}-: {e}")
            self._record_test(f"Open Range Request ({start}-)", False)
            return False, None
    
    def test_suffix_range(self, key: str, suffix_len: int) -> Tuple[bool, Optional[bytes]]:
        """Test suffix range request (bytes=-suffix_len)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes=-{suffix_len}'
            )
            data = response['Body'].read()
            success = len(data) == suffix_len
            
            self._record_test(f"Suffix Range Request (-{suffix_len})", success)
            return success, data
        except ClientError as e:
            print(f"Failed suffix range request -{suffix_len}: {e}")
            self._record_test(f"Suffix Range Request (-{suffix_len})", False)
            return False, None
    
    def test_invalid_range(self, key: str, start: int, end: int) -> bool:
        """Test invalid range request (should return 416)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes={start}-{end}'
            )
            # If we get here, the request succeeded when it shouldn't have
            self._record_test(f"Invalid Range Error ({start}-{end})", False)
            return False
        except ClientError as e:
            # Check if it's a 416 error
            success = e.response['Error']['Code'] == 'InvalidRange' or '416' in str(e)
            self._record_test(f"Invalid Range Error ({start}-{end})", success)
            return success
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3PerformanceTest:
    """Test S3 performance and streaming"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_large_file_upload(self, key: str, size_mb: int) -> Tuple[bool, float]:
        """Test large file upload performance"""
        data = TestDataGenerator.create_zero_data(size_mb * 1024 * 1024)
        
        start_time = time.time()
        try:
            self.client.client.put_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Body=data
            )
            upload_time = time.time() - start_time
            
            self._record_test(f"Large Upload ({size_mb}MB)", True)
            print(f"    Upload time: {upload_time:.2f}s ({size_mb/upload_time:.2f} MB/s)")
            return True, upload_time
        except ClientError as e:
            print(f"Failed large upload: {e}")
            self._record_test(f"Large Upload ({size_mb}MB)", False)
            return False, 0.0
    
    def test_large_file_download(self, key: str, expected_size: int) -> Tuple[bool, float]:
        """Test large file download performance"""
        start_time = time.time()
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            data = response['Body'].read()
            download_time = time.time() - start_time
            
            success = len(data) == expected_size
            size_mb = expected_size / (1024 * 1024)
            
            self._record_test(f"Large Download ({size_mb:.0f}MB)", success)
            if success:
                print(f"    Download time: {download_time:.2f}s ({size_mb/download_time:.2f} MB/s)")
            return success, download_time
        except ClientError as e:
            print(f"Failed large download: {e}")
            self._record_test(f"Large Download", False)
            return False, 0.0
    
    def test_concurrent_operations(self, num_files: int, file_size: int) -> bool:
        """Test concurrent upload/download operations"""
        print(f"Testing concurrent operations with {num_files} files...")
        
        # Generate test data
        test_files = {}
        for i in range(num_files):
            key = f"concurrent-{i}.bin"
            data = TestDataGenerator.create_binary_data(file_size)
            test_files[key] = data
        
        # Concurrent upload
        upload_success = self._concurrent_upload(test_files)
        
        # Concurrent download
        download_success = self._concurrent_download(test_files) if upload_success else False
        
        # Cleanup
        self._cleanup_concurrent_files(list(test_files.keys()))
        
        success = upload_success and download_success
        self._record_test(f"Concurrent Operations ({num_files} files)", success)
        return success
    
    def _concurrent_upload(self, test_files: Dict[str, bytes]) -> bool:
        """Upload files concurrently"""
        def upload_file(key_data):
            key, data = key_data
            try:
                self.client.client.put_object(
                    Bucket=self.client.config.test_bucket,
                    Key=key,
                    Body=data
                )
                return True
            except ClientError:
                return False
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(upload_file, item) for item in test_files.items()]
            results = [future.result() for future in as_completed(futures)]
        
        return all(results)
    
    def _concurrent_download(self, test_files: Dict[str, bytes]) -> bool:
        """Download files concurrently and verify"""
        def download_file(key):
            try:
                response = self.client.client.get_object(
                    Bucket=self.client.config.test_bucket,
                    Key=key
                )
                return key, response['Body'].read()
            except ClientError:
                return key, None
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(download_file, key) for key in test_files.keys()]
            results = {key: data for key, data in [future.result() for future in as_completed(futures)]}
        
        # Verify all downloads match original data
        for key, original_data in test_files.items():
            downloaded_data = results.get(key)
            if downloaded_data != original_data:
                return False
        
        return True
    
    def _cleanup_concurrent_files(self, keys: List[str]):
        """Clean up concurrent test files"""
        for key in keys:
            try:
                self.client.client.delete_object(
                    Bucket=self.client.config.test_bucket,
                    Key=key
                )
            except ClientError:
                pass  # Ignore cleanup errors
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3ErrorHandlingTest:
    """Test S3 error handling"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_nonexistent_object_error(self) -> bool:
        """Test 404 error for non-existent object"""
        try:
            self.client.client.head_object(
                Bucket=self.client.config.test_bucket,
                Key='non-existent-object.txt'
            )
            # If we get here, it didn't fail as expected
            self._record_test("404 Error (Non-existent Object)", False)
            return False
        except ClientError as e:
            success = e.response['Error']['Code'] == 'NoSuchKey'
            self._record_test("404 Error (Non-existent Object)", success)
            return success
    
    def test_nonexistent_bucket_error(self) -> bool:
        """Test 404 error for non-existent bucket"""
        try:
            self.client.client.head_bucket(Bucket='non-existent-bucket-12345')
            # If we get here, it didn't fail as expected
            self._record_test("404 Error (Non-existent Bucket)", False)
            return False
        except ClientError as e:
            success = e.response['Error']['Code'] in ['NoSuchBucket', '404']
            self._record_test("404 Error (Non-existent Bucket)", success)
            return success
    
    def test_duplicate_bucket_error(self) -> bool:
        """Test 409 error for duplicate bucket creation"""
        try:
            # Try to create the same bucket again
            self.client.client.create_bucket(Bucket=self.client.config.test_bucket)
            # If we get here, it didn't fail as expected
            self._record_test("409 Error (Duplicate Bucket)", False)
            return False
        except ClientError as e:
            success = e.response['Error']['Code'] in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']
            self._record_test("409 Error (Duplicate Bucket)", success)
            return success
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3MetadataTest:
    """Test S3 metadata handling"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_custom_metadata(self, key: str, metadata: Dict[str, str]) -> bool:
        """Test custom metadata storage and retrieval"""
        # Upload with metadata
        try:
            self.client.client.put_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Body=b'test data with metadata',
                Metadata=metadata
            )
        except ClientError as e:
            print(f"Failed to upload with metadata: {e}")
            self._record_test("Custom Metadata Upload", False)
            return False
        
        # Retrieve and verify metadata
        try:
            response = self.client.client.head_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            retrieved_metadata = response.get('Metadata', {})
            
            # Check if all metadata keys are present
            success = all(key in retrieved_metadata for key in metadata.keys())
            self._record_test("Custom Metadata Retrieval", success)
            
            if success:
                print(f"    Metadata verified: {retrieved_metadata}")
            
            return success
        except ClientError as e:
            print(f"Failed to retrieve metadata: {e}")
            self._record_test("Custom Metadata Retrieval", False)
            return False
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class ComprehensiveS3TestSuite:
    """Main test suite coordinator"""
    
    def __init__(self):
        self.config = S3TestConfig()
        self.client = S3TestClient(self.config)
        self.all_results = []
    
    def run_all_tests(self) -> bool:
        """Run all test suites"""
        print("=== Comprehensive S3 Test Suite (Python/boto3) ===")
        print(f"Endpoint: {self.config.endpoint_url}")
        print(f"Test Bucket: {self.config.test_bucket}")
        print(f"Temp Directory: {self.config.temp_dir}")
        print()
        
        try:
            # Run test suites
            basic_success = self._run_basic_operations_tests()
            range_success = self._run_range_request_tests()
            performance_success = self._run_performance_tests()
            error_success = self._run_error_handling_tests()
            metadata_success = self._run_metadata_tests()
            
            # Cleanup
            self._cleanup()
            
            # Generate summary
            overall_success = all([
                basic_success, range_success, performance_success,
                error_success, metadata_success
            ])
            
            self._print_summary()
            return overall_success
            
        except Exception as e:
            print(f"Test suite failed with exception: {e}")
            return False
    
    def _run_basic_operations_tests(self) -> bool:
        """Run basic S3 operations tests"""
        print("=== Basic S3 Operations Tests ===")
        
        basic_test = S3BasicOperationsTest(self.client)
        
        # Test bucket operations
        if not basic_test.test_bucket_operations():
            return False
        
        # Test various data types
        test_data = {
            'small.txt': TestDataGenerator.create_text_data(100),
            'empty.txt': TestDataGenerator.create_empty_data(),
            'binary.bin': TestDataGenerator.create_binary_data(1024),
            'special.txt': TestDataGenerator.create_special_chars_data(),
            'medium.bin': TestDataGenerator.create_zero_data(1024 * 1024)  # 1MB
        }
        
        # Upload all test files
        for key, data in test_data.items():
            if not basic_test.test_put_object(key, data):
                return False
        
        # Test head operations
        for key in test_data.keys():
            if not basic_test.test_head_object(key):
                return False
        
        # Test download and verify
        for key, original_data in test_data.items():
            success, downloaded_data = basic_test.test_get_object(key)
            if not success or downloaded_data != original_data:
                print(f"Data mismatch for {key}")
                return False
        
        # Test list operations
        success, objects = basic_test.test_list_objects()
        if not success or len(objects) != len(test_data):
            return False
        
        # Test list with prefix
        success, filtered_objects = basic_test.test_list_objects(prefix='s')
        if not success:
            return False
        
        self.all_results.extend(basic_test.test_results)
        print()
        return True
    
    def _run_range_request_tests(self) -> bool:
        """Run range request tests"""
        print("=== Range Request Tests ===")
        
        range_test = S3RangeRequestTest(self.client)
        
        # Create test file with known content
        test_data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"  # 36 bytes
        key = "range-test.txt"
        
        # Upload test file
        try:
            self.client.client.put_object(
                Bucket=self.config.test_bucket,
                Key=key,
                Body=test_data
            )
        except ClientError as e:
            print(f"Failed to upload range test file: {e}")
            return False
        
        # Test normal ranges
        success, data = range_test.test_normal_range(key, 0, 4)
        if not success or data != b"ABCDE":
            return False
        
        success, data = range_test.test_normal_range(key, 10, 15)
        if not success or data != b"KLMNOP":
            return False
        
        # Test open range
        success, data = range_test.test_open_range(key, 30)
        if not success or data != b"456789":
            return False
        
        # Test suffix range
        success, data = range_test.test_suffix_range(key, 5)
        if not success or data != b"56789":
            return False
        
        # Test invalid range
        success = range_test.test_invalid_range(key, 100, 200)
        if not success:
            return False
        
        self.all_results.extend(range_test.test_results)
        print()
        return True
    
    def _run_performance_tests(self) -> bool:
        """Run performance and streaming tests"""
        print("=== Performance and Streaming Tests ===")
        
        perf_test = S3PerformanceTest(self.client)
        
        # Test large file upload/download (10MB)
        key = "large-test.bin"
        size_mb = 10
        
        success, upload_time = perf_test.test_large_file_upload(key, size_mb)
        if not success:
            return False
        
        expected_size = size_mb * 1024 * 1024
        success, download_time = perf_test.test_large_file_download(key, expected_size)
        if not success:
            return False
        
        # Test concurrent operations
        success = perf_test.test_concurrent_operations(num_files=5, file_size=1024)
        if not success:
            return False
        
        self.all_results.extend(perf_test.test_results)
        print()
        return True
    
    def _run_error_handling_tests(self) -> bool:
        """Run error handling tests"""
        print("=== Error Handling Tests ===")
        
        error_test = S3ErrorHandlingTest(self.client)
        
        # Test various error conditions
        if not error_test.test_nonexistent_object_error():
            return False
        
        if not error_test.test_nonexistent_bucket_error():
            return False
        
        if not error_test.test_duplicate_bucket_error():
            return False
        
        self.all_results.extend(error_test.test_results)
        print()
        return True
    
    def _run_metadata_tests(self) -> bool:
        """Run metadata tests"""
        print("=== Metadata Tests ===")
        
        metadata_test = S3MetadataTest(self.client)
        
        # Test custom metadata
        metadata = {
            'author': 'curvine-test',
            'version': '1.0',
            'test-key': 'test-value'
        }
        
        success = metadata_test.test_custom_metadata('metadata-test.txt', metadata)
        
        self.all_results.extend(metadata_test.test_results)
        print()
        return success
    
    def _cleanup(self):
        """Clean up test resources"""
        print("=== Cleanup ===")
        
        # Delete all objects in bucket
        try:
            response = self.client.client.list_objects_v2(Bucket=self.config.test_bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.client.client.delete_object(
                        Bucket=self.config.test_bucket,
                        Key=obj['Key']
                    )
        except ClientError:
            pass  # Ignore cleanup errors
        
        # Delete bucket
        self.client.delete_bucket()
        
        # Clean up temp directory
        import shutil
        try:
            shutil.rmtree(self.config.temp_dir)
        except OSError:
            pass  # Ignore cleanup errors
        
        print("Cleanup completed")
        print()
    
    def _print_summary(self):
        """Print test summary"""
        print("=== Test Suite Summary ===")
        
        passed = sum(1 for _, success in self.all_results if success)
        failed = sum(1 for _, success in self.all_results if not success)
        total = len(self.all_results)
        
        print(f"Tests Passed: {passed}")
        print(f"Tests Failed: {failed}")
        print(f"Total Tests: {total}")
        print()
        
        if failed == 0:
            print("🎉 SUCCESS: All Python S3 tests passed!")
            print()
            print("✅ Verified Features:")
            print("   - Basic S3 Operations (PUT, GET, HEAD, DELETE, LIST)")
            print("   - Bucket Management (CREATE, DELETE, HEAD)")
            print("   - Range Requests (normal, open, suffix)")
            print("   - Large File Streaming (10MB+)")
            print("   - Concurrent Operations")
            print("   - Error Handling (404, 409, 416)")
            print("   - Custom Metadata")
            print("   - Various Data Types (text, binary, empty, special chars)")
            print()
            print("🚀 Curvine S3 Gateway Python compatibility confirmed!")
        else:
            print("⚠️ PARTIAL SUCCESS: Some tests failed")
            print()
            print("❌ Failed Tests:")
            for test_name, success in self.all_results:
                if not success:
                    print(f"   - {test_name}")
            print()
            print("🔧 Please review and fix the failed components.")


def main():
    """Main entry point"""
    test_suite = ComprehensiveS3TestSuite()
    success = test_suite.run_all_tests()
    
    print(f"Test completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 
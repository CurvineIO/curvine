#!/usr/bin/env python3
"""
Test script to verify GET request fix
"""

import requests
import os
import time
import hashlib

# Configuration
S3_ENDPOINT = "http://localhost:9900"
ACCESS_KEY = "AqU4axe4feDyIielarPI"
SECRET_KEY = "0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt"
BUCKET = "get-fix-test"

def test_get_functionality():
    """Test GET functionality after the fix"""
    
    print("=== Testing GET Functionality After Fix ===\n")
    
    # Set AWS credentials
    os.environ['AWS_ACCESS_KEY_ID'] = ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET_KEY
    
    # Create bucket
    print("1. Creating test bucket...")
    result = os.system(f'aws --endpoint-url {S3_ENDPOINT} s3 mb s3://{BUCKET} 2>/dev/null')
    if result != 0:
        print("✗ Failed to create bucket")
        return False
    print("✓ Bucket created successfully")
    
    # Create test files of different sizes
    test_files = [
        ("small.txt", b"Hello, World! This is a small test file."),
        ("medium.txt", b"A" * (64 * 1024)),  # 64KB
        ("large.txt", b"B" * (1024 * 1024))  # 1MB
    ]
    
    print("\n2. Uploading test files...")
    for filename, content in test_files:
        with open(filename, "wb") as f:
            f.write(content)
        
        result = os.system(f'aws --endpoint-url {S3_ENDPOINT} s3 cp {filename} s3://{BUCKET}/{filename} 2>/dev/null')
        if result == 0:
            print(f"✓ Uploaded {filename} ({len(content)} bytes)")
        else:
            print(f"✗ Failed to upload {filename}")
            return False
    
    print("\n3. Testing GET requests...")
    
    # Test each file
    for filename, expected_content in test_files:
        print(f"\nTesting {filename}:")
        
        # Test with AWS CLI
        result = os.system(f'aws --endpoint-url {S3_ENDPOINT} s3api get-object --bucket {BUCKET} --key {filename} /tmp/{filename} 2>/dev/null')
        if result == 0:
            print(f"  ✓ AWS CLI GET successful")
            
            # Verify content
            with open(f"/tmp/{filename}", "rb") as f:
                downloaded_content = f.read()
            
            if downloaded_content == expected_content:
                print(f"  ✓ Content verification passed ({len(downloaded_content)} bytes)")
            else:
                print(f"  ✗ Content mismatch! Expected {len(expected_content)}, got {len(downloaded_content)}")
                return False
        else:
            print(f"  ✗ AWS CLI GET failed")
            return False
    
    print("\n4. Testing Range requests...")
    
    # Test range request on large file
    result = os.system(f'aws --endpoint-url {S3_ENDPOINT} s3api get-object --bucket {BUCKET} --key large.txt --range "bytes=0-1023" /tmp/range_test.txt 2>/dev/null')
    if result == 0:
        print("  ✓ Range request successful")
        
        with open("/tmp/range_test.txt", "rb") as f:
            range_content = f.read()
        
        expected_range = b"B" * 1024
        if range_content == expected_range:
            print(f"  ✓ Range content verified ({len(range_content)} bytes)")
        else:
            print(f"  ✗ Range content mismatch! Expected {len(expected_range)}, got {len(range_content)}")
            return False
    else:
        print("  ✗ Range request failed")
        return False
    
    print("\n5. Performance test...")
    
    # Test multiple requests
    start_time = time.time()
    successful_requests = 0
    total_bytes = 0
    
    for i in range(10):
        try:
            result = os.system(f'aws --endpoint-url {S3_ENDPOINT} s3api get-object --bucket {BUCKET} --key medium.txt /tmp/perf_test_{i}.txt 2>/dev/null')
            if result == 0:
                successful_requests += 1
                with open(f"/tmp/perf_test_{i}.txt", "rb") as f:
                    content = f.read()
                total_bytes += len(content)
                os.remove(f"/tmp/perf_test_{i}.txt")
        except Exception as e:
            print(f"Request {i} failed: {e}")
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"  Results: {successful_requests}/10 successful requests")
    print(f"  Total time: {elapsed:.2f} seconds")
    print(f"  Total bytes: {total_bytes}")
    print(f"  Average speed: {(total_bytes / elapsed) / (1024 * 1024):.2f} MB/sec")
    
    if successful_requests == 10 and total_bytes > 0:
        print("  ✓ Performance test passed")
    else:
        print("  ✗ Performance test failed")
        return False
    
    # Cleanup
    print("\n6. Cleaning up...")
    for filename, _ in test_files:
        os.remove(filename)
        os.remove(f"/tmp/{filename}")
    
    os.remove("/tmp/range_test.txt")
    
    os.system(f'aws --endpoint-url {S3_ENDPOINT} s3 rb s3://{BUCKET} --force 2>/dev/null')
    print("✓ Cleanup completed")
    
    return True

def test_status_codes():
    """Test HTTP status codes"""
    
    print("\n=== Testing HTTP Status Codes ===\n")
    
    # Test 1: Invalid path (should be 400)
    print("1. Testing invalid path...")
    response = requests.get(f"{S3_ENDPOINT}/invalid-path")
    print(f"   Status: {response.status_code} (Expected: 400)")
    if response.status_code == 400:
        print("   ✓ Correct status code")
    else:
        print("   ✗ Incorrect status code")
    
    # Test 2: Non-existent object (should be 400 due to no auth)
    print("\n2. Testing non-existent object without auth...")
    response = requests.get(f"{S3_ENDPOINT}/non-existent/object")
    print(f"   Status: {response.status_code} (Expected: 400)")
    if response.status_code == 400:
        print("   ✓ Correct status code")
    else:
        print("   ✗ Incorrect status code")
    
    return True

if __name__ == "__main__":
    print("Testing GET functionality after DataSlice fix...\n")
    
    success = True
    
    try:
        success &= test_get_functionality()
        success &= test_status_codes()
        
        if success:
            print("\n🎉 All tests passed! GET functionality is working correctly.")
        else:
            print("\n❌ Some tests failed. Please check the output above.")
            
    except Exception as e:
        print(f"\n💥 Test failed with exception: {e}")
        success = False
    
    exit(0 if success else 1)

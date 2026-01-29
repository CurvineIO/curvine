#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
#

"""
FUSE reload test script.

This script tests the hot reload functionality of curvine-fuse:
1. Check if /curvine-fuse is a curvine mount point
2. Create a file, write 1KB data, close, then read 100 bytes
3. Create another file and write 1KB data
4. Send USR1 signal to curvine-fuse process, sleep 3 seconds
5. Write 1KB to the file, then read and verify data matches
6. Continue reading the file and verify data matches
"""

import os
import sys
import time
import signal
import subprocess


MNT_PATH = "/curvine-fuse"
KB_SIZE = 1024


def check_mount_point(mount_path):
    """Check if mount_path is a curvine FUSE mount point.
    
    Args:
        mount_path: Path to check
        
    Returns:
        tuple: (is_curvine_mount: bool, error_message: str)
    """
    if not os.path.exists(mount_path):
        return False, f"Mount path does not exist: {mount_path}"
    
    if not os.path.isdir(mount_path):
        return False, f"Mount path is not a directory: {mount_path}"
    
    # Check if it's a mount point using mountpoint command
    try:
        result = subprocess.run(
            ['mountpoint', '-q', mount_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5
        )
        if result.returncode != 0:
            return False, f"{mount_path} is not a mount point"
    except FileNotFoundError:
        return False, "mountpoint command not found"
    except Exception as e:
        return False, f"Failed to check mount point: {e}"
    
    # Check if it's a FUSE filesystem
    try:
        result = subprocess.run(
            ['findmnt', '-n', '-o', 'FSTYPE', mount_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            fstype = result.stdout.strip()
            if 'fuse' not in fstype.lower():
                return False, f"{mount_path} is not a FUSE filesystem (type: {fstype})"
        else:
            # findmnt not available, try alternative method
            # Check /proc/mounts
            with open('/proc/mounts', 'r') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3 and parts[1] == mount_path:
                        if 'fuse' not in parts[2].lower():
                            return False, f"{mount_path} is not a FUSE filesystem (type: {parts[2]})"
                        break
                else:
                    return False, f"{mount_path} not found in /proc/mounts"
    except Exception as e:
        return False, f"Failed to check filesystem type: {e}"
    
    return True, ""


def get_fuse_process_pid():
    """Get the PID of curvine-fuse process.
    
    Returns:
        int: Process PID, or None if not found
    """
    try:
        result = subprocess.run(
            ['pgrep', '-f', 'curvine-fuse'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
            # Return the last PID (newest process)
            if pids:
                return int(pids[-1])
    except Exception as e:
        print(f"Warning: Failed to get fuse process PID: {e}")
    return None


def send_reload_signal(pid):
    """Send USR1 signal to the process for hot reload.
    
    Args:
        pid: Process ID
        
    Returns:
        bool: True if signal sent successfully
    """
    try:
        os.kill(pid, signal.SIGUSR1)
        print(f"Sent SIGUSR1 signal to process {pid}")
        return True
    except ProcessLookupError:
        print(f"Error: Process {pid} not found")
        return False
    except PermissionError:
        print(f"Error: Permission denied to send signal to process {pid}")
        return False
    except Exception as e:
        print(f"Error: Failed to send signal: {e}")
        return False


def generate_test_data(size):
    """Generate test data of specified size.
    
    Args:
        size: Size in bytes
        
    Returns:
        bytes: Test data
    """
    # Generate predictable test data
    pattern = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" * ((size // 36) + 1)
    return pattern[:size]


def verify_data(data, expected_size, file_path):
    """Verify data matches expected pattern.
    
    Args:
        data: Data to verify
        expected_size: Expected size
        file_path: File path for error message
        
    Returns:
        bool: True if data matches
    """
    if len(data) != expected_size:
        print(f"Error: Data size mismatch for {file_path}: expected {expected_size}, got {len(data)}")
        return False
    
    expected = generate_test_data(expected_size)
    if data != expected:
        print(f"Error: Data content mismatch for {file_path}")
        # Show first difference
        for i, (a, b) in enumerate(zip(data, expected)):
            if a != b:
                print(f"  First difference at byte {i}: got {a:02x}, expected {b:02x}")
                break
        return False
    
    return True


def main():
    """Main test function."""
    print("=" * 60)
    print("FUSE Reload Test")
    print("=" * 60)
    
    # Step 1: Check if /curvine-fuse is a curvine mount point
    print("\n[Step 1] Checking if /curvine-fuse is a curvine mount point...")
    is_mount, error_msg = check_mount_point(MNT_PATH)
    if not is_mount:
        print(f"ERROR: {error_msg}")
        sys.exit(1)
    print(f"✓ {MNT_PATH} is a FUSE mount point")
    
    # Step 2: Create a file, write 1KB data, then read 100 bytes (keep file open)
    print("\n[Step 2] Creating file1, writing 1KB data, then reading 100 bytes (keeping file open)...")
    file1_path = os.path.join(MNT_PATH, "curvine_fuse_reload_test_file1.txt")
    test_data_1kb = generate_test_data(KB_SIZE)
    file1_f = None
    
    try:
        # Write data
        with open(file1_path, 'wb') as f:
            f.write(test_data_1kb)
        print(f"✓ Created {file1_path} and wrote 1KB data")
        
        # Open for reading and read 100 bytes, but keep file open
        file1_f = open(file1_path, 'rb')
        read_data = file1_f.read(100)
        if verify_data(read_data, 100, file1_path):
            print(f"✓ Read 100 bytes from {file1_path}, data matches (file kept open)")
            # Store file handle ID for verification
            file1_f_id_before = id(file1_f)
            print(f"  File handle ID before reload: {file1_f_id_before}")
        else:
            print(f"ERROR: Data verification failed for {file1_path}")
            if file1_f:
                file1_f.close()
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to create/read file1: {e}")
        if file1_f:
            file1_f.close()
        sys.exit(1)
    
    # Step 3: Create another file and write 1KB data (keep file open in read-write mode)
    print("\n[Step 3] Creating file2 and writing 1KB data (keeping file open in read-write mode)...")
    file2_path = os.path.join(MNT_PATH, "curvine_fuse_reload_test_file2.txt")
    test_data_2kb = generate_test_data(KB_SIZE)
    file2_f = None
    
    try:
        # Open in read-write mode so we can use the same handle after reload
        file2_f = open(file2_path, 'w+b')  # Use w+b to allow read and write
        file2_f.write(test_data_2kb)
        file2_f.flush()  # Ensure data is written
        print(f"✓ Created {file2_path} and wrote 1KB data (file kept open in read-write mode)")
        # Store file handle ID for verification
        file2_f_id_before = id(file2_f)
        print(f"  File handle ID before reload: {file2_f_id_before}")
    except Exception as e:
        print(f"ERROR: Failed to create file2: {e}")
        if file1_f:
            file1_f.close()
        if file2_f:
            file2_f.close()
        sys.exit(1)
    
    # Step 4: Send USR1 signal to curvine-fuse process, sleep 3 seconds
    print("\n[Step 4] Sending USR1 signal to curvine-fuse process for hot reload...")
    fuse_pid = get_fuse_process_pid()
    if fuse_pid is None:
        print("ERROR: Could not find curvine-fuse process")
        sys.exit(1)
    
    print(f"Found curvine-fuse process: PID {fuse_pid}")
    if not send_reload_signal(fuse_pid):
        print("ERROR: Failed to send reload signal")
        sys.exit(1)

    print("Sleeping 3 seconds for hot reload to complete...")
    time.sleep(3)
    print("✓ Hot reload completed")

    # Verify file handles are still the same after reload
    file1_f_id_after = id(file1_f)
    file2_f_id_after = id(file2_f)
    print(f"\n  File handle IDs after reload:")
    print(f"    file1_f: {file1_f_id_after} (same handle: {file1_f_id_after == file1_f_id_before})")
    print(f"    file2_f: {file2_f_id_after} (same handle: {file2_f_id_after == file2_f_id_before})")
    
    # Verify handles are actually the same Python objects
    if file1_f_id_after != file1_f_id_before:
        print(f"  WARNING: file1_f handle changed after reload!")
    if file2_f_id_after != file2_f_id_before:
        print(f"  WARNING: file2_f handle changed after reload!")
    
    # Step 5: Write 1KB to file2 using the open file handle, then read and verify
    print("\n[Step 5] Writing 1KB to file2 (using open file handle), then reading and verifying...")
    test_data_2kb_new = generate_test_data(KB_SIZE)
    
    try:
        # Use the same file2_f handle that was opened before reload
        # Write new data to file2 (overwrite existing data)
        file2_f.seek(0)  # Seek to beginning
        file2_f.write(test_data_2kb_new)
        file2_f.flush()  # Ensure data is written
        print(f"✓ Wrote 1KB data to {file2_path} (using same file handle from before reload)")
        print(f"  File handle ID: {id(file2_f)} (should be same as before: {file2_f_id_before == id(file2_f)})")
        
        # Read and verify - should match the newly written data
        file2_f.seek(0)  # Seek to beginning for reading
        read_data = file2_f.read(KB_SIZE)
        if read_data == test_data_2kb_new:
            print(f"✓ Read 1KB from {file2_path}, data matches written data")
        else:
            print(f"ERROR: Data verification failed for {file2_path}")
            print(f"  Expected size: {len(test_data_2kb_new)}, got: {len(read_data)}")
            # Show first difference
            for i, (a, b) in enumerate(zip(read_data, test_data_2kb_new)):
                if a != b:
                    print(f"  First difference at byte {i}: got {a:02x}, expected {b:02x}")
                    break
            file2_f.close()
            if file1_f:
                file1_f.close()
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to write/read file2: {e}")
        if file2_f:
            file2_f.close()
        if file1_f:
            file1_f.close()
        sys.exit(1)
    
    # Step 6: Continue reading file1 using the open file handle and verify data matches
    print("\n[Step 6] Continuing to read file1 (using open file handle) and verify data matches...")
    try:
        # file1_f is still open from Step 2, continue reading from current position
        # Current position should be at byte 100 (after reading 100 bytes)
        # Verify we're still using the same handle
        file1_f_id_step6 = id(file1_f)
        print(f"  File handle ID in Step 6: {file1_f_id_step6} (should be same as before: {file1_f_id_step6 == file1_f_id_before})")
        if file1_f_id_step6 != file1_f_id_before:
            print(f"  WARNING: file1_f handle changed in Step 6!")
        
        # Read the remaining data (1KB - 100 bytes = 924 bytes)
        remaining_data = file1_f.read(KB_SIZE - 100)
        if len(remaining_data) != KB_SIZE - 100:
            print(f"ERROR: Expected {KB_SIZE - 100} bytes, got {len(remaining_data)}")
            if file1_f:
                file1_f.close()
            if file2_f:
                file2_f.close()
            sys.exit(1)
        
        # Verify the remaining data matches the expected pattern
        # The remaining data should be bytes 100-1023 of the original test_data_1kb
        expected_remaining = test_data_1kb[100:]
        if remaining_data == expected_remaining:
            print(f"✓ Read remaining {KB_SIZE - 100} bytes from {file1_path}, data matches")
        else:
            print(f"ERROR: Data verification failed for remaining data in {file1_path}")
            # Show first difference
            for i, (a, b) in enumerate(zip(remaining_data, expected_remaining)):
                if a != b:
                    print(f"  First difference at byte {i+100}: got {a:02x}, expected {b:02x}")
                    break
            if file1_f:
                file1_f.close()
            if file2_f:
                file2_f.close()
            sys.exit(1)
        
        # Verify we've read all data
        eof_data = file1_f.read(1)
        if eof_data:
            print(f"WARNING: Expected EOF but got more data from {file1_path}")
        else:
            print(f"✓ Reached EOF for {file1_path}")
    except Exception as e:
        print(f"ERROR: Failed to read remaining data from file1: {e}")
        if file1_f:
            file1_f.close()
        if file2_f:
            file2_f.close()
        sys.exit(1)
    finally:
        # Close file handles
        if file1_f:
            file1_f.close()
        if file2_f:
            file2_f.close()
    
    # Cleanup
    print("\n[Cleanup] Removing test files...")
    try:
        if os.path.exists(file1_path):
            os.remove(file1_path)
            print(f"✓ Removed {file1_path}")
        if os.path.exists(file2_path):
            os.remove(file2_path)
            print(f"✓ Removed {file2_path}")
    except Exception as e:
        print(f"WARNING: Failed to cleanup test files: {e}")
    
    print("\n" + "=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())


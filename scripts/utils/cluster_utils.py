"""Cluster utilities for Curvine testing

This module provides functions for preparing and checking Curvine cluster status.
"""
import subprocess
import os
import shutil
import time


def prepare_curvine_cluster(project_path, test_path='/curvine-fuse'):
    """Prepare Curvine cluster for LTP testing
    
    Steps:
    1. Unmount existing mount point
    2. Delete build/dist/testing directory
    3. Start Curvine cluster
    
    Args:
        project_path: Path to the project root
        test_path: Mount point path to unmount (default: /curvine-fuse)
        
    Returns:
        tuple: (success: bool, error_message: str)
    """
    try:
        # Step 1: Unmount existing mount point
        print(f"Unmounting {test_path}...")
        try:
            unmount_process = subprocess.Popen(
                ['umount', '-l', test_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            unmount_process.wait()
            if unmount_process.returncode != 0:
                stderr = unmount_process.stderr.read()
                # Ignore error if not mounted
                if 'not mounted' not in stderr.lower() and 'no such file' not in stderr.lower():
                    print(f"Warning: umount returned {unmount_process.returncode}: {stderr}")
        except Exception as e:
            print(f"Warning: Failed to unmount {test_path}: {e}")
            # Continue anyway, might not be mounted
        
        # Step 2: Delete build/dist/testing directory
        testing_dir = os.path.join(project_path, 'build', 'dist', 'testing')
        if os.path.exists(testing_dir):
            print(f"Deleting {testing_dir}...")
            try:
                shutil.rmtree(testing_dir)
                print(f"Deleted {testing_dir}")
            except Exception as e:
                error_msg = f"Failed to delete {testing_dir}: {str(e)}"
                print(f"Error: {error_msg}")
                return False, error_msg
        else:
            print(f"Testing directory does not exist: {testing_dir}")
        
        # Step 3: Start Curvine cluster services one by one
        original_cwd = os.getcwd()
        dist_dir = os.path.join(project_path, 'build', 'dist')
        os.chdir(dist_dir)
        
        try:
            # Services to start in order
            services = [
                ('./bin/curvine-master.sh', ['restart'], 'Curvine Master'),
                ('./bin/curvine-worker.sh', ['restart'], 'Curvine Worker'),
                ('./bin/curvine-fuse.sh', ['restart', '-d'], 'Curvine Fuse')
            ]
            
            for script_path, args, service_name in services:
                # Check if script exists (relative to dist_dir since we chdir'd)
                if not os.path.exists(script_path):
                    error_msg = f"Script not found: {script_path} (in {dist_dir})"
                    print(f"Error: {error_msg}")
                    return False, error_msg
                
                print(f"Starting {service_name}: {script_path} {' '.join(args)}")
                process = subprocess.Popen(
                    [script_path] + args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                # Stream output in real time
                for line in process.stdout:
                    print(line, end='')
                
                process.wait()
                
                if process.returncode != 0:
                    stderr = process.stderr.read()
                    error_msg = f"Failed to start {service_name}. Return code: {process.returncode}, Error: {stderr}"
                    print(f"Error: {error_msg}")
                    return False, error_msg
                
                print(f"{service_name} started successfully")
                # Wait a bit for service to be ready before starting next one
                time.sleep(2)
            
            print("Curvine cluster started successfully")
            
            # Wait a bit for cluster to be fully ready
            print("Waiting for cluster to be ready...")
            time.sleep(3)
            
            return True, ""
        finally:
            os.chdir(original_cwd)
            
    except Exception as e:
        error_msg = f"Error preparing Curvine cluster: {str(e)}"
        print(f"Error: {error_msg}")
        import traceback
        traceback.print_exc()
        return False, error_msg


def ensure_cluster_ready(project_path, test_path='/curvine-fuse', max_wait_seconds=10):
    """Ensure Curvine cluster is ready (started and mounted)
    
    This function checks if the mount point is ready, and if not,
    prepares the cluster by calling prepare_curvine_cluster.
    
    Args:
        project_path: Path to the project root
        test_path: Mount point path to check/prepare (default: /curvine-fuse)
        max_wait_seconds: Maximum seconds to wait for mount (default: 10)
        
    Returns:
        tuple: (success: bool, error_message: str)
    """
    # First check if mount point is already ready
    is_mounted, mount_error = check_mount_point(test_path, max_wait_seconds=max_wait_seconds)
    if is_mounted:
        print(f"Mount point {test_path} is already mounted and ready")
        return True, ""
    
    # If not mounted, prepare the cluster
    print(f"Mount point {test_path} is not ready, preparing cluster...")
    success, error_msg = prepare_curvine_cluster(project_path, test_path)
    if not success:
        return False, f"Failed to prepare cluster: {error_msg}"
    
    # After preparing, check mount point again
    is_mounted, mount_error = check_mount_point(test_path, max_wait_seconds=max_wait_seconds)
    if not is_mounted:
        return False, f"Cluster prepared but mount point check failed: {mount_error}"
    
    print(f"Cluster is ready, mount point {test_path} is mounted")
    return True, ""


def check_mount_point(mount_path, max_wait_seconds=10):
    """Check if a mount point is properly mounted using mountpoint command
    
    Args:
        mount_path: Path to check for mount status
        max_wait_seconds: Maximum seconds to wait if not mounted (default: 10)
        
    Returns:
        tuple: (is_mounted: bool, error_message: str)
    """
    if not os.path.exists(mount_path):
        return False, f"Mount path does not exist: {mount_path}"
    
    if not os.path.isdir(mount_path):
        return False, f"Mount path is not a directory: {mount_path}"
    
    # Use mountpoint command to check if it's a mount point
    # Must use mountpoint command, no fallback
    start_time = time.time()
    check_interval = 1  # Check every 1 second
    first_check = True
    
    while True:
        try:
            # Run mountpoint command
            process = subprocess.Popen(
                ['mountpoint', mount_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            stdout, stderr = process.communicate()
            
            # mountpoint returns 0 if it's a mount point, non-zero otherwise
            if process.returncode == 0:
                if not first_check:
                    print(f"Mount point {mount_path} is now mounted (waited {int(time.time() - start_time)}s)")
                return True, ""
            else:
                # Not a mount point, check if we should wait
                elapsed = time.time() - start_time
                if elapsed >= max_wait_seconds:
                    error_msg = stderr.strip() if stderr else stdout.strip() if stdout else "not a mountpoint"
                    return False, f"Path {mount_path} is not a mount point after waiting {max_wait_seconds}s: {error_msg}"
                
                # Log waiting message on first check
                if first_check:
                    print(f"Mount point {mount_path} is not mounted yet, waiting up to {max_wait_seconds}s...")
                    first_check = False
                
                # Wait before next check
                time.sleep(check_interval)
                continue
                
        except FileNotFoundError:
            # mountpoint command not found, return error immediately
            return False, f"mountpoint command not found. Cannot verify if {mount_path} is a mount point."
        except Exception as e:
            elapsed = time.time() - start_time
            if elapsed >= max_wait_seconds:
                return False, f"Failed to check mount status: {str(e)}"
            
            if first_check:
                print(f"Error checking mount point, retrying...: {str(e)}")
                first_check = False
            
            time.sleep(check_interval)
            continue


#!/usr/bin/env python3
import json
import os
import sys

def main():
    try:
        # Read Fluid configuration - compatible with both single-line and multi-line JSON
        config_file = "/etc/fluid/config/config.json"
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Fluid config file not found: {config_file}")
        
        with open(config_file, "r") as f:
            content = f.read().strip()

        # Try to parse as complete JSON first
        try:
            config = json.loads(content)
            config_str = content
            print("Fluid config (multi-line):", config_str)
        except json.JSONDecodeError:
            # Fallback to first-line parsing (original ThinRuntime approach)
            lines = content.split('\n')
            config_str = lines[0].strip()
            config = json.loads(config_str)
            print("Fluid config (single-line):", config_str)

        # Generate curvine-cluster.toml configuration
        toml_config = """format_master = false
format_worker = false
testing = false
cluster_id = "curvine"

[master]
hostname = "{master_hostname}"
rpc_port = {master_rpc_port}
web_port = {master_web_port}


[[client.master_addrs]]
hostname = "{master_hostname}"
port = {master_rpc_port}

[fuse]
debug = false
io_threads = 32
worker_threads = 56
mnt_path = "{mount_path}"
fs_path = "{fs_path}"

"""

        # Extract configuration parameters with environment variable fallback
        def get_config_value(config_path, env_var, required=True, default=None):
            """
            Get configuration value from config file first, then environment variable, then default.
            If required=True and no value found, raises an error.
            """
            try:
                # Try to get from config file
                if isinstance(config_path, list):
                    value = config
                    for key in config_path:
                        value = value[key]
                else:
                    value = config[config_path]
                if value:
                    return value
            except (KeyError, TypeError, IndexError):
                pass
            
            # Try to get from environment variable
            env_value = os.getenv(env_var)
            if env_value:
                return env_value
            
            # Use default if provided
            if default is not None:
                return default
            
            # If required and no value found, raise error
            if required:
                raise ValueError(f"Required configuration not found: config path {config_path}, environment variable {env_var}")
            
            return None

        # Extract configuration parameters
        try:
            mount_options = config['mounts'][0]['options']
        except (KeyError, IndexError):
            mount_options = {}

        mount_point = get_config_value(['mounts', 0, 'mountPoint'], 'CURVINE_MOUNT_POINT')
        target_path = get_config_value('targetPath', 'CURVINE_TARGET_PATH')

        # Parse master endpoints
        master_endpoints = mount_options.get('master-endpoints') or os.getenv('CURVINE_MASTER_ENDPOINTS')
        if not master_endpoints:
            raise ValueError("Master endpoints not found in config file or CURVINE_MASTER_ENDPOINTS environment variable")

        try:
            master_hostname, master_rpc_port = master_endpoints.split(':')[0], master_endpoints.split(':')[1]
        except ValueError:
            raise ValueError(f"Invalid master endpoints format: {master_endpoints}, expected format: hostname:port")

        master_web_port = mount_options.get('master-web-port') or os.getenv('CURVINE_MASTER_WEB_PORT', '8080')

        # Parse FUSE performance parameters from environment variables
        io_threads = os.getenv('CURVINE_IO_THREADS', '32')
        worker_threads = os.getenv('CURVINE_WORKER_THREADS', '56')

        # Parse filesystem path from mountPoint
        # Expected format: curvine://path or /path
        if mount_point.startswith("curvine://"):
            fs_path = mount_point[len("curvine://"):]
        else:
            fs_path = mount_point if mount_point.startswith("/") else "/" + mount_point

        # Generate configuration file
        with open("/opt/curvine/conf/curvine-cluster.toml", "w") as f:
            f.write(toml_config.format(
                master_hostname=master_hostname,
                master_rpc_port=master_rpc_port,
                master_web_port=master_web_port,
                mount_path=target_path,
                fs_path=fs_path
            ))

        # Generate mount script
        mount_script = """#!/bin/bash
set -ex

export CURVINE_HOME="/opt/curvine"
export CURVINE_CONF_FILE="/opt/curvine/conf/curvine-cluster.toml"

# Create necessary directories
mkdir -p {target_path}
mkdir -p /tmp/curvine/meta
mkdir -p /opt/curvine/logs

# Cleanup previous mounts
umount -f {target_path} 2>/dev/null || true

# Start curvine-fuse
exec /opt/curvine/lib/curvine-fuse \\
    --mnt-path {target_path} \\
    --mnt-number 1 \\
    --conf $CURVINE_CONF_FILE \\
    --io-threads {io_threads} \\
    --worker-threads {worker_threads}
""".format(target_path=target_path, io_threads=io_threads, worker_threads=worker_threads)

        with open("/opt/curvine/mount-curvine.sh", "w") as f:
            f.write(mount_script)

        os.chmod("/opt/curvine/mount-curvine.sh", 0o755)

        print("Configuration generated successfully!")
        print("Configuration details:")
        print(f"  Mount path: {target_path}")
        print(f"  FS path: {fs_path}")
        print(f"  Master endpoint: {master_hostname}:{master_rpc_port}")
        print(f"  Master web port: {master_web_port}")
        print(f"  Mount point: {mount_point}")
        print(f"  IO threads: {io_threads}")
        print(f"  Worker threads: {worker_threads}")
        print("Files generated:")
        print(f"  Config file: /opt/curvine/conf/curvine-cluster.toml")
        print(f"  Mount script: /opt/curvine/mount-curvine.sh")

    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        print("Available environment variables as fallback:", file=sys.stderr)
        print("  CURVINE_MOUNT_POINT - Mount point (e.g., curvine:///data)", file=sys.stderr)
        print("  CURVINE_TARGET_PATH - Target mount path (e.g., /mnt/data)", file=sys.stderr)
        print("  CURVINE_MASTER_ENDPOINTS - Master endpoints (e.g., master:9000)", file=sys.stderr)
        print("  CURVINE_MASTER_WEB_PORT - Master web port (default: 8080)", file=sys.stderr)
        print("  CURVINE_IO_THREADS - IO threads (default: 32)", file=sys.stderr)
        print("  CURVINE_WORKER_THREADS - Worker threads (default: 56)", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in config file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 
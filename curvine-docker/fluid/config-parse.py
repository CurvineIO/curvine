#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


class ConfigParser:
    """Parse Fluid configuration and generate Curvine configuration."""
    
    CONFIG_FILE = "/etc/fluid/config/config.json"
    TOML_TEMPLATE = """format_master = false
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
io_threads = 16
worker_threads = 32
mnt_path = "{mount_path}"
fs_path = "{fs_path}"

"""
    
    MOUNT_SCRIPT_TEMPLATE = """#!/bin/bash
set -ex

export CURVINE_HOME="{curvine_home}"
export CURVINE_CONF_FILE="{curvine_home}/conf/curvine-cluster.toml"

mkdir -p /tmp/curvine/meta {curvine_home}/logs
umount -f {target_path} 2>/dev/null || true

PARENT_DIR=$(dirname {target_path})
if [ ! -d "$PARENT_DIR" ]; then
    echo "Waiting for parent directory $PARENT_DIR to be created..."
    for i in $(seq 1 30); do
        [ -d "$PARENT_DIR" ] && break
        sleep 1
    done
fi

[ -d "$PARENT_DIR" ] && mkdir -p {target_path} 2>/dev/null || true

exec {fuse_cmd}
"""
    
    FUSE_OPTIONS = [
        'mnt-per-task', 'clone-fd', 'fuse-channel-size', 'stream-channel-size',
        'direct-io', 'cache-readdir', 'entry-timeout', 'attr-timeout'
    ]
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self.CONFIG_FILE
        self.config: Dict[str, Any] = {}
        self.mount_options: Dict[str, Any] = {}
    
    def load_config(self) -> None:
        """Load and parse Fluid configuration file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Fluid config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            content = f.read().strip()
        
        try:
            self.config = json.loads(content)
        except json.JSONDecodeError:
            lines = content.split('\n')
            self.config = json.loads(lines[0].strip())
        
        self.mount_options = self.config.get('mounts', [{}])[0].get('options', {})
    
    def get_value(self, config_path: Union[str, List], env_var: str, 
                  required: bool = True, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value with fallback: config > env > default."""
        try:
            if isinstance(config_path, list):
                value = self.config
                for key in config_path:
                    value = value[key]
            else:
                value = self.config[config_path]
            if value:
                return str(value)
        except (KeyError, TypeError, IndexError):
            pass
        
        env_value = os.getenv(env_var)
        if env_value:
            return env_value
        
        if default is not None:
            return default
        
        if required:
            raise ValueError(
                f"Required configuration not found: config path {config_path}, "
                f"environment variable {env_var}"
            )
        
        return None
    
    def get_option(self, key: str, env_var: str, default: Optional[str] = None) -> Optional[str]:
        """Get option from mount_options or environment variable."""
        return self.mount_options.get(key) or os.getenv(env_var, default)
    
    def parse_master_endpoints(self) -> Tuple[str, str]:
        """Parse master endpoints and return (hostname, port)."""
        endpoints = self.get_option('master-endpoints', 'CURVINE_MASTER_ENDPOINTS')
        if not endpoints:
            raise ValueError(
                "Master endpoints not found in config file or "
                "CURVINE_MASTER_ENDPOINTS environment variable"
            )
        
        try:
            hostname, port = endpoints.split(':', 1)
            return hostname, port
        except ValueError:
            raise ValueError(
                f"Invalid master endpoints format: {endpoints}, "
                f"expected format: hostname:port"
            )
    
    def parse_fs_path(self, mount_point: str) -> str:
        """Parse filesystem path from mount point."""
        if mount_point.startswith("curvine://"):
            return mount_point[len("curvine://"):]
        return mount_point if mount_point.startswith("/") else f"/{mount_point}"
    
    def build_fuse_command(self, curvine_home: str, target_path: str, 
                          mnt_number: str, io_threads: str, worker_threads: str) -> str:
        """Build FUSE command with optional parameters."""
        cmd_parts = [
            f"{curvine_home}/lib/curvine-fuse",
            "--mnt-path", target_path,
            "--mnt-number", mnt_number,
            "--conf", "$CURVINE_CONF_FILE",
            "--io-threads", io_threads,
            "--worker-threads", worker_threads
        ]
        
        for option in self.FUSE_OPTIONS:
            value = self.get_option(option, f'CURVINE_{option.upper().replace("-", "_")}')
            if value:
                cmd_parts.extend([f"--{option}", str(value)])
        
        return " ".join(f'"{arg}"' if " " in arg else arg for arg in cmd_parts)
    
    def generate_toml_config(self, curvine_home: str, master_hostname: str,
                             master_rpc_port: str, master_web_port: str,
                             target_path: str, fs_path: str) -> None:
        """Generate Curvine TOML configuration file."""
        os.makedirs(f"{curvine_home}/conf", exist_ok=True)
        config_file = f"{curvine_home}/conf/curvine-cluster.toml"
        
        with open(config_file, 'w') as f:
            f.write(self.TOML_TEMPLATE.format(
                master_hostname=master_hostname,
                master_rpc_port=master_rpc_port,
                master_web_port=master_web_port,
                mount_path=target_path,
                fs_path=fs_path
            ))
    
    def generate_mount_script(self, curvine_home: str, target_path: str,
                              fuse_cmd: str) -> None:
        """Generate FUSE mount script."""
        script_path = f"{curvine_home}/mount-curvine.sh"
        
        with open(script_path, 'w') as f:
            f.write(self.MOUNT_SCRIPT_TEMPLATE.format(
                curvine_home=curvine_home,
                target_path=target_path,
                fuse_cmd=fuse_cmd
            ))
        
        os.chmod(script_path, 0o755)
    
    def print_config_summary(self, config: Dict[str, Any]) -> None:
        """Print configuration summary."""
        print("Configuration generated successfully!")
        print("Configuration details:")
        for key, value in config.items():
            if value:
                print(f"  {key}: {value}")
        print("Files generated:")
        print(f"  Config file: {config['curvine_home']}/conf/curvine-cluster.toml")
        print(f"  Mount script: {config['curvine_home']}/mount-curvine.sh")
    
    def run(self) -> None:
        """Main execution flow."""
        self.load_config()
        
        mount_point = self.get_value(['mounts', 0, 'mountPoint'], 'CURVINE_MOUNT_POINT')
        target_path = self.get_value('targetPath', 'CURVINE_TARGET_PATH')
        master_hostname, master_rpc_port = self.parse_master_endpoints()
        
        master_web_port = self.get_option('master-web-port', 'CURVINE_MASTER_WEB_PORT', '8080')
        io_threads = self.get_option('io-threads', 'CURVINE_IO_THREADS', '32')
        worker_threads = self.get_option('worker-threads', 'CURVINE_WORKER_THREADS', '56')
        mnt_number = self.get_option('mnt-number', 'CURVINE_MNT_NUMBER', '1')
        
        fs_path = self.parse_fs_path(mount_point)
        curvine_home = os.getenv('CURVINE_HOME', '/app/curvine')
        
        self.generate_toml_config(
            curvine_home, master_hostname, master_rpc_port,
            master_web_port, target_path, fs_path
        )
        
        fuse_cmd = self.build_fuse_command(
            curvine_home, target_path, mnt_number, io_threads, worker_threads
        )
        
        self.generate_mount_script(curvine_home, target_path, fuse_cmd)
        
        config_summary = {
            'Mount path': target_path,
            'FS path': fs_path,
            'Master endpoint': f"{master_hostname}:{master_rpc_port}",
            'Master web port': master_web_port,
            'Mount point': mount_point,
            'IO threads': io_threads,
            'Worker threads': worker_threads,
            'Mount number': mnt_number,
            'curvine_home': curvine_home
        }
        
        for option in self.FUSE_OPTIONS:
            value = self.get_option(option, f'CURVINE_{option.upper().replace("-", "_")}')
            if value:
                config_summary[option.replace('-', ' ').title()] = value
        
        self.print_config_summary(config_summary)


def print_error_help() -> None:
    """Print error help message with available options."""
    print("ERROR: Configuration file not found", file=sys.stderr)
    print("\nAvailable environment variables as fallback:", file=sys.stderr)
    env_vars = [
        ("CURVINE_MOUNT_POINT", "Mount point (e.g., curvine:///data)"),
        ("CURVINE_TARGET_PATH", "Target mount path (e.g., /mnt/data)"),
        ("CURVINE_MASTER_ENDPOINTS", "Master endpoints (e.g., master:9000)"),
        ("CURVINE_MASTER_WEB_PORT", "Master web port (default: 8080)"),
        ("CURVINE_IO_THREADS", "IO threads (default: 32)"),
        ("CURVINE_WORKER_THREADS", "Worker threads (default: 56)"),
        ("CURVINE_MNT_NUMBER", "Mount number (default: 1)"),
    ]
    
    for var, desc in env_vars:
        print(f"  {var} - {desc}", file=sys.stderr)
    
    print("\nSupported Dataset options (in spec.mounts[].options):", file=sys.stderr)
    dataset_options = [
        ("master-endpoints", "Master RPC endpoint (hostname:port) [required]"),
        ("master-web-port", "Master web port (default: 8080)"),
        ("io-threads", "IO threads (default: 32)"),
        ("worker-threads", "Worker threads (default: 56)"),
        ("mnt-number", "Mount number (default: 1)"),
    ]
    
    for opt, desc in dataset_options:
        print(f"  {opt} - {desc}", file=sys.stderr)


def main() -> None:
    """Main entry point."""
    try:
        parser = ConfigParser()
        parser.run()
    except FileNotFoundError:
        print_error_help()
        sys.exit(1)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

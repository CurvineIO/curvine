#!/usr/bin/env python3
import json
import os
import re
import sys
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

[client]
{client_options}

{client_master_addrs}

[fuse]
debug = false
io_threads = 16
worker_threads = 32
mnt_path = "{mount_path}"
fs_path = "{fs_path}"
{fuse_options}

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
    
    FUSE_CLI_OPTIONS = [
        ('tasks-per-mnt', 'tasks-per-mnt'),
        ('mnt-per-task', 'tasks-per-mnt'),
        ('clone-fd', 'clone-fd'),
        ('fuse-channel-size', 'fuse-channel-size'),
        ('stream-channel-size', 'stream-channel-size'),
        ('direct-io', 'direct-io'),
        ('write-back-cache', 'write-back-cache'),
        ('cache-readdir', 'cache-readdir'),
        ('non-seekable', 'non-seekable'),
        ('entry-timeout-ms', 'entry-timeout-ms'),
        ('entry-timeout', 'entry-timeout-ms'),
        ('attr-timeout-ms', 'attr-timeout-ms'),
        ('attr-timeout', 'attr-timeout-ms'),
        ('negative-timeout-ms', 'negative-timeout-ms'),
        ('negative-timeout', 'negative-timeout-ms'),
        ('max-background', 'max-background'),
        ('congestion-threshold', 'congestion-threshold'),
        ('node-cache-timeout', 'node-cache-timeout'),
        ('enable-meta-cache', 'enable-meta-cache'),
        ('meta-cache-ttl', 'meta-cache-ttl'),
        ('meta-cache-timeout', 'meta-cache-ttl'),
        ('read-dir-fill-ino', 'read-dir-fill-ino'),
        ('remember', 'remember'),
        ('check-permission', 'check-permission'),
        ('metrics-enabled', 'metrics-enabled'),
        ('list-limit', 'list-limit'),
        ('web-port', 'web-port'),
    ]

    CLIENT_TOML_PREFIX = "client."
    FUSE_TOML_PREFIX = "fuse."
    FUSE_TOML_ALIASES = {
        "enable_write_back": "write_back_cache",
        "meta_cache_ttl": "meta_cache_timeout",
    }
    MANAGED_FUSE_TOML_KEYS = {
        "debug",
        "io_threads",
        "worker_threads",
        "mnt_path",
        "fs_path",
    }
    MANAGED_CLIENT_TOML_KEYS = {
        "master_addrs",
    }
    
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
        
        mounts = self.config.get('mounts', [])
        if not isinstance(mounts, list):
            raise ValueError("Curvine ThinRuntime config field mounts must be a list")
        if len(mounts) != 1:
            raise ValueError(
                f"Curvine ThinRuntime expects exactly one Dataset mount, got {len(mounts)}"
            )
        if not isinstance(mounts[0], dict):
            raise ValueError("Curvine ThinRuntime mount entry must be an object")

        mount_options = mounts[0].get('options', {})
        if mount_options is None:
            mount_options = {}
        if not isinstance(mount_options, dict):
            raise ValueError("Curvine ThinRuntime mount options must be an object")
        self.mount_options = mount_options
    
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
    
    @staticmethod
    def has_value(value: Any) -> bool:
        return value is not None and str(value) != ""

    def get_option(self, key: str, env_var: str, default: Optional[str] = None) -> Optional[str]:
        """Get option from mount_options or environment variable."""
        if key in self.mount_options and self.has_value(self.mount_options[key]):
            return self.mount_options[key]
        return os.getenv(env_var, default)

    def get_cli_option(self, dataset_key: str) -> Optional[str]:
        """Get a FUSE CLI option from Dataset options or environment variables."""
        env_var = f'CURVINE_{dataset_key.upper().replace("-", "_")}'
        if dataset_key in self.mount_options and self.has_value(self.mount_options[dataset_key]):
            return self.mount_options[dataset_key]
        return os.getenv(env_var)

    @staticmethod
    def render_toml_value(value: Any) -> str:
        """Render a Dataset option value as a TOML scalar."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        if isinstance(value, float):
            return str(value)

        text = str(value).strip()
        lower = text.lower()
        if lower in ("true", "false"):
            return lower
        if re.fullmatch(r"[+-]?(0|[1-9][0-9]*)", text):
            return text
        if re.fullmatch(r"[+-]?(0|[1-9][0-9]*)\.[0-9]+", text):
            return text
        return json.dumps(text)

    @staticmethod
    def render_toml_options(options: Dict[str, Any]) -> str:
        rendered = []
        for key in sorted(options):
            rendered.append(f"{key} = {ConfigParser.render_toml_value(options[key])}")
        return "\n".join(rendered)

    def collect_advanced_toml_options(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Collect Dataset options that should be written into Curvine TOML."""
        client_options: Dict[str, Any] = {}
        fuse_options: Dict[str, Any] = {}

        for key, value in self.mount_options.items():
            if key.startswith(self.CLIENT_TOML_PREFIX):
                option_name = key[len(self.CLIENT_TOML_PREFIX):].strip()
                if not option_name or "." in option_name:
                    raise ValueError(f"Invalid client option key: {key}")
                if option_name in self.MANAGED_CLIENT_TOML_KEYS:
                    raise ValueError(f"Dataset option {key} is managed by Curvine ThinRuntime")
                client_options[option_name] = value
                continue

            if key.startswith(self.FUSE_TOML_PREFIX):
                option_name = key[len(self.FUSE_TOML_PREFIX):].strip()
                if not option_name or "." in option_name:
                    raise ValueError(f"Invalid fuse option key: {key}")
                option_name = self.FUSE_TOML_ALIASES.get(option_name, option_name)
                if option_name in self.MANAGED_FUSE_TOML_KEYS:
                    raise ValueError(f"Dataset option {key} is managed by Curvine ThinRuntime")
                fuse_options[option_name] = value

        return client_options, fuse_options
    
    @staticmethod
    def _split_master_endpoints(endpoints: Any) -> List[str]:
        if isinstance(endpoints, list):
            raw_entries = [str(entry).strip() for entry in endpoints]
        else:
            raw_entries = re.split(r"[,;\s]+", str(endpoints).strip())
        return [entry for entry in raw_entries if entry]

    def parse_master_endpoints(self) -> List[Tuple[str, str]]:
        """Parse master endpoints and return all (hostname, port) pairs."""
        endpoints = self.get_option('master-endpoints', 'CURVINE_MASTER_ENDPOINTS')
        if not endpoints:
            raise ValueError(
                "Master endpoints not found in config file or "
                "CURVINE_MASTER_ENDPOINTS environment variable"
            )
        
        parsed_endpoints = []
        for endpoint in self._split_master_endpoints(endpoints):
            hostname, separator, port = endpoint.rpartition(':')
            if not separator or not hostname or not port:
                raise ValueError(
                    f"Invalid master endpoint: {endpoint}, "
                    f"expected format: hostname:port[,hostname:port...]"
                )
            try:
                port_num = int(port)
            except ValueError:
                raise ValueError(
                    f"Invalid master endpoint port: {endpoint}, "
                    f"port must be an integer"
                )
            if port_num <= 0 or port_num > 65535:
                raise ValueError(
                    f"Invalid master endpoint port: {endpoint}, "
                    f"port must be between 1 and 65535"
                )
            parsed_endpoints.append((hostname, str(port_num)))

        if not parsed_endpoints:
            raise ValueError(
                "Master endpoints are empty, expected format: "
                "hostname:port[,hostname:port...]"
            )

        return parsed_endpoints
    
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
        
        emitted_cli_flags = set()
        for dataset_key, cli_flag in self.FUSE_CLI_OPTIONS:
            if cli_flag in emitted_cli_flags:
                continue
            value = self.get_cli_option(dataset_key)
            if self.has_value(value):
                cmd_parts.extend([f"--{cli_flag}", str(value)])
                emitted_cli_flags.add(cli_flag)
        
        return " ".join(f'"{arg}"' if " " in arg else arg for arg in cmd_parts)
    
    @staticmethod
    def render_client_master_addrs(master_endpoints: List[Tuple[str, str]]) -> str:
        """Render Curvine client master_addrs TOML entries."""
        rendered_entries = []
        for hostname, port in master_endpoints:
            rendered_entries.append(
                "[[client.master_addrs]]\n"
                f"hostname = {json.dumps(hostname)}\n"
                f"port = {port}"
            )
        return "\n\n".join(rendered_entries)

    def generate_toml_config(self, curvine_home: str, master_endpoints: List[Tuple[str, str]],
                             master_web_port: str, client_options: Dict[str, Any],
                             fuse_options: Dict[str, Any],
                             target_path: str, fs_path: str) -> None:
        """Generate Curvine TOML configuration file."""
        os.makedirs(f"{curvine_home}/conf", exist_ok=True)
        config_file = f"{curvine_home}/conf/curvine-cluster.toml"

        master_hostname, master_rpc_port = master_endpoints[0]
        
        with open(config_file, 'w') as f:
            f.write(self.TOML_TEMPLATE.format(
                master_hostname=master_hostname,
                master_rpc_port=master_rpc_port,
                master_web_port=master_web_port,
                client_options=self.render_toml_options(client_options),
                client_master_addrs=self.render_client_master_addrs(master_endpoints),
                mount_path=target_path,
                fs_path=fs_path,
                fuse_options=self.render_toml_options(fuse_options)
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
        master_endpoints = self.parse_master_endpoints()
        
        master_web_port = self.get_option('master-web-port', 'CURVINE_MASTER_WEB_PORT', '8080')
        io_threads = self.get_option('io-threads', 'CURVINE_IO_THREADS', '32')
        worker_threads = self.get_option('worker-threads', 'CURVINE_WORKER_THREADS', '56')
        mnt_number = self.get_option('mnt-number', 'CURVINE_MNT_NUMBER', '1')
        client_options, fuse_options = self.collect_advanced_toml_options()
        
        fs_path = self.parse_fs_path(mount_point)
        curvine_home = os.getenv('CURVINE_HOME', '/app/curvine')
        
        self.generate_toml_config(
            curvine_home, master_endpoints,
            master_web_port, client_options, fuse_options, target_path, fs_path
        )
        
        fuse_cmd = self.build_fuse_command(
            curvine_home, target_path, mnt_number, io_threads, worker_threads
        )
        
        self.generate_mount_script(curvine_home, target_path, fuse_cmd)
        
        config_summary = {
            'Mount path': target_path,
            'FS path': fs_path,
            'Master endpoints': ",".join(f"{host}:{port}" for host, port in master_endpoints),
            'Master web port': master_web_port,
            'Mount point': mount_point,
            'IO threads': io_threads,
            'Worker threads': worker_threads,
            'Mount number': mnt_number,
            'curvine_home': curvine_home
        }
        if client_options:
            config_summary['Client TOML options'] = ",".join(sorted(client_options))
        if fuse_options:
            config_summary['FUSE TOML options'] = ",".join(sorted(fuse_options))
        
        emitted_cli_flags = set()
        for dataset_key, cli_flag in self.FUSE_CLI_OPTIONS:
            if cli_flag in emitted_cli_flags:
                continue
            value = self.get_cli_option(dataset_key)
            if self.has_value(value):
                config_summary[cli_flag.replace('-', ' ').title()] = value
                emitted_cli_flags.add(cli_flag)
        
        self.print_config_summary(config_summary)


def print_error_help() -> None:
    """Print error help message with available options."""
    print("ERROR: Configuration file not found", file=sys.stderr)
    print("\nAvailable environment variables as fallback:", file=sys.stderr)
    env_vars = [
        ("CURVINE_MOUNT_POINT", "Mount point (e.g., curvine:///data)"),
        ("CURVINE_TARGET_PATH", "Target mount path (e.g., /mnt/data)"),
        ("CURVINE_MASTER_ENDPOINTS", "Master endpoints (e.g., master-0:8995,master-1:8995)"),
        ("CURVINE_MASTER_WEB_PORT", "Master web port (default: 8080)"),
        ("CURVINE_IO_THREADS", "IO threads (default: 32)"),
        ("CURVINE_WORKER_THREADS", "Worker threads (default: 56)"),
        ("CURVINE_MNT_NUMBER", "Mount number (default: 1)"),
    ]
    
    for var, desc in env_vars:
        print(f"  {var} - {desc}", file=sys.stderr)
    
    print("\nSupported Dataset options (in spec.mounts[].options):", file=sys.stderr)
    dataset_options = [
        ("master-endpoints", "Master RPC endpoints (hostname:port[,hostname:port...]) [required]"),
        ("master-web-port", "Master web port (default: 8080)"),
        ("io-threads", "IO threads (default: 32)"),
        ("worker-threads", "Worker threads (default: 56)"),
        ("mnt-number", "Mount number (default: 1)"),
        ("entry-timeout-ms", "FUSE entry timeout in milliseconds"),
        ("attr-timeout-ms", "FUSE attr timeout in milliseconds"),
        ("write-back-cache", "FUSE CLI write-back cache override"),
        ("client.<key>", "Advanced Curvine [client] TOML option"),
        ("fuse.<key>", "Advanced Curvine [fuse] TOML option"),
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

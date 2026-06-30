#!/usr/bin/env python3
import os
import sys
import json
from copy import deepcopy

TOP_LEVEL_KEYS = {'format_master', 'format_worker', 'testing', 'cluster_id'}
INTEGER_OPTION_KEYS = {'rpc_port', 'web_port', 'journal_port', 'raft_port'}
SECTION_ALIASES = {
    'journal_port': ('journal', 'rpc_port'),
    'journal_dir': ('journal', 'journal_dir'),
    'master_hostname': ('master', 'hostname'),
    'worker_hostname': ('worker', 'hostname'),
}

def generate_curvine_config():
    """Generate Curvine configuration from Fluid runtime config"""
    curvine_home = os.environ.get('CURVINE_HOME', '/app/curvine')
    config_path = os.environ.get('FLUID_RUNTIME_CONFIG_PATH', '/etc/fluid/config/config.json')
    config_file = os.environ.get('CURVINE_CONF_DIR', f'{curvine_home}/conf') + '/curvine-cluster.toml'
    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    data_dir = os.environ.get('CURVINE_DATA_DIR', f'{curvine_home}/data')
    log_dir = os.environ.get('CURVINE_LOG_DIR', f'{curvine_home}/logs')
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Fluid config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            content = f.read().strip()
            
            if not content or content == '""':
                raise ValueError(f"Fluid config file is empty or contains only empty string: {config_path}")
            
            try:
                fluid_config = json.loads(content)
            except json.JSONDecodeError as e:
                if content.startswith('"') and content.endswith('"'):
                    fluid_config = json.loads(content)
                else:
                    raise ValueError(f"Invalid JSON in config file: {e}")
            
            if isinstance(fluid_config, str):
                if not fluid_config or fluid_config == '""':
                    raise ValueError("Fluid config is empty string after parsing")
                fluid_config = json.loads(fluid_config)
            
            if not isinstance(fluid_config, dict):
                raise ValueError(f"Expected dict, got {type(fluid_config)}")

        cluster_id = _cluster_id(fluid_config)
        
        default_config = {}
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                default_config = _load_existing_toml(f)
        
        current_hostname = os.environ.get('HOSTNAME', 'localhost')
        component_type = _determine_component_type(current_hostname)
        namespace = os.environ.get('FLUID_DATASET_NAMESPACE', 'default')
        
        master_runtime = _component_config(fluid_config, 'master')
        worker_runtime = _component_config(fluid_config, 'worker')
        master_service = _service_name(master_runtime)
        worker_service = _service_name(worker_runtime)
        
        journal_addrs = _generate_journal_addrs(fluid_config, master_service, namespace)
        
        merged_config = _build_base_config(default_config, cluster_id, data_dir, log_dir)
        _merge_fluid_options(merged_config, fluid_config)
        _set_hostnames_and_journal(merged_config, component_type, current_hostname, 
                                 master_service, worker_service, namespace, journal_addrs)
        _set_cache_paths(merged_config, fluid_config)
        _set_client_endpoints(merged_config, journal_addrs)
        _set_target_path(merged_config, fluid_config)
        
        with open(config_file, 'w') as f:
            f.write(_to_toml(merged_config))
        
        _export_environment_variables(component_type, master_service, worker_service, 
                                    journal_addrs, fluid_config)

    except Exception as e:
        print(f'Error generating Curvine config: {e}', file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

def _load_existing_toml(file_obj):
    try:
        import toml  # type: ignore
        return toml.load(file_obj)
    except Exception:
        return {}

def _toml_quote(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'

def _toml_scalar(value):
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    return _toml_quote(value)

def _toml_array(value):
    if not value:
        return '[]'
    if all(isinstance(item, dict) for item in value):
        rendered = []
        for item in value:
            parts = [f"{key} = {_toml_scalar(val)}" for key, val in item.items()]
            rendered.append("{ " + ", ".join(parts) + " }")
        return "[\n    " + ",\n    ".join(rendered) + "\n]"
    return "[" + ", ".join(_toml_scalar(item) for item in value) + "]"

def _to_toml(data):
    lines = []

    def emit_value(key, value):
        if isinstance(value, list):
            lines.append(f"{key} = {_toml_array(value)}")
        else:
            lines.append(f"{key} = {_toml_scalar(value)}")

    for key, value in data.items():
        if not isinstance(value, dict):
            emit_value(key, value)
    if lines:
        lines.append("")

    for section, values in data.items():
        if not isinstance(values, dict):
            continue
        lines.append(f"[{section}]")
        for key, value in values.items():
            if isinstance(value, dict):
                continue
            emit_value(key, value)
        for subsection, subvalues in values.items():
            if not isinstance(subvalues, dict):
                continue
            lines.append("")
            lines.append(f"[{section}.{subsection}]")
            for key, value in subvalues.items():
                emit_value(key, value)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"

def _component_config(fluid_config, component_name):
    """Return component config from either the latest top-level shape or old topology shape."""
    component = fluid_config.get(component_name, {})
    if component:
        return component
    return fluid_config.get('topology', {}).get(component_name, {})

def _cluster_id(fluid_config):
    dataset_name = os.environ.get('FLUID_DATASET_NAME') or os.environ.get('CURVINE_DATASET_NAME')
    if dataset_name:
        return dataset_name
    mounts = fluid_config.get('mounts') or []
    if mounts:
        cluster_id = (mounts[0].get('options') or {}).get('cluster_id')
        if cluster_id:
            return cluster_id
    return 'curvine'

def _service_name(component_config):
    service = component_config.get('service', {})
    if isinstance(service, dict):
        return service.get('name', '') or service.get('serviceName', '')
    return ''

def _runtime_name(fluid_config, master_service):
    master_config = _component_config(fluid_config, 'master')
    if master_config.get('name'):
        return master_config['name'].rsplit('-master', 1)[0]

    topology_master = fluid_config.get('topology', {}).get('master', {})
    master_pods = topology_master.get('podConfigs', [])
    if master_pods:
        first_pod = master_pods[0].get('podName', '')
        if first_pod and '-master-' in first_pod:
            return first_pod.split('-master-')[0]

    if master_service and '-master' in master_service:
        service_prefix = master_service
        if service_prefix.startswith('svc-'):
            service_prefix = service_prefix[len('svc-'):]
        return service_prefix.rsplit('-master', 1)[0]

    dataset_name = os.environ.get('FLUID_DATASET_NAME')
    return dataset_name or 'curvine'

def _determine_component_type(hostname):
    """Determine component type from hostname"""
    component_type = os.environ.get('FLUID_RUNTIME_COMPONENT_TYPE', '')
    if not component_type:
        if 'master' in hostname:
            component_type = 'master'
        elif 'worker' in hostname:
            component_type = 'worker'
        else:
            component_type = 'master'  # fallback
    return component_type

def _generate_journal_addrs(fluid_config, master_service, namespace):
    """Generate journal addresses for master cluster"""
    journal_port = 8996
    master_config = _component_config(fluid_config, 'master')
    master_options = master_config.get('options', {})
    journal_port = int(master_options.get('journal_port') or master_options.get('raft_port') or journal_port)
    journal_addrs = []
    
    topology = fluid_config.get('topology', {})
    master_pods = topology.get('master', {}).get('podConfigs', [])
    
    for i, pod in enumerate(master_pods):
        pod_name = pod.get('podName', '')
        if pod_name:
            hostname = f"{pod_name}.{master_service}.{namespace}.svc.cluster.local"
            journal_addrs.append({"id": i + 1, "hostname": hostname, "port": journal_port})
    
    if not journal_addrs:
        namespace = os.environ.get('FLUID_DATASET_NAMESPACE', namespace or 'default')
        runtime_name = _runtime_name(fluid_config, master_service)
        replicas = int(master_config.get('replicas') or 1)
        if master_service and namespace:
            for i in range(replicas):
                master_pod_name = f"{runtime_name}-master-{i}"
                hostname = f"{master_pod_name}.{master_service}.{namespace}.svc.cluster.local"
                journal_addrs.append({"id": i + 1, "hostname": hostname, "port": journal_port})
        else:
            hostname = 'localhost'
            journal_addrs.append({"id": 1, "hostname": hostname, "port": journal_port})
        print(f"WARNING: No master pods found in topology, using generated journal address list: {journal_addrs}", file=sys.stderr)
    
    journal_addrs.sort(key=lambda x: x['hostname'])
    for i, addr in enumerate(journal_addrs):
        addr['id'] = i + 1
    
    return journal_addrs

def _build_base_config(default_config, cluster_id, data_dir, log_dir):
    """Build base configuration with essential settings"""
    merged_config = deepcopy(default_config)
    merged_config['cluster_id'] = cluster_id
    
    # Initialize sections
    for section in ['master', 'journal', 'worker', 'client', 'fuse', 'log']:
        if section not in merged_config:
            merged_config[section] = {}
    if 'rpc_port' not in merged_config['master']:
        merged_config['master']['rpc_port'] = 8995
    if 'web_port' not in merged_config['master']:
        merged_config['master']['web_port'] = 9000
    if 'rpc_port' not in merged_config['journal']:
        merged_config['journal']['rpc_port'] = 8996
    if 'rpc_port' not in merged_config['worker']:
        merged_config['worker']['rpc_port'] = 8997
    if 'web_port' not in merged_config['worker']:
        merged_config['worker']['web_port'] = 9001
    
    # Update directories
    if not merged_config['master'].get('meta_dir') or merged_config['master'].get('meta_dir', '').startswith('testing/'):
        merged_config['master']['meta_dir'] = f"{data_dir}/meta"
    
    if not merged_config['journal'].get('journal_dir') or merged_config['journal'].get('journal_dir', '').startswith('testing/'):
        merged_config['journal']['journal_dir'] = f"{data_dir}/journal"
    
    # Update log directories
    for component in ['master', 'worker']:
        if 'log' not in merged_config[component] or not isinstance(merged_config[component].get('log'), dict):
            merged_config[component]['log'] = {}
        log_config = merged_config[component]['log']
        if not log_config.get('log_dir') or log_config.get('log_dir') == 'stdout':
            log_config['log_dir'] = log_dir
        if not log_config.get('file_name'):
            log_config['file_name'] = f"{component}.log"

    if not merged_config['log'].get('log_dir'):
        merged_config['log']['log_dir'] = log_dir
    if not merged_config['log'].get('file_name'):
        merged_config['log']['file_name'] = "curvine.log"
    
    return merged_config

def _merge_fluid_options(merged_config, fluid_config):
    """Merge Fluid component options into configuration"""
    def merge_component_options(component_name):
        component_config = _component_config(fluid_config, component_name)
        options = component_config.get('options', {})
        
        if component_name not in merged_config:
            merged_config[component_name] = {}
        
        for key, value in options.items():
            if isinstance(value, str):
                if value.lower() in ['true', 'false']:
                    value = value.lower() == 'true'
                elif key in INTEGER_OPTION_KEYS and value.isdigit():
                    value = int(value)
            
            if key in TOP_LEVEL_KEYS:
                merged_config[key] = value
            elif key in SECTION_ALIASES:
                section, alias_key = SECTION_ALIASES[key]
                if section not in merged_config:
                    merged_config[section] = {}
                merged_config[section][alias_key] = value
            else:
                merged_config[component_name][key] = value
    
    for component in ['master', 'worker', 'client']:
        merge_component_options(component)

def _set_hostnames_and_journal(merged_config, component_type, current_hostname, 
                              master_service, worker_service, namespace, journal_addrs):
    """Set hostnames and journal configuration based on component type"""
    merged_config['journal']['journal_addrs'] = journal_addrs
    
    if component_type == 'master':
        match = next((addr for addr in journal_addrs if addr['hostname'].startswith(f"{current_hostname}.")), None)
        if match:
            master_fqdn = match['hostname']
        elif journal_addrs:
            master_fqdn = journal_addrs[0]['hostname']
        elif master_service and current_hostname != 'localhost':
            master_fqdn = f"{current_hostname}.{master_service}.{namespace}.svc.cluster.local"
        else:
            master_fqdn = current_hostname
        
        merged_config['master']['hostname'] = master_fqdn
        merged_config['journal']['hostname'] = master_fqdn
    else:
        if journal_addrs:
            master_fqdn = journal_addrs[0]['hostname']
            merged_config['master']['hostname'] = master_fqdn
            merged_config['journal']['hostname'] = master_fqdn
        
        if master_service and current_hostname != 'localhost' and namespace:
            worker_fqdn = f"{current_hostname}.{worker_service}.{namespace}.svc.cluster.local"
            merged_config['worker']['hostname'] = worker_fqdn

def _set_cache_paths(merged_config, fluid_config):
    """Set cache paths from tieredStore configuration"""
    worker_config = _component_config(fluid_config, 'worker')
    
    if 'data_dir' in worker_config.get('options', {}):
        data_dir = worker_config['options']['data_dir']
        if isinstance(data_dir, str):
            data_dirs = [path.strip() for path in data_dir.split(',')]
            merged_config['worker']['data_dir'] = data_dirs
        elif isinstance(data_dir, list):
            merged_config['worker']['data_dir'] = data_dir
        else:
            merged_config['worker']['data_dir'] = [str(data_dir)]
    else:
        tiered_store = worker_config.get('tieredStoreLevels') or worker_config.get('tieredStore', [])
        if isinstance(tiered_store, dict):
            tiered_store = tiered_store.get('levels', [])
        
        if tiered_store and len(tiered_store) > 0:
            data_dirs = []
            for level in tiered_store:
                mount_paths = level.get('mountPaths') or [level.get('path', '/cache-data')]
                medium_type = level.get('mediumType')
                medium = level.get('medium', {})
                if not medium_type:
                    if 'emptyDir' in level:
                        medium_type = 'HDD'
                    elif 'emptyDir' in medium and medium['emptyDir'].get('medium') == 'Memory':
                        medium_type = 'MEM'
                    else:
                        medium_type = 'SSD'

                quotas = level.get('quotas') or [level.get('quota') or level.get('emptyDir', {}).get('quota', '')]
                for index, path in enumerate(mount_paths):
                    quota = quotas[index] if index < len(quotas) else ''
                    quota = _normalize_capacity(quota)
                    if quota:
                        data_dirs.append(f"[{medium_type}:{quota}]{path}")
                    else:
                        data_dirs.append(f"[{medium_type}]{path}")
            
            merged_config['worker']['data_dir'] = data_dirs if data_dirs else [f"[SSD]/cache-data"]
        else:
            merged_config['worker']['data_dir'] = [f"[SSD]/cache-data"]

def _normalize_capacity(value):
    if value is None:
        return ''
    value = str(value)
    return value.replace('Ki', 'KB').replace('Mi', 'MB').replace('Gi', 'GB').replace('Ti', 'TB')
        
def _set_client_endpoints(merged_config, journal_addrs):
    """Set client master endpoints"""
    master_rpc_port = merged_config.get('master', {}).get('rpc_port', 8995)
    master_endpoints = []
    
    for journal_addr in journal_addrs:
        hostname = journal_addr['hostname']
        master_endpoints.append({'hostname': hostname, 'port': master_rpc_port})
    
    if not master_endpoints:
        master_endpoints.append({'hostname': 'localhost', 'port': 8995})
    
    merged_config['client']['master_addrs'] = master_endpoints
        
def _set_target_path(merged_config, fluid_config):
    """Set FUSE target path"""
    client_config = _component_config(fluid_config, 'client')
    target_path = fluid_config.get('targetPath') or client_config.get('targetPath', '/runtime-mnt/cache/default/curvine-demo/fuse')
    merged_config['fuse']['mnt_path'] = target_path
        
def _export_environment_variables(component_type, master_service, worker_service, 
                                journal_addrs, fluid_config):
    """Export environment variables for entrypoint script"""
    client_config = _component_config(fluid_config, 'client')
    target_path = fluid_config.get('targetPath') or client_config.get('targetPath', '/runtime-mnt/cache/default/curvine-demo/fuse')
    print(f'export CURVINE_TARGET_PATH="{target_path}"')
    
    if component_type == 'master':
        current_hostname = os.environ.get('HOSTNAME', 'localhost')
        match = next((addr for addr in journal_addrs if addr['hostname'].startswith(f"{current_hostname}.")), None)
        if journal_addrs:
            master_fqdn = (match or journal_addrs[0])['hostname']
            print(f'export CURVINE_MASTER_HOSTNAME="{master_fqdn}"')
        else:
            namespace = os.environ.get('FLUID_DATASET_NAMESPACE', 'default')
            if master_service and current_hostname != 'localhost':
                master_fqdn = f"{current_hostname}.{master_service}.{namespace}.svc.cluster.local"
                print(f'export CURVINE_MASTER_HOSTNAME="{master_fqdn}"')
        
        print(f'export CURVINE_MASTER_SERVICE="{master_service}"')
    else:
        print(f'export CURVINE_MASTER_SERVICE="{master_service}"')
        if worker_service:
            print(f'export CURVINE_WORKER_SERVICE="{worker_service}"')
    
    if journal_addrs:
        master_rpc_port = 8995
        endpoints = []
        for journal_addr in journal_addrs:
            endpoints.append(f"{journal_addr['hostname']}:{master_rpc_port}")
        endpoints_str = ';'.join(endpoints)
        print(f'export CURVINE_MASTER_ENDPOINTS="{endpoints_str}"')

if __name__ == '__main__':
    generate_curvine_config()

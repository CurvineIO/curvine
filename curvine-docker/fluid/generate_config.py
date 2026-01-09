#!/usr/bin/env python3
import os
import sys
import json
import toml
from copy import deepcopy

def generate_curvine_config():
    """Generate Curvine configuration from Fluid runtime config"""
    curvine_home = os.environ.get('CURVINE_HOME', '/app/curvine')
    config_path = os.environ.get('FLUID_RUNTIME_CONFIG_PATH', '/etc/fluid/config/config.json')
    config_file = os.environ.get('CURVINE_CONF_DIR', f'{curvine_home}/conf') + '/curvine-cluster.toml'
    data_dir = os.environ.get('CURVINE_DATA_DIR', f'{curvine_home}/data')
    log_dir = os.environ.get('CURVINE_LOG_DIR', f'{curvine_home}/logs')
    cluster_id = os.environ.get('CURVINE_DATASET_NAME', 'curvine')

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
        
        default_config = {}
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                default_config = toml.load(f)
        
        current_hostname = os.environ.get('HOSTNAME', 'localhost')
        component_type = _determine_component_type(current_hostname)
        namespace = os.environ.get('FLUID_DATASET_NAMESPACE', 'default')
        
        topology = fluid_config.get('topology', {})
        master_service = topology.get('master', {}).get('service', {}).get('name', '')
        worker_service = topology.get('worker', {}).get('service', {}).get('name', '')
        
        journal_addrs = _generate_journal_addrs(fluid_config, master_service, namespace)
        
        merged_config = _build_base_config(default_config, cluster_id, data_dir, log_dir)
        _merge_fluid_options(merged_config, fluid_config)
        _set_hostnames_and_journal(merged_config, component_type, current_hostname, 
                                 master_service, worker_service, namespace, journal_addrs)
        _set_cache_paths(merged_config, fluid_config)
        _set_client_endpoints(merged_config, journal_addrs)
        _set_target_path(merged_config, fluid_config)
        
        with open(config_file, 'w') as f:
            toml.dump(merged_config, f)
        
        _export_environment_variables(component_type, master_service, worker_service, 
                                    journal_addrs, fluid_config)

    except Exception as e:
        print(f'Error generating Curvine config: {e}', file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

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
    journal_addrs = []
    
    topology = fluid_config.get('topology', {})
    master_pods = topology.get('master', {}).get('podConfigs', [])
    
    runtime_name = 'curvine'
    if master_pods:
        first_pod = master_pods[0].get('podName', '')
        if first_pod and '-master-' in first_pod:
            runtime_name = first_pod.split('-master-')[0]
    
    for i, pod in enumerate(master_pods):
        pod_name = pod.get('podName', '')
        if pod_name:
            hostname = f"{pod_name}.{master_service}.{namespace}.svc.cluster.local"
            journal_addrs.append({"id": i + 1, "hostname": hostname, "port": journal_port})
    
    if not journal_addrs:
        namespace = os.environ.get('FLUID_DATASET_NAMESPACE', namespace or 'default')
        if master_service and namespace:
            if '-master' in master_service:
                runtime_name = master_service.rsplit('-master', 1)[0]
                master_pod_name = f"{runtime_name}-master-0"
            else:
                master_pod_name = f"{master_service}-0"
            hostname = f"{master_pod_name}.{master_service}.{namespace}.svc.cluster.local"
        else:
            hostname = 'localhost'
        journal_addrs.append({"id": 1, "hostname": hostname, "port": journal_port})
        print(f"WARNING: No master pods found in topology, using default journal address: {hostname}:{journal_port}", file=sys.stderr)
    
    journal_addrs.sort(key=lambda x: x['hostname'])
    for i, addr in enumerate(journal_addrs):
        addr['id'] = i + 1
    
    return journal_addrs

def _build_base_config(default_config, cluster_id, data_dir, log_dir):
    """Build base configuration with essential settings"""
    merged_config = deepcopy(default_config)
    merged_config['cluster_id'] = cluster_id
    
    # Initialize sections
    for section in ['master', 'journal', 'worker', 'client', 'fuse']:
        if section not in merged_config:
            merged_config[section] = {}
    
    # Update directories
    if merged_config['master'].get('meta_dir', '').startswith('testing/'):
        merged_config['master']['meta_dir'] = f"{data_dir}/meta"
    
    if merged_config['journal'].get('journal_dir', '').startswith('testing/'):
        merged_config['journal']['journal_dir'] = f"{data_dir}/journal"
    
    # Update log directories
    for component in ['master', 'worker']:
        if component in merged_config and 'log' in merged_config[component]:
            log_config = merged_config[component]['log']
            if isinstance(log_config, dict) and log_config.get('log_dir') == 'stdout':
                log_config['log_dir'] = log_dir
    
    return merged_config

def _merge_fluid_options(merged_config, fluid_config):
    """Merge Fluid component options into configuration"""
    TOP_LEVEL_KEYS = {'format_master', 'format_worker', 'testing', 'cluster_id'}
    
    def merge_component_options(component_name):
        component_config = fluid_config.get(component_name, {})
        options = component_config.get('options', {})
        
        if component_name not in merged_config:
            merged_config[component_name] = {}
        
        for key, value in options.items():
            if isinstance(value, str):
                if value.lower() in ['true', 'false']:
                    value = value.lower() == 'true'
                elif value.isdigit():
                    value = int(value)
            
            if key in TOP_LEVEL_KEYS:
                merged_config[key] = value
            else:
                merged_config[component_name][key] = value
    
    for component in ['master', 'worker', 'client']:
        merge_component_options(component)

def _set_hostnames_and_journal(merged_config, component_type, current_hostname, 
                              master_service, worker_service, namespace, journal_addrs):
    """Set hostnames and journal configuration based on component type"""
    merged_config['journal']['journal_addrs'] = journal_addrs
    
    if component_type == 'master':
        if journal_addrs:
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
    worker_config = fluid_config.get('worker', {})
    
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
        tiered_store = worker_config.get('tieredStore', [])
        
        if tiered_store and len(tiered_store) > 0:
            data_dirs = []
            for level in tiered_store:
                path = level.get('path', '/cache-data')
                medium = level.get('medium', {})
                if 'emptyDir' in medium and medium['emptyDir'].get('medium') == 'Memory':
                    storage_type = 'MEM'
                else:
                    storage_type = 'SSD'
                
                quota = level.get('quota', '')
                if quota:
                    data_dirs.append(f"[{storage_type}:{quota}]{path}")
                else:
                    data_dirs.append(f"[{storage_type}]{path}")
            
            merged_config['worker']['data_dir'] = data_dirs if data_dirs else [f"[SSD]/cache-data"]
        else:
            merged_config['worker']['data_dir'] = [f"[SSD]/cache-data"]
        
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
    client_config = fluid_config.get('client', {})
    target_path = client_config.get('targetPath', '/runtime-mnt/cache/default/curvine-demo/fuse')
    merged_config['fuse']['mnt_path'] = target_path
        
def _export_environment_variables(component_type, master_service, worker_service, 
                                journal_addrs, fluid_config):
    """Export environment variables for entrypoint script"""
    client_config = fluid_config.get('client', {})
    target_path = client_config.get('targetPath', '/runtime-mnt/cache/default/curvine-demo/fuse')
    print(f'export CURVINE_TARGET_PATH="{target_path}"')
    
    if component_type == 'master':
        if journal_addrs:
            master_fqdn = journal_addrs[0]['hostname']
            print(f'export CURVINE_MASTER_HOSTNAME="{master_fqdn}"')
        else:
            current_hostname = os.environ.get('HOSTNAME', 'localhost')
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
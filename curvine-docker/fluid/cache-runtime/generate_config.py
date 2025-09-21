#!/usr/bin/env python3
import os
import sys
import json
import toml
from copy import deepcopy

def generate_curvine_config():
    """Generate Curvine configuration from Fluid runtime config"""
    
    # Configuration paths
    config_path = os.environ.get('FLUID_RUNTIME_CONFIG_PATH', '/etc/fluid/config/config.json')
    config_file = os.environ.get('CURVINE_CONF_DIR', '/opt/curvine/conf') + '/curvine-cluster.toml'
    data_dir = os.environ.get('CURVINE_DATA_DIR', '/opt/curvine/data')
    log_dir = os.environ.get('CURVINE_LOG_DIR', '/opt/curvine/logs')
    cluster_id = os.environ.get('CURVINE_DATASET_NAME', 'curvine')

    try:
        # 1. Load Fluid configuration
        with open(config_path, 'r') as f:
            fluid_config = json.load(f)
        
        
        # 2. Load default configuration template
        default_config = {}
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                default_config = toml.load(f)
        
        # 3. Determine component type and basic info
        current_hostname = os.environ.get('HOSTNAME', 'localhost')
        component_type = _determine_component_type(current_hostname)
        namespace = os.environ.get('FLUID_DATASET_NAMESPACE', 'default')
        
        
        # 4. Extract service information from topology
        topology = fluid_config.get('topology', {})
        master_service = topology.get('master', {}).get('service', {}).get('name', '')
        worker_service = topology.get('worker', {}).get('service', {}).get('name', '')
        
        # 5. Generate journal addresses for master cluster
        journal_addrs = _generate_journal_addrs(fluid_config, master_service, namespace)
        
        # 6. Build final configuration
        merged_config = _build_base_config(default_config, cluster_id, data_dir, log_dir)
        _merge_fluid_options(merged_config, fluid_config)
        _set_hostnames_and_journal(merged_config, component_type, current_hostname, 
                                 master_service, worker_service, namespace, journal_addrs)
        _set_cache_paths(merged_config, fluid_config)
        _set_client_endpoints(merged_config, journal_addrs)
        _set_target_path(merged_config, fluid_config)
        
        # 7. Write configuration file
        with open(config_file, 'w') as f:
            toml.dump(merged_config, f)
        
        
        # 8. Export environment variables
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
    journal_port = 8996  # Default journal port
    journal_addrs = []
    
    # Get master pods from podConfigs
    topology = fluid_config.get('topology', {})
    master_pods = topology.get('master', {}).get('podConfigs', [])
    
    # Extract runtime name from pod names or use fallback
    runtime_name = 'curvine'
    if master_pods:
        first_pod = master_pods[0].get('podName', '')
        if first_pod and '-master-' in first_pod:
            runtime_name = first_pod.split('-master-')[0]
    
    # Track which master indices we have
    expected_masters = set()
    
    # Add masters from podConfigs
    for i, pod in enumerate(master_pods):
        pod_name = pod.get('podName', '')
        if pod_name:
            hostname = f"{pod_name}.{master_service}.{namespace}.svc.cluster.local"
            journal_addrs.append({"id": i + 1, "hostname": hostname, "port": journal_port})
            # Extract master index
            if '-master-' in pod_name:
                try:
                    master_idx = int(pod_name.split('-master-')[-1])
                    expected_masters.add(master_idx)
                except ValueError:
                    pass
    
    # Fill in missing masters (assume 3 masters total)
    for i in range(3):
        if i not in expected_masters:
            pod_name = f"{runtime_name}-master-{i}"
            hostname = f"{pod_name}.{master_service}.{namespace}.svc.cluster.local"
            journal_addrs.append({"id": len(journal_addrs) + 1, "hostname": hostname, "port": journal_port})
    
    # Sort and reassign IDs
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
    def merge_component_options(component_name):
        component_config = fluid_config.get(component_name, {})
        options = component_config.get('options', {})
        
        if component_name not in merged_config:
            merged_config[component_name] = {}
        
        for key, value in options.items():
            # Convert string values to appropriate types
            if isinstance(value, str):
                if value.lower() in ['true', 'false']:
                    value = value.lower() == 'true'
                elif value.isdigit():
                    value = int(value)
            
            merged_config[component_name][key] = value
    
    # Merge options for all components
    for component in ['master', 'worker', 'client']:
        merge_component_options(component)

def _set_hostnames_and_journal(merged_config, component_type, current_hostname, 
                              master_service, worker_service, namespace, journal_addrs):
    """Set hostnames and journal configuration based on component type"""
    
    # Set journal addresses
    merged_config['journal']['journal_addrs'] = journal_addrs
    
    if component_type == 'master':
        # Master uses its own FQDN
        if master_service and current_hostname != 'localhost':
            master_fqdn = f"{current_hostname}.{master_service}.{namespace}.svc.cluster.local"
        else:
            master_fqdn = current_hostname
        
        merged_config['master']['hostname'] = master_fqdn
        merged_config['journal']['hostname'] = master_fqdn
        
    else:  # worker or client
        # Use first master as the master hostname reference
        if journal_addrs:
            master_fqdn = journal_addrs[0]['hostname']
            merged_config['master']['hostname'] = master_fqdn
            merged_config['journal']['hostname'] = master_fqdn

def _set_cache_paths(merged_config, fluid_config):
    """Set cache paths from tieredStore configuration"""
    worker_config = fluid_config.get('worker', {})
    tiered_store = worker_config.get('tieredStore', [])
    
    cache_path = '/cache-data'
    if tiered_store and len(tiered_store) > 0:
        cache_path = tiered_store[0].get('cacheDir', '/cache-data')
    
    merged_config['worker']['data_dir'] = [f"[SSD]{cache_path}"]

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
    
    # Common variables
    client_config = fluid_config.get('client', {})
    target_path = client_config.get('targetPath', '/runtime-mnt/cache/default/curvine-demo/fuse')
    print(f'export CURVINE_TARGET_PATH="{target_path}"')
    
    # Component-specific variables
    if component_type == 'master':
        # Master needs its own hostname for validation
        current_hostname = os.environ.get('HOSTNAME', 'localhost')
        namespace = os.environ.get('FLUID_DATASET_NAMESPACE', 'default')
        if master_service and current_hostname != 'localhost':
            master_fqdn = f"{current_hostname}.{master_service}.{namespace}.svc.cluster.local"
            print(f'export CURVINE_MASTER_HOSTNAME="{master_fqdn}"')
        
        print(f'export CURVINE_MASTER_SERVICE="{master_service}"')
        
    else:  # worker or client
        # Worker/client only need service references, not master hostname
        print(f'export CURVINE_MASTER_SERVICE="{master_service}"')
        if worker_service:
            print(f'export CURVINE_WORKER_SERVICE="{worker_service}"')
    
    # Master endpoints for all components
    if journal_addrs:
        master_rpc_port = 8995  # Default RPC port
        endpoints = []
        for journal_addr in journal_addrs:
            endpoints.append(f"{journal_addr['hostname']}:{master_rpc_port}")
        endpoints_str = ';'.join(endpoints)
        print(f'export CURVINE_MASTER_ENDPOINTS="{endpoints_str}"')

if __name__ == '__main__':
    generate_curvine_config()
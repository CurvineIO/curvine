// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.Set;

public class FilesystemConf {
    // Master address list
    public String master_addrs;
    public String client_hostname;
    public int io_threads = 16;
    public int worker_threads = Math.max(16, 2 * Runtime.getRuntime().availableProcessors());
    public int replicas = 1;
    public String block_size = "128MB";
    public boolean short_circuit = true;

    public String write_chunk_size = "128KB";
    public int write_chunk_num = 8;

    public String read_chunk_size = "128KB";
    public int read_chunk_num = 8;
    public int read_parallel = 1;
    public String read_slice_size = "0";
    public int close_reader_limit = 20;

    public String storage_type = "disk";
    public String ttl_ms = "0";
    public String ttl_action = "none";

    // Set up the customer service retry policy
    public long conn_retry_max_duration_ms = 2 * 60 * 1000;
    public long conn_retry_min_sleep_ms = 300;
    public long conn_retry_max_sleep_ms = 10 * 1000;

    // rpc requests retry policy.
    public long rpc_retry_max_duration_ms = 5 * 60 * 1000;
    public long rpc_retry_min_sleep_ms = 300;
    public long rpc_retry_max_sleep_ms = 30 * 1000;

    // Whether to close the idle rpc connection.
    public boolean rpc_close_idle = true;

    //Configuration of timeout for a request.
    public long conn_timeout_ms = 30 * 1000;
    public long rpc_timeout_ms = 120 * 1000;
    public long data_timeout_ms = 5 * 60 * 1000;

    // Number of fs master connections.
    public int master_conn_pool_size = 1;

    // Whether to enable pre-reading, it only controls whether short-circuit read and write, and whether it is turned on.
    public boolean enable_read_ahead = true;
    public String read_ahead_len = "0";
    public String drop_cache_len = "1MB";

    public String failed_worker_ttl = "10m";

    public boolean enable_unified_fs  = true;

    public boolean enable_rust_read_ufs  = false;

    public boolean enable_fallback_read_ufs = true;

    public int umask = 022;

    public String mount_update_ttl = "10m";

    // Log configuration, default to standard output.
    public String log_level = "info";
    public String log_dir = "stderr";
    public String log_file_name = "";
    public int max_log_files = 3;
    public boolean display_thread = false;
    public boolean display_position = true;

    // curvine configures prefix.
    public static final String PREFIX = "fs.cv";

    public static final String HOSTNAME_KEY = "CURVINE_CLIENT_HOSTNAME";

    public FilesystemConf(Configuration conf) throws IllegalAccessException {
        client_hostname = getClientHostname();

        // Get the configuration set by -D, which overwrites the configuration file.
        Set<String> names = System.getProperties().stringPropertyNames();
        for(String name : names) {
            if (name.startsWith(PREFIX)) {
                conf.set(name, System.getProperty(name));
            }
        }

        for(Field field : getClass().getDeclaredFields())  {
            String key = String.format("%s.%s", PREFIX, field.getName());
            String value = conf.get(key);

            if (value == null) {
                continue;
            }

            field.setAccessible(true);
            String fieldType = field.getType().getName();
            switch (fieldType) {
                case "java.lang.String":
                    field.set(this, value);
                    break;
                case "int":
                    field.setInt(this, Integer.parseInt(value));
                    break;
                case "long":
                    field.setLong(this, Long.parseLong(value));
                    break;
                case "boolean" :
                    field.setBoolean(this, Boolean.parseBoolean(value));
                    break;
                default:
                    throw new RuntimeException("Unsupported Type: " + fieldType);
            }
        }
    }

    public String toToml() throws IllegalAccessException {
        StringBuilder builder = new StringBuilder();
        for(Field field : getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            String fieldType = field.getType().getName();
            Object value = field.get(this);
            if (value == null) {
                continue;
            }
            switch (fieldType) {
                case "java.lang.String":
                    value = "\"" + value + "\"";
                    break;
                case "int":
                case "long":
                case "boolean" :
                    break;
                default:
                    throw new RuntimeException("Unsupported Type: " + fieldType);
            }
            builder.append(String.format("%s = %s\n", field.getName(), value));
        }

        return builder.toString();
    }

    private String getClientHostname() {
        // In the k8s environment, POD_IP is used by default as client hostname
        String hostname = System.getenv("POD_IP");
        if (StringUtils.isEmpty(hostname)) {
            // Use CURVINE_CLIENT_HOSTNAME environment variable.
            hostname = System.getenv(HOSTNAME_KEY);
        }

        if (StringUtils.isNotEmpty(hostname)) {
            return hostname;
        } else {
            try {
                // Use native IP.
                return InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String toString() {
        return "FilesystemConf{" +
                "master_addrs='" + master_addrs + '\'' +
                ", client_hostname='" + client_hostname + '\'' +
                ", io_threads=" + io_threads +
                ", worker_threads=" + worker_threads +
                ", replicas=" + replicas +
                ", block_size='" + block_size + '\'' +
                ", short_circuit=" + short_circuit +
                ", write_chunk_size='" + write_chunk_size + '\'' +
                ", write_chunk_num=" + write_chunk_num +
                ", read_chunk_size='" + read_chunk_size + '\'' +
                ", read_chunk_num=" + read_chunk_num +
                ", read_parallel=" + read_parallel +
                ", read_slice_size='" + read_slice_size + '\'' +
                ", close_reader_limit=" + close_reader_limit +
                ", storage_type='" + storage_type + '\'' +
                ", ttl_ms=" + ttl_ms +
                ", ttl_action='" + ttl_action + '\'' +
                ", conn_retry_max_duration_ms=" + conn_retry_max_duration_ms +
                ", conn_retry_min_sleep_ms=" + conn_retry_min_sleep_ms +
                ", conn_retry_max_sleep_ms=" + conn_retry_max_sleep_ms +
                ", rpc_retry_max_duration_ms=" + rpc_retry_max_duration_ms +
                ", rpc_retry_min_sleep_ms=" + rpc_retry_min_sleep_ms +
                ", rpc_retry_max_sleep_ms=" + rpc_retry_max_sleep_ms +
                ", rpc_close_idle=" + rpc_close_idle +
                ", conn_timeout_ms=" + conn_timeout_ms +
                ", rpc_timeout_ms=" + rpc_timeout_ms +
                ", data_timeout_ms=" + data_timeout_ms +
                ", master_conn_pool_size=" + master_conn_pool_size +
                ", enable_read_ahead=" + enable_read_ahead +
                ", read_ahead_len='" + read_ahead_len + '\'' +
                ", drop_cache_len='" + drop_cache_len + '\'' +
                ", failed_worker_ttl='" + failed_worker_ttl + '\'' +
                ", enable_unified_fs=" + enable_unified_fs +
                ", enable_read_ufs=" + enable_rust_read_ufs +
                ", enable_fallback_read_ufs=" + enable_fallback_read_ufs +
                ", umask=" + umask +
                ", mount_update_ttl='" + mount_update_ttl + '\'' +
                ", log_level='" + log_level + '\'' +
                ", log_dir='" + log_dir + '\'' +
                ", log_file_name='" + log_file_name + '\'' +
                ", max_log_files=" + max_log_files +
                ", display_thread=" + display_thread +
                ", display_position=" + display_position +
                '}';
    }
}

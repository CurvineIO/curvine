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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CurvineFilesystemProvider implements Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(CurvineFilesystemProvider.class);

    public static String ZK_SERVER = "fs.cv.proxy.zk.server";
    public static String ZK_PATH = "fs.cv.proxy.zk.path";
    public static String PROXY_ENABLED = "fs.cv.proxy.enabled";
    public static String PROXY_TIMEOUT = "fs.cv.proxy.timeout";
    public static String CLUSTER_NAME = "fs.cv.proxy.cluster.name";

    public static String ZK_ENABLED_VALUE = "1";

    private final String zkServer;
    private final String zkPath;
    private final boolean proxyEnabled;
    private final int proxyTimeout;
    private final String clusterName;

    private final Configuration conf;

    private CuratorFramework zkClient;
    private NodeCache nodeCache;
    private final AtomicBoolean enabled;

    private CurvineFileSystem fs;

    public CurvineFilesystemProvider(Configuration conf) {
        zkServer = conf.get(ZK_SERVER, "");
        zkPath = conf.get(ZK_PATH, "/curvine/proxy_enabled");
        proxyEnabled = conf.getBoolean(PROXY_ENABLED, true);
        proxyTimeout = (int) conf.getTimeDuration(PROXY_TIMEOUT, "60s", TimeUnit.MILLISECONDS);
        clusterName = conf.get(CLUSTER_NAME, "default");

        this.conf = conf;
        this.enabled = new AtomicBoolean(true);

        LOGGER.debug("{}", this);

        init();
    }

    public void init() {
        if (!proxyEnabled) {
            return;
        }

        if (StringUtils.isEmpty(zkServer)) {
            enabled.set(true);
        } else {
            setupZkWatcher();
        }
    }

    private void setupZkWatcher() {
        try {
            zkClient = CuratorFrameworkFactory.newClient(
                    zkServer,
                    proxyTimeout,
                    proxyTimeout,
                    new RetryNTimes(1, 1000)
            );
            zkClient.start();
            nodeCache = new NodeCache(zkClient, zkPath);

            String curValue = new String(zkClient.getData().forPath(zkPath)).trim();
            enabled.set(ZK_ENABLED_VALUE.equals(curValue));

            // Start data listener
            nodeCache.getListenable().addListener(this::handleZkDataChange);
            nodeCache.start(false);
        } catch (Exception e) {
            enabled.set(false);
            LOGGER.warn("Failed to setup ZooKeeper listener", e);
            cleanupZkResources();
        }
    }

    private void handleZkDataChange() {
        try {
            ChildData currentData = nodeCache.getCurrentData();
            if (currentData == null) {
               enabled.set(false);
            } else {
                String value = new String(currentData.getData()).trim();
                enabled.set(ZK_ENABLED_VALUE.equals(value));
            }
        } catch (Exception e) {
            enabled.set(false);
            LOGGER.warn("Error handling ZooKeeper data change", e);
        }
    }

    public CurvineFileSystem getFs() {
        if (!enabled.get()) {
            return null;
        }

        if (fs == null) {
            try {
                fs = createCurvineFileSystem();
            } catch (Exception e) {
                enabled.set(false);
                LOGGER.warn("createCurvineFileSystem", e);
            }
        }
        return fs;
    }

    private CurvineFileSystem createCurvineFileSystem() throws Exception {
        CompletableFuture<CurvineFileSystem> future = CompletableFuture.supplyAsync(() -> {
            try {
                URI uri = new URI(CurvineFileSystem.SCHEME + "://" + clusterName);
                FileSystem fileSystem = FileSystem.get(uri, conf);

                if (!(fileSystem instanceof CurvineFileSystem)) {
                    throw new RuntimeException("Failed to create CurvineFileSystem instance");
                }

                CurvineFileSystem curvineFs = (CurvineFileSystem) fileSystem;

                // Check configuration parameters
                FilesystemConf filesystemConf = curvineFs.getFilesystemConf();
                if (!filesystemConf.enable_unified_fs) {
                    throw new RuntimeException("enable_unified_fs must be set true");
                }

                if (filesystemConf.enable_rust_read_ufs) {
                    throw new RuntimeException("enable_read_ufs must be set false");
                }

                return curvineFs;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create CurvineFileSystem", e);
            }
        });

        return future.get(proxyTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        enabled.set(false);
        cleanupZkResources();
        IOUtils.closeQuietly(fs);
        fs = null;
    }


    private void cleanupZkResources() {
        if (nodeCache != null) {
            try {
                nodeCache.close();
            } catch (IOException e) {
                // pass
            }
            nodeCache = null;
        }

        if (zkClient != null) {
            IOUtils.closeQuietly(zkClient);
            zkClient = null;
        }
    }

    @Override
    public String toString() {
        return "CurvineFilesystemProvider{" +
                "zkServer='" + zkServer + '\'' +
                ", zkPath='" + zkPath + '\'' +
                ", proxyEnabled=" + proxyEnabled +
                ", proxyTimeout=" + proxyTimeout +
                ", clusterName='" + clusterName + '\'' +
                '}';
    }
}
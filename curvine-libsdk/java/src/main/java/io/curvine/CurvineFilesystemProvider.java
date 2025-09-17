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
    public static String CACHE_ENABLE = "fs.cv.proxy.enable";
    public static String CACHE_TIMEOUT = "fs.cv.proxy.timeout";
    public static String CLUSTER_NAME = "fs.cv.proxy.cluster.name";

    public static String ZK_ENABLE_VALUE = "1";

    private final String zkServer;
    private final String zkPath;
    private final boolean proxyEnable;
    private final int proxyTimeout;
    private final String clusterName;

    private final Configuration conf;

    private CuratorFramework zkClient;
    private NodeCache nodeCache;
    private final AtomicBoolean enable;

    private CurvineFileSystem fs;

    public CurvineFilesystemProvider(Configuration conf) {
        zkServer = conf.get(ZK_SERVER, "");
        zkPath = conf.get(ZK_PATH, "/curvine/proxy_enable");
        proxyEnable = conf.getBoolean(CACHE_ENABLE, true);
        proxyTimeout = (int) conf.getTimeDuration(CACHE_TIMEOUT, "60s", TimeUnit.MILLISECONDS);
        clusterName = conf.get(CLUSTER_NAME, "default");

        this.conf = conf;
        this.enable = new AtomicBoolean(true);

        LOGGER.debug("{}", this);

        init();
    }

    public void init() {
        if (!proxyEnable) {
            return;
        }

        if (StringUtils.isEmpty(zkServer)) {
            enable.set(true);
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
            enable.set(ZK_ENABLE_VALUE.equals(curValue));

            // Start data listener
            nodeCache.getListenable().addListener(this::handleZkDataChange);
            nodeCache.start(false);
        } catch (Exception e) {
            enable.set(false);
            LOGGER.warn("Failed to setup ZooKeeper listener", e);
            cleanupZkResources();
        }
    }

    private void handleZkDataChange() {
        try {
            ChildData currentData = nodeCache.getCurrentData();
            if (currentData == null) {
               enable.set(false);
            } else {
                String value = new String(currentData.getData()).trim();
                enable.set(ZK_ENABLE_VALUE.equals(value));
            }
        } catch (Exception e) {
            enable.set(false);
            LOGGER.warn("Error handling ZooKeeper data change", e);
        }
    }

    public CurvineFileSystem getFs() {
        if (!enable.get()) {
            return null;
        }

        if (fs == null) {
            try {
                fs = createCurvineFileSystem();
            } catch (Exception e) {
                enable.set(false);
                LOGGER.warn("createCurvineFileSystem", e);
            }
        }
        return fs;
    }

    private CurvineFileSystem createCurvineFileSystem() throws Exception {
        CompletableFuture<CurvineFileSystem> future = CompletableFuture.supplyAsync(() -> {
            try {
                String scheme = "cv";
                URI uri = new URI(scheme + "://" + clusterName);
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
        enable.set(false);
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
            zkClient.close();
            zkClient = null;
        }
    }

    @Override
    public String toString() {
        return "CurvineFilesystemProvider{" +
                "zkServer='" + zkServer + '\'' +
                ", zkPath='" + zkPath + '\'' +
                ", proxyEnable=" + proxyEnable +
                ", proxyTimeout=" + proxyTimeout +
                ", clusterName='" + clusterName + '\'' +
                '}';
    }
}
## 路径挂载

### 挂载模式

使用缓存之前，需要先将底层存储（ufs）挂载到一个curvine路径上。

目前挂载支持2种模式

1. Cst：路径一致模式，需要ufs、curvine路径完全一致。比如s3://bucket/a/，那么挂载路径必须是 /bucket/a。这样好处是路径对于，方便维护和管理
2. Arch：编排模式，对路径没有要求。

 

### 挂载命令使用

使用bin/cv mount命令挂载一个路径，命令格式

```
bin/cv mount ufs_path cv_path [options]
```

一个挂载案例如下

```
bin/cv mount \
s3://bucket/warehouse/tpch_500g.db/order
/bucket/warehouse/tpch_500g.db/order
--ttl-ms 24h \
--ttl-action delete \
--replicas 1 \
--block-size 128MB \
-c s3.endpoint_url=http://s3.ap-southeast-1.amazonaws.com \
-c s3.credentials.access=access_key\
-c s3.credentials.secret=secret_key\
-c s3.region_name=ap-southeast-1
```

 

挂载参数说明

| 参数                 | 说明                                   |
| -------------------- | -------------------------------------- |
| ttl-ms               | 缓存数据的过期时间                     |
| ttl-action           | 缓存数据过期策略                       |
| replicas             | 缓存数据副本数                         |
| block-size           | 缓存数据的block大小                    |
| mnt_type             | 挂载类型，cst或者arch。默认cst         |
| storage_type         | 缓存优先存储类型。默认为disk，任意类型 |
| consistency_strategy | 缓存有效性检查策略                     |

 

 

卸载：

```
bin/cv unmount cv_path
```

 

 

查看挂载列表：

```
bin/cv mount
```

 

## 数据缓存

### 主动加载数据

ufs挂载到curvine后，可以通过load命令提前缓存数据到curvine:

```
bin/load ufs_path
```

如果ufs_path是目录，会递归加载目录下所有文件。

 

加载过程是异步执行的，如果需要同步观察加载加载进度，使用 -w参数：

```
bin/load ufs_path -w
```

 

### 自动加载数据

ufs挂载到curvine后，访问ufs目录下任意文件，在ttl_ms 大于0的情况下，都会除非自动加载数据。这样第一次访问，从ufs读取数据同事异步缓存数据。第二次就可以从缓存读取数据。

 

传统的自动缓存模式（比如Alluxio），是以block为单位，在读取数据时候提交一个任务同步缓存该block，这种模式问题比较多：

1. 如果文件并发读取，存在重复加载的问题，大量浪费缓存集群资源。
2. 容易缓存不完整的数据，用户可能只读取了一部分数据。
3. 数据一致性问题，如果底层存储更新无法感知。

鉴于上面的问题，在使用上更加建议使用load提前加载数据，然后使用。在数据更新后，需要重新加载数据。这个带来了额外的维护成本。

 

curvine 的自动加载使用job-manager， task-manager分布式架构实现，内嵌在curvine集群中，优势如下：

1. 加载以文件、目录为单位。主动和自动加载使用同一套逻辑，互补同时不冲突。
2. 并发读取的情况下，相同文件的缓存只会加载1次。
3. 可以感知底层存储文件的变化，解决一致性问题。第一次可以使用load加载数据，后续使用自动加载模式。

### 数据一致性检查

目前提供了3种数据一致性检查模式，在挂载底层存储使用 consistency_strategy参数指定：

1. none：不检查，缓存在ttl时间内都是有效的，只有过期后才会重新从ufs加载数据。
2. always：每次读取都检查缓存是否有效，会检查ufs文件是否已经更新
3. period：没间隔一段时间检查缓存是否有效。

 

默认为always，每次读取都检查缓存有效性，保证数据一致性。

 

### 缓存命中率评估

每个挂载点的缓存命中情况有metrics系统记录，通过master 9001端口可以获取：

其中id为挂载点的id，通过mount命令可以获取id对应的挂载路径。

```
curl http://master:9001/metrics|grep cace


# HELP client_mount_cache_hits client_mount_cache_hits
# TYPE client_mount_cache_hits counter
client_mount_cache_hits{id="2944546843"} 155900
client_mount_cache_hits{id="3108497238"} 823307
client_mount_cache_hits{id="3459234012"} 63242
client_mount_cache_hits{id="3549302948"} 107331
client_mount_cache_hits{id="547222020"} 668
client_mount_cache_hits{id="753919484"} 45133
# HELP client_mount_cache_misses client_mount_cache_misses
# TYPE client_mount_cache_misses counter
client_mount_cache_misses{id="2944546843"} 16475
client_mount_cache_misses{id="3108497238"} 4380
client_mount_cache_misses{id="3459234012"} 9600
client_mount_cache_misses{id="3549302948"} 2824
client_mount_cache_misses{id="547222020"} 412
client_mount_cache_misses{id="753919484"} 2071
```

## AI场景缓存使用

 

## 大数据场景使用

准备工作，将编译好的curvien-hadoop-*-client.jar放到spark jars目录下或者 archive zip里面。

在hdfs-site.xml中添加如下配置：

```
<property>
    <name>fs.cv.impl</name>
    <value>io.curvine.CurvineFileSystem</value>
</property>


<property>
    <name>fs.cv.master_addrs</name>
    <!-- 配置curvine集群master节点地址 ->>
    <value>m0:8995,m1:8995,m2:8995</value>
</property>
```

如果使用了多个curvine集群，可以使用nameservice区分：

```
<property>
    <name>fs.cv.impl</name>
    <value>io.curvine.CurvineFileSystem</value>
</property>


<property>
    <name>fs.cv.cluster1.master_addrs</name>
    <!-- 配置curvine集群master节点地址 ->>
    <value>m0:8995,m1:8995,m2:8995</value>
</property>


<property>
    <name>fs.cv.cluster2.master_addrs</name>
    <!-- 配置curvine集群master节点地址 ->>
    <value>n0:8995,n1:8995,n2:8995</value>
</property>
```

 

也可以在spark 启动时候指定参数：

```
--conf spark.hadoop.fs.cv.impl=io.curvine.CurvineFileSystem \
--conf spark.hadoop.fs.cv.cluster1.master_addrs=m0:8995,m1:8995,m2:8995
```

### spark 代码读取文件

在路径挂载后，将读取文件的路径修改为：cv://default

```
// 之前
spark.read.csv("s3://bucket/a")


// 替换之后
spark.read.csv("cv://bucket/a")


// 多集群
spark.read.csv("cv://cluster1/ bucket/a")
```

 

### spark sql、trnio 插件化

通过使用spark 插件动态修改路径，可以做到无侵入性实现缓存加速，不需要业务修改代码。

 

插件工作流程如下：

1. 在sql解析阶段获取表路径
2. 访问curvine集群，查询表路径是否挂载
3. 如果已经挂载，将路径替换为cv://
4. 如果没有挂载，不做任何处理。

 

插件化的要求是，挂载路径以表路径为单位，方便精细化管理，缓存高频访问的表。
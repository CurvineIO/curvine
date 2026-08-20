export default {
  mounts: {
    metrics: {
      mountPoints: 'Mount Points',
      mountPointsMeta: 'configured UFS mappings',
      fsMode: 'FS Mode',
      fsModeMeta: 'metadata sync mounts',
      cacheMode: 'Cache Mode',
      cacheModeMeta: 'UFS passthrough mounts',
      providers: 'Providers',
      providersMeta: 'detected UFS types',
      current: 'current',
      resync: 'resync',
      cache: 'cache'
    },
    table: {
      title: 'Mount Table',
      subtitle: 'Manage UFS mappings exposed through Curvine',
      addMount: 'Add Mount',
      curvinePath: 'Curvine Path',
      ufsUri: 'UFS URI',
      mode: 'Mode',
      ttl: 'TTL',
      provider: 'Provider',
      resync: 'Resync',
      cacheModeLabel: 'cache mode',
      empty: 'No mounts in this mode.',
      emptySearch: 'No mounts matched this Curvine path.'
    },
    filters: {
      all: 'All',
      fsMode: 'FS Mode',
      cacheMode: 'Cache Mode',
      ariaLabel: 'Mount mode filter',
      pathSearchPlaceholder: 'Fuzzy search by Curvine path, e.g. lake/raw',
      pathSearchAria: 'Search mounts by Curvine path'
    },
    form: {
      addTitle: 'Add Mount',
      updateTitle: 'Update Mount',
      subtitle: 'Configure or update a UFS path mounted into Curvine',
      curvinePath: 'Curvine Path',
      curvinePathPlaceholder: '/lake/raw',
      ufsUri: 'UFS URI',
      ufsUriPlaceholder: 'file:///data/raw or s3://bucket/raw',
      writeType: 'Write Type',
      writeTypeLocked: 'Write type cannot be changed while updating a mount',
      ttl: 'TTL',
      ttlPlaceholder: '7d',
      provider: 'Provider',
      replicas: 'Replicas',
      replicasPlaceholder: 'default',
      blockSize: 'Block Size',
      blockSizePlaceholder: '128MB',
      readVerifyUfs: 'Read verify UFS',
      properties: 'Properties',
      addProperty: 'Add Property',
      valuePlaceholder: 'value',
      remove: 'Remove',
      mount: 'Mount',
      update: 'Update',
      cancel: 'Cancel',
      auto: 'auto',
      default: 'default',
      fs_mode: 'fs_mode',
      cache_mode: 'cache_mode',
      opendal: 'opendal',
      ossHdfs: 'oss-hdfs'
    },
    propertiesHints: {
      ossHdfs: 'OSS-HDFS property keys: oss.endpoint, oss.accessKeyId, oss.accessKeySecret, oss.region',
      hdfs: 'HDFS property keys: hdfs.namenode, hdfs.user, hdfs.root',
      webhdfs: 'WebHDFS property keys: webhdfs.endpoint, webhdfs.root',
      file: 'Local file:// mounts usually need no extra properties. S3 example: s3.endpoint_url, s3.credentials.access, s3.credentials.secret, s3.region_name',
      s3: 'S3/Opendal property keys: s3.endpoint_url, s3.credentials.access, s3.credentials.secret, s3.region_name'
    },
    validation: {
      succeeded: 'Validation succeeded',
      failed: 'Validation failed',
      reachableDir: 'UFS path is reachable. Directory {path} contains {count} entries.',
      reachableFile: 'UFS path is reachable: {path}',
      existingDir: 'Directory contains {count} entries.',
      existingFile: 'File path is reachable.',
      existingSuccess: 'Validation succeeded: {path}\n{detail}',
      existingFailed: 'Validation failed: {message}',
      invalidProperty: 'Invalid property: {line}',
      curvinePathRequired: 'Curvine Path is required',
      ufsUriRequired: 'UFS URI is required'
    },
    alerts: {
      created: 'Mount created.',
      updated: 'Mount updated.',
      mountFailed: 'Mount failed: {error}',
      resyncFailed: 'Resync failed: {error}',
      deleteConfirm: 'Delete mount {path}?',
      deleted: 'Deleted mount: {path}',
      deleteFailed: 'Delete failed: {error}'
    },
    resync: {
      label: '{status}: scanned {scanned}, recreated {recreated}, skipped {skipped}, failed {failed}, pending {pending}'
    }
  }
}

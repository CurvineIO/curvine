export default {
  browse: {
    title: 'File Browser',
    subtitle: 'Browse Curvine namespace and inspect file blocks',
    pathPlaceholder: '/path',
    cacheOnly: 'Cache only',
    mkdir: 'Mkdir',
    uploadFile: 'Upload File',
    uploadFolder: 'Upload Folder',
    free: 'Free',
    copyPath: 'Copy absolute path',
    goUpTitle: 'Open parent folder',
    view: {
      list: 'List',
      blocks: 'Blocks'
    },
    itemType: {
      directory: 'Directory',
      file: 'File'
    },
    table: {
      name: 'Name',
      type: 'Type',
      size: 'Size',
      modified: 'Modified',
      replicas: 'Replicas',
      permission: 'Permission',
      owner: 'Owner',
      group: 'Group',
      nlink: 'Links',
      blockSize: 'Block size',
      storageType: 'Storage type',
      ttl: 'TTL',
      atime: 'Accessed'
    },
    columns: {
      button: 'Columns',
      title: 'Visible columns',
      hint: 'Applies to this listing only',
      replicasHint: 'Configured target replica count, matching `curvine-cli fs ls -l`',
      reset: 'Reset'
    },
    empty: {
      cacheOnly: 'No cached files in this path.',
      directory: 'Directory is empty.'
    },
    upload: {
      progress: 'Upload Progress',
      clearCompleted: 'Clear completed',
      clear: 'Clear',
      uploadedCount: '{uploaded} / {total} uploaded',
      uploadingFiles: 'Uploading {count} file(s) to Curvine...',
      uploaded: 'Uploaded: {path}',
      uploadedFolder: 'Uploaded {count} file(s) from folder.',
      folderUpload: 'Folder upload',
      failed: 'Upload failed: {error}',
      folderFailed: 'Folder upload failed: {error}'
    },
    ufsSync: {
      title: 'FS mode UFS Sync Progress',
      collapse: 'Collapse',
      expand: 'Expand',
      collapseAria: 'Collapse UFS sync panel',
      expandAria: 'Expand UFS sync panel',
      viewJobs: 'View details in Sync Jobs',
      activeTotal: '{active} active · {total} total',
      tracked: '{total} tracked',
      ufsSynced: '{synced} / {total} UFS synced',
      states: {
        queued: 'Queued',
        completed: 'Completed',
        needsAttention: 'Needs attention',
        syncingToUfs: 'Syncing to UFS',
        waitingForUfsSync: 'Waiting for UFS sync',
        uploadingToCurvine: 'Uploading to Curvine',
        finishedWithSkipped: 'Finished with skipped files',
        uploadedToCurvine: 'Uploaded to Curvine',
        checkingUfsSync: 'Checking UFS sync',
        uploadFailed: 'Upload failed',
        interrupted: 'Interrupted',
        timedOut: 'Timed out',
        unsupported: 'Unsupported',
        notMounted: 'NotMounted',
        notStarted: 'NotStarted',
        pending: 'Pending',
        loading: 'Loading',
        failed: 'Failed',
        canceled: 'Canceled',
        unknown: 'Unknown'
      },
      messages: {
        interrupted: 'Upload progress stopped updating. The browser upload was interrupted or the page was refreshed.',
        timedOut: 'UFS sync status did not finish after 6 minutes. Refresh status or check the load job.',
        unsupported: 'The running web API does not expose /v1/fs/ufs-sync. Rebuild or restart the web service.',
        statusCheckFailed: 'Status check failed (attempt {attempt}): {message}',
        timedOutRecovery: 'UFS sync status did not recover after repeated errors. Refresh or check the load job.',
        waitingForJob: 'waiting for job',
        filesPending: 'files pending',
        filesCount: '{completed} / {total} files'
      },
      tooltip: {
        cv: 'CV: {path}',
        ufs: 'UFS: {path}',
        job: 'Job: {id}',
        mount: 'mount {path}'
      }
    },
    download: {
      file: 'Downloading {path}...',
      folder: 'Downloading {path} as tar. Large folders may take a while.'
    },
    freeAction: {
      currentTitle: 'Free cached data under this mounted path',
      onlyMount: 'Only paths under a UFS mount can be freed',
      itemDir: 'Free cached data under this mounted directory',
      itemFile: 'Free cached data for this file',
      confirmDir: 'Free cached data under {path} recursively?',
      confirmFile: 'Free cached data for {path}?',
      confirmCurrent: 'Free cached data under {path} recursively?',
      result: 'inodes: {inodes}, space: {space}',
      failed: 'Free failed: {error}'
    },
    delete: {
      mountRoot: 'Use Mounts page to delete mount',
      mounted: 'Delete from Curvine and backing UFS',
      directory: 'Delete directory recursively',
      file: 'Delete file',
      confirmDirMounted: 'Delete directory {path}? This also deletes the backing UFS data.',
      confirmFileMounted: 'Delete file {path}? This also deletes the backing UFS data.',
      confirmDir: 'Delete directory {path}?',
      confirmFile: 'Delete file {path}?',
      success: 'Deleted: {path}',
      failed: 'Delete failed: {error}'
    },
    mkdirAction: {
      prompt: 'Directory path',
      defaultName: 'new-dir',
      success: 'Created directory: {path}',
      failed: 'Mkdir failed: {error}'
    },
    copy: {
      failed: 'Copy failed: {error}'
    }
  }
}

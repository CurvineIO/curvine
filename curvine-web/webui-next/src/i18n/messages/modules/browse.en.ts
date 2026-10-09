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
    copyName: 'Copy name',
    breadcrumbHidden: 'Show {count} hidden levels',
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
      atime: 'Accessed',
      expireTime: 'Expire time'
    },
    expire: {
      never: 'Never'
    },
    storageLocation: {
      both: 'both',
      cv: 'CV',
      ufs: 'UFS'
    },
    columns: {
      replicasHint: 'Configured target replica count, matching `curvine-cli fs ls -l`',
      expireHint: 'mtime + TTL when ttl_action is not None',
      locationHint: 'Whether data is on UFS, Curvine, or both'
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
    bulk: {
      selectAll: 'Select all',
      selectRow: 'Select {path}',
      selected: '{count} selected',
      clear: 'Clear selection',
      freeConfirm: 'Free cached data for {count} selected item(s)?',
      freeSkipped: '{count} UFS-only item(s) will be skipped.',
      freeNothing: 'None of the selected items are cached in Curvine',
      freeSuccess: 'Freed {count} item(s): {inodes} inodes, {space}.',
      freeFailed: 'Free failed for {failed} item(s); {done} succeeded.',
      deleteConfirm: 'Delete {count} selected item(s)?',
      deleteSkipped: '{count} mount root(s) will be skipped.',
      deleteReadOnlySkipped: '{count} item(s) under read-only mounts will be skipped.',
      deleteNothing: 'Mount roots cannot be deleted here',
      deleteNothingReadOnly: 'Read-only mounts cannot be deleted here',
      deleteWarningMounted: 'Danger: {count} selected item(s) are under UFS mounts. Delete removes them from Curvine and the backing UFS in both FS mode and cache mode; this cannot be undone.',
      deleteWarningCurvine: 'Danger: This permanently removes the selected data from Curvine and cannot be undone.',
      deleteSuccess: 'Deleted {count} item(s).',
      deleteFailed: 'Delete failed for {failed} item(s); {done} succeeded.'
    },
    delete: {
      mountRoot: 'Use Mounts page to delete mount',
      readOnlyMount: 'This mount is read-only; delete is disabled',
      mounted: 'Delete from Curvine and backing UFS',
      directory: 'Delete directory recursively',
      file: 'Delete file',
      confirmDirMounted: 'Delete directory {path}? This also deletes the backing UFS data.',
      confirmFileMounted: 'Delete file {path}? This also deletes the backing UFS data.',
      confirmDir: 'Delete directory {path}?',
      confirmFile: 'Delete file {path}?',
      warningMounted: 'Danger: This also removes the item from the backing UFS in both FS mode and cache mode. This cannot be undone.',
      warningCurvine: 'Danger: This permanently removes the item from Curvine and cannot be undone.',
      success: 'Deleted: {path}',
      failed: 'Delete failed: {error}'
    },
    mkdirAction: {
      prompt: 'Directory path',
      defaultName: 'new-dir',
      success: 'Created directory: {path}',
    },
    copy: {
      failed: 'Copy failed: {error}',
      pathCopied: 'Absolute path copied',
      nameCopied: 'File name copied'
    },
    preview: {
      title: 'Preview',
      downloadFile: 'Download',
      unsupported: 'No preview for this kind of file — download it to open it locally.',
      tooLarge: 'Too large to preview (limit {limit}).',
      tooLargeHint: 'Download the file to open it locally.',
      unknownSize: 'Cannot preview: the file size is unknown.',
      unknownSizeHint: 'Download the file to open it locally.',
      truncated: 'Showing the first {shown} of this file.',
      empty: 'This file is empty.',
      failed: 'Could not read the file.',
      imageAlt: 'Preview of {name}',
      previousPage: 'Previous',
      nextPage: 'Next',
      pageOf: '{page} / {total}',
      goToPage: 'Go to page {page}'
    }
  }
}

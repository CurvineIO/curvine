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

import io.curvine.proto.FileStatusProto;
import io.curvine.proto.FileTypeProto;
import io.curvine.proto.StoragePolicyProto;
import io.curvine.proto.StorageStateProto;
import io.curvine.proto.StorageTypeProto;
import io.curvine.proto.TtlActionProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URI;

public class CurvineFileSystemToHadoopTest {

    @Test
    public void toHadoopMapsOwnerGroupAndModeFromProto() throws Exception {
        CurvineFileSystem fs = new CurvineFileSystem();
        fs.setConf(new Configuration());
        Field uriField = CurvineFileSystem.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(fs, URI.create("cv://localhost"));

        FileStatusProto proto = FileStatusProto.newBuilder()
                .setId(1)
                .setPath("/demo/file")
                .setName("file")
                .setIsDir(false)
                .setMtime(1_700_000_000_000L)
                .setAtime(1_700_000_001_000L)
                .setChildrenNum(0)
                .setIsComplete(true)
                .setLen(42)
                .setReplicas(2)
                .setBlockSize(128L * 1024L * 1024L)
                .setFileType(FileTypeProto.FILE_TYPE_PROTO_FILE)
                .setStoragePolicy(StoragePolicyProto.newBuilder()
                        .setStorageType(StorageTypeProto.STORAGE_TYPE_PROTO_DISK)
                        .setTtlMs(0)
                        .setTtlAction(TtlActionProto.TTL_ACTION_PROTO_NONE)
                        .setUfsMtime(0)
                        .setState(StorageStateProto.STORAGE_STATE_PROTO_CV)
                        .build())
                .setOwner("alice")
                .setGroup("staff")
                .setMode(0644)
                .setNlink(1)
                .build();

        FileStatus status = fs.toHadoop(proto, new Path("/demo/file"));
        Assert.assertEquals("alice", status.getOwner());
        Assert.assertEquals("staff", status.getGroup());
        Assert.assertEquals(new FsPermission((short) 0644), status.getPermission());
        Assert.assertEquals(42, status.getLen());
        Assert.assertEquals(1_700_000_000_000L, status.getModificationTime());
        Assert.assertEquals(1_700_000_001_000L, status.getAccessTime());
    }
}

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

import io.curvine.proto.AccessModeProto;
import io.curvine.proto.ProviderProto;
import io.curvine.proto.StorageTypeProto;
import io.curvine.proto.TtlActionProto;
import io.curvine.proto.WriteTypeProto;
import org.junit.Assert;
import org.junit.Test;

public class MountRequestTest {

    @Test
    public void builderUsesCacheModeDefaults() {
        MountRequest request = MountRequest.builder()
                .ufsPath("oss://bucket/data")
                .cvPath("/data")
                .addProperty("oss.endpoint", "https://oss.example.com")
                .build();

        Assert.assertEquals("oss://bucket/data", request.getUfsPath());
        Assert.assertEquals("/data", request.getCvPath());
        Assert.assertFalse(request.getOptions().getUpdate());
        Assert.assertEquals(7L * 24L * 60L * 60L * 1000L, request.getOptions().getTtlMs());
        Assert.assertEquals(
                TtlActionProto.TTL_ACTION_PROTO_DELETE, request.getOptions().getTtlAction());
        Assert.assertFalse(request.getOptions().getReadVerifyUfs());
        Assert.assertEquals(
                WriteTypeProto.WRITE_TYPE_PROTO_CACHE_MODE, request.getOptions().getWriteType());
        Assert.assertTrue(request.getOptions().getAutoCache());
        Assert.assertEquals(
                AccessModeProto.ACCESS_MODE_PROTO_READ_ONLY, request.getOptions().getAccessMode());
        Assert.assertEquals(
                "https://oss.example.com", request.getOptions().getAddPropertiesOrThrow("oss.endpoint"));
        Assert.assertFalse(request.getOptions().hasProvider());
        Assert.assertFalse(request.getOptions().hasStorageType());
    }

    @Test
    public void builderEncodesAllMountOptions() {
        MountRequest request = MountRequest.builder()
                .ufsPath("oss://bucket/data")
                .cvPath("/data")
                .update(true)
                .ttlMs(60_000L)
                .readVerifyUfs(true)
                .storageType(StorageTypeProto.STORAGE_TYPE_PROTO_SSD)
                .blockSize(16L * 1024L * 1024L)
                .replicas(3)
                .removeProperty("old.property")
                .writeType(WriteTypeProto.WRITE_TYPE_PROTO_FS_MODE)
                .provider(ProviderProto.PROVIDER_PROTO_OPENDAL)
                .autoCache(false)
                .accessMode(AccessModeProto.ACCESS_MODE_PROTO_READ_WRITE)
                .build();

        Assert.assertTrue(request.getOptions().getUpdate());
        Assert.assertEquals(60_000L, request.getOptions().getTtlMs());
        Assert.assertTrue(request.getOptions().getReadVerifyUfs());
        Assert.assertEquals(
                StorageTypeProto.STORAGE_TYPE_PROTO_SSD, request.getOptions().getStorageType());
        Assert.assertEquals(16L * 1024L * 1024L, request.getOptions().getBlockSize());
        Assert.assertEquals(3, request.getOptions().getReplicas());
        Assert.assertEquals("old.property", request.getOptions().getRemoveProperties(0));
        Assert.assertEquals(
                WriteTypeProto.WRITE_TYPE_PROTO_FS_MODE, request.getOptions().getWriteType());
        Assert.assertEquals(
                TtlActionProto.TTL_ACTION_PROTO_FREE, request.getOptions().getTtlAction());
        Assert.assertEquals(
                ProviderProto.PROVIDER_PROTO_OPENDAL, request.getOptions().getProvider());
        Assert.assertFalse(request.getOptions().getAutoCache());
        Assert.assertEquals(
                AccessModeProto.ACCESS_MODE_PROTO_READ_WRITE, request.getOptions().getAccessMode());
    }

    @Test
    public void builderRejectsBlankPaths() {
        assertInvalid("ufsPath", MountRequest.builder().cvPath("/data"));
        assertInvalid("cvPath", MountRequest.builder().ufsPath("oss://bucket/data"));
        assertInvalid("ufsPath", MountRequest.builder().ufsPath("  ").cvPath("/data"));
        assertInvalid("cvPath", MountRequest.builder().ufsPath("oss://bucket/data").cvPath("  "));
    }

    private static void assertInvalid(String field, MountRequest.Builder builder) {
        try {
            builder.build();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains(field));
        }
    }
}

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

import io.curvine.proto.GetMountInfoResponse;
import io.curvine.proto.GetMountTableResponse;
import io.curvine.proto.MountInfoProto;
import io.curvine.proto.TtlActionProto;
import io.curvine.proto.WriteTypeProto;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class CurvineMountClientTest {

    @Test
    public void parsesPresentAndAbsentMountInfo() throws Exception {
        MountInfoProto mount = mount("/data", "oss://bucket/data", 7);
        Optional<MountInfoProto> present = CurvineMountClient.parseMountInfo(
                GetMountInfoResponse.newBuilder().setMountInfo(mount).build().toByteArray());
        Optional<MountInfoProto> absent = CurvineMountClient.parseMountInfo(
                GetMountInfoResponse.getDefaultInstance().toByteArray());

        Assert.assertTrue(present.isPresent());
        Assert.assertEquals(mount, present.get());
        Assert.assertFalse(absent.isPresent());
    }

    @Test
    public void parsesMountTableInServerOrder() throws Exception {
        MountInfoProto first = mount("/a", "oss://bucket/a", 1);
        MountInfoProto second = mount("/b", "oss://bucket/b", 2);
        List<MountInfoProto> mounts = CurvineMountClient.parseMountTable(
                GetMountTableResponse.newBuilder()
                        .addMountTable(first)
                        .addMountTable(second)
                        .build()
                        .toByteArray());

        Assert.assertEquals(2, mounts.size());
        Assert.assertEquals(first, mounts.get(0));
        Assert.assertEquals(second, mounts.get(1));
    }

    private static MountInfoProto mount(String cvPath, String ufsPath, int mountId) {
        return MountInfoProto.newBuilder()
                .setCvPath(cvPath)
                .setUfsPath(ufsPath)
                .setMountId(mountId)
                .setTtlMs(60_000L)
                .setTtlAction(TtlActionProto.TTL_ACTION_PROTO_DELETE)
                .setReadVerifyUfs(false)
                .setWriteType(WriteTypeProto.WRITE_TYPE_PROTO_CACHE_MODE)
                .build();
    }
}

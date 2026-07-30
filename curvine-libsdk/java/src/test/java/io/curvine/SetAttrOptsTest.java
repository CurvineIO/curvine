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

import io.curvine.proto.SetAttrOptsProto;
import io.curvine.proto.TtlActionProto;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class SetAttrOptsTest {

    @Test
    public void builderEncodesAllSetAttrOptions() throws Exception {
        SetAttrOpts opts = SetAttrOpts.builder()
                .recursive(true)
                .replicas(3)
                .owner("alice")
                .group("staff")
                .mode(0755)
                .atime(1_700_000_000_000L)
                .mtime(1_700_000_001_000L)
                .ttlMs(60_000L)
                .ttlAction(TtlActionProto.TTL_ACTION_PROTO_DELETE)
                .addXAttr("attr1", "value1")
                .removeXAttr("attr2")
                .ufsMtime(1_700_000_002_000L)
                .build();

        SetAttrOptsProto proto = SetAttrOptsProto.parseFrom(opts.toByteArray());
        Assert.assertTrue(proto.getRecursive());
        Assert.assertEquals(3, proto.getReplicas());
        Assert.assertEquals("alice", proto.getOwner());
        Assert.assertEquals("staff", proto.getGroup());
        Assert.assertEquals(0755, proto.getMode());
        Assert.assertEquals(1_700_000_000_000L, proto.getAtime());
        Assert.assertEquals(1_700_000_001_000L, proto.getMtime());
        Assert.assertEquals(60_000L, proto.getTtlMs());
        Assert.assertEquals(TtlActionProto.TTL_ACTION_PROTO_DELETE, proto.getTtlAction());
        Assert.assertArrayEquals(
                "value1".getBytes(StandardCharsets.UTF_8),
                proto.getAddXAttrOrThrow("attr1").toByteArray());
        Assert.assertEquals("attr2", proto.getRemoveXAttr(0));
        Assert.assertEquals(1_700_000_002_000L, proto.getUfsMtime());
    }

    @Test
    public void builderDefaultsToNonRecursive() {
        SetAttrOpts opts = SetAttrOpts.builder().owner("bob").build();
        Assert.assertFalse(opts.toProto().getRecursive());
        Assert.assertEquals("bob", opts.toProto().getOwner());
        Assert.assertFalse(opts.toProto().hasGroup());
    }
}

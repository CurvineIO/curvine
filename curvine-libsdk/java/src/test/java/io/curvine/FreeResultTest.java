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

import io.curvine.proto.FreeResultProto;
import org.junit.Assert;
import org.junit.Test;

public class FreeResultTest {
    @Test
    public void fromProtoPreservesCounts() {
        FreeResult result = FreeResult.fromProto(FreeResultProto.newBuilder()
                .setInodes(7)
                .setBytes(1024)
                .build());

        Assert.assertEquals(7, result.getInodes());
        Assert.assertEquals(1024, result.getBytes());
    }
}

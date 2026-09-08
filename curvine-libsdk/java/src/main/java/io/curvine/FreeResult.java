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

import java.util.Objects;

/** Result of releasing Curvine file data and metadata. */
public final class FreeResult {
    private final long inodes;
    private final long bytes;

    private FreeResult(long inodes, long bytes) {
        this.inodes = inodes;
        this.bytes = bytes;
    }

    public static FreeResult fromProto(FreeResultProto proto) {
        Objects.requireNonNull(proto, "proto");
        return new FreeResult(proto.getInodes(), proto.getBytes());
    }

    public long getInodes() {
        return inodes;
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "FreeResult{inodes=" + inodes + ", bytes=" + bytes + '}';
    }
}

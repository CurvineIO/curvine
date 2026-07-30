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

import com.google.protobuf.ByteString;
import io.curvine.proto.SetAttrOptsProto;
import io.curvine.proto.TtlActionProto;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable options for Curvine {@code set_attr}. */
public final class SetAttrOpts {
    private final SetAttrOptsProto proto;

    private SetAttrOpts(SetAttrOptsProto proto) {
        this.proto = proto;
    }

    public static Builder builder() {
        return new Builder();
    }

    public SetAttrOptsProto toProto() {
        return proto;
    }

    public byte[] toByteArray() {
        return proto.toByteArray();
    }

    public static final class Builder {
        private boolean recursive;
        private Integer replicas;
        private String owner;
        private String group;
        private Integer mode;
        private Long atime;
        private Long mtime;
        private Long ttlMs;
        private TtlActionProto ttlAction;
        private final Map<String, byte[]> addXAttr = new LinkedHashMap<>();
        private final java.util.List<String> removeXAttr = new java.util.ArrayList<>();
        private Long ufsMtime;

        private Builder() {
        }

        public Builder recursive(boolean recursive) {
            this.recursive = recursive;
            return this;
        }

        public Builder replicas(int replicas) {
            this.replicas = replicas;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = requireText(owner, "owner");
            return this;
        }

        public Builder group(String group) {
            this.group = requireText(group, "group");
            return this;
        }

        public Builder mode(int mode) {
            this.mode = mode;
            return this;
        }

        public Builder atime(long atime) {
            this.atime = atime;
            return this;
        }

        public Builder mtime(long mtime) {
            this.mtime = mtime;
            return this;
        }

        public Builder ttlMs(long ttlMs) {
            this.ttlMs = ttlMs;
            return this;
        }

        public Builder ttlAction(TtlActionProto ttlAction) {
            this.ttlAction = Objects.requireNonNull(ttlAction, "ttlAction");
            return this;
        }

        public Builder addXAttr(String key, byte[] value) {
            addXAttr.put(requireText(key, "xattr key"), Objects.requireNonNull(value, "value"));
            return this;
        }

        public Builder addXAttr(String key, String value) {
            return addXAttr(key, value.getBytes(StandardCharsets.UTF_8));
        }

        public Builder removeXAttr(String key) {
            removeXAttr.add(requireText(key, "xattr key"));
            return this;
        }

        public Builder ufsMtime(long ufsMtime) {
            this.ufsMtime = ufsMtime;
            return this;
        }

        public SetAttrOpts build() {
            SetAttrOptsProto.Builder options = SetAttrOptsProto.newBuilder()
                    .setRecursive(recursive)
                    .putAllAddXAttr(toByteStringMap(addXAttr))
                    .addAllRemoveXAttr(removeXAttr);
            if (replicas != null) {
                options.setReplicas(replicas);
            }
            if (owner != null) {
                options.setOwner(owner);
            }
            if (group != null) {
                options.setGroup(group);
            }
            if (mode != null) {
                options.setMode(mode);
            }
            if (atime != null) {
                options.setAtime(atime);
            }
            if (mtime != null) {
                options.setMtime(mtime);
            }
            if (ttlMs != null) {
                options.setTtlMs(ttlMs);
            }
            if (ttlAction != null) {
                options.setTtlAction(ttlAction);
            }
            if (ufsMtime != null) {
                options.setUfsMtime(ufsMtime);
            }
            return new SetAttrOpts(options.build());
        }

        private static Map<String, ByteString> toByteStringMap(Map<String, byte[]> values) {
            Map<String, ByteString> encoded = new LinkedHashMap<>();
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                encoded.put(entry.getKey(), ByteString.copyFrom(entry.getValue()));
            }
            return encoded;
        }

        private static String requireText(String value, String field) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(field + " cannot be empty");
            }
            return value.trim();
        }
    }
}

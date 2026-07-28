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
import io.curvine.proto.MountOptionsProto;
import io.curvine.proto.ProviderProto;
import io.curvine.proto.StorageTypeProto;
import io.curvine.proto.TtlActionProto;
import io.curvine.proto.WriteTypeProto;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable request for creating or updating a Curvine UFS mount. */
public final class MountRequest {
    private static final long DEFAULT_TTL_MS = 7L * 24L * 60L * 60L * 1000L;

    private final String ufsPath;
    private final String cvPath;
    private final MountOptionsProto options;

    private MountRequest(String ufsPath, String cvPath, MountOptionsProto options) {
        this.ufsPath = ufsPath;
        this.cvPath = cvPath;
        this.options = options;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getUfsPath() {
        return ufsPath;
    }

    public String getCvPath() {
        return cvPath;
    }

    public MountOptionsProto getOptions() {
        return options;
    }

    public static final class Builder {
        private String ufsPath;
        private String cvPath;
        private boolean update;
        private final Map<String, String> addProperties = new LinkedHashMap<>();
        private Long ttlMs;
        private TtlActionProto ttlAction;
        private boolean readVerifyUfs;
        private StorageTypeProto storageType;
        private Long blockSize;
        private Integer replicas;
        private final java.util.List<String> removeProperties = new java.util.ArrayList<>();
        private WriteTypeProto writeType = WriteTypeProto.WRITE_TYPE_PROTO_CACHE_MODE;
        private ProviderProto provider;
        private Boolean autoCache;
        private AccessModeProto accessMode;

        private Builder() {
        }

        public Builder ufsPath(String ufsPath) {
            this.ufsPath = ufsPath;
            return this;
        }

        public Builder cvPath(String cvPath) {
            this.cvPath = cvPath;
            return this;
        }

        /**
         * Select replacement-style update semantics for an existing mount.
         *
         * <p>All options in this request, including defaults for options not explicitly set,
         * are sent to the master and can replace existing mount settings. Include each setting's
         * current value in the request when it must be preserved.
         */
        public Builder update(boolean update) {
            this.update = update;
            return this;
        }

        public Builder addProperty(String key, String value) {
            addProperties.put(requireText(key, "property key"), Objects.requireNonNull(value, "value"));
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

        public Builder readVerifyUfs(boolean readVerifyUfs) {
            this.readVerifyUfs = readVerifyUfs;
            return this;
        }

        public Builder storageType(StorageTypeProto storageType) {
            this.storageType = Objects.requireNonNull(storageType, "storageType");
            return this;
        }

        public Builder blockSize(long blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder replicas(int replicas) {
            this.replicas = replicas;
            return this;
        }

        public Builder removeProperty(String key) {
            removeProperties.add(requireText(key, "property key"));
            return this;
        }

        public Builder writeType(WriteTypeProto writeType) {
            this.writeType = Objects.requireNonNull(writeType, "writeType");
            return this;
        }

        public Builder provider(ProviderProto provider) {
            this.provider = Objects.requireNonNull(provider, "provider");
            return this;
        }

        public Builder autoCache(boolean autoCache) {
            this.autoCache = autoCache;
            return this;
        }

        public Builder accessMode(AccessModeProto accessMode) {
            this.accessMode = Objects.requireNonNull(accessMode, "accessMode");
            return this;
        }

        public MountRequest build() {
            String checkedUfsPath = requireText(ufsPath, "ufsPath");
            String checkedCvPath = requireText(cvPath, "cvPath");
            MountOptionsProto.Builder options = MountOptionsProto.newBuilder()
                    .setUpdate(update)
                    .putAllAddProperties(addProperties)
                    .setTtlMs(ttlMs == null ? DEFAULT_TTL_MS : ttlMs)
                    .setTtlAction(ttlAction == null ? defaultTtlAction(writeType) : ttlAction)
                    .setReadVerifyUfs(readVerifyUfs)
                    .addAllRemoveProperties(removeProperties)
                    .setWriteType(writeType)
                    .setAutoCache(autoCache == null || autoCache)
                    .setAccessMode(accessMode == null
                            ? AccessModeProto.ACCESS_MODE_PROTO_READ_ONLY : accessMode);
            if (storageType != null) {
                options.setStorageType(storageType);
            }
            if (blockSize != null) {
                options.setBlockSize(blockSize);
            }
            if (replicas != null) {
                options.setReplicas(replicas);
            }
            if (provider != null) {
                options.setProvider(provider);
            }
            return new MountRequest(checkedUfsPath, checkedCvPath, options.build());
        }

        private static TtlActionProto defaultTtlAction(WriteTypeProto writeType) {
            return writeType == WriteTypeProto.WRITE_TYPE_PROTO_FS_MODE
                    ? TtlActionProto.TTL_ACTION_PROTO_FREE
                    : TtlActionProto.TTL_ACTION_PROTO_DELETE;
        }

        private static String requireText(String value, String field) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(field + " cannot be empty");
            }
            return value.trim();
        }
    }
}

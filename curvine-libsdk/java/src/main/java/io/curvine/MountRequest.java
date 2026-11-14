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
        private Boolean writeCache;

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
         * Select update semantics for an existing mount.
         *
         * <p>Only the options set on this builder are sent; every option left unset keeps the
         * mount's stored value, so an update that touches one setting no longer resets the others
         * to this builder's defaults. An update also never derives {@link #ttlAction} from
         * {@link #writeType}: the master keeps the stored write type, so the stored action is the
         * only one consistent with it. Set the option explicitly to change it.
         *
         * <p>Two options are exceptions to that preservation contract:
         *
         * <ul>
         *   <li>{@link #readVerifyUfs(boolean)} is a required proto field, so an update always
         *       sends it and the master overwrites the stored value; an update that leaves it
         *       unset sends {@code false} and disables UFS verification on the mount. Pass the
         *       mount's current value when it must be preserved; #2104 tracks making the field
         *       optional.
         *   <li>{@link #writeType(WriteTypeProto)} is also always sent, but the master keeps the
         *       stored write type, so setting it on an update has no effect.
         * </ul>
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

        public Builder writeCache(boolean writeCache) {
            this.writeCache = writeCache;
            return this;
        }

        public MountRequest build() {
            String checkedUfsPath = requireText(ufsPath, "ufsPath");
            String checkedCvPath = requireText(cvPath, "cvPath");
            return new MountRequest(checkedUfsPath, checkedCvPath, buildOptions());
        }

        private MountOptionsProto buildOptions() {
            MountOptionsProto.Builder options = MountOptionsProto.newBuilder()
                    .setUpdate(update)
                    .putAllAddProperties(addProperties)
                    // `read_verify_ufs` and `write_type` are required proto fields, so they are sent
                    // on both paths. The master keeps the stored write type on update and ignores
                    // the requested one; `read_verify_ufs` needs the presence-aware proto change
                    // tracked by #2104 before an update can stop sending it.
                    .setReadVerifyUfs(readVerifyUfs)
                    .setWriteType(writeType)
                    .addAllRemoveProperties(removeProperties);

            if (update) {
                // An update only sends the options the caller named; `MountInfo::merge_with`
                // keeps every absent optional field, so an unnamed setting keeps its stored value.
                if (ttlMs != null) {
                    options.setTtlMs(ttlMs);
                }
                if (ttlAction != null) {
                    options.setTtlAction(ttlAction);
                }
                if (autoCache != null) {
                    options.setAutoCache(autoCache);
                }
                if (accessMode != null) {
                    options.setAccessMode(accessMode);
                }
            } else {
                // A create still sends the mount defaults so a new mount matches `cv mount`.
                options.setTtlMs(ttlMs == null ? DEFAULT_TTL_MS : ttlMs)
                        .setTtlAction(ttlAction == null ? defaultTtlAction(writeType) : ttlAction)
                        .setAutoCache(autoCache == null || autoCache)
                        .setAccessMode(accessMode == null
                                ? AccessModeProto.ACCESS_MODE_PROTO_READ_ONLY : accessMode);
            }

            if (writeCache != null) {
                options.setWriteCache(writeCache);
            }
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
            return options.build();
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

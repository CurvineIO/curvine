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

/**
 * Request to submit a UFS-to-Curvine load job.
 *
 * <p>Aligned with CLI {@code cv load} / Rust {@code LoadJobCommand} minimal fields.
 */
public final class LoadJobRequest {
    private final String sourcePath;
    private final String targetPath;
    private final boolean overwrite;

    private LoadJobRequest(Builder builder) {
        this.sourcePath = builder.sourcePath;
        this.targetPath = builder.targetPath;
        this.overwrite = builder.overwrite;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    /** Optional explicit CV target path; may be {@code null}. */
    public String getTargetPath() {
        return targetPath;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String sourcePath;
        private String targetPath;
        private boolean overwrite = true;

        private Builder() {
        }

        public Builder sourcePath(String sourcePath) {
            this.sourcePath = sourcePath;
            return this;
        }

        public Builder targetPath(String targetPath) {
            this.targetPath = targetPath;
            return this;
        }

        public Builder overwrite(boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }

        public LoadJobRequest build() {
            if (sourcePath == null || sourcePath.trim().isEmpty()) {
                throw new IllegalArgumentException("sourcePath cannot be empty");
            }
            if (targetPath != null && targetPath.trim().isEmpty()) {
                throw new IllegalArgumentException("targetPath cannot be empty when set");
            }
            return new LoadJobRequest(this);
        }
    }
}

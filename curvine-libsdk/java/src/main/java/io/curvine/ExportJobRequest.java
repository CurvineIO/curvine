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

/** Request to submit a Curvine-to-UFS export job. */
public final class ExportJobRequest {
    private final String sourcePath;
    private final boolean overwrite;

    private ExportJobRequest(Builder builder) {
        this.sourcePath = builder.sourcePath;
        this.overwrite = builder.overwrite;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String sourcePath;
        private boolean overwrite = true;

        private Builder() {
        }

        public Builder sourcePath(String sourcePath) {
            this.sourcePath = sourcePath;
            return this;
        }

        public Builder overwrite(boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }

        public ExportJobRequest build() {
            if (sourcePath == null || sourcePath.trim().isEmpty()) {
                throw new IllegalArgumentException("sourcePath cannot be empty");
            }
            return new ExportJobRequest(this);
        }
    }
}

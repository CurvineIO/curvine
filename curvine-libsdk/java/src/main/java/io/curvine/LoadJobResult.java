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

import io.curvine.proto.JobTaskStateProto;
import io.curvine.proto.SubmitJobResponse;

/**
 * Outcome of submitting a load job. Mirrors Rust {@code LoadJobResult}.
 */
public final class LoadJobResult {
    private final String jobId;
    private final String targetPath;
    private final JobTaskStateProto state;

    public LoadJobResult(String jobId, String targetPath, JobTaskStateProto state) {
        this.jobId = jobId;
        this.targetPath = targetPath;
        this.state = state;
    }

    public static LoadJobResult fromProto(SubmitJobResponse response) {
        return new LoadJobResult(
                response.getJobId(),
                response.getTargetPath(),
                response.getState());
    }

    public String getJobId() {
        return jobId;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public JobTaskStateProto getState() {
        return state;
    }

    @Override
    public String toString() {
        return "LoadJobResult{jobId='" + jobId + "', targetPath='" + targetPath
                + "', state=" + state + '}';
    }
}

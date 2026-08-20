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

import io.curvine.proto.GetJobStatusResponse;
import io.curvine.proto.JobTaskProgressProto;
import io.curvine.proto.JobTaskStateProto;

/**
 * Load job status snapshot. Mirrors Rust {@code JobStatus}.
 */
public final class LoadJobStatus {
    private final String jobId;
    private final JobTaskStateProto state;
    private final int stateValue;
    private final String sourcePath;
    private final String targetPath;
    private final JobTaskProgressProto progress;

    public LoadJobStatus(
            String jobId,
            JobTaskStateProto state,
            String sourcePath,
            String targetPath,
            JobTaskProgressProto progress) {
        this(
                jobId,
                state,
                state == null ? 0 : state.getNumber(),
                sourcePath,
                targetPath,
                progress);
    }

    private LoadJobStatus(
            String jobId,
            JobTaskStateProto state,
            int stateValue,
            String sourcePath,
            String targetPath,
            JobTaskProgressProto progress) {
        this.jobId = jobId;
        this.state = state;
        this.stateValue = stateValue;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.progress = progress;
    }

    public static LoadJobStatus fromProto(GetJobStatusResponse response) {
        return new LoadJobStatus(
                response.getJobId(),
                response.getState(),
                response.getState().getNumber(),
                response.getSourcePath(),
                response.getTargetPath(),
                response.getProgress());
    }

    public String getJobId() {
        return jobId;
    }

    public JobTaskStateProto getState() {
        return state;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public JobTaskProgressProto getProgress() {
        return progress;
    }

    /**
     * True when the job is no longer running (completed, failed, canceled, partial success,
     * or any future terminal enum value / {@code UNRECOGNIZED} wire value).
     */
    public boolean isFinished() {
        switch (state) {
            case PENDING:
            case LOADING:
            case UNKNOWN:
                return false;
            default:
                return true;
        }
    }

    public boolean isSuccessful() {
        return state == JobTaskStateProto.COMPLETED;
    }

    /**
     * True when some files/tasks succeeded and others failed.
     * Callers should inspect progress message / retry failed items as needed.
     */
    public boolean isPartialSuccess() {
        return state == JobTaskStateProto.PARTIAL_SUCCESS
                || stateValue == JobTaskStateProto.PARTIAL_SUCCESS.getNumber();
    }

    @Override
    public String toString() {
        return "LoadJobStatus{jobId='" + jobId + "', state=" + state
                + ", sourcePath='" + sourcePath + "', targetPath='" + targetPath + "'}";
    }
}

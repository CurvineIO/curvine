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
import io.curvine.proto.SubmitJobResponse;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

public class LoadJobClientTest {

    @Test
    public void requestBuilderRequiresSource() {
        try {
            LoadJobRequest.builder().build();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("sourcePath"));
        }
    }

    @Test
    public void requestBuilderRejectsEmptyTarget() {
        try {
            LoadJobRequest.builder()
                    .sourcePath("s3://bucket/a")
                    .targetPath("   ")
                    .build();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("targetPath"));
        }
    }

    @Test
    public void requestBuilderDefaultsOverwriteTrue() {
        LoadJobRequest request = LoadJobRequest.builder()
                .sourcePath("s3://bucket/a")
                .targetPath("/mnt/a")
                .build();
        Assert.assertEquals("s3://bucket/a", request.getSourcePath());
        Assert.assertEquals("/mnt/a", request.getTargetPath());
        Assert.assertTrue(request.isOverwrite());
    }

    @Test
    public void loadJobResultFromProto() {
        SubmitJobResponse response = SubmitJobResponse.newBuilder()
                .setJobId("job-1")
                .setTargetPath("/mnt/a")
                .setState(JobTaskStateProto.PENDING)
                .build();
        LoadJobResult result = LoadJobResult.fromProto(response);
        Assert.assertEquals("job-1", result.getJobId());
        Assert.assertEquals("/mnt/a", result.getTargetPath());
        Assert.assertEquals(JobTaskStateProto.PENDING, result.getState());
    }

    @Test
    public void loadJobStatusFinishedHelpers() {
        LoadJobStatus completed = LoadJobStatus.fromProto(GetJobStatusResponse.newBuilder()
                .setJobId("job-1")
                .setState(JobTaskStateProto.COMPLETED)
                .setSourcePath("s3://bucket/a")
                .setTargetPath("/mnt/a")
                .setProgress(JobTaskProgressProto.newBuilder()
                        .setLoadedSize(10)
                        .setTotalSize(10)
                        .setUpdateTime(1)
                        .setState(JobTaskStateProto.COMPLETED.getNumber())
                        .setMessage("ok")
                        .build())
                .build());
        Assert.assertTrue(completed.isFinished());
        Assert.assertTrue(completed.isSuccessful());

        LoadJobStatus failed = LoadJobStatus.fromProto(GetJobStatusResponse.newBuilder()
                .setJobId("job-2")
                .setState(JobTaskStateProto.FAILED)
                .setSourcePath("s3://bucket/b")
                .setTargetPath("/mnt/b")
                .setProgress(JobTaskProgressProto.newBuilder()
                        .setLoadedSize(0)
                        .setTotalSize(0)
                        .setUpdateTime(1)
                        .setState(JobTaskStateProto.FAILED.getNumber())
                        .setMessage("boom")
                        .build())
                .build());
        Assert.assertTrue(failed.isFinished());
        Assert.assertFalse(failed.isSuccessful());
    }

    @Test
    public void saturatingDeadlineDoesNotWrap() {
        long deadline = CurvineLoadClient.saturatingDeadlineNanos(Duration.ofNanos(Long.MAX_VALUE));
        Assert.assertEquals(Long.MAX_VALUE, deadline);
    }

    @Test
    public void sleepNanosAcceptsSubMillisInterval() throws InterruptedException {
        long start = System.nanoTime();
        CurvineLoadClient.sleepNanos(500_000L); // 0.5ms
        Assert.assertTrue(System.nanoTime() - start >= 500_000L);
    }
}

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

import com.google.protobuf.CodedOutputStream;
import io.curvine.proto.GetJobStatusResponse;
import io.curvine.proto.JobTaskProgressProto;
import io.curvine.proto.JobTaskStateProto;
import io.curvine.proto.SubmitJobResponse;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        Assert.assertFalse(failed.isPartialSuccess());

        LoadJobStatus partial = LoadJobStatus.fromProto(GetJobStatusResponse.newBuilder()
                .setJobId("job-3")
                .setState(JobTaskStateProto.PARTIAL_SUCCESS)
                .setSourcePath("s3://bucket/c")
                .setTargetPath("/mnt/c")
                .setProgress(JobTaskProgressProto.newBuilder()
                        .setLoadedSize(5)
                        .setTotalSize(10)
                        .setUpdateTime(1)
                        .setState(JobTaskStateProto.PARTIAL_SUCCESS.getNumber())
                        .setMessage("partial")
                        .build())
                .build());
        Assert.assertTrue(partial.isFinished());
        Assert.assertFalse(partial.isSuccessful());
        Assert.assertTrue(partial.isPartialSuccess());

        LoadJobStatus running = LoadJobStatus.fromProto(GetJobStatusResponse.newBuilder()
                .setJobId("job-4")
                .setState(JobTaskStateProto.LOADING)
                .setSourcePath("s3://bucket/d")
                .setTargetPath("/mnt/d")
                .setProgress(JobTaskProgressProto.newBuilder()
                        .setLoadedSize(1)
                        .setTotalSize(10)
                        .setUpdateTime(1)
                        .setState(JobTaskStateProto.LOADING.getNumber())
                        .setMessage("running")
                        .build())
                .build());
        Assert.assertFalse(running.isFinished());

        LoadJobStatus unknownState = LoadJobStatus.fromProto(GetJobStatusResponse.newBuilder()
                .setJobId("job-5")
                .setState(JobTaskStateProto.UNKNOWN)
                .setSourcePath("s3://bucket/e")
                .setTargetPath("/mnt/e")
                .setProgress(JobTaskProgressProto.newBuilder()
                        .setLoadedSize(1)
                        .setTotalSize(10)
                        .setUpdateTime(1)
                        .setState(JobTaskStateProto.UNKNOWN.getNumber())
                        .setMessage("unknown-state")
                        .build())
                .build());
        Assert.assertFalse(unknownState.isFinished());
        Assert.assertFalse(unknownState.isSuccessful());
        Assert.assertFalse(unknownState.isPartialSuccess());
    }

    @Test
    public void loadJobStatusHandlesUnknownWireEnumValue() throws Exception {
        JobTaskProgressProto progress = JobTaskProgressProto.newBuilder()
                .setLoadedSize(1)
                .setTotalSize(10)
                .setUpdateTime(1)
                .setState(99)
                .setMessage("future-state")
                .build();
        byte[] bytes = encodeGetJobStatusResponse(99, progress);
        GetJobStatusResponse response = GetJobStatusResponse.PARSER.parsePartialFrom(bytes);
        LoadJobStatus status = LoadJobStatus.fromProto(response);

        Assert.assertEquals(JobTaskStateProto.UNKNOWN, status.getState());
        Assert.assertFalse(status.isFinished());
        Assert.assertFalse(status.isSuccessful());
        Assert.assertFalse(status.isPartialSuccess());
    }

    private static byte[] encodeGetJobStatusResponse(
            int stateWireValue, JobTaskProgressProto progress) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeString(1, "job-unknown-wire");
        out.writeEnum(2, stateWireValue);
        out.writeString(3, "s3://bucket/future");
        out.writeString(4, "/mnt/future");
        out.writeMessage(5, progress);
        out.flush();
        return baos.toByteArray();
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

    @Test
    public void filesystemConfSerializesTransferRouting() throws IllegalAccessException {
        Configuration conf = new Configuration(false);
        conf.set("fs.cv.master_addrs", "master-0:8995");
        conf.set("fs.cv.transfer.enabled", "true");
        conf.set("fs.cv.transfer.endpoints", "transfer-0:9010, transfer-1:9010");
        conf.set("fs.cv.transfer.client_pending_queue_size", "2048");
        conf.set("fs.cv.transfer.client_submit_concurrency", "128");

        String toml = new FilesystemConf(conf).toToml();

        Assert.assertTrue(toml.contains("[transfer]"));
        Assert.assertTrue(toml.contains("enabled = true"));
        Assert.assertTrue(toml.contains("endpoints = [\"transfer-0:9010\", \"transfer-1:9010\"]"));
        Assert.assertTrue(toml.contains("client_pending_queue_size = 2048"));
        Assert.assertTrue(toml.contains("client_submit_concurrency = 128"));
    }
}

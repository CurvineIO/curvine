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

import io.curvine.exception.CurvineException;
import io.curvine.proto.GetJobStatusResponse;
import io.curvine.proto.SubmitJobResponse;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Independent Java client for Curvine load jobs.
 *
 * <p>Mirrors Rust libsdk {@code JobClient}: submit / get status / cancel, plus a
 * Java-side wait helper with explicit timeout and poll interval.
 *
 * <pre>{@code
 * try (CurvineLoadClient client = CurvineLoadClient.from(conf)) {
 *     LoadJobResult result = client.submitLoad(
 *             LoadJobRequest.builder()
 *                     .sourcePath("oss://bucket/path/file")
 *                     .targetPath("/mnt/path/file")
 *                     .overwrite(true)
 *                     .build());
 *     LoadJobStatus status = client.waitJobComplete(
 *             result.getJobId(), Duration.ofMinutes(10), Duration.ofSeconds(1));
 * }
 * }</pre>
 *
 * <p>Load remains UFS-to-Curvine and must stay inside an existing mount mapping;
 * this client does not implement arbitrary URI copy.
 */
public final class CurvineLoadClient implements Closeable {
    private static final long SUCCESS = 0;

    private final long nativeHandle;
    private volatile boolean closed;

    private CurvineLoadClient(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public static CurvineLoadClient from(FilesystemConf conf) throws IOException {
        Objects.requireNonNull(conf, "conf");
        try {
            long handle = CurvineNative.newFilesystem(conf.toToml());
            if (handle < SUCCESS) {
                throw CurvineException.create((int) handle, "failed to create CurvineLoadClient");
            }
            return new CurvineLoadClient(handle);
        } catch (IllegalAccessException e) {
            throw new IOException("failed to serialize FilesystemConf", e);
        }
    }

    public static CurvineLoadClient from(Configuration conf) throws IOException {
        try {
            return from(new FilesystemConf(conf));
        } catch (IllegalAccessException e) {
            throw new IOException("failed to build FilesystemConf", e);
        }
    }

    /**
     * Submit a load job. Equivalent to Rust {@code JobClient::submit_load_job}.
     */
    public LoadJobResult submitLoad(LoadJobRequest request) throws IOException {
        ensureOpen();
        Objects.requireNonNull(request, "request");
        byte[] bytes = CurvineNative.submitLoadJob(
                nativeHandle,
                request.getSourcePath(),
                request.getTargetPath(),
                request.isOverwrite());
        checkBytes(bytes);
        return LoadJobResult.fromProto(SubmitJobResponse.parseFrom(bytes));
    }

    /**
     * Query job status by id. Equivalent to Rust {@code JobClient::get_status}.
     */
    public LoadJobStatus getJobStatus(String jobId) throws IOException {
        ensureOpen();
        requireJobId(jobId);
        byte[] bytes = CurvineNative.getJobStatus(nativeHandle, jobId);
        checkBytes(bytes);
        return LoadJobStatus.fromProto(GetJobStatusResponse.parseFrom(bytes));
    }

    /**
     * Cancel a job by id. Equivalent to Rust {@code JobClient::cancel}.
     */
    public void cancelJob(String jobId) throws IOException {
        ensureOpen();
        requireJobId(jobId);
        long errno = CurvineNative.cancelJob(nativeHandle, jobId);
        if (errno < SUCCESS) {
            throw CurvineException.create((int) errno, "cancelJob failed: " + jobId);
        }
    }

    /**
     * Poll job status until finished or timeout.
     *
     * @param jobId        job id from {@link #submitLoad}
     * @param timeout      max wait duration
     * @param pollInterval sleep between status queries
     * @return final status when the job reaches a terminal state
     * @throws TimeoutException if the job is still running after {@code timeout}
     * @throws IOException      if the job fails / is canceled, or RPC fails
     */
    public LoadJobStatus waitJobComplete(String jobId, Duration timeout, Duration pollInterval)
            throws IOException, TimeoutException, InterruptedException {
        ensureOpen();
        requireJobId(jobId);
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(pollInterval, "pollInterval");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }

        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        LoadJobStatus status;
        while (true) {
            status = getJobStatus(jobId);
            if (status.isFinished()) {
                if (status.isSuccessful()) {
                    return status;
                }
                throw new IOException(
                        "load job " + jobId + " ended with state " + status.getState()
                                + (status.getProgress() != null
                                ? ": " + status.getProgress().getMessage()
                                : ""));
            }
            if (System.nanoTime() >= deadlineNanos) {
                throw new TimeoutException(
                        "load job " + jobId + " not complete after " + timeout
                                + ", last state=" + status.getState());
            }
            long sleepMs = Math.min(
                    pollInterval.toMillis(),
                    Math.max(1L, (deadlineNanos - System.nanoTime()) / 1_000_000L));
            Thread.sleep(sleepMs);
        }
    }

    /**
     * Convenience wait with 1s poll interval.
     */
    public LoadJobStatus waitJobComplete(String jobId, Duration timeout)
            throws IOException, TimeoutException, InterruptedException {
        return waitJobComplete(jobId, timeout, Duration.ofSeconds(1));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        long errno = CurvineNative.closeFilesystem(nativeHandle);
        if (errno < SUCCESS) {
            throw CurvineException.create((int) errno, "close CurvineLoadClient failed");
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("CurvineLoadClient is closed");
        }
    }

    private static void requireJobId(String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId cannot be empty");
        }
    }

    private static void checkBytes(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new CurvineException("native load job call returned null");
        }
    }
}

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

/** Java client for Curvine load and export jobs. */
public final class CurvineTransferClient implements Closeable {
    private static final long SUCCESS = 0;

    private final NativeFilesystemHandle filesystem;
    private final boolean transferEnabled;

    private CurvineTransferClient(long nativeHandle, boolean transferEnabled) {
        this.filesystem = new NativeFilesystemHandle(nativeHandle);
        this.transferEnabled = transferEnabled;
    }

    public static CurvineTransferClient from(FilesystemConf conf) throws IOException {
        Objects.requireNonNull(conf, "conf");
        try {
            long handle = CurvineNative.newFilesystem(conf.toToml());
            if (handle < SUCCESS) {
                throw CurvineException.create((int) handle, "failed to create CurvineTransferClient");
            }
            return new CurvineTransferClient(handle, conf.transfer_enabled);
        } catch (IllegalAccessException e) {
            throw new IOException("failed to serialize FilesystemConf", e);
        }
    }

    public static CurvineTransferClient from(Configuration conf) throws IOException {
        try {
            return from(new FilesystemConf(conf));
        } catch (IllegalAccessException e) {
            throw new IOException("failed to build FilesystemConf", e);
        }
    }

    /** Submit a UFS-to-Curvine load job. */
    public LoadJobResult submitLoad(LoadJobRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        return filesystem.withOpen(nativeHandle -> {
            byte[] response = request.getReplicas() == null
                    ? CurvineNative.submitLoadJob(
                            nativeHandle,
                            request.getSourcePath(),
                            request.getTargetPath(),
                            request.isOverwrite())
                    : CurvineNative.submitLoadJobWithReplicas(
                            nativeHandle,
                            request.getSourcePath(),
                            request.getTargetPath(),
                            request.isOverwrite(),
                            request.getReplicas());
            return parseSubmitResponse(response);
        });
    }

    /**
     * Submit a load and retry it when transfer routing returns an existing terminal job.
     * Equivalent to CLI {@code cv load --force}.
     */
    public LoadJobResult submitLoad(LoadJobRequest request, boolean force) throws IOException {
        checkForceAllowed(force, transferEnabled);
        LoadJobResult result = submitLoad(request);
        if (!force || !isTerminal(result.getState())) {
            return result;
        }
        String retryJobId = retryJob(result.getJobId());
        return new LoadJobResult(
                retryJobId,
                result.getTargetPath(),
                io.curvine.proto.JobTaskStateProto.PENDING);
    }

    /** Submit a Curvine-to-UFS export job. */
    public LoadJobResult submitExport(ExportJobRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        return filesystem.withOpen(nativeHandle -> parseSubmitResponse(CurvineNative.submitExportJob(
                nativeHandle,
                request.getSourcePath(),
                request.isOverwrite())));
    }

    /** Query a load or export job by id. */
    public LoadJobStatus getJobStatus(String jobId) throws IOException {
        requireJobId(jobId);
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getJobStatus(nativeHandle, jobId);
            checkBytes(bytes);
            return LoadJobStatus.fromProto(GetJobStatusResponse.parseFrom(bytes));
        });
    }

    /** Cancel a load or export job by id. */
    public void cancelJob(String jobId) throws IOException {
        requireJobId(jobId);
        filesystem.withOpen(nativeHandle -> {
            long errno = CurvineNative.cancelJob(nativeHandle, jobId);
            if (errno < SUCCESS) {
                throw CurvineException.create((int) errno, "cancelJob failed: " + jobId);
            }
            return null;
        });
    }

    /** Retry a failed, partial-success, or canceled transfer job. */
    public String retryJob(String jobId) throws IOException {
        requireJobId(jobId);
        return filesystem.withOpen(nativeHandle -> {
            String retryJobId = CurvineNative.retryJob(nativeHandle, jobId);
            if (retryJobId == null || retryJobId.trim().isEmpty()) {
                throw new CurvineException("native retry job call returned an empty job id");
            }
            return retryJobId;
        });
    }

    /** Poll a job until it reaches a terminal state or the timeout expires. */
    public LoadJobStatus waitJobComplete(String jobId, Duration timeout, Duration pollInterval)
            throws IOException, TimeoutException, InterruptedException {
        requireJobId(jobId);
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(pollInterval, "pollInterval");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }

        long deadlineNanos = saturatingDeadlineNanos(timeout);
        LoadJobStatus status;
        while (true) {
            status = getJobStatus(jobId);
            if (status.isFinished()) {
                if (status.isSuccessful() || status.isPartialSuccess()) {
                    return status;
                }
                throw new IOException(
                        "job " + jobId + " ended with state " + status.getState()
                                + (status.getProgress() != null
                                ? ": " + status.getProgress().getMessage()
                                : ""));
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new TimeoutException(
                        "job " + jobId + " not complete after " + timeout
                                + ", last state=" + status.getState());
            }
            sleepNanos(Math.min(saturatingDurationNanos(pollInterval), remainingNanos));
        }
    }

    public LoadJobStatus waitJobComplete(String jobId, Duration timeout)
            throws IOException, TimeoutException, InterruptedException {
        return waitJobComplete(jobId, timeout, Duration.ofSeconds(1));
    }

    @Override
    public void close() throws IOException {
        filesystem.close("close CurvineTransferClient failed");
    }

    static long saturatingDeadlineNanos(Duration timeout) {
        try {
            return Math.addExact(System.nanoTime(), timeout.toNanos());
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    static long saturatingDurationNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    static void sleepNanos(long nanos) throws InterruptedException {
        long sleepNanos = Math.max(1L, nanos);
        long sleepMs = sleepNanos / 1_000_000L;
        int nanoPart = (int) (sleepNanos % 1_000_000L);
        Thread.sleep(sleepMs, nanoPart);
    }

    static boolean isTerminal(io.curvine.proto.JobTaskStateProto state) {
        return state != io.curvine.proto.JobTaskStateProto.PENDING
                && state != io.curvine.proto.JobTaskStateProto.LOADING
                && state != io.curvine.proto.JobTaskStateProto.UNKNOWN;
    }

    static void checkForceAllowed(boolean force, boolean transferEnabled) throws IOException {
        if (force && !transferEnabled) {
            throw new IOException("load --force requires fs.cv.transfer.enabled=true");
        }
    }

    private LoadJobResult parseSubmitResponse(byte[] bytes) throws IOException {
        checkBytes(bytes);
        return LoadJobResult.fromProto(SubmitJobResponse.parseFrom(bytes));
    }

    private static void requireJobId(String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId cannot be empty");
        }
    }

    private static void checkBytes(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new CurvineException("native transfer job call returned null");
        }
    }
}

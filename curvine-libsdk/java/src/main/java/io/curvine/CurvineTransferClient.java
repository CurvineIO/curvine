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
import io.curvine.proto.GetTransferStatusResponse;
import io.curvine.proto.SubmitTransferResponse;
import io.curvine.proto.TransferStateProto;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Java client for the Curvine transfer service, the only data-movement engine.
 *
 * <p>Every method maps onto the transfer protocol: load (UFS to Curvine) and export
 * (Curvine to UFS) only add path resolution, so responses are the raw
 * {@code SubmitTransferResponse} / {@code GetTransferStatusResponse} protobufs.
 */
public final class CurvineTransferClient implements Closeable {
    private static final long SUCCESS = 0;

    /**
     * Replica-count sentinel meaning "let the transfer service choose".
     *
     * <p>A non-positive replica count is unset on every layer: the JNI bridge maps
     * {@code replicas <= 0} to {@code None} ({@code Java_io_curvine_CurvineNative_submitLoadTransfer}),
     * and {@code TransferCommand#replicas()} ignores non-positive values, so a load falls back to
     * the mount replica setting and then to the transfer service default.
     */
    static final int DEFAULT_REPLICAS = 0;

    private final NativeFilesystemHandle filesystem;

    private CurvineTransferClient(long nativeHandle) {
        this.filesystem = new NativeFilesystemHandle(nativeHandle);
    }

    public static CurvineTransferClient from(FilesystemConf conf) throws IOException {
        Objects.requireNonNull(conf, "conf");
        try {
            long handle = CurvineNative.newFilesystem(conf.toToml());
            if (handle < SUCCESS) {
                throw CurvineException.create((int) handle, "failed to create CurvineTransferClient");
            }
            return new CurvineTransferClient(handle);
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

    /**
     * Submit a UFS-to-Curvine load, inferring the target from the mount table.
     */
    public SubmitTransferResponse submitLoad(String sourcePath) throws IOException {
        return submitLoad(sourcePath, null, true, DEFAULT_REPLICAS);
    }

    /**
     * Submit a UFS-to-Curvine load with an explicit target path.
     */
    public SubmitTransferResponse submitLoad(String sourcePath, String targetPath, boolean overwrite)
            throws IOException {
        return submitLoad(sourcePath, targetPath, overwrite, DEFAULT_REPLICAS);
    }

    /**
     * Submit a UFS-to-Curvine load.
     *
     * @param sourcePath UFS path, or a mounted CV path when the target is explicit
     * @param targetPath optional CV target; {@code null} infers it from the mount table
     * @param overwrite whether to overwrite existing target files
     * @param replicas replica count; a non-positive value means "use the transfer service
     *                 default" (mount replica setting, then the transfer service configuration)
     */
    public SubmitTransferResponse submitLoad(
            String sourcePath, String targetPath, boolean overwrite, int replicas)
            throws IOException {
        LoadRequest request = loadRequest(sourcePath, targetPath, overwrite, replicas);
        return filesystem.withOpen(nativeHandle -> parseSubmitResponse(
                CurvineNative.submitLoadTransfer(
                        nativeHandle,
                        request.sourcePath,
                        request.targetPath,
                        request.overwrite,
                        request.replicas)));
    }

    /**
     * Validates the load arguments and resolves the replica count. Pure Java on purpose: the
     * replica semantics of {@code submitLoad} are checked here, without the native library.
     */
    static LoadRequest loadRequest(
            String sourcePath, String targetPath, boolean overwrite, int replicas) {
        if (sourcePath == null || sourcePath.trim().isEmpty()) {
            throw new IllegalArgumentException("sourcePath cannot be empty");
        }
        return new LoadRequest(
                sourcePath, targetPath, overwrite, replicas > 0 ? replicas : DEFAULT_REPLICAS);
    }

    /** Validated load arguments, ready to be forwarded to the native call. */
    static final class LoadRequest {
        final String sourcePath;
        final String targetPath;
        final boolean overwrite;
        final int replicas;

        LoadRequest(String sourcePath, String targetPath, boolean overwrite, int replicas) {
            this.sourcePath = sourcePath;
            this.targetPath = targetPath;
            this.overwrite = overwrite;
            this.replicas = replicas;
        }
    }

    /**
     * Submit a Curvine-to-UFS export. Existing targets are overwritten.
     */
    public SubmitTransferResponse submitExport(String sourcePath) throws IOException {
        if (sourcePath == null || sourcePath.trim().isEmpty()) {
            throw new IllegalArgumentException("sourcePath cannot be empty");
        }
        return filesystem.withOpen(nativeHandle -> parseSubmitResponse(
                CurvineNative.submitExportTransfer(nativeHandle, sourcePath)));
    }

    /** Query a load or export transfer by id. */
    public GetTransferStatusResponse getTransferStatus(String jobId) throws IOException {
        requireJobId(jobId);
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getTransferStatus(nativeHandle, jobId);
            checkBytes(bytes, "getTransferStatus");
            return GetTransferStatusResponse.parseFrom(bytes);
        });
    }

    /** Cancel a load or export transfer by id. */
    public void cancelTransfer(String jobId) throws IOException {
        requireJobId(jobId);
        filesystem.withOpen(nativeHandle -> {
            long errno = CurvineNative.cancelTransfer(nativeHandle, jobId);
            if (errno < SUCCESS) {
                throw CurvineException.create((int) errno, "cancelTransfer failed: " + jobId);
            }
            return null;
        });
    }

    /** Retry a failed, partial-success, or canceled transfer; returns the new job id. */
    public String retryTransfer(String jobId) throws IOException {
        requireJobId(jobId);
        return filesystem.withOpen(nativeHandle -> {
            String retryJobId = CurvineNative.retryTransfer(nativeHandle, jobId);
            if (retryJobId == null || retryJobId.trim().isEmpty()) {
                throw new CurvineException("native retry transfer call returned an empty job id");
            }
            return retryJobId;
        });
    }

    /** Poll a transfer until it reaches a terminal state or the timeout expires. */
    public GetTransferStatusResponse waitTransferComplete(
            String jobId, Duration timeout, Duration pollInterval)
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
        GetTransferStatusResponse status;
        while (true) {
            status = getTransferStatus(jobId);
            if (isTerminal(status.getState())) {
                if (isSuccessful(status.getState()) || isPartialSuccess(status.getState())) {
                    return status;
                }
                throw new IOException(
                        "transfer " + jobId + " ended with state " + status.getState()
                                + (status.hasProgress()
                                ? ": " + status.getProgress().getMessage()
                                : ""));
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new TimeoutException(
                        "transfer " + jobId + " not complete after " + timeout
                                + ", last state=" + status.getState());
            }
            sleepNanos(Math.min(saturatingDurationNanos(pollInterval), remainingNanos));
        }
    }

    public GetTransferStatusResponse waitTransferComplete(String jobId, Duration timeout)
            throws IOException, TimeoutException, InterruptedException {
        return waitTransferComplete(jobId, timeout, Duration.ofSeconds(1));
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

    /** True for completed, failed, canceled, and partial-success states. */
    static boolean isTerminal(TransferStateProto state) {
        if (state == null) {
            return false;
        }
        switch (state) {
            case TRANSFER_COMPLETED:
            case TRANSFER_FAILED:
            case TRANSFER_CANCELED:
            case TRANSFER_PARTIAL_SUCCESS:
                return true;
            default:
                return false;
        }
    }

    static boolean isSuccessful(TransferStateProto state) {
        return state == TransferStateProto.TRANSFER_COMPLETED;
    }

    static boolean isPartialSuccess(TransferStateProto state) {
        return state == TransferStateProto.TRANSFER_PARTIAL_SUCCESS;
    }

    static SubmitTransferResponse parseSubmitResponse(byte[] bytes) throws IOException {
        checkBytes(bytes, "submitTransfer");
        return SubmitTransferResponse.parseFrom(bytes);
    }

    private static void requireJobId(String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId cannot be empty");
        }
    }

    private static void checkBytes(byte[] bytes, String operation) throws IOException {
        if (bytes == null) {
            throw new CurvineException("native " + operation + " call returned null");
        }
    }
}

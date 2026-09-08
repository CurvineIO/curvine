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

import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Compatibility facade for the load-only name of {@link CurvineTransferClient}.
 *
 * <p>Backend routing follows {@code transfer.enabled}. With transfer routing enabled, this
 * client uses the standalone Transfer service; otherwise it uses the legacy Master job API.
 * Use {@link CurvineTransferClient} when export or retry operations are needed.
 */
public final class CurvineLoadClient implements Closeable {
    private final CurvineTransferClient delegate;

    private CurvineLoadClient(CurvineTransferClient delegate) {
        this.delegate = delegate;
    }

    public static CurvineLoadClient from(FilesystemConf conf) throws IOException {
        return new CurvineLoadClient(CurvineTransferClient.from(conf));
    }

    public static CurvineLoadClient from(Configuration conf) throws IOException {
        return new CurvineLoadClient(CurvineTransferClient.from(conf));
    }

    /**
     * Submit a load job. Equivalent to Rust {@code JobClient::submit_load_job}.
     */
    public LoadJobResult submitLoad(LoadJobRequest request) throws IOException {
        return delegate.submitLoad(request);
    }

    public LoadJobResult submitLoad(LoadJobRequest request, boolean force) throws IOException {
        return delegate.submitLoad(request, force);
    }

    /** Query a load job by id. */
    public LoadJobStatus getJobStatus(String jobId) throws IOException {
        return delegate.getJobStatus(jobId);
    }

    /** Cancel a load job by id. */
    public void cancelJob(String jobId) throws IOException {
        delegate.cancelJob(jobId);
    }

    /** Poll a load job until it reaches a terminal state or the timeout expires. */
    public LoadJobStatus waitJobComplete(String jobId, Duration timeout, Duration pollInterval)
            throws IOException, TimeoutException, InterruptedException {
        return delegate.waitJobComplete(jobId, timeout, pollInterval);
    }

    /**
     * Convenience wait with 1s poll interval.
     */
    public LoadJobStatus waitJobComplete(String jobId, Duration timeout)
            throws IOException, TimeoutException, InterruptedException {
        return delegate.waitJobComplete(jobId, timeout);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /** Compute {@code nanoTime + timeout} without wrapping on overflow. */
    static long saturatingDeadlineNanos(Duration timeout) {
        return CurvineTransferClient.saturatingDeadlineNanos(timeout);
    }

    /** Sleep at least 1ns to avoid busy-looping when the interval truncates to 0ms. */
    static void sleepNanos(long nanos) throws InterruptedException {
        CurvineTransferClient.sleepNanos(nanos);
    }
}

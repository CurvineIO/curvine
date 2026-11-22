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

import io.curvine.proto.SubmitTransferResponse;
import io.curvine.proto.TransferStateProto;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

/**
 * Pure-Java checks for the transfer client helpers. Anything touching the native library is
 * covered by the integration suites, not here.
 */
public class TransferClientTest {

    @Test
    public void terminalStatesMatchTheTransferProtocol() {
        Assert.assertFalse(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_PENDING));
        Assert.assertFalse(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_PLANNING));
        Assert.assertFalse(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_DISPATCHING));
        Assert.assertFalse(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_RUNNING));
        Assert.assertFalse(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_CANCELING));
        Assert.assertTrue(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_COMPLETED));
        Assert.assertTrue(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_FAILED));
        Assert.assertTrue(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_CANCELED));
        Assert.assertTrue(CurvineTransferClient.isTerminal(TransferStateProto.TRANSFER_PARTIAL_SUCCESS));
    }

    @Test
    public void nullStateIsNotTerminal() {
        Assert.assertFalse(CurvineTransferClient.isTerminal(null));
        Assert.assertFalse(CurvineTransferClient.isSuccessful(null));
        Assert.assertFalse(CurvineTransferClient.isPartialSuccess(null));
    }

    @Test
    public void onlyCompletedIsSuccessful() {
        Assert.assertTrue(CurvineTransferClient.isSuccessful(TransferStateProto.TRANSFER_COMPLETED));
        Assert.assertFalse(CurvineTransferClient.isSuccessful(
                TransferStateProto.TRANSFER_PARTIAL_SUCCESS));
        Assert.assertFalse(CurvineTransferClient.isSuccessful(TransferStateProto.TRANSFER_FAILED));
        Assert.assertTrue(CurvineTransferClient.isPartialSuccess(
                TransferStateProto.TRANSFER_PARTIAL_SUCCESS));
    }

    @Test
    public void submitResponseRoundTripsThroughTheClientParser() throws Exception {
        SubmitTransferResponse response = SubmitTransferResponse.newBuilder()
                .setJobId("job-1")
                .setRunId(7L)
                .setState(TransferStateProto.TRANSFER_PENDING)
                .build();
        SubmitTransferResponse parsed =
                CurvineTransferClient.parseSubmitResponse(response.toByteArray());
        Assert.assertEquals("job-1", parsed.getJobId());
        Assert.assertEquals(7L, parsed.getRunId());
        Assert.assertEquals(TransferStateProto.TRANSFER_PENDING, parsed.getState());
    }

    @Test(expected = io.curvine.exception.CurvineException.class)
    public void nullSubmitResponseBytesAreRejected() throws Exception {
        CurvineTransferClient.parseSubmitResponse(null);
    }

    @Test
    public void singleArgumentSubmitLoadForwardsTheDefaultReplicas() {
        // Exactly what submitLoad(sourcePath) forwards.
        CurvineTransferClient.LoadRequest request = CurvineTransferClient.loadRequest(
                "s3://bucket/model/v1", null, true, CurvineTransferClient.DEFAULT_REPLICAS);
        Assert.assertEquals(0, request.replicas);
        Assert.assertNull(request.targetPath);
        Assert.assertTrue(request.overwrite);
    }

    @Test
    public void explicitTargetSubmitLoadForwardsTheDefaultReplicas() {
        // Exactly what submitLoad(sourcePath, targetPath, overwrite) forwards.
        CurvineTransferClient.LoadRequest request = CurvineTransferClient.loadRequest(
                "s3://bucket/model/v1", "/bucket/model/v1", true,
                CurvineTransferClient.DEFAULT_REPLICAS);
        Assert.assertEquals(0, request.replicas);
        Assert.assertEquals("/bucket/model/v1", request.targetPath);
        Assert.assertTrue(request.overwrite);
    }

    @Test
    public void nonPositiveReplicasMeanTheTransferServiceDefault() {
        Assert.assertEquals(0, CurvineTransferClient.loadRequest("s3://b/v", null, true, 0).replicas);
        Assert.assertEquals(0, CurvineTransferClient.loadRequest("s3://b/v", null, true, -1).replicas);
        Assert.assertEquals(0, CurvineTransferClient.loadRequest("s3://b/v", null, true, Integer.MIN_VALUE)
                .replicas);
    }

    @Test
    public void positiveReplicasAreForwardedUnchanged() {
        Assert.assertEquals(1, CurvineTransferClient.loadRequest("s3://b/v", null, true, 1).replicas);
        Assert.assertEquals(3, CurvineTransferClient.loadRequest("s3://b/v", null, false, 3).replicas);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptySubmitLoadSourcePathIsRejected() {
        CurvineTransferClient.loadRequest("  ", null, true, CurvineTransferClient.DEFAULT_REPLICAS);
    }

    @Test
    public void deadlineHelpersDoNotOverflow() {
        long deadline = CurvineTransferClient.saturatingDeadlineNanos(Duration.ofNanos(Long.MAX_VALUE));
        Assert.assertTrue(deadline > 0L);
        deadline = CurvineTransferClient.saturatingDeadlineNanos(Duration.ofSeconds(Long.MAX_VALUE));
        Assert.assertEquals(Long.MAX_VALUE, deadline);
        Assert.assertEquals(
                Long.MAX_VALUE,
                CurvineTransferClient.saturatingDurationNanos(Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    public void sleepNanosSleepsAtLeastOneNanosecond() throws InterruptedException {
        long start = System.nanoTime();
        CurvineTransferClient.sleepNanos(500_000L); // 0.5ms
        CurvineTransferClient.sleepNanos(0L);
        Assert.assertTrue(System.nanoTime() - start >= 0L);
    }
}

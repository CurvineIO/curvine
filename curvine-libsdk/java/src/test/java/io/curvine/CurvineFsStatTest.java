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

import io.curvine.proto.GetFilesystemInfoResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Locks in the CurvineFsStat invariant: the FsStatus triple
 * (capacity, used, remaining) must be self-consistent on the Live-only
 * allocatable view, i.e. {@code used == max(0, capacity - remaining)} and
 * {@code used} must NOT fall back to the master's all-workers {@code fs_used}
 * (which can exceed {@code capacity - remaining} when Blacklist/Decommission
 * workers still hold data). Mirrors the Rust-side assertions in
 * {@code get_filesystem_info_compat_test.rs} and the {@code cv df} path.
 *
 * <p>See https://github.com/CurvineIO/curvine/pull/1615 (follow-up to #1610).
 */
public class CurvineFsStatTest {

    private static GetFilesystemInfoResponse.Builder baseResponse() {
        // All proto2 required fields are populated so build() succeeds; the
        // allocatable fields are left to each case to set (or omit).
        return GetFilesystemInfoResponse.newBuilder()
                .setActiveMaster("master:8995")
                .setInodeDirNum(0)
                .setInodeFileNum(0)
                .setBlockNum(0)
                .setNonFsUsed(0)
                .setReservedBytes(0);
    }

    @Test
    public void allocatableViewIsSelfConsistentAndIgnoresFsUsed() {
        // capacity=1000/available=400 across ALL workers; allocatable_* are the
        // Live-only subset (600/200). fs_used=300 is intentionally a distractor
        // to prove getUsed() is derived from the allocatable view, not fs_used.
        GetFilesystemInfoResponse info = baseResponse()
                .setCapacity(1000)
                .setAvailable(400)
                .setFsUsed(300)
                .setAllocatableCapacity(600)
                .setAllocatableAvailable(200)
                .build();

        CurvineFsStat stat = new CurvineFsStat(info);

        Assert.assertEquals(600, stat.getCapacity());
        Assert.assertEquals(200, stat.getRemaining());
        // used must be max(0, capacity - remaining) = 400, NOT fs_used (300).
        Assert.assertEquals(400, stat.getUsed());
        Assert.assertNotEquals(
                "getUsed() must derive from the allocatable view, not fs_used",
                info.getFsUsed(), stat.getUsed());
        assertSelfConsistent(stat);
    }

    @Test
    public void legacyMasterFallsBackToTotalAndStillDerivesUsed() {
        // A legacy master omits the allocatable fields (tags 15/16). The stat
        // must fall back to the aggregate capacity/available and still derive
        // used = max(0, capacity - remaining) so it never reports zero free
        // space against a mixed-version master.
        GetFilesystemInfoResponse info = baseResponse()
                .setCapacity(1000)
                .setAvailable(400)
                .setFsUsed(300)
                // allocatable_capacity / allocatable_available intentionally absent.
                .build();

        Assert.assertFalse(info.hasAllocatableCapacity());
        Assert.assertFalse(info.hasAllocatableAvailable());

        CurvineFsStat stat = new CurvineFsStat(info);

        Assert.assertEquals(1000, stat.getCapacity());
        Assert.assertEquals(400, stat.getRemaining());
        Assert.assertEquals(600, stat.getUsed());
        assertSelfConsistent(stat);
    }

    private static void assertSelfConsistent(CurvineFsStat stat) {
        long expectedUsed = Math.max(0, stat.getCapacity() - stat.getRemaining());
        Assert.assertEquals(
                "used must equal max(0, capacity - remaining)",
                expectedUsed, stat.getUsed());
    }
}

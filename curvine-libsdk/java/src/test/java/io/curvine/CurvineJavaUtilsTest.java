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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CurvineJavaUtilsTest {
    @Test
    public void byteFromStringParsesBinaryUnitsWithoutHadoop3RuntimeClasses() {
        assertEquals(0L, CurvineJavaUtils.byteFromString("0"));
        assertEquals(42L, CurvineJavaUtils.byteFromString("42"));
        assertEquals(128L * 1024L, CurvineJavaUtils.byteFromString("128KB"));
        assertEquals(10L * 1024L * 1024L, CurvineJavaUtils.byteFromString("10MB"));
        assertEquals(10L * 1024L * 1024L * 1024L, CurvineJavaUtils.byteFromString("10g"));
        assertEquals(1536L, CurvineJavaUtils.byteFromString("1.5 KB"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void byteFromStringRejectsUnknownUnits() {
        CurvineJavaUtils.byteFromString("10XB");
    }
}

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

/** Runs in a child JVM so CurvineNative must load from the assembled client JAR. */
public final class ClientArtifactNativeSmoke {
    private ClientArtifactNativeSmoke() {}

    public static void main(String[] args) {
        System.out.println(CurvineNative.getLibraryName());
    }
}

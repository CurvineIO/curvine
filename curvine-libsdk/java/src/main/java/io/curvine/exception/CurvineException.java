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

package io.curvine.exception;

import java.io.IOException;

public class CurvineException extends IOException {
    public final static int FILE_EXPIRED = 21;

    private final int errno;

    public CurvineException(String message) {
        super(message);
        errno = 10000;
    }

    public CurvineException(int errno, String message) {
        super(String.format("[errno %s] %s", errno, message));
        this.errno = errno;
    }

    public boolean isExpired() {
        return errno == FILE_EXPIRED;
    }
}

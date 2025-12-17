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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NotDirectoryException;

/**
 * Curvine filesystem exception with error code mapping.
 * Maps Curvine error codes to standard Java IO exceptions for HDFS compatibility.
 */
public class CurvineException extends IOException {
    // Error codes from curvine-common/src/error.rs
    public final static int INVALID_ARGUMENT = 1;
    public final static int IO_ERROR = 2;
    public final static int TIMEOUT = 3;
    public final static int INTERNAL_ERROR = 4;
    public final static int NOT_SUPPORTED = 5;
    public final static int PERMISSION_DENIED = 6;
    public final static int FILE_ALREADY_EXISTS = 7;
    public final static int FILE_NOT_FOUND = 8;
    public final static int NOT_A_DIRECTORY = 9;
    public final static int IS_A_DIRECTORY = 10;
    public final static int DIRECTORY_NOT_EMPTY = 11;
    public final static int FILE_EXPIRED = 21;
    public final static int UNSUPPORTED_UFS_READ = 22;

    private final int errno;

    public CurvineException(String message) {
        super(message);
        errno = 10000;
    }

    public CurvineException(int errno, String message) {
        super(String.format("[errno %s] %s", errno, message));
        this.errno = errno;
    }

    public int getErrno() {
        return errno;
    }

    /**
     * Create appropriate IOException subclass based on error code.
     * This ensures HDFS compatibility by throwing standard Java exceptions.
     * 
     * Note: Curvine server may return Common(10000) error code with "not exits" message
     * for file not found cases. We handle this by checking the message content.
     */
    public static IOException create(int errno, String message) {
        // First check by error code
        switch (errno) {
            case FILE_NOT_FOUND:
                return new FileNotFoundException(message);
            case FILE_ALREADY_EXISTS:
                return new FileAlreadyExistsException(message);
            case PERMISSION_DENIED:
                return new AccessDeniedException(message);
            case NOT_A_DIRECTORY:
                return new NotDirectoryException(message);
            case DIRECTORY_NOT_EMPTY:
                return new DirectoryNotEmptyException(message);
            case IS_A_DIRECTORY:
                // Java doesn't have IsADirectoryException, use IOException with clear message
                return new IOException("Is a directory: " + message);
            default:
                // Fallback: check message content for common error patterns
                // Curvine server sometimes returns Common(10000) with descriptive messages
                if (message != null) {
                    String lowerMsg = message.toLowerCase();
                    if (lowerMsg.contains("not exits") || lowerMsg.contains("not exist") 
                            || lowerMsg.contains("no such file") || lowerMsg.contains("file not found")) {
                        return new FileNotFoundException(message);
                    }
                    if (lowerMsg.contains("already exists") || lowerMsg.contains("file exists")) {
                        return new FileAlreadyExistsException(message);
                    }
                    if (lowerMsg.contains("permission denied") || lowerMsg.contains("access denied")) {
                        return new AccessDeniedException(message);
                    }
                    if (lowerMsg.contains("not a directory")) {
                        return new NotDirectoryException(message);
                    }
                    if (lowerMsg.contains("directory not empty")) {
                        return new DirectoryNotEmptyException(message);
                    }
                }
                return new CurvineException(errno, message);
        }
    }
}

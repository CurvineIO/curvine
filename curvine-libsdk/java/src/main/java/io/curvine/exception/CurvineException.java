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
 *
 * <p>Numeric values MUST stay in lock-step with
 * {@code curvine-common/src/error/fs_error.rs::ErrorKind}.
 */
public class CurvineException extends IOException {
    public static final int IO = 1;
    public static final int NOT_LEADER_MASTER = 2;
    public static final int RAFT = 3;
    public static final int TIMEOUT = 4;
    public static final int PB_DECODE = 5;
    public static final int PB_ENCODE = 6;
    public static final int FILE_ALREADY_EXISTS = 7;
    public static final int FILE_NOT_FOUND = 8;
    public static final int INVALID_FILE_SIZE = 9;
    public static final int PARENT_NOT_DIR = 10;
    public static final int DIRECTORY_NOT_EMPTY = 11;
    public static final int ABNORMAL_DATA = 12;
    public static final int BLOCK_IS_WRITING = 13;
    public static final int BLOCK_INFO = 14;
    public static final int LEASE = 15;
    public static final int INVALID_PATH = 16;
    public static final int DISK_OUT_OF_SPACE = 17;
    public static final int IN_PROGRESS = 18;
    public static final int NOT_SUPPORTED = 19;
    public static final int UFS = 20;
    public static final int FILE_EXPIRED = 21;
    public static final int UNSUPPORTED_UFS_READ = 22;
    public static final int JOB_NOT_FOUND = 23;
    public static final int PIPELINE = 24;
    public static final int MIN_REPLICAS_NOT_MET = 25;
    public static final int IS_A_DIRECTORY = 26;
    public static final int NOT_A_DIRECTORY = 27;
    public static final int INVALID_ARGUMENT = 28;
    public static final int COMMON = 10000;

    private final int errno;

    public CurvineException(String message) {
        super(message);
        errno = COMMON;
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
     * <p>Curvine server may return Common(10000) with descriptive messages;
     * those are handled via message-pattern fallbacks below.
     */
    public static IOException create(int errno, String message) {
        switch (errno) {
            case FILE_NOT_FOUND:
                return new FileNotFoundException(message);
            case FILE_ALREADY_EXISTS:
                return new FileAlreadyExistsException(message);
            case PARENT_NOT_DIR:
            case NOT_A_DIRECTORY:
                return new NotDirectoryException(message);
            case DIRECTORY_NOT_EMPTY:
                return new DirectoryNotEmptyException(message);
            case IS_A_DIRECTORY:
                // Java doesn't have IsADirectoryException, use IOException with clear message
                return new IOException("Is a directory: " + message);
            case INVALID_PATH:
            case INVALID_ARGUMENT:
                return new IOException("Invalid argument: " + message);
            default:
                // Fallback: check message content for common error patterns
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
                    if (lowerMsg.contains("is a directory")) {
                        return new IOException("Is a directory: " + message);
                    }
                }
                return new CurvineException(errno, message);
        }
    }
}

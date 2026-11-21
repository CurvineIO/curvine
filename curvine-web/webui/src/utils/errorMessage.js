/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Shown when an error answer carries no message of its own.
const STATUS_TEXT = {
    401: 'UnAuthorized Request',
    403: 'Request Reject',
    404: 'Request URI Not Found',
    408: 'Request Timeout',
    500: 'Internal service error',
    501: 'Not support service',
    502: 'Gateway Error',
    503: 'Service temporary unavailable',
    504: 'Gateway Timeout',
    505: 'HTTP version not support',
};

/**
 * Message to show for a failed API request.
 *
 * The master answers a failed call with `{"message": "..."}` (FsError and the
 * load API's AppError), which says what went wrong, for example that a path
 * does not exist. That text is preferred over the generic one for the status.
 */
export function requestErrorMessage(error) {
    const response = error && error.response;
    if (!response) {
        // No answer at all: network error, timeout, or the request never left.
        return String((error && error.message) || error);
    }
    const body = response.data;
    if (body && typeof body.message === 'string' && body.message) {
        return body.message;
    }
    return STATUS_TEXT[response.status] || `Request failed with status ${response.status}`;
}

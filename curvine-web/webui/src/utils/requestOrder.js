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

/**
 * Decides whether a successful request may clear the error banner.
 *
 * Requests are numbered when they are sent. A success clears the banner only
 * if its request was sent after the last request that failed. A slow request
 * from the page the user has left (for example Overview) can answer after a
 * newer request on the current page (Browse of a missing path) has failed;
 * that answer says nothing about the new failure, so the banner stays.
 */
export function createRequestOrder() {
    let sent = 0;
    let lastFailed = 0;
    return {
        /** Number for a request that is about to be sent. */
        next() {
            sent += 1;
            return sent;
        },
        /**
         * Records a failure of the request numbered `seq`. A request that
         * failed before it got a number counts as the latest one sent.
         */
        failed(seq) {
            lastFailed = Math.max(lastFailed, seq || sent);
        },
        /** Whether a success of the request numbered `seq` clears the banner. */
        clears(seq) {
            return seq > lastFailed;
        },
    };
}

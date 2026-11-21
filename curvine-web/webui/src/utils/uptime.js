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
 * When the master started, in milliseconds since the Unix epoch, or null.
 *
 * `/api/overview` sends `start_time_ms` for this. Its `start_time` is the
 * master's local wall-clock time without a time zone, which a browser in
 * another zone reads hours off, so it is only used for masters that do not
 * send `start_time_ms` yet.
 */
export function masterStartMillis(overview) {
    if (!overview) return null;
    const millis = overview.start_time_ms;
    if (Number.isFinite(millis) && millis > 0) return millis;
    const parsed = overview.start_time ? new Date(overview.start_time).getTime() : NaN;
    return Number.isNaN(parsed) ? null : parsed;
}

/**
 * Time since `startMillis` as "Xd Yh Zm". A browser clock that runs behind
 * the master's shows 0m rather than a negative uptime.
 */
export function uptimeText(startMillis, nowMillis = Date.now()) {
    if (startMillis === null || startMillis === undefined) return 'N/A';
    const diff = Math.max(0, nowMillis - startMillis);
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
    return `${days}d ${hours}h ${minutes}m`;
}

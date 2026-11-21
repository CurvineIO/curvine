
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

export function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

export function toGB(bytes) {
    return (bytes / 1024 / 1024 / 1024).toFixed(2)
}

/**
 * Link to a page of this UI that takes a file system path as `?path=`.
 *
 * The path is percent-encoded, so a name holding `&`, `#`, `+`, `%`, `?` or `=`
 * reaches the page unchanged; `/` stays readable.
 */
export function pathLink(page, path) {
    return `${page}?path=${encodeURIComponent(path).replace(/%2F/g, '/')}`
}

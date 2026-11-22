
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

/**
 * A byte count for display, in the largest binary unit that keeps the number
 * at 1 or more, with up to two decimals: 134217728 gives "128 MB" and 1536
 * gives "1.5 KB". Missing or zero gives "0 B".
 */
export function formatBytes(bytes) {
    if (!(bytes > 0)) return '0 B'
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
    return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + units[i]
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

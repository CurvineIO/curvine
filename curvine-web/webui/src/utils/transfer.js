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

// Helpers shared by the transfer views.

// Job states that no longer change without a new run.
export const TERMINAL_STATES = ['completed', 'failed', 'canceled', 'partial_success']

export function isTerminalState(state) {
    return TERMINAL_STATES.includes(state)
}

export function canCancel(state) {
    return !isTerminalState(state) && state !== 'canceling'
}

export function canRetry(state) {
    return isTerminalState(state)
}

// Tone names map to the badge colors in TransferStateBadge.vue.
export function transferStateTone(state) {
    if (state === 'completed') return 'success'
    if (state === 'partial_success') return 'warning'
    if (state === 'failed') return 'danger'
    if (state === 'canceled' || state === 'canceling' || state === 'stale') return 'muted'
    return 'active'
}

export function progressPercent(progress) {
    if (!progress) return 0
    const total = Number(progress.total_size) || 0
    const loaded = Number(progress.loaded_size) || 0
    if (total <= 0) return 0
    return Math.min(100, Math.max(0, Math.floor((loaded * 100) / total)))
}

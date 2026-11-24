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

import axios from 'axios'
import request from '@/utils/request'

// Resolved once per page load: the transfer API only exists on the transfer
// web port. A master web server answers unknown /api paths with index.html,
// so a 200 alone does not prove the API is there.
let availabilityPromise = null

export function transferApiAvailable() {
    if (!availabilityPromise) {
        availabilityPromise = axios
            .get(`${process.env.VUE_APP_BASE_API}/transfer/jobs`, {
                params: { limit: 1 },
                timeout: 5000
            })
            .then(response => {
                const data = response.data
                return Boolean(data && Array.isArray(data.jobs))
            })
            .catch(() => false)
    }
    return availabilityPromise
}

export function fetchTransferJobs(params) {
    return request({
        url: '/transfer/jobs',
        method: 'get',
        params
    })
}

export function fetchTransferJob(jobId, params) {
    return request({
        url: `/transfer/jobs/${encodeURIComponent(jobId)}`,
        method: 'get',
        params
    })
}

export function fetchTransferTasks(jobId, params) {
    return request({
        url: `/transfer/jobs/${encodeURIComponent(jobId)}/tasks`,
        method: 'get',
        params
    })
}

export function fetchTransferTenants(params) {
    return request({
        url: '/transfer/tenants',
        method: 'get',
        params
    })
}

export function cancelTransferJob(jobId) {
    return request({
        url: `/transfer/jobs/${encodeURIComponent(jobId)}/cancel`,
        method: 'post'
    })
}

export function retryTransferJob(jobId) {
    return request({
        url: `/transfer/jobs/${encodeURIComponent(jobId)}/retry`,
        method: 'post'
    })
}

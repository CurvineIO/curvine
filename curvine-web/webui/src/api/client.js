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

import request from '@/utils/request'

export function fetchOverviewData() {
    return request({
        url: '/overview',
        method: 'get',
    })
}

export function fetchConfigData() {
    return request({
        url: '/config',
        method: 'get',
    })
}

export function fetchBrowseData(query) {
    return request({
        url: '/browse',
        method: 'get',
        params: query
    })
}

export function fetchWorkersData() {
    return request({
        url: '/workers',
        method: 'get',
    })
}

export function fetchBlocksData(query) {
    return request({
        url: '/block_locations',
        method: 'get',
        params: query
    })
}


// export function login(data) {
//     return request({
//         url: '/vue-element-admin/user/login',
//         method: 'post',
//         data
//     })
// }
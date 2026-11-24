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

import axios from 'axios';
import store from '@/store';
import { requestErrorMessage } from '@/utils/errorMessage';
import { createRequestOrder } from '@/utils/requestOrder';

/**
 * http request utils
 *
 */

// create an axios instance
const service = axios.create({
    baseURL: process.env.VUE_APP_BASE_API,
    timeout: 10000 // request timeout
});
// Sending order of requests, so a stale success cannot clear a newer error.
const order = createRequestOrder();
// request interceptor
service.interceptors.request.use(
    config => {
        config.seq = order.next();
        return config;
    },
    error => {
        // do something with request error
        console.log(error) // for debug
        order.failed();
        store.commit('SET_ERRMSG', requestErrorMessage(error))
        return Promise.reject(error);
    }
);

// response interceptor
service.interceptors.response.use(
    response => {
        // A request that succeeds again clears the banner of an earlier failure,
        // unless it was sent before that failure (a slow answer from a page the
        // user has left).
        if (store.state.errmsg && order.clears(response.config.seq)) {
            store.commit('SET_ERRMSG', '')
        }
        return response;
    },
    error => {
        console.log('request err: ' + error) // for debug
        order.failed(error.config && error.config.seq);
        const errMsg = requestErrorMessage(error);
        console.log(errMsg)
        store.commit('SET_ERRMSG', errMsg)
        return Promise.reject(errMsg);
    }
);
export default service;
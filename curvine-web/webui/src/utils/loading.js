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

import { createApp } from 'vue'
import LoadingOverlay from '@/components/Loading.vue'

const loadingDirective = {
    mounted(el, binding) {
        const app = createApp(LoadingOverlay)
        const instance = app.mount(document.createElement('div'))

        el.instance = instance

        if (binding.value) {
            append(el)
        }
    },
    updated(el, binding) {
        if (binding.value !== binding.oldValue) {
            if (binding.value) {
                append(el)
            } else {
                remove(el)
            }
        }
    },
    unmounted(el) {
        remove(el)
    }
}

function append(el) {
    const style = getComputedStyle(el)
    if (style.position === 'static') {
        el.style.position = 'relative'
    }
    el.appendChild(el.instance.$el)
}

function remove(el) {
    if (el.instance && el.instance.$el.parentNode === el) {
        el.removeChild(el.instance.$el)
    }
    //   el.removeChild(el.instance.$el)
}

export default loadingDirective

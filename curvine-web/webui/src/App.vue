<!--
  - Copyright 2025 OPPO.
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  -     http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  -->

<template>
  <div class="App h-100">
    <div class="w-100 sticky-top header-wrapper">
      <Header />
    </div>
    <div v-if="errmsg" class="alert alert-danger fade show">
      <button type="button" class="close" data-dismiss="alert" aria-hidden="true">x</button>
      {{ errmsg }}
    </div>
    <div class="w-100 pt-5 mt-3 pb-4 mb-2">
      <router-view />
    </div>
    <div class="w-100 footer-wrapper">
      <Footer />
    </div>
  </div>
</template>

<script>
import Header from '@/components/Header.vue'
import Footer from '@/components/Footer.vue'

export default {
  name: 'App',
  components: {
    Header,
    Footer
  },
  data() {
    return {
      errmsg: this.$store.state.errmsg
      // errmsg: "error message"
    }
  }
}
</script>

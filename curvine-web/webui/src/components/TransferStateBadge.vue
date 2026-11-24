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
  <span class="state-badge" :class="tone">{{ label }}</span>
</template>

<script>
import { transferStateTone } from '@/utils/transfer'

export default {
  name: 'TransferStateBadge',
  props: {
    state: {
      type: String,
      default: 'unknown'
    }
  },
  computed: {
    tone() {
      return transferStateTone(this.state)
    },
    label() {
      return (this.state || 'unknown').replace(/_/g, ' ')
    }
  }
}
</script>

<style lang="scss" scoped>
.state-badge {
  display: inline-block;
  padding: 0.2rem 0.6rem;
  border-radius: 999px;
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.4px;
  text-transform: uppercase;
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  background: var(--secondary-bg);

  &.active {
    color: var(--accent-blue);
    border-color: var(--accent-blue);
    background: rgba(0, 212, 255, 0.12);
  }

  &.success {
    color: var(--accent-green);
    border-color: var(--accent-green);
    background: rgba(0, 255, 136, 0.12);
  }

  &.warning {
    color: #ffc857;
    border-color: #ffc857;
    background: rgba(255, 200, 87, 0.12);
  }

  &.danger {
    color: #ff6b6b;
    border-color: #ff6b6b;
    background: rgba(255, 107, 107, 0.12);
  }

  &.muted {
    color: var(--text-secondary);
  }
}
</style>

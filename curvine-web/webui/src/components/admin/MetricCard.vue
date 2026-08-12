<template>
  <article class="admin-metric-card">
    <span>{{ label }}</span>
    <strong :class="{ 'wrap-value': wrapValue }">{{ value }}</strong>
    <div class="metric-foot">
      <small>{{ meta }}</small>
      <em :class="tone">{{ delta }}</em>
    </div>
  </article>
</template>

<script>
export default {
  name: 'MetricCard',
  props: {
    label: { type: String, required: true },
    value: { type: [String, Number], required: true },
    meta: { type: String, default: '' },
    delta: { type: String, default: '' },
    tone: { type: String, default: 'neutral' },
    wrapValue: { type: Boolean, default: false }
  }
}
</script>

<style scoped>
.admin-metric-card strong.wrap-value {
  font-size: 17px;
  line-height: 1.35;
  white-space: normal;
  word-break: break-word;
  overflow: visible;
  text-overflow: unset;
}
</style>

<template>
  <div class="panel">
    <h4>📜 执行日志（明细列表，与导出内容一致）</h4>
    <div class="log-list">
      <div v-for="r in records" :key="r.id" class="log-row" :class="r.status.toLowerCase()">
        <span class="l-id">{{ r.id }}</span>
        <span class="l-status">{{ r.status }}</span>
        <span class="l-task">{{ r.taskId }}</span>
        <span class="l-msg">{{ r.message }}</span>
      </div>
      <div v-if="!records.length" class="empty">等待执行...</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useDAGStore } from '../store/dag'
const store = useDAGStore()
const records = computed(() => store.records)
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a;flex:1}
.panel h4{color:#bb86fc;font-size:12px;margin-bottom:6px}
.log-list{max-height:220px;overflow-y:auto;font-size:10px;font-family:monospace}
.log-row{display:flex;gap:6px;padding:2px 4px;border-radius:2px;margin:1px 0}
.log-row.running{background:#3182ce15}.log-row.success{color:#38a169}.log-row.failed{color:#e53e3e;background:#e53e3e10}
.l-id{color:#bb86fc;min-width:30px}.l-status{font-weight:700;min-width:78px}.l-task{color:#888;min-width:80px}.l-msg{color:#ccc}.empty{color:#4a5568}
</style>

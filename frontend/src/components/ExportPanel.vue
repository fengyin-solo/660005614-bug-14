<template>
  <div class="panel export-panel">
    <h4>📦 明细导出</h4>

    <div v-if="!records.length" class="empty">暂无明细，执行工作流后可勾选导出</div>

    <template v-else>
      <div class="toolbar">
        <el-checkbox
          :model-value="allChecked"
          :indeterminate="someChecked && !allChecked"
          :disabled="store.isRunning"
          size="small"
          @change="toggleAll"
        >全选</el-checkbox>
        <span class="sel-count">已选 {{ selectedIds.size }}/{{ records.length }}</span>
      </div>

      <div class="record-list">
        <label v-for="r in records" :key="r.id" class="record-row" :class="r.status.toLowerCase()">
          <el-checkbox v-model="checkedMap[r.id]" :disabled="store.isRunning" size="small"/>
          <span class="r-id">{{ r.id }}</span>
          <span class="r-status">{{ r.status }}</span>
          <span class="r-task">{{ r.taskId }}</span>
          <span class="r-msg">{{ r.message }}</span>
        </label>
      </div>

      <div class="actions">
        <el-button
          type="primary" size="small"
          :disabled="selectedIds.size !== 1 || store.isRunning"
          :loading="store.exporting"
          @click="doExport([[...selectedIds][0]])"
        >单条导出</el-button>
        <el-button
          type="success" size="small"
          :disabled="selectedIds.size === 0 || store.isRunning"
          :loading="store.exporting"
          @click="doExport([...selectedIds])"
        >批量导出 ({{ selectedIds.size }})</el-button>
      </div>
      <div v-if="store.isRunning" class="hint">执行进行中，完成后才可导出</div>
    </template>

    <div class="archive-section">
      <div class="archive-head">
        <h5>历史归档</h5>
        <el-button link type="primary" size="small" @click="store.refreshArchives()">刷新</el-button>
      </div>
      <div v-if="!store.archives.length" class="empty">暂无归档文件</div>
      <div v-for="a in store.archives" :key="a.id" class="archive-row">
        <span class="a-name" :title="a.fileName">{{ a.fileName }}</span>
        <span class="a-meta">{{ a.count }}条 · {{ formatSize(a.size) }} · {{ formatTime(a.createdAt) }}</span>
        <el-button link type="primary" size="small" @click="store.triggerDownload(a.id, a.fileName)">下载</el-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useDAGStore } from '../store/dag'

const store = useDAGStore()
const records = computed(() => store.records)
const checkedMap = reactive<Record<string, boolean>>({})
const selectedIds = computed(() => {
  const ids = records.value.map(r => r.id).filter(id => checkedMap[id])
  return new Set(ids)
})
const allChecked = computed(() => records.value.length > 0 && selectedIds.value.size === records.value.length)
const someChecked = computed(() => selectedIds.value.size > 0)

const lastExportKey = ref('')

function toggleAll(val: any) {
  const checked = Boolean(val)
  records.value.forEach(r => { checkedMap[r.id] = checked })
}

// 切换执行时清空勾选，避免跨范围误选
watch(() => store.currentExecutionId, () => {
  Object.keys(checkedMap).forEach(k => { delete checkedMap[k] })
})

async function doExport(ids: string[]) {
  if (!store.currentExecutionId || ids.length === 0) return
  // 保持与页面相同的明细顺序（记录 ID 升序）
  const ordered = records.value.map(r => r.id).filter(id => ids.includes(id))
  const key = `${store.currentExecutionId}:${ordered.join(',')}`
  try {
    const res = await store.exportRecords(store.currentExecutionId, ordered)
    lastExportKey.value = key
    ElMessage({
      type: 'success',
      message: res.reused ? '该范围已导出过，直接提供既有归档（未重复生成）' : '导出完成，已开始下载',
      duration: 2500,
    })
  } catch (e: any) {
    // 失败不留脏状态：用户修正后可直接重新发起
    ElMessage.error(e?.response?.data?.detail || '导出失败，请重试')
  }
}

function formatSize(n: number) {
  if (n < 1024) return `${n}B`
  return `${(n / 1024).toFixed(1)}KB`
}
function formatTime(t: number) {
  const d = new Date(t * 1000)
  const p = (x: number) => String(x).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
}
</script>

<style scoped>
.export-panel { background:#1a1a2e; border-radius:8px; padding:10px; border:1px solid #2a2a4a; }
h4 { color:#bb86fc; font-size:12px; margin-bottom:6px; }
.toolbar { display:flex; align-items:center; gap:8px; margin-bottom:4px; }
.sel-count { color:#888; font-size:10px; }
.record-list { max-height:200px; overflow-y:auto; font-size:10px; font-family:monospace; }
.record-row { display:flex; align-items:center; gap:6px; padding:2px 4px; border-radius:2px; margin:1px 0; cursor:pointer; }
.record-row.running { background:#3182ce15; }
.record-row.success .r-status { color:#38a169; }
.record-row.failed .r-status, .record-row.circuit_open .r-status { color:#e53e3e; }
.r-id { color:#bb86fc; min-width:30px; }
.r-status { font-weight:700; min-width:78px; }
.r-task { color:#888; min-width:80px; }
.r-msg { color:#ccc; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.actions { display:flex; gap:6px; margin-top:6px; }
.hint { color:#d69e2e; font-size:10px; margin-top:4px; }
.archive-section { margin-top:10px; border-top:1px solid #2a2a4a; padding-top:6px; }
.archive-head { display:flex; justify-content:space-between; align-items:center; }
.archive-head h5 { color:#9ec5fe; font-size:11px; }
.archive-row { display:flex; align-items:center; gap:6px; padding:3px 0; font-size:10px; }
.a-name { color:#cfe2ff; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-family:monospace; }
.a-meta { color:#888; white-space:nowrap; }
.empty { color:#4a5568; font-size:11px; padding:4px 0; }
</style>

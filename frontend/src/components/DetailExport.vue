<template>
  <div class="export-page">
    <div class="toolbar">
      <div>
        <h2>明细导出与归档</h2>
        <p>勾选范围、导出文件和归档内容均以当前明细列表为准；单条与多选走同一套导出逻辑。</p>
      </div>
      <div class="actions">
        <el-button @click="reload" :loading="store.loading">刷新明细</el-button>
        <el-button type="primary" :disabled="!store.selectedIds.length" :loading="store.exporting" @click="exportSelected">
          批量导出 {{ store.selectedIds.length ? `(${store.selectedIds.length}条)` : '' }}
        </el-button>
      </div>
    </div>

    <el-alert v-if="store.error" :title="store.error" type="error" show-icon :closable="false" class="error-alert" />

    <div class="content-grid">
      <el-card class="records-card" shadow="never">
        <template #header>
          <div class="card-header">
            <span>页面明细（{{ store.records.length }}）</span>
            <span>已选 {{ store.selectedIds.length }}</span>
          </div>
        </template>
        <el-table
          :data="store.records"
          row-key="id"
          height="100%"
          @selection-change="onSelectionChange"
          :row-class-name="recordRowClass"
        >
          <el-table-column type="selection" width="46" reserve-selection />
          <el-table-column prop="id" label="ID" width="70" />
          <el-table-column prop="taskName" label="任务" width="130" />
          <el-table-column prop="taskId" label="任务编码" width="130" />
          <el-table-column label="状态" width="100">
            <template #default="{ row }">
              <el-tag :type="statusType(row.status)" size="small">{{ statusText(row.status) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="耗时" width="90">
            <template #default="{ row }">{{ row.duration.toFixed(1) }}s</template>
          </el-table-column>
          <el-table-column prop="detail" label="明细内容" min-width="220" show-overflow-tooltip />
          <el-table-column label="开始时间" width="175">
            <template #default="{ row }">{{ formatTime(row.startedAt) }}</template>
          </el-table-column>
          <el-table-column label="操作" width="100" fixed="right">
            <template #default="{ row }">
              <el-button link type="primary" :loading="store.exporting" @click="exportOne(row)">单条导出</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <el-card class="archive-card" shadow="never">
        <template #header>
          <div class="card-header">
            <span>历史归档</span>
            <el-button link type="primary" @click="store.loadJobs()">刷新</el-button>
          </div>
        </template>
        <div class="archive-list">
          <div v-for="job in store.jobs" :key="job.id" class="archive-item">
            <div class="archive-title">
              <strong>#{{ job.id }}</strong>
              <span>{{ rangeText(job.recordIds) }}</span>
              <el-tag :type="exportStatusType(job.status)" size="small">{{ exportStatusText(job.status) }}</el-tag>
            </div>
            <div class="archive-meta">
              <span>{{ job.recordCount }} 条</span>
              <span>第 {{ job.attempts }} 次</span>
              <span>{{ formatTime(job.updatedAt || job.createdAt) }}</span>
              <span v-if="job.fileSize">{{ formatSize(job.fileSize) }}</span>
            </div>
            <div v-if="job.fileName" class="archive-file" :title="job.fileName">{{ job.fileName }}</div>
            <div v-if="job.error" class="archive-error">失败原因：{{ job.error }}</div>
            <div class="archive-actions">
              <el-button
                size="small"
                type="primary"
                :disabled="job.status !== 'SUCCESS'"
                @click="download(job)"
              >继续下载</el-button>
              <el-button
                size="small"
                :disabled="job.status !== 'FAILED'"
                :loading="store.exporting"
                @click="restart(job)"
              >重新开始</el-button>
            </div>
          </div>
          <div v-if="!store.jobs.length" class="empty-archive">暂无归档任务</div>
        </div>
      </el-card>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import type { DetailRecord, ExportJob, ExportStatus } from '@/types'
import { useExportStore } from '@/store/export'

const store = useExportStore()
let timer: number | undefined

function onSelectionChange(rows: DetailRecord[]) {
  store.selectedIds = rows.map(row => row.id)
}

function recordRowClass({ row }: { row: DetailRecord }) {
  return `status-${row.status.toLowerCase()}`
}

async function exportOne(record: DetailRecord) {
  await startExport([record.id])
}

async function exportSelected() {
  await startExport([...store.selectedIds])
}

async function startExport(ids: number[]) {
  if (!ids.length) return
  try {
    const job = await store.createExport(ids)
    ElMessage.success(job.status === 'SUCCESS' ? '归档已存在，可继续下载' : '导出已开始')
  } catch (err: unknown) {
    ElMessage.error(getErrorMessage(err))
  }
}

async function restart(job: ExportJob) {
  try {
    await store.restartExport(job.id)
    ElMessage.success('已清空失败状态并重新开始')
  } catch (err: unknown) {
    ElMessage.error(getErrorMessage(err))
  }
}

async function download(job: ExportJob) {
  try {
    await store.downloadJob(job.id)
  } catch (err: unknown) {
    ElMessage.error(getErrorMessage(err))
  }
}

async function reload() {
  await Promise.all([store.loadRecords(), store.loadJobs()])
}

function hasActiveJob() {
  return store.jobs.some(job => job.status === 'RUNNING' || job.status === 'PENDING')
}

function pollJobs() {
  timer = window.setInterval(async () => {
    if (!hasActiveJob()) return
    try {
      await store.loadJobs()
    } catch {
      // 保留页面已有状态，下一轮继续轮询。
    }
  }, 1000)
}

function statusText(status: DetailRecord['status']) {
  return { PENDING: '等待', RUNNING: '执行中', SUCCESS: '成功', FAILED: '失败' }[status]
}

function statusType(status: DetailRecord['status']) {
  return { PENDING: 'info', RUNNING: 'warning', SUCCESS: 'success', FAILED: 'danger' }[status]
}

function exportStatusText(status: ExportStatus) {
  return { PENDING: '等待', RUNNING: '导出中', SUCCESS: '已归档', FAILED: '失败' }[status]
}

function exportStatusType(status: ExportStatus) {
  return { PENDING: 'info', RUNNING: 'warning', SUCCESS: 'success', FAILED: 'danger' }[status]
}

function rangeText(ids: number[]) {
  if (ids.length === 1) return `明细 #${ids[0]}`
  const contiguous = ids[ids.length - 1] - ids[0] + 1 === ids.length
  if (contiguous) return `明细 #${ids[0]} - #${ids[ids.length - 1]}（${ids.length}条）`
  return `非连续明细 ${ids.map(id => `#${id}`).join('、')}`
}

function formatTime(value: string | null) {
  if (!value) return '-'
  return value.replace('T', ' ')
}

function formatSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`
}

function getErrorMessage(err: unknown) {
  if (typeof err === 'object' && err !== null && 'message' in err) return String((err as { message: unknown }).message)
  return '操作失败'
}

onMounted(async () => {
  await reload()
  pollJobs()
})

onUnmounted(() => window.clearInterval(timer))
</script>

<style scoped>
.export-page {
  flex: 1;
  overflow: hidden;
  padding: 14px 18px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  background: #0f0f23;
}
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}
.toolbar h2 {
  color: #bb86fc;
  font-size: 18px;
  margin-bottom: 4px;
}
.toolbar p {
  color: #888;
  font-size: 12px;
}
.actions {
  display: flex;
  gap: 8px;
}
.error-alert {
  flex: none;
}
.content-grid {
  flex: 1;
  min-height: 0;
  display: grid;
  grid-template-columns: minmax(0, 1fr) 360px;
  gap: 12px;
}
.records-card,
.archive-card {
  min-height: 0;
  background: #1a1a2e;
  border-color: #2a2a4a;
}
:deep(.el-card__header),
:deep(.el-card__body) {
  border-color: #2a2a4a;
  background: #1a1a2e;
  color: #e0e0e0;
}
:deep(.el-card__body) {
  height: calc(100% - 50px);
  padding: 0;
}
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  color: #e0e0e0;
  font-size: 13px;
}
:deep(.el-table) {
  background: #1a1a2e;
  color: #d8d8e8;
  --el-table-border-color: #2a2a4a;
  --el-table-header-bg-color: #14142b;
  --el-table-tr-bg-color: #1a1a2e;
  --el-table-row-hover-bg-color: #232342;
}
:deep(.el-table th.el-table__cell) {
  background: #14142b;
  color: #bb86fc;
}
.archive-list {
  height: 100%;
  overflow-y: auto;
  padding: 2px 2px 8px;
}
.archive-item {
  border: 1px solid #2a2a4a;
  border-radius: 8px;
  padding: 10px;
  margin-bottom: 8px;
  background: #14142b;
}
.archive-title {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #e0e0e0;
  font-size: 13px;
}
.archive-title span {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.archive-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  color: #888;
  font-size: 11px;
  margin: 8px 0;
}
.archive-file {
  color: #93c5fd;
  font-family: monospace;
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  margin-bottom: 8px;
}
.archive-error {
  color: #f87171;
  font-size: 12px;
  margin-bottom: 8px;
}
.archive-actions {
  display: flex;
  gap: 8px;
}
.empty-archive {
  color: #666;
  text-align: center;
  padding: 30px 0;
  font-size: 12px;
}
@media (max-width: 1100px) {
  .content-grid {
    grid-template-columns: 1fr;
    overflow-y: auto;
  }
  .records-card {
    min-height: 420px;
  }
  .archive-card {
    min-height: 300px;
  }
}
</style>

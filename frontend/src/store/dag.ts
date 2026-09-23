import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo, ExecutionDetail, ExecutionSummary, ArchiveInfo, ExecutionRecord } from '@/types'

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const currentExecutionId = ref<string | null>(null)
  const executions = ref<ExecutionSummary[]>([])
  const archives = ref<ArchiveInfo[]>([])
  const exporting = ref(false)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  let ws: WebSocket | null = null
  let pollTimer: number | null = null

  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onclose = () => { wsConnected.value = false }
    ws.onmessage = (e) => {
      try { execution.value = JSON.parse(e.data) } catch { /* ignore */ }
    }
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try {
      const { data } = await axios.post('/api/workflow', { name })
      workflow.value = data
    } finally {
      loading.value = false
    }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value
      })
      execution.value = data
      currentExecutionId.value = data.executionId
      startPolling(data.executionId)
    } finally {
      loading.value = false
    }
  }

  // 轮询兜底：WS 不可用时页面仍能拿到完整明细
  function startPolling(execId: string) {
    stopPolling()
    pollTimer = window.setInterval(async () => {
      try {
        const { data }: { data: ExecutionDetail } = await axios.get(`/api/executions/${execId}`)
        execution.value = {
          executionId: data.id,
          workflow: data.workflow,
          records: data.records,
          logs: data.records.map(r => ({ taskId: r.taskId, status: r.status, timestamp: r.timestamp, message: r.message })),
          circuitBreakers: [],
          completed: data.status !== 'RUNNING',
          status: data.status,
        }
        if (data.status !== 'RUNNING') {
          stopPolling()
          await refreshExecutions()
        }
      } catch { /* transient: keep polling */ }
    }, 1500)
  }

  function stopPolling() {
    if (pollTimer !== null) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  }

  async function refreshExecutions() {
    const { data } = await axios.get('/api/executions')
    executions.value = data
  }

  async function loadExecution(execId: string) {
    stopPolling()
    currentExecutionId.value = execId
    const { data }: { data: ExecutionDetail } = await axios.get(`/api/executions/${execId}`)
    execution.value = {
      executionId: data.id,
      workflow: data.workflow,
      records: data.records,
      logs: data.records.map(r => ({ taskId: r.taskId, status: r.status, timestamp: r.timestamp, message: r.message })),
      circuitBreakers: [],
      completed: data.status !== 'RUNNING',
      status: data.status,
    }
  }

  /** 批量与单条导出走同一个接口；单条只是 ids 长度为 1 */
  async function exportRecords(execId: string, recordIds: string[]) {
    exporting.value = true
    try {
      const { data } = await axios.post('/api/exports', { executionId: execId, recordIds })
      triggerDownload(data.archive.id, data.archive.fileName)
      await refreshArchives()
      return data as { archive: ArchiveInfo; reused: boolean }
    } finally {
      exporting.value = false
    }
  }

  async function refreshArchives() {
    const { data } = await axios.get('/api/archives')
    archives.value = data
  }

  function triggerDownload(archiveId: string, _fileName: string) {
    // 文件名由后端 Content-Disposition 提供，保证与勾选范围一致
    const a = document.createElement('a')
    a.href = `/api/archives/${archiveId}/download`
    a.rel = 'noopener'
    document.body.appendChild(a)
    a.click()
    a.remove()
  }

  function disconnectWS() {
    stopPolling()
    ws?.close()
    ws = null
  }

  const records = computed<ExecutionRecord[]>(() => execution.value?.records ?? [])
  const isRunning = computed(() => execution.value?.status === 'RUNNING' ||
    (execution.value ? !execution.value.completed : false))

  return {
    loading, workflow, execution, currentExecutionId, executions, archives,
    exporting, wsConnected, workers, strategy, records, isRunning,
    connectWS, createWorkflow, run, refreshExecutions, loadExecution,
    exportRecords, refreshArchives, triggerDownload, disconnectWS,
  }
})

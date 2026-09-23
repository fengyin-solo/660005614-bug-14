import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DetailRecord, ExportJob } from '@/types'

export const useExportStore = defineStore('export', () => {
  const records = ref<DetailRecord[]>([])
  const jobs = ref<ExportJob[]>([])
  const selectedIds = ref<number[]>([])
  const loading = ref(false)
  const exporting = ref(false)
  const error = ref('')

  async function loadRecords() {
    loading.value = true
    error.value = ''
    try {
      const { data } = await axios.get<DetailRecord[]>('/api/records')
      records.value = data
    } catch (err: unknown) {
      error.value = axios.isAxiosError(err) ? err.message : '明细加载失败'
    } finally {
      loading.value = false
    }
  }

  async function loadJobs() {
    const { data } = await axios.get<ExportJob[]>('/api/exports')
    jobs.value = data
  }

  async function createExport(ids: number[]) {
    exporting.value = true
    error.value = ''
    try {
      const { data } = await axios.post<ExportJob>('/api/exports', { recordIds: ids })
      await loadJobs()
      return data
    } finally {
      exporting.value = false
    }
  }

  async function restartExport(jobId: number) {
    exporting.value = true
    error.value = ''
    try {
      const { data } = await axios.post<ExportJob>(`/api/exports/${jobId}/restart`, {})
      await loadJobs()
      return data
    } finally {
      exporting.value = false
    }
  }

  async function refreshJob(jobId: number) {
    const { data } = await axios.get<ExportJob>(`/api/exports/${jobId}`)
    const index = jobs.value.findIndex(job => job.id === jobId)
    if (index >= 0) jobs.value[index] = data
    else jobs.value.unshift(data)
    return data
  }

  async function downloadJob(jobId: number) {
    const response = await axios.get(`/api/exports/${jobId}/download`, { responseType: 'blob' })
    const disposition = response.headers['content-disposition'] as string | undefined
    let fileName = `export-${jobId}.zip`
    if (disposition) {
      const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i)
      const plainMatch = disposition.match(/filename="?([^";]+)"?/i)
      const encodedName = utf8Match?.[1] ?? plainMatch?.[1]
      if (encodedName) fileName = decodeURIComponent(encodedName)
    }

    const url = URL.createObjectURL(response.data as Blob)
    const link = document.createElement('a')
    link.href = url
    link.download = fileName
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  return {
    records,
    jobs,
    selectedIds,
    loading,
    exporting,
    error,
    loadRecords,
    loadJobs,
    createExport,
    restartExport,
    refreshJob,
    downloadJob,
  }
})

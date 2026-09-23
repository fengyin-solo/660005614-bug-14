export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }
export interface ExecutionInfo { workflow: DAGWorkflow; logs: ExecutionLog[]; circuitBreakers: CircuitBreaker[]; completed: boolean }

export interface DetailRecord {
  id: number
  taskId: string
  taskName: string
  status: 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED'
  duration: number
  detail: string
  startedAt: string | null
  finishedAt: string | null
  createdAt: string | null
}

export type ExportStatus = 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED'

export interface ExportJob {
  id: number
  recordIds: number[]
  recordCount: number
  status: ExportStatus
  fileName: string | null
  filePath: string | null
  checksum: string | null
  fileSize: number | null
  error: string | null
  attempts: number
  createdAt: string | null
  updatedAt: string | null
}

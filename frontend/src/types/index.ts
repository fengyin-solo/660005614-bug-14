export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }

/** 明细记录：页面明细列表与导出文件共用同一份数据结构 */
export interface ExecutionRecord {
  id: string
  taskId: string
  status: string
  timestamp: number
  message: string
}

export interface ExecutionInfo {
  executionId?: string
  workflow: DAGWorkflow
  records?: ExecutionRecord[]
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  completed: boolean
  status?: string
}

export interface ExecutionSummary {
  id: string
  name: string
  status: string
  workers?: number
  startedAt: number
  finishedAt: number | null
  recordCount: number
}

export interface ExecutionDetail {
  id: string
  workflowId: number
  name: string
  status: string
  workers: number
  strategy: string
  startedAt: number
  finishedAt: number | null
  records: ExecutionRecord[]
  workflow: DAGWorkflow
}

export interface ArchiveInfo {
  id: string
  executionId: string
  fileName: string
  recordIds: string[]
  count: number
  size: number
  createdAt: number
}

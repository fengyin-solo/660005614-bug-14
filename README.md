# 分布式任务工作流DAG编排与执行引擎

基于 Vue 3 + FastAPI 的任务编排平台，支持 DAG 拓扑排序、任务状态机、多 Worker 并发池、实时执行监控，以及执行明细的单条/批量导出归档。

## 目标用户
数据工程师、ETL/ML Pipeline 开发者、技术架构师

## 技术栈
- 前端: Vue 3 + TypeScript + Vite + Pinia + Element Plus + ECharts
- 后端: Python FastAPI + SQLite + WebSocket（归档使用标准库 zipfile/csv）

## 核心功能
1. DAG 工作流编辑器：拖拽添加任务节点、连线建立依赖关系、BFS 拓扑排序验证环检测
2. Spring StateMachine 风格任务状态机：PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT
3. 多 Worker 并发池模拟：可配置 Worker 数量、任务执行耗时模拟
4. 任务编排策略：FIFO/优先级/最大并发三种调度策略
5. 重试机制：可配置最大重试次数、指数退避延迟
6. 执行监控：实时 WebSocket 推送任务状态
7. 熔断保护：连续失败阈值触发熔断，冷却时间后自动恢复
8. 明细导出与归档：
   - 单条导出与多选批量导出共用同一后端取数和打包逻辑
   - 归档固定包含 `records.csv`、`manifest.json` 和每条明细的 JSON
   - manifest 记录本次归档的完整 ID 列表和条数，用于与页面勾选范围逐项核对
   - 每个任务使用独立临时目录，成功后原子发布 zip，避免混入上次文件或半成品
   - 同一组已成功归档的 ID 重复导出时复用历史归档，不再生成两份同名文件
   - 失败任务保留错误信息，可从原范围重新开始
   - 归档元数据持久化在 SQLite，zip 保存在 `backend/data/archives/`，历史文件可继续下载

## 导出接口
- `GET /api/records`：获取页面明细，按 ID 升序
- `POST /api/exports`：请求导出，body 为 `{ "recordIds": [1, 2, 3] }`
- `GET /api/exports`：查看历史归档任务
- `GET /api/exports/{id}`：查看任务状态和文件信息
- `POST /api/exports/{id}/restart`：重新开始失败任务
- `GET /api/exports/{id}/download`：下载已成功归档的 zip

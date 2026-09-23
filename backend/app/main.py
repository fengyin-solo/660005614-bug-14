import asyncio
import json
import os
import random
import re
import threading
import time
import tempfile
import uuid
import zipfile
import hashlib
from collections import defaultdict, deque
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS: List[WebSocket] = []
WORKFLOW_ID = 0

# 执行记录存储（明细列表的唯一数据源，页面与导出共用）
EXECUTIONS: Dict[str, dict] = {}

# 归档存储：data/archives 下持久化 zip + 元数据，重启后可继续下载
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(BASE_DIR, "data", "archives")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

ARCHIVES: Dict[str, dict] = {}      # archiveId -> 元数据
ARCHIVE_KEY_INDEX: Dict[str, str] = {}  # 范围指纹 -> archiveId（幂等：同一范围重复导出复用同一归档）
EXPORT_LOCK = threading.Lock()

# WebSocket 推送专用事件循环（execute_workflow 跑在普通线程里，不能用 asyncio.get_event_loop）
WS_LOOP = asyncio.new_event_loop()
threading.Thread(target=WS_LOOP.run_forever, daemon=True).start()


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"


class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


class ExportRequest(BaseModel):
    executionId: str
    recordIds: List[str]   # 单条导出就是长度为 1 的列表，批量/单条走完全相同的路径


def generate_dag_workflow(name: str):
    """Create a realistic DAG pipeline"""
    nodes = [
        {"id": "extract", "name": "数据提取", "deps": [], "duration": 2.0},
        {"id": "validate", "name": "数据校验", "deps": ["extract"], "duration": 1.5},
        {"id": "clean_a", "name": "清洗分支A", "deps": ["validate"], "duration": 1.8},
        {"id": "clean_b", "name": "清洗分支B", "deps": ["validate"], "duration": 1.2},
        {"id": "transform", "name": "数据转换", "deps": ["clean_a"], "duration": 3.0},
        {"id": "enrich", "name": "数据增强", "deps": ["clean_a", "clean_b"], "duration": 2.0},
        {"id": "aggregate", "name": "聚合计算", "deps": ["transform", "enrich"], "duration": 2.5},
        {"id": "quality", "name": "质量检查", "deps": ["aggregate"], "duration": 1.0},
        {"id": "export_db", "name": "入库", "deps": ["quality"], "duration": 1.8},
        {"id": "export_report", "name": "报表生成", "deps": ["quality"], "duration": 2.2},
        {"id": "notify", "name": "通知", "deps": ["export_db", "export_report"], "duration": 0.5},
    ]
    positions = [
        (0, 0), (0, 1), (-1, 2), (1, 2), (-1, 3),
        (0.5, 3), (-0.3, 4), (-0.3, 5), (-1, 6), (0.5, 6), (-0.3, 7)
    ]
    for i, n in enumerate(nodes):
        n["x"] = positions[i][0] * 2.5 + 2.5
        n["y"] = positions[i][1] * 0.9
        n["status"] = "PENDING"
        n["retries"] = 0
        n["startTime"] = None
        n["endTime"] = None

    edges = []
    for n in nodes:
        for d in n["deps"]:
            edges.append([d, n["id"]])

    return {"nodes": [{
        "id": n["id"], "name": n["name"], "deps": n["deps"],
        "x": n["x"], "y": n["y"], "status": n["status"],
        "startTime": None, "endTime": None, "retries": n["retries"]
    } for n in nodes], "edges": edges, "durations": {n["id"]: n["duration"] for n in nodes}}


@app.on_event("startup")
def _recover_archives():
    """服务重启后从磁盘恢复历史归档，保证仍可下载。"""
    for fname in os.listdir(ARCHIVE_DIR):
        if not fname.endswith(".meta.json"):
            continue
        try:
            with open(os.path.join(ARCHIVE_DIR, fname), "r", encoding="utf-8") as f:
                meta = json.load(f)
            if not os.path.exists(_archive_zip_path(meta["id"])):
                continue
            ARCHIVES[meta["id"]] = meta
            ARCHIVE_KEY_INDEX[meta["scopeKey"]] = meta["id"]
        except (json.JSONDecodeError, KeyError, OSError):
            continue


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


@app.post("/api/run")
def run_workflow(req: RunRequest):
    dag = generate_dag_workflow("workflow")
    exec_id = f"exec_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    EXECUTIONS[exec_id] = {
        "id": exec_id,
        "workflowId": req.workflowId,
        "name": "workflow",
        "status": "RUNNING",
        "workers": req.workers,
        "strategy": req.strategy,
        "startedAt": time.time(),
        "finishedAt": None,
        "records": [],
        "workflow": {"id": req.workflowId, "name": "workflow",
                     "nodes": dag["nodes"], "edges": dag["edges"]},
    }
    t = threading.Thread(target=execute_workflow, args=(exec_id, dag, req.workers, req.strategy), daemon=True)
    t.start()
    return {
        "executionId": exec_id,
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "records": [], "logs": [], "circuitBreakers": [], "completed": False
    }


def build_records(logs):
    """日志明细列表 -> 带稳定 ID 的记录（页面与导出的共同数据源）。"""
    return [{"id": f"r{i + 1:02d}",
             "taskId": l["taskId"], "status": l["status"],
             "timestamp": l["timestamp"], "message": l["message"]}
            for i, l in enumerate(logs)]


def execute_workflow(exec_id, dag, workers, strategy):
    execution = EXECUTIONS[exec_id]
    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)

    # BFS topological sort
    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    node_map = {n["id"]: n for n in nodes}
    logs = []
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0})
    failure_threshold = 3
    running_tasks = {}
    completed = set()
    failed = set()

    def send_update(completed_flag=False):
        records = build_records(list(logs))
        execution["records"] = records
        status = "FAILED" if failed else ("SUCCESS" if completed_flag else "RUNNING")
        if completed_flag:
            execution["status"] = status
            execution["finishedAt"] = time.time()
        payload = {
            "executionId": exec_id,
            "workflow": execution["workflow"],
            "records": records,
            "logs": list(logs),
            "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
            "completed": completed_flag,
            "status": execution["status"],
        }
        dead = []
        for ws in ACTIVE_CLIENTS:
            try:
                asyncio.run_coroutine_threadsafe(ws.send_text(json.dumps(payload)), WS_LOOP)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in ACTIVE_CLIENTS:
                ACTIVE_CLIENTS.remove(ws)
        time.sleep(0.3)

    while ready or running_tasks:
        # Start tasks
        while ready and len(running_tasks) < workers:
            tid = ready.popleft()
            node = node_map[tid]
            cb = cb_state[tid]
            if cb["state"] == "OPEN" and time.time() < cb["cooldownUntil"]:
                ready.appendleft(tid)
                break
            if cb["state"] == "OPEN":
                cb["state"] = "HALF_OPEN"

            node["status"] = "RUNNING"
            node["startTime"] = time.time()

            # Simulate task execution (random success/failure)
            will_fail = random.random() < 0.12  # 12% failure rate
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {
                "end_time": time.time() + runtime,
                "will_fail": will_fail,
                "retries": node["retries"]
            }
            logs.append({"taskId": tid, "status": "RUNNING", "timestamp": time.time(), "message": f"开始执行 {node['name']}"})

        # Check completed tasks
        now = time.time()
        finished = []
        for tid, info in running_tasks.items():
            if now >= info["end_time"]:
                node = node_map[tid]
                if info["will_fail"] and node["retries"] < 3:
                    node["retries"] += 1
                    node["status"] = "PENDING"
                    ready.appendleft(tid)
                    cb = cb_state[tid]
                    cb["failureCount"] += 1
                    logs.append({"taskId": tid, "status": "FAILED", "timestamp": now, "message": f"重试 {node['retries']}/3"})
                    if cb["failureCount"] >= failure_threshold:
                        cb["state"] = "OPEN"
                        cb["cooldownUntil"] = now + 5
                        logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now, "message": f"熔断! {failure_threshold}次连续失败"})
                else:
                    # 重试 3 次仍失败 -> FAILED 终态；成功 -> SUCCESS。两种终态都释放下游
                    if info["will_fail"]:
                        node["status"] = "FAILED"
                        failed.add(tid)
                        logs.append({"taskId": tid, "status": "FAILED", "timestamp": now, "message": f"失败 {node['name']}（重试已耗尽）"})
                    else:
                        node["status"] = "SUCCESS"
                        logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now, "message": f"完成 {node['name']}"})
                    node["endTime"] = now
                    completed.add(tid)
                    cb_state[tid]["failureCount"] = 0
                    cb_state[tid]["state"] = "CLOSED"
                    for next_tid in adj[tid]:
                        in_degree[next_tid] -= 1
                        if in_degree[next_tid] == 0:
                            ready.append(next_tid)
                finished.append(tid)

        for tid in finished:
            del running_tasks[tid]

        send_update()
        if len(completed) == len(nodes):
            break

    send_update(True)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    try:
        while True:
            await ws.receive_text()
    except Exception:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)


# ------------------------- 执行明细（页面明细列表的数据源） -------------------------

@app.get("/api/executions")
def list_executions():
    items = [{
        "id": e["id"],
        "name": e["name"],
        "status": e["status"],
        "workers": e.get("workers"),
        "startedAt": e["startedAt"],
        "finishedAt": e["finishedAt"],
        "recordCount": len(e["records"]),
    } for e in EXECUTIONS.values()]
    items.sort(key=lambda x: x["startedAt"], reverse=True)
    return items


@app.get("/api/executions/{exec_id}")
def get_execution(exec_id: str):
    execution = EXECUTIONS.get(exec_id)
    if not execution:
        raise HTTPException(status_code=404, detail="执行记录不存在")
    return execution


# ------------------------- 导出与归档 -------------------------

def _archive_zip_path(archive_id: str) -> str:
    return os.path.join(ARCHIVE_DIR, f"{archive_id}.zip")


def _archive_meta_path(archive_id: str) -> str:
    return os.path.join(ARCHIVE_DIR, f"{archive_id}.meta.json")


def _scope_key(exec_id: str, record_ids: List[str]) -> str:
    """同一执行 + 同一组明细 -> 同一指纹（与选择顺序无关）。"""
    h = hashlib.sha256()
    h.update(exec_id.encode("utf-8"))
    for rid in sorted(record_ids):
        h.update(b"\x00")
        h.update(rid.encode("utf-8"))
    return h.hexdigest()


_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]")


def _archive_filename(exec_id: str, record_ids: List[str], key: str) -> str:
    """文件名如实反映勾选范围：条数 + 首末编号 + 内容指纹。"""
    safe_exec = _SAFE_RE.sub("_", exec_id)
    if len(record_ids) == 1:
        scope = record_ids[0]
    else:
        scope = f"{len(record_ids)}records_{record_ids[0]}-{record_ids[-1]}"
    return f"{safe_exec}__{scope}__{key[:8]}.zip"


def _record_file_bytes(exec_id: str, record: dict) -> bytes:
    payload = {"executionId": exec_id, "record": record}
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


@app.post("/api/exports")
def create_export(req: ExportRequest):
    """批量/单条导出的唯一入口：勾选哪些明细就导出哪些，不多不少。

    - 每条明细在 zip 内对应一个独立 JSON 文件，单条导出与批量导出内容逐字节一致
    - 同一执行重复导出同一范围：复用已有归档，不会再生成一份同名文件
    - 失败（参数非法/执行不存在等）不产生任何残留文件，可直接重新发起
    """
    if not req.recordIds:
        raise HTTPException(status_code=400, detail="请至少勾选一条明细")
    if len(req.recordIds) != len(set(req.recordIds)):
        raise HTTPException(status_code=400, detail="勾选范围中存在重复明细")

    key = _scope_key(req.executionId, req.recordIds)

    with EXPORT_LOCK:
        # 幂等优先：即使服务重启导致内存中的执行快照丢失，同一范围仍复用既有归档
        existing_id = ARCHIVE_KEY_INDEX.get(key)
        if existing_id:
            meta = ARCHIVES[existing_id]
            return {"archive": meta, "reused": True}

        execution = EXECUTIONS.get(req.executionId)
        if not execution:
            raise HTTPException(status_code=404, detail="执行记录不存在")
        if execution["status"] == "RUNNING":
            raise HTTPException(status_code=409, detail="执行尚未完成，请完成后再导出")

        by_id = {r["id"]: r for r in execution["records"]}
        unknown = [rid for rid in req.recordIds if rid not in by_id]
        if unknown:
            raise HTTPException(status_code=400, detail=f"明细不存在: {', '.join(unknown)}")

        records = [by_id[rid] for rid in req.recordIds]
        archive_id = f"arc_{key[:12]}"
        file_name = _archive_filename(req.executionId, req.recordIds, key)
        now = time.time()

        # 每次导出使用全新的临时目录，绝不复用上一次的文件；失败自动整体清理
        try:
            with tempfile.TemporaryDirectory(dir=ARCHIVE_DIR) as tmp_dir:
                tmp_zip = os.path.join(tmp_dir, file_name)
                with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                    for record in records:
                        zf.writestr(f"{record['id']}.json", _record_file_bytes(req.executionId, record))
                    manifest = {
                        "executionId": req.executionId,
                        "exportedAt": now,
                        "count": len(records),
                        "records": [{"id": r["id"], "taskId": r["taskId"], "status": r["status"],
                                     "timestamp": r["timestamp"], "message": r["message"]}
                                    for r in records],
                    }
                    zf.writestr("manifest.json",
                                json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
                size = os.path.getsize(tmp_zip)
                # 原子落位，避免半成品被登记进归档
                os.replace(tmp_zip, _archive_zip_path(archive_id))
        except (OSError, zipfile.BadZipFile) as exc:
            raise HTTPException(status_code=500, detail=f"导出失败，请重试: {exc}") from exc

        meta = {
            "id": archive_id,
            "executionId": req.executionId,
            "fileName": file_name,
            "recordIds": list(req.recordIds),
            "count": len(req.recordIds),
            "size": size,
            "createdAt": now,
            "scopeKey": key,
        }
        ARCHIVES[archive_id] = meta
        ARCHIVE_KEY_INDEX[key] = archive_id
        _persist_meta(meta)
        return {"archive": meta, "reused": False}


def _persist_meta(meta: dict):
    path = _archive_meta_path(meta["id"])
    fd, tmp_path = tempfile.mkstemp(dir=ARCHIVE_DIR, suffix=".meta.tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
        os.replace(tmp_path, path)
    except OSError:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


@app.get("/api/archives")
def list_archives():
    items = sorted(ARCHIVES.values(), key=lambda m: m["createdAt"], reverse=True)
    return list(items)


@app.get("/api/archives/{archive_id}/download")
def download_archive(archive_id: str):
    meta = ARCHIVES.get(archive_id)
    if not meta:
        raise HTTPException(status_code=404, detail="归档不存在")
    path = _archive_zip_path(archive_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=410, detail="归档文件已从磁盘移除")
    return FileResponse(path, media_type="application/zip", filename=meta["fileName"])

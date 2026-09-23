import asyncio
import csv
import hashlib
import io
import json
import random
import shutil
import sqlite3
import threading
import time
import zipfile
from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
ARCHIVE_DIR = DATA_DIR / "archives"
DB_PATH = DATA_DIR / "workflow.db"

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0
EXPORT_JOB_LOCK = threading.RLock()

SEED_RECORDS = [
    ("extract", "数据提取", "SUCCESS", 2.1, "输入表 1,200 行"),
    ("validate", "数据校验", "SUCCESS", 1.4, "校验规则 8 条"),
    ("clean_a", "清洗分支A", "SUCCESS", 1.9, "空值 12 个已填充"),
    ("clean_b", "清洗分支B", "FAILED", 1.1, "分区 2026-09-22 读取失败"),
    ("transform", "数据转换", "SUCCESS", 3.2, "输出字段 24 个"),
    ("enrich", "数据增强", "RUNNING", 2.0, "正在关联地区维表"),
    ("aggregate", "聚合计算", "PENDING", 2.5, "等待上游数据"),
    ("quality", "质量检查", "PENDING", 1.0, "等待执行"),
    ("export_db", "入库", "PENDING", 1.8, "目标：analytics.daily_summary"),
    ("export_report", "报表生成", "PENDING", 2.2, "模板：daily_report.xlsx"),
    ("notify", "通知", "PENDING", 0.5, "接收人：数据平台组"),
]


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"


class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


class ExportRequest(BaseModel):
    recordIds: List[int]


class RestartExportRequest(BaseModel):
    pass


def now_text() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def iso_time(ts: Optional[float]) -> Optional[str]:
    if not ts:
        return None
    return datetime.fromtimestamp(ts).isoformat(timespec="seconds")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_session():
    db = get_db()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    with db_session() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL UNIQUE,
                task_name TEXT NOT NULL,
                status TEXT NOT NULL,
                duration REAL NOT NULL DEFAULT 0,
                detail TEXT NOT NULL,
                started_at REAL,
                finished_at REAL,
                created_at REAL NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL UNIQUE,
                record_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                file_name TEXT,
                file_path TEXT,
                checksum TEXT,
                file_size INTEGER,
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        count = db.execute("SELECT COUNT(*) AS count FROM records").fetchone()["count"]
        if count == 0:
            base = time.time() - 3600
            for index, (task_id, name, status, duration, detail) in enumerate(SEED_RECORDS):
                started = base + index * 60
                finished = started + duration if status == "SUCCESS" else None
                db.execute(
                    """
                    INSERT INTO records
                        (task_id, task_name, status, duration, detail, started_at, finished_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (task_id, name, status, duration, detail, started, finished, started),
                )


@app.on_event("startup")
def startup() -> None:
    init_db()


def row_to_record(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "taskId": row["task_id"],
        "taskName": row["task_name"],
        "status": row["status"],
        "duration": row["duration"],
        "detail": row["detail"],
        "startedAt": iso_time(row["started_at"]),
        "finishedAt": iso_time(row["finished_at"]),
        "createdAt": iso_time(row["created_at"]),
    }


def row_to_export(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "recordIds": json.loads(row["scope_key"]),
        "recordCount": row["record_count"],
        "status": row["status"],
        "fileName": row["file_name"],
        "filePath": row["file_path"],
        "checksum": row["checksum"],
        "fileSize": row["file_size"],
        "error": row["error"],
        "attempts": row["attempts"],
        "createdAt": iso_time(row["created_at"]),
        "updatedAt": iso_time(row["updated_at"]),
    }


def get_records_by_ids(db: sqlite3.Connection, record_ids: List[int]) -> List[sqlite3.Row]:
    ordered_ids = list(dict.fromkeys(record_ids))
    placeholders = ",".join("?" for _ in ordered_ids)
    rows = db.execute(
        f"SELECT * FROM records WHERE id IN ({placeholders}) ORDER BY id",
        ordered_ids,
    ).fetchall()
    return rows


def validate_record_ids(db: sqlite3.Connection, record_ids: List[int]) -> List[int]:
    normalized = sorted(set(record_ids))
    rows = get_records_by_ids(db, normalized)
    found = [row["id"] for row in rows]
    missing = [record_id for record_id in normalized if record_id not in found]
    if missing:
        raise HTTPException(status_code=400, detail=f"明细不存在: {missing}")
    return found


def update_export(db: sqlite3.Connection, export_id: int, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    assignments = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [export_id]
    db.execute(f"UPDATE exports SET {assignments} WHERE id = ?", values)


def export_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": record["id"],
        "taskId": record["taskId"],
        "taskName": record["taskName"],
        "status": record["status"],
        "duration": record["duration"],
        "detail": record["detail"],
        "startedAt": record["startedAt"],
        "finishedAt": record["finishedAt"],
    }


def build_archive(export_id: int, rows: List[sqlite3.Row]) -> Dict[str, Any]:
    """Write into an isolated temp directory, then publish the zip atomically."""
    records = [row_to_record(row) for row in rows]
    first_id = records[0]["id"]
    last_id = records[-1]["id"]
    if len(records) == 1:
        base_name = f"export_{export_id}_detail_{first_id}_{now_text()}"
    else:
        contiguous = last_id - first_id + 1 == len(records)
        id_part = f"{first_id}-{last_id}" if contiguous else "ids-" + "-".join(str(record["id"]) for record in records)
        base_name = f"export_{export_id}_{id_part}_{len(records)}rows_{now_text()}"
    file_name = f"{base_name}.zip"

    job_tmp = DATA_DIR / "tmp" / f"export_{export_id}_{time.time_ns()}"
    if job_tmp.exists():
        shutil.rmtree(job_tmp)
    job_tmp.mkdir(parents=True, exist_ok=False)
    target_path = ARCHIVE_DIR / file_name

    try:
        records_csv = io.StringIO()
        writer = csv.DictWriter(
            records_csv,
            fieldnames=["id", "taskId", "taskName", "status", "duration", "detail", "startedAt", "finishedAt"],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(export_payload(record))

        manifest = {
            "exportId": export_id,
            "generatedAt": datetime.now().isoformat(timespec="seconds"),
            "recordCount": len(records),
            "recordIds": [record["id"] for record in records],
            "files": ["records.csv"] + [f"records/{record['id']}.json" for record in records],
        }

        zip_tmp = job_tmp / file_name
        with zipfile.ZipFile(zip_tmp, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("records.csv", records_csv.getvalue())
            archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
            for record in records:
                archive.writestr(
                    f"records/{record['id']}.json",
                    json.dumps(export_payload(record), ensure_ascii=False, indent=2),
                )

        # Atomic move prevents partial archives or files from a previous attempt being visible.
        zip_tmp.replace(target_path)
        checksum = f"sha256:{file_sha256(target_path)}"
        return {
            "file_name": file_name,
            "file_path": str(target_path),
            "checksum": checksum,
            "file_size": target_path.stat().st_size,
            "error": None,
        }
    finally:
        shutil.rmtree(job_tmp, ignore_errors=True)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_export(export_id: int) -> None:
    try:
        with EXPORT_JOB_LOCK, db_session() as db:
            export_row = db.execute("SELECT * FROM exports WHERE id = ?", (export_id,)).fetchone()
            if export_row is None or export_row["status"] != "RUNNING":
                return
            record_ids = json.loads(export_row["scope_key"])
            rows = get_records_by_ids(db, record_ids)
            valid_ids = [row["id"] for row in rows]
            if valid_ids != record_ids:
                raise RuntimeError("勾选的明细在导出期间发生变化，请重新选择后再导出")

        result = build_archive(export_id, rows)

        with EXPORT_JOB_LOCK, db_session() as db:
            current = db.execute("SELECT status FROM exports WHERE id = ?", (export_id,)).fetchone()
            if current and current["status"] == "RUNNING":
                update_export(db, export_id, status="SUCCESS", **result)
    except Exception as exc:  # A failed job must remain visible and be restartable.
        with EXPORT_JOB_LOCK, db_session() as db:
            current = db.execute("SELECT status FROM exports WHERE id = ?", (export_id,)).fetchone()
            if current and current["status"] == "RUNNING":
                update_export(
                    db,
                    export_id,
                    status="FAILED",
                    error=str(exc),
                    file_name=None,
                    file_path=None,
                    checksum=None,
                    file_size=None,
                )


def create_or_reuse_export(record_ids: List[int], restart_row_id: Optional[int] = None) -> Dict[str, Any]:
    if not record_ids:
        raise HTTPException(status_code=400, detail="请至少选择一条明细")
    with EXPORT_JOB_LOCK, db_session() as db:
        normalized = validate_record_ids(db, record_ids)
        scope_key = json.dumps(normalized, separators=(",", ":"))
        now = time.time()

        if restart_row_id is not None:
            row = db.execute("SELECT * FROM exports WHERE id = ?", (restart_row_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="归档任务不存在")
            if row["status"] == "RUNNING":
                raise HTTPException(status_code=409, detail="归档任务正在执行")
            if row["status"] == "SUCCESS":
                raise HTTPException(status_code=409, detail="归档已成功生成，可直接下载")
            if json.loads(row["scope_key"]) != normalized:
                raise HTTPException(status_code=400, detail="重新开始时不能改变原来的勾选范围")
            update_export(
                db,
                restart_row_id,
                status="RUNNING",
                error=None,
                attempts=row["attempts"] + 1,
                updated_at=now,
            )
            export_id = restart_row_id
        else:
            row = db.execute("SELECT * FROM exports WHERE scope_key = ?", (scope_key,)).fetchone()
            if row is not None and row["status"] in ("SUCCESS", "RUNNING", "PENDING"):
                return row_to_export(row)

            if row is None:
                cursor = db.execute(
                    """
                    INSERT INTO exports
                        (scope_key, record_count, status, attempts, created_at, updated_at)
                    VALUES (?, ?, 'RUNNING', 1, ?, ?)
                    """,
                    (scope_key, len(normalized), now, now),
                )
                export_id = cursor.lastrowid
            else:
                update_export(
                    db,
                    row["id"],
                    status="RUNNING",
                    record_count=len(normalized),
                    error=None,
                    file_name=None,
                    file_path=None,
                    checksum=None,
                    file_size=None,
                    attempts=row["attempts"] + 1,
                )
                export_id = row["id"]

    thread = threading.Thread(target=run_export, args=(export_id,), daemon=True)
    thread.start()

    with EXPORT_JOB_LOCK, db_session() as db:
        return row_to_export(db.execute("SELECT * FROM exports WHERE id = ?", (export_id,)).fetchone())


@app.get("/api/records")
def list_records():
    with db_session() as db:
        rows = db.execute("SELECT * FROM records ORDER BY id").fetchall()
    return [row_to_record(row) for row in rows]


@app.post("/api/exports")
def create_export(req: ExportRequest):
    return create_or_reuse_export(req.recordIds)


@app.get("/api/exports")
def list_exports():
    with db_session() as db:
        rows = db.execute("SELECT * FROM exports ORDER BY id DESC").fetchall()
    return [row_to_export(row) for row in rows]


@app.get("/api/exports/{export_id}")
def get_export(export_id: int):
    with db_session() as db:
        row = db.execute("SELECT * FROM exports WHERE id = ?", (export_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="归档任务不存在")
    return row_to_export(row)


@app.post("/api/exports/{export_id}/restart")
def restart_export(export_id: int, req: RestartExportRequest):
    with db_session() as db:
        row = db.execute("SELECT * FROM exports WHERE id = ?", (export_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="归档任务不存在")
    return create_or_reuse_export(json.loads(row["scope_key"]), restart_row_id=export_id)


@app.get("/api/exports/{export_id}/download")
def download_export(export_id: int):
    with db_session() as db:
        row = db.execute("SELECT * FROM exports WHERE id = ?", (export_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="归档任务不存在")
    if row["status"] != "SUCCESS" or not row["file_path"]:
        raise HTTPException(status_code=409, detail="归档尚未成功生成")

    path = Path(row["file_path"])
    if not path.exists():
        raise HTTPException(status_code=410, detail="历史归档文件已不存在")
    encoded_name = quote(row["file_name"])
    headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_name}"}
    return FileResponse(path, media_type="application/zip", headers=headers)


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
    t = threading.Thread(target=execute_workflow, args=(dag, req.workers, req.strategy), daemon=True)
    t.start()
    return {
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "completed": False
    }


def execute_workflow(dag, workers, strategy):
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
    loop = asyncio.new_event_loop()
    sender_thread = threading.Thread(target=loop.run_forever, daemon=True)
    sender_thread.start()

    def send_update(completed_flag=False):
        payload = {
            "workflow": {"id": 1, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-30:],
            "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
            "completed": completed_flag
        }
        for ws in ACTIVE_CLIENTS:
            try: asyncio.run_coroutine_threadsafe(ws.send_text(json.dumps(payload)), loop)
            except: pass
        time.sleep(0.3)

    try:
        while ready or running_tasks:
            # Start tasks
            while ready and len(running_tasks) < workers:
                tid = ready.popleft()
                node = node_map[tid]
                cb = cb_state[tid]
                if cb["state"] == "OPEN" and time.time() < cb["cooldownUntil"]:
                    ready.appendleft(tid)
                    continue
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
                        node["status"] = "SUCCESS"
                        node["endTime"] = now
                        completed.add(tid)
                        cb_state[tid]["failureCount"] = 0
                        cb_state[tid]["state"] = "CLOSED"
                        logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now, "message": f"完成 {node['name']}"})
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
    finally:
        send_update(True)
        loop.call_soon_threadsafe(loop.stop)
        sender_thread.join(timeout=1)
        if not loop.is_closed():
            loop.close()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        if ws in ACTIVE_CLIENTS: ACTIVE_CLIENTS.remove(ws)
    except Exception:
        if ws in ACTIVE_CLIENTS: ACTIVE_CLIENTS.remove(ws)

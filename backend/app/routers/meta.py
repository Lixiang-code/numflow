"""元信息 API（对齐文档 /meta 与工具 get_project_config 等）。"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.data.default_rules_02 import get_default_rules_payload
from app.deps import ProjectDB, get_project_read, get_project_write
from app.services.snapshot_ops import compare_snapshot, create_snapshot, list_snapshots

router = APIRouter(prefix="/meta", tags=["meta"])


@router.get("/project-config")
def get_project_config(p: ProjectDB = Depends(get_project_read)):
    conn = p.conn
    cur = conn.execute("SELECT key, value_json FROM project_settings")
    settings: Dict[str, Any] = {}
    for k, v in cur.fetchall():
        try:
            settings[k] = json.loads(v)
        except json.JSONDecodeError:
            settings[k] = v
    return {"project": dict(p.row), "settings": settings, "can_write": p.can_write}


@router.get("/constants")
def get_constants(p: ProjectDB = Depends(get_project_read)):
    """返回项目中所有常量及标签。

    输出结构：
        {
            "constants": [ {name_en,name_zh,value,brief,scope_table,tags[]} ... ],
            "tags": [ {name,parent,brief} ... ]
        }
    """
    conn = p.conn
    constants: List[Dict[str, Any]] = []
    try:
        cur = conn.execute(
            "SELECT name_en, name_zh, value_json, brief, scope_table, "
            "COALESCE(tags, '[]') AS tags FROM _constants ORDER BY name_en"
        )
        for r in cur.fetchall():
            try:
                v = json.loads(r["value_json"])
            except Exception:  # noqa: BLE001
                v = None
            try:
                tags = json.loads(r["tags"]) if r["tags"] else []
                if not isinstance(tags, list):
                    tags = []
            except Exception:  # noqa: BLE001
                tags = []
            constants.append(
                {
                    "name_en": r["name_en"],
                    "name_zh": r["name_zh"],
                    "value": v,
                    "brief": r["brief"],
                    "scope_table": r["scope_table"],
                    "tags": tags,
                }
            )
    except Exception:  # noqa: BLE001
        constants = []

    tags: List[Dict[str, Any]] = []
    try:
        cur_t = conn.execute(
            "SELECT name, parent, brief FROM _const_tags ORDER BY name"
        )
        for r in cur_t.fetchall():
            tags.append(
                {"name": r["name"], "parent": r["parent"], "brief": r["brief"]}
            )
    except Exception:  # noqa: BLE001
        tags = []

    return {"constants": constants, "tags": tags}


@router.get("/tables")
def get_table_list(p: ProjectDB = Depends(get_project_read)):
    cur = p.conn.execute(
        "SELECT table_name, layer, purpose, validation_status, schema_json FROM _table_registry ORDER BY table_name"
    )
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        sj = d.pop("schema_json", None) or "{}"
        try:
            parsed = json.loads(sj) if isinstance(sj, str) else {}
        except json.JSONDecodeError:
            parsed = {}
        d["display_name"] = (parsed.get("display_name") if isinstance(parsed, dict) else "") or ""
        rows.append(d)
    return {"tables": rows}


@router.get("/tables/{table_name}/readme")
def get_table_readme(table_name: str, p: ProjectDB = Depends(get_project_read)):
    cur = p.conn.execute(
        "SELECT readme FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="未知表")
    return {"table_name": table_name, "readme": row["readme"] or ""}


@router.get("/dependency-graph")
def get_dependency_graph(
    table_name: Optional[str] = Query(None),
    direction: str = Query("full", pattern="^(upstream|downstream|full)$"),
    p: ProjectDB = Depends(get_project_read),
):
    conn = p.conn
    if table_name:
        if direction == "upstream":
            cur = conn.execute(
                """
                SELECT * FROM _dependency_graph
                WHERE to_table = ?
                """,
                (table_name,),
            )
        elif direction == "downstream":
            cur = conn.execute(
                """
                SELECT * FROM _dependency_graph
                WHERE from_table = ?
                """,
                (table_name,),
            )
        else:
            cur = conn.execute(
                """
                SELECT * FROM _dependency_graph
                WHERE from_table = ? OR to_table = ?
                """,
                (table_name, table_name),
            )
    else:
        cur = conn.execute("SELECT * FROM _dependency_graph")
    return {"edges": [dict(r) for r in cur.fetchall()]}


class ReadmeBody(BaseModel):
    content: str


@router.put("/tables/{table_name}/readme")
def update_table_readme(
    table_name: str,
    body: ReadmeBody,
    p: ProjectDB = Depends(get_project_write),
):
    conn = p.conn
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    conn.execute(
        "UPDATE _table_registry SET readme = ? WHERE table_name = ?",
        (body.content, table_name),
    )
    conn.commit()
    return {"ok": True}


class GlobalReadmeBody(BaseModel):
    content: str


@router.put("/global-readme")
def update_global_readme(body: GlobalReadmeBody, p: ProjectDB = Depends(get_project_write)):
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn = p.conn
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES ('global_readme', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (json.dumps({"text": body.content}, ensure_ascii=False), now),
    )
    conn.commit()
    return {"ok": True}


@router.get("/default-rules")
def get_default_rules():
    """文档 02 子集：可机读默认规则（全局，非项目内）。"""
    return get_default_rules_payload()


class SnapshotCreateBody(BaseModel):
    label: str = Field(min_length=1, max_length=120)
    note: str = ""


@router.post("/snapshots")
def post_snapshot(body: SnapshotCreateBody, p: ProjectDB = Depends(get_project_write)):
    return create_snapshot(p.conn, label=body.label.strip(), note=body.note.strip())


@router.get("/snapshots")
def get_snapshots(p: ProjectDB = Depends(get_project_read)):
    return {"snapshots": list_snapshots(p.conn)}


@router.get("/snapshots/{snapshot_id}/compare")
def get_snapshot_compare(snapshot_id: int, p: ProjectDB = Depends(get_project_read)):
    try:
        return compare_snapshot(p.conn, snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


class ValidationRulesBody(BaseModel):
    """MVP：{ \"rules\": [ { \"id\": \"r1\", \"type\": \"not_null\", \"column\": \"atk\" } ] }"""

    rules: List[Dict[str, Any]] = Field(default_factory=list)


@router.put("/tables/{table_name}/validation-rules")
def put_validation_rules(
    table_name: str,
    body: ValidationRulesBody,
    p: ProjectDB = Depends(get_project_write),
):
    conn = p.conn
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (table_name,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    payload = json.dumps({"rules": body.rules}, ensure_ascii=False)
    conn.execute(
        "UPDATE _table_registry SET validation_rules_json = ? WHERE table_name = ?",
        (payload, table_name),
    )
    conn.commit()
    return {"ok": True}


# ─── AI 模型设置 ──────────────────────────────────────────────────────────────

_TEXT_MODEL_PREFIXES = (
    "qwen3.6", "qwen3.5", "qwen3-", "qwen2.5", "qwen-max", "qwen-plus", "qwen-turbo",
    "qwen-flash", "qwen-long", "deepseek-v", "deepseek-r1", "glm-", "kimi-k2",
    "qwq-", "qwen3-coder", "MiniMax/MiniMax-M2",
)
_TEXT_MODEL_EXCLUDES = (
    "image", "speech", "tts", "asr", "vl", "ocr", "omni", "wan", "realtime",
    "livetranslate", "translate", "qvq", "char", "mt-", "deep-research", "deep-search",
    "gui-", "s2s", "vc-", "vd-", "terminus",
)

FALLBACK_MODELS = [
    "qwen3.6-flash",
    "qwen3.6-plus",
    "qwen-turbo",
    "qwen-plus",
    "qwen-max",
    "qwen3-coder-flash",
    "deepseek-v4-flash",
    "deepseek-v4-pro",
]

# DeepSeek 固定可用模型（不通过 API 枚举，直接提供）
_DEEPSEEK_STATIC_MODELS = ["deepseek-v4-flash", "deepseek-v4-pro"]


@router.get("/ai-models")
def list_ai_models():
    """返回可用于 Agent 的文本生成模型列表，优先从 DashScope 拉取，失败则返回内置列表；DeepSeek 模型静态附加。"""
    from app.config import DASHSCOPE_API_KEY, DASHSCOPE_BASE_URL
    from openai import OpenAI
    qwen_models = []
    source = "fallback"
    error_msg = None
    try:
        client = OpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL)
        raw = list(client.models.list())
        for m in raw:
            mid = m.id
            if any(ex in mid for ex in _TEXT_MODEL_EXCLUDES):
                continue
            if any(mid.startswith(p) for p in _TEXT_MODEL_PREFIXES):
                qwen_models.append(mid)
        # 排序：qwen3.6 系列优先
        qwen_models.sort(key=lambda x: (
            0 if x.startswith("qwen3.6") else
            1 if x.startswith("qwen3.5") else
            2 if x.startswith("qwen3-") else
            3 if x.startswith("qwen-") else 4
        ))
        source = "dashscope"
    except Exception as e:  # noqa: BLE001
        qwen_models = [m for m in FALLBACK_MODELS if not m.startswith("deepseek-")]
        error_msg = str(e)

    # 合并 DeepSeek 静态模型（去重）
    models = qwen_models + [m for m in _DEEPSEEK_STATIC_MODELS if m not in qwen_models]
    result: dict = {"models": models, "source": source}
    if error_msg:
        result["error"] = error_msg
    return result


class AiModelBody(BaseModel):
    model: str = Field(min_length=1, max_length=100)


@router.get("/ai-model")
def get_ai_model(p: ProjectDB = Depends(get_project_read)):
    """获取当前项目绑定的 AI 模型（未设置则返回系统默认）。"""
    from app.config import QWEN_MODEL
    cur = p.conn.execute(
        "SELECT value_json FROM project_settings WHERE key = 'agent_model'"
    )
    row = cur.fetchone()
    if row:
        try:
            model = json.loads(row[0])
        except Exception:
            model = row[0]
    else:
        model = QWEN_MODEL
    return {"model": model}


@router.put("/ai-model")
def set_ai_model(body: AiModelBody, p: ProjectDB = Depends(get_project_write)):
    """为当前项目设置 AI 模型（持久化到 project_settings）。"""
    p.conn.execute(
        """INSERT INTO project_settings (key, value_json)
           VALUES ('agent_model', ?)
           ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json""",
        (json.dumps(body.model),),
    )
    p.conn.commit()
    return {"ok": True, "model": body.model}

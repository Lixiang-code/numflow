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
from app.services.matrix_table_ops import read_matrix as _read_matrix, list_matrix_tables as _list_matrix_tables
from app.services.recalc_lock import set_recalc_lock
from app.services.table_ops import read_3d_table as _read_3d_table

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
            "constants": [ {name_en,name_zh,value,formula,brief,design_intent,scope_table,tags[]} ... ],
            "tags": [ {name,parent,brief} ... ]
        }
    """
    conn = p.conn
    constants: List[Dict[str, Any]] = []
    try:
        cur = conn.execute(
            "SELECT name_en, name_zh, value_json, formula, brief, design_intent, scope_table, "
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
                    "formula": r["formula"],
                    "brief": r["brief"],
                    "design_intent": r["design_intent"] or "",
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


class PatchConstantBody(BaseModel):
    value: Any = Field(default=None, description="新值（与 formula 二选一；提供 value 会清除公式）")
    formula: Optional[str] = Field(default=None, description="新公式字符串（与 value 二选一）")
    brief: Optional[str] = Field(default=None, description="更新概念定义（可选）")
    design_intent: Optional[str] = Field(default=None, description="更新设计意图（可选）")


@router.patch("/constants/{name_en}")
def patch_constant(
    name_en: str,
    body: PatchConstantBody,
    p: ProjectDB = Depends(get_project_write),
):
    """更新常量值或公式。写权限保护。value 与 formula 二选一。"""
    from app.services.agent_tools import _eval_const_formula, _build_const_dep_graph, _has_const_cycle, _cascade_update_formula_consts
    from app.services.formula_engine import parse_constant_refs

    conn = p.conn
    cur = conn.execute("SELECT 1 FROM _constants WHERE name_en = ?", (name_en,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail=f"常量 {name_en} 不存在")

    formula_given = body.formula is not None
    value_given = "value" in body.model_fields_set

    if formula_given and value_given:
        raise HTTPException(status_code=400, detail="value 与 formula 不能同时提供")
    if not formula_given and not value_given:
        raise HTTPException(status_code=400, detail="value 或 formula 必填其一")

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    extra_sets = []
    extra_vals = []
    if body.brief is not None:
        extra_sets.append("brief = ?")
        extra_vals.append(body.brief)
    if body.design_intent is not None:
        extra_sets.append("design_intent = ?")
        extra_vals.append(body.design_intent)

    if formula_given:
        formula_str = body.formula.strip()  # type: ignore[union-attr]
        if not formula_str:
            raise HTTPException(status_code=400, detail="formula 不能为空")
        new_deps = parse_constant_refs(formula_str)
        dep_graph = _build_const_dep_graph(conn, exclude_name=name_en)
        if _has_const_cycle(dep_graph, name_en, new_deps):
            raise HTTPException(status_code=400, detail=f"公式常量 {name_en} 存在循环依赖")
        value, err_msg = _eval_const_formula(conn, formula_str)
        if err_msg:
            raise HTTPException(status_code=400, detail=err_msg)
        value_json = json.dumps(float(value) if isinstance(value, (int, float)) else value)
        sets = "value_json = ?, formula = ?, updated_at = ?"
        vals = [value_json, formula_str, now]
        if extra_sets:
            sets += ", " + ", ".join(extra_sets)
            vals = [value_json, formula_str] + extra_vals + [now]
        conn.execute(
            f"UPDATE _constants SET {sets} WHERE name_en = ?",
            tuple(vals) + (name_en,),
        )
    else:
        value_json = json.dumps(body.value, ensure_ascii=False)
        sets = "value_json = ?, formula = NULL, updated_at = ?"
        vals = [value_json, now]
        if extra_sets:
            sets += ", " + ", ".join(extra_sets)
            vals = [value_json] + extra_vals + [now]
        conn.execute(
            f"UPDATE _constants SET {sets} WHERE name_en = ?",
            tuple(vals) + (name_en,),
        )

    conn.commit()
    _cascade_update_formula_consts(conn)

    # 常量值变更后自动触发 DAG 重算
    recalc_result: Optional[Dict[str, Any]] = None
    recalc_warning: Optional[str] = None
    try:
        from app.services.formula_engine import parse_constant_refs
        from app.services.formula_exec import recalculate_downstream_dag
        seeds: List[tuple] = []
        for r in conn.execute(
            "SELECT table_name, column_name, formula FROM _formula_registry"
        ).fetchall():
            if name_en in parse_constant_refs(r[2]):
                seeds.append((r[0], r[1]))
        if seeds:
            recalc_result = recalculate_downstream_dag(conn, seeds, execute_seeds=True)
            # 对涉及的每个表设置 3 秒去重锁，避免前端重复请求
            now_ms = int(time.time() * 1000)
            for tbl in sorted({s[0] for s in seeds}):
                set_recalc_lock(conn, table_name=tbl, now_ms=now_ms, commit=False)
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        recalc_warning = f"常量已更新，但自动重算失败：{exc}"

    result_value = json.loads(value_json)
    out = {"ok": True, "name_en": name_en, "value": result_value, "formula": body.formula if formula_given else None}
    if recalc_result is not None:
        out["recalculate"] = recalc_result
    if recalc_warning is not None:
        out["warning"] = recalc_warning
    return out


@router.get("/tables")
def get_table_list(p: ProjectDB = Depends(get_project_read)):
    cur = p.conn.execute(
        "SELECT table_name, layer, purpose, validation_status, schema_json, directory, matrix_meta_json, "
        "table_kind, column_kinds_json "
        "FROM _table_registry ORDER BY directory, table_name"
    )
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        sj = d.pop("schema_json", None) or "{}"
        mm = d.pop("matrix_meta_json", None) or ""
        try:
            parsed = json.loads(sj) if isinstance(sj, str) else {}
        except json.JSONDecodeError:
            parsed = {}
        d["display_name"] = (parsed.get("display_name") if isinstance(parsed, dict) else "") or ""
        d["directory"] = d.get("directory") or ""
        matrix_kind = ""
        if mm:
            try:
                parsed_mm = json.loads(mm) if isinstance(mm, str) else {}
            except json.JSONDecodeError:
                parsed_mm = {}
            if isinstance(parsed_mm, dict):
                matrix_kind = str(parsed_mm.get("kind") or "")
        d["matrix_kind"] = matrix_kind
        d["is_matrix"] = bool(mm) and d.get("layer") == "matrix"
        d["is_3d_matrix"] = matrix_kind == "3d_matrix"
        rows.append(d)
    return {"tables": rows}


@router.get("/matrix/{table_name}")
def get_matrix_snapshot(
    table_name: str,
    level: Optional[int] = Query(None),
    p: ProjectDB = Depends(get_project_read),
):
    """以宽表 JSON 形式读取 matrix 表内容（前端只读视图）。"""
    try:
        return _read_matrix(p.conn, table_name=table_name, level=level)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/3d-matrix/{table_name}")
def get_three_dim_snapshot(table_name: str, p: ProjectDB = Depends(get_project_read)):
    """读取三维表快照，供前端三轴查看器渲染。"""
    try:
        return _read_3d_table(p.conn, table_name=table_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/matrix")
def list_matrix(p: ProjectDB = Depends(get_project_read)):
    return {"tables": _list_matrix_tables(p.conn)}


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
def get_default_rules(p: ProjectDB = Depends(get_project_read)):
    """读取默认规则；优先返回本项目的覆盖版本，否则返回全局硬编码默认。"""
    row = p.conn.execute(
        "SELECT value_json FROM project_settings WHERE key='default_rules_02'"
    ).fetchone()
    if row:
        try:
            return {"data": json.loads(row["value_json"]), "is_override": True}
        except Exception:  # noqa: BLE001
            pass
    return {"data": get_default_rules_payload(), "is_override": False}


@router.put("/default-rules")
def put_default_rules(body: Dict[str, Any], p: ProjectDB = Depends(get_project_write)):
    """保存本项目的默认规则覆盖版本。"""
    p.conn.execute(
        """INSERT INTO project_settings (key, value_json, updated_at)
           VALUES ('default_rules_02', ?, datetime('now'))
           ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
                                          updated_at  = excluded.updated_at""",
        (json.dumps(body),),
    )
    p.conn.commit()
    return {"ok": True}


@router.delete("/default-rules")
def delete_default_rules(p: ProjectDB = Depends(get_project_write)):
    """删除本项目的默认规则覆盖，恢复全局硬编码默认。"""
    p.conn.execute("DELETE FROM project_settings WHERE key='default_rules_02'")
    p.conn.commit()
    return {"ok": True}


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


class DirectoryBody(BaseModel):
    directory: str = Field(default="", max_length=200)


@router.put("/tables/{table_name}/directory")
def update_table_directory(
    table_name: str,
    body: DirectoryBody,
    p: ProjectDB = Depends(get_project_write),
):
    """更新表的 directory 字段（目录拖拽）。"""
    conn = p.conn
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (table_name,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    conn.execute(
        "UPDATE _table_registry SET directory = ? WHERE table_name = ?",
        (body.directory.strip(), table_name),
    )
    conn.commit()
    return {"ok": True, "table_name": table_name, "directory": body.directory.strip()}


@router.get("/calculators")
def get_calculators(p: ProjectDB = Depends(get_project_read)):
    """返回项目内所有 calculator 列表（name / kind / table / axes / brief）。"""
    from app.services.calculator_ops import list_calculators
    return {"calculators": list_calculators(p.conn)}


@router.get("/exposed-params")
def get_exposed_params(
    target_step: str = Query(..., description="目标步骤 ID"),
    p: ProjectDB = Depends(get_project_read),
):
    """返回 _step_exposed_params 中针对 target_step 的暴露参数。"""
    from app.services.agent_tools import _list_exposed_params
    result = _list_exposed_params(p.conn, target_step)
    return result


@router.get("/glossary")
def get_glossary(p: ProjectDB = Depends(get_project_read)):
    """返回项目词汇表（term_en → term_zh / term_en），用于前端 $name$ 替换。"""
    conn = p.conn
    items: List[Dict[str, Any]] = []
    try:
        cur = conn.execute(
            "SELECT term_en, term_zh FROM _glossary ORDER BY term_en"
        )
        for r in cur.fetchall():
            items.append({"term_en": r[0], "term_zh": r[1] or r[0]})
    except Exception:  # noqa: BLE001
        items = []
    return {"glossary": items}


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
    """返回可用于 Agent 的文本生成模型列表，按提供商分组。"""
    from app.config import DASHSCOPE_API_KEY, DASHSCOPE_BASE_URL, MIMO_CHAT_MODELS
    from openai import OpenAI

    # ── 通义千问 (DashScope) ──────────────────────────────────────────────
    qwen_models: List[str] = []
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

    # ── DeepSeek ──────────────────────────────────────────────────────────
    deepseek_models = list(_DEEPSEEK_STATIC_MODELS)

    # ── Mimo (小米) ───────────────────────────────────────────────────────
    mimo_models = list(MIMO_CHAT_MODELS)

    groups = [
        {"label": "Mimo（小米）", "models": mimo_models},
        {"label": "DeepSeek", "models": deepseek_models},
        {"label": "通义千问", "models": qwen_models},
    ]

    # 保留扁平 models 字段向下兼容
    flat = mimo_models + deepseek_models + qwen_models
    result: dict = {"models": flat, "groups": groups, "source": source}
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
        """INSERT INTO project_settings (key, value_json, updated_at)
           VALUES ('agent_model', ?, datetime('now'))
           ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
                                          updated_at  = excluded.updated_at""",
        (json.dumps(body.model),),
    )
    p.conn.commit()
    return {"ok": True, "model": body.model}

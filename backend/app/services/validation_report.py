"""验证报告生成（/validate 与 Agent 工具复用）。"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

from app.services.cell_writes import assert_col_or_table


# ────────────────────────── 默认规则 / 自动挂载 ──────────────────────────

# 表 kind 识别：建表方调用时显式传 kind；缺省按"启发式"识别。
TABLE_KINDS = ("base", "alloc", "attr", "quant", "landing", "resource", "unknown")


def _is_id_like(col_name: str) -> bool:
    n = col_name.lower()
    return n in ("level", "stage", "tier", "rank", "id", "row_id") or n.endswith("_id") or n.endswith("_level")


def _is_percent_like(col_name: str, number_format: str = "") -> bool:
    return "%" in str(number_format or "")


def _is_cost_perf_like(col_name: str) -> bool:
    n = col_name.lower()
    return any(tok in n for tok in ("perf", "value_per", "cost_eff", "efficiency"))


def default_rules_for(
    kind: str,
    schema_columns: Optional[List[Dict[str, Any]]] = None,
    formula_columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """根据表 kind 与列 schema 推导一组默认规则。

    schema_columns: [{name, sql_type, dtype, number_format, display_name, ...}]
    formula_columns: 已注册公式的列名列表（用于挂 formula_has_value）
    """
    cols = list(schema_columns or [])
    formula_cols = set(formula_columns or [])
    rules: List[Dict[str, Any]] = []
    seen_ids = set()

    for col in cols:
        name = str(col.get("name", ""))
        if not name or name == "row_id":
            continue
        nf = str(col.get("number_format") or "")
        sqlt = str(col.get("sql_type") or "").upper()

        if _is_id_like(name) and name not in seen_ids:
            rules.append({"id": f"id_{name}_not_empty", "type": "not_empty_id", "column": name})
            seen_ids.add(name)
            continue

        if _is_percent_like(name, nf):
            rules.append({
                "id": f"pct_{name}_bounds",
                "type": "percent_bounds",
                "column": name,
                "min": 0.0,
                "max": 0.95 if "rate" in name.lower() or "ratio" in name.lower() else 1.0,
            })
            rules.append({"id": f"fmt_{name}", "type": "format_consistency", "column": name})

        if name in formula_cols:
            rules.append({"id": f"f_{name}_has_value", "type": "formula_has_value", "column": name})

        if kind in ("quant", "landing") and _is_cost_perf_like(name):
            rules.append({
                "id": f"mono_{name}",
                "type": "monotone_warning",
                "column": name,
                "order_by": "row_id",
            })

    return rules


def attach_default_rules(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    kind: str = "unknown",
    schema_columns: Optional[List[Dict[str, Any]]] = None,
    formula_columns: Optional[List[str]] = None,
    commit: bool = True,
) -> Dict[str, Any]:
    """将默认规则集合写入 _table_registry.validation_rules_json（覆盖式 upsert）。"""
    rules = default_rules_for(kind, schema_columns, formula_columns)
    doc = {"rules": rules, "kind": kind}
    conn.execute(
        "UPDATE _table_registry SET validation_rules_json = ? WHERE table_name = ?",
        (json.dumps(doc, ensure_ascii=False), table_name),
    )
    if commit:
        conn.commit()
    return {"ok": True, "table_name": table_name, "kind": kind, "rules_count": len(rules)}


def create_validation_rule(
    conn: sqlite3.Connection,
    table_name: str,
    rules: List[Dict[str, Any]],
    *,
    kind: str = "",
) -> Dict[str, Any]:
    """为指定表新增或覆盖校验规则。"""
    cur = conn.execute(
        "SELECT validation_rules_json FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": f"表 {table_name!r} 不存在于 _table_registry"}
    if not isinstance(rules, list) or not rules:
        return {"error": "rules 必填，且至少包含一条规则"}

    raw = row["validation_rules_json"] if isinstance(row, sqlite3.Row) else row[0]
    try:
        doc = json.loads(raw or "{}") or {}
    except (json.JSONDecodeError, TypeError):
        doc = {}

    existing_rules = list(doc.get("rules") or [])
    by_id: Dict[str, Dict[str, Any]] = {
        str(item.get("id", "")): dict(item)
        for item in existing_rules
        if isinstance(item, dict) and str(item.get("id", "")).strip()
    }
    created_ids: List[str] = []
    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            return {"error": f"rules[{idx}] 必须是对象"}
        rule_id = str(rule.get("id", "")).strip()
        rule_type = str(rule.get("type", "")).strip()
        if not rule_id or not rule_type:
            return {"error": f"rules[{idx}] 缺少 id/type"}
        normalized = dict(rule)
        if rule_type == "notnull":
            normalized["type"] = "not_null"
        by_id[rule_id] = normalized
        created_ids.append(rule_id)

    effective_kind = kind or str(doc.get("kind") or "unknown")
    payload = {
        "kind": effective_kind,
        "rules": list(by_id.values()),
    }
    conn.execute(
        "UPDATE _table_registry SET validation_rules_json = ? WHERE table_name = ?",
        (json.dumps(payload, ensure_ascii=False), table_name),
    )
    conn.commit()
    return {
        "ok": True,
        "table_name": table_name,
        "kind": effective_kind,
        "created_rule_ids": created_ids,
        "rules_count": len(payload["rules"]),
    }


def confirm_validation_rule(
    conn: sqlite3.Connection,
    table_name: str,
    rule_id: str,
    reason: str = "",
) -> Dict[str, Any]:
    """将指定规则标记为已确认（confirmed=True），后续 run_validation 跳过该规则报警。

    典型场景：percent_bounds 检测到 crit_dmg=1.5 并报警，
    设计者确认这是合理的暴击倍率（> 1），调用此接口标记后不再触发报警。
    """
    cur = conn.execute(
        "SELECT validation_rules_json FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": f"表 {table_name!r} 不存在于 _table_registry"}
    raw = row["validation_rules_json"] or "{}"
    try:
        doc = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {"error": "validation_rules_json 解析失败，无法更新"}

    rules: List[Dict[str, Any]] = list(doc.get("rules") or [])
    matched = False
    for r in rules:
        if str(r.get("id", "")) == rule_id:
            r["confirmed"] = True
            if reason:
                r["confirmed_reason"] = reason
            matched = True
            break

    if not matched:
        # 规则不在已存储列表中（可能是运行时自动生成的）——插入一条占位确认条目
        rules.append({"id": rule_id, "type": "confirmed_override", "confirmed": True, "confirmed_reason": reason or "manual"})

    doc["rules"] = rules
    conn.execute(
        "UPDATE _table_registry SET validation_rules_json = ? WHERE table_name = ?",
        (json.dumps(doc, ensure_ascii=False), table_name),
    )
    conn.commit()
    return {"ok": True, "table_name": table_name, "rule_id": rule_id, "confirmed": True}


def append_validation_history(
        conn: sqlite3.Connection, table_name: Optional[str], report: Dict[str, Any]) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        "INSERT INTO _validation_history (table_name, created_at, result_json) VALUES (?,?,?)",
        (table_name, now, json.dumps(report, ensure_ascii=False)),
    )
    conn.commit()


def list_validation_history(
    conn: sqlite3.Connection,
    *,
    table_name: Optional[str] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    lim = max(1, min(limit, 100))
    if table_name:
        cur = conn.execute(
            """
            SELECT id, table_name, created_at, result_json
            FROM _validation_history
            WHERE table_name = ?
            ORDER BY id DESC LIMIT ?
            """,
            (table_name, lim),
        )
    else:
        cur = conn.execute(
            """
            SELECT id, table_name, created_at, result_json
            FROM _validation_history
            ORDER BY id DESC LIMIT ?
            """,
            (lim,),
        )
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        try:
            parsed = json.loads(r["result_json"])
        except json.JSONDecodeError:
            parsed = {}
        out.append(
            {
                "id": r["id"],
                "table_name": r["table_name"],
                "created_at": r["created_at"],
                "summary": {
                    "passed": parsed.get("passed"),
                    "warnings_count": len(parsed.get("warnings") or []),
                    "violations_count": len(parsed.get("violations") or []),
                },
            }
        )
    return out


def _load_rules_doc(conn: sqlite3.Connection, table_name: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """返回 (rules, parse_violations)。"""
    cur = conn.execute(
        "SELECT validation_rules_json FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return [], [{"table": table_name, "rule_id": "missing", "message": "未知表"}]
    raw = row["validation_rules_json"]
    if not raw:
        return [], []
    try:
        doc = json.loads(raw)
        rules = list(doc.get("rules") or [])
        if not isinstance(rules, list):
            return [], [{"table": table_name, "rule_id": "parse", "message": "rules 须为数组"}]
        out_rules: List[Dict[str, Any]] = []
        for r in rules:
            if isinstance(r, dict):
                out_rules.append(r)
        return out_rules, []
    except (json.JSONDecodeError, TypeError):
        return [], [{"table": table_name, "rule_id": "parse", "message": "validation_rules_json 非合法 JSON"}]


def _evaluate_rules_for_table(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    violations: List[Dict[str, Any]] = []
    summaries: List[Dict[str, Any]] = []
    t = table_name
    rules, parse_errs = _load_rules_doc(conn, t)
    violations.extend(parse_errs)
    for pe in parse_errs:
        summaries.append(
            {
                "rule_id": str(pe.get("rule_id", "")),
                "type": "parse",
                "column": None,
                "passed": False,
                "violation_count": 1,
            }
        )
    if parse_errs:
        return {"violations": violations, "rule_summaries": summaries}

    for rule in rules:
        rid = str(rule.get("id", ""))
        rtype = str(rule.get("type", ""))
        col = str(rule.get("column", ""))

        # 已确认跳过的规则：跳过校验并标记为已确认通过
        if rule.get("confirmed"):
            summaries.append({
                "rule_id": rid, "type": rtype, "column": col or None,
                "passed": True, "violation_count": 0, "confirmed_override": True,
            })
            continue

        if rtype == "not_null" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            cur = conn.execute(f'SELECT row_id FROM "{t}" WHERE "{cq}" IS NULL')
            local: List[Dict[str, Any]] = []
            for rr in cur.fetchall():
                local.append(
                    {
                        "table": t,
                        "rule_id": rid,
                        "row_id": rr["row_id"],
                        "column": cq,
                        "message": "违反 not_null",
                    }
                )
            violations.extend(local)
            summaries.append(
                {
                    "rule_id": rid,
                    "type": rtype,
                    "column": cq,
                    "passed": len(local) == 0,
                    "violation_count": len(local),
                }
            )
            continue

        if rtype in {"gte", "gt", "lte", "lt"} and col:
            bound = rule.get("value", rule.get("min"))
            if bound is None:
                violations.append({"table": t, "rule_id": rid, "message": f"{rtype} 需配置 value"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            try:
                fbound = float(bound)
            except (TypeError, ValueError):
                violations.append({"table": t, "rule_id": rid, "message": f"{rtype} 的 value 非法：{bound!r}"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": cq,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            cur = conn.execute(f'SELECT row_id, "{cq}" FROM "{t}"')
            local = []
            for rr in cur.fetchall():
                val = rr[cq]
                if val is None:
                    continue
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    local.append(
                        {
                            "table": t,
                            "rule_id": rid,
                            "row_id": rr["row_id"],
                            "column": cq,
                            "message": f"{rtype}: 非数值 {val!r}",
                        }
                    )
                    continue
                violated = (
                    (rtype == "gte" and fv < fbound)
                    or (rtype == "gt" and fv <= fbound)
                    or (rtype == "lte" and fv > fbound)
                    or (rtype == "lt" and fv >= fbound)
                )
                if violated:
                    local.append(
                        {
                            "table": t,
                            "rule_id": rid,
                            "row_id": rr["row_id"],
                            "column": cq,
                            "message": f"{rtype}: {fv} 相对阈值 {fbound} 不满足",
                        }
                    )
            violations.extend(local)
            summaries.append(
                {
                    "rule_id": rid,
                    "type": rtype,
                    "column": cq,
                    "passed": len(local) == 0,
                    "violation_count": len(local),
                }
            )
            continue

        if rtype == "min_max" and col:
            rmin, rmax = rule.get("min"), rule.get("max")
            if rmin is None and rmax is None:
                violations.append(
                    {"table": t, "rule_id": rid, "message": "min_max 需至少配置 min 或 max 之一"}
                )
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            cur = conn.execute(f'SELECT row_id, "{cq}" FROM "{t}"')
            local = []
            for rr in cur.fetchall():
                val = rr[cq]
                if val is None:
                    continue
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    local.append(
                        {
                            "table": t,
                            "rule_id": rid,
                            "row_id": rr["row_id"],
                            "column": cq,
                            "message": "min_max: 非数值",
                        }
                    )
                    continue
                if rmin is not None and fv < float(rmin):
                    local.append(
                        {
                            "table": t,
                            "rule_id": rid,
                            "row_id": rr["row_id"],
                            "column": cq,
                            "message": f"小于 min {rmin}",
                        }
                    )
                if rmax is not None and fv > float(rmax):
                    local.append(
                        {
                            "table": t,
                            "rule_id": rid,
                            "row_id": rr["row_id"],
                            "column": cq,
                            "message": f"大于 max {rmax}",
                        }
                    )
            violations.extend(local)
            summaries.append(
                {
                    "rule_id": rid,
                    "type": rtype,
                    "column": cq,
                    "passed": len(local) == 0,
                    "violation_count": len(local),
                }
            )
            continue

        if rtype == "regex" and col:
            pattern = str(rule.get("pattern", ""))
            full_match = bool(rule.get("full_match"))
            if not pattern:
                violations.append({"table": t, "rule_id": rid, "message": "regex 规则缺少 pattern"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": col,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            try:
                rx = re.compile(pattern)
            except re.error as e:
                violations.append({"table": t, "rule_id": rid, "message": f"regex 非法: {e}"})
                summaries.append(
                    {
                        "rule_id": rid,
                        "type": rtype,
                        "column": cq,
                        "passed": False,
                        "violation_count": 1,
                    }
                )
                continue
            cur = conn.execute(f'SELECT row_id, "{cq}" FROM "{t}"')
            local = []
            for rr in cur.fetchall():
                val = rr[cq]
                if val is None:
                    continue
                s = str(val)
                okm = rx.fullmatch(s) if full_match else rx.search(s)
                if not okm:
                    local.append(
                        {
                            "table": t,
                            "rule_id": rid,
                            "row_id": rr["row_id"],
                            "column": cq,
                            "message": "regex 不匹配",
                        }
                    )
            violations.extend(local)
            summaries.append(
                {
                    "rule_id": rid,
                    "type": rtype,
                    "column": cq,
                    "passed": len(local) == 0,
                    "violation_count": len(local),
                }
            )
            continue

        if rtype == "percent_bounds" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append({"rule_id": rid, "type": rtype, "column": col, "passed": False, "violation_count": 1})
                continue
            lo = float(rule.get("min", 0.0))
            hi = float(rule.get("max", 0.95))
            cur = conn.execute(f'SELECT row_id, "{cq}" FROM "{t}"')
            local = []
            for rr in cur.fetchall():
                val = rr[cq]
                if val is None:
                    continue
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    continue
                if fv < lo or fv > hi:
                    local.append({
                        "table": t, "rule_id": rid, "row_id": rr["row_id"], "column": cq,
                        "message": (
                            f"percent_bounds: {fv} 不在 [{lo}, {hi}]。"
                            f"若此设计合理（如暴击伤害倍率 > 1），请调用 confirm_validation_rule("
                            f"table_name='{t}', rule_id='{rid}') 确认，确认后本规则不再报警。"
                        ),
                    })
            violations.extend(local)
            summaries.append({"rule_id": rid, "type": rtype, "column": cq, "passed": len(local) == 0, "violation_count": len(local)})
            continue

        if rtype == "format_consistency" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append({"rule_id": rid, "type": rtype, "column": col, "passed": False, "violation_count": 1})
                continue
            try:
                cur = conn.execute('SELECT number_format FROM _column_meta WHERE table_name = ? AND column_name = ?', (t, cq))
                row_meta = cur.fetchone()
            except sqlite3.OperationalError:
                row_meta = None
            nf = (row_meta["number_format"] if row_meta else "") or ""
            local = []
            if nf == "0.00%":
                cur2 = conn.execute(f'SELECT row_id, "{cq}" FROM "{t}"')
                for rr in cur2.fetchall():
                    val = rr[cq]
                    if val is None:
                        continue
                    try:
                        fv = float(val)
                    except (TypeError, ValueError):
                        continue
                    if fv > 10.0:
                        local.append({"table": t, "rule_id": rid, "row_id": rr["row_id"], "column": cq, "message": f"format_consistency: number_format=0.00% 但值 {fv} > 10，疑似整数百分比写法"})
            violations.extend(local)
            summaries.append({"rule_id": rid, "type": rtype, "column": cq, "passed": len(local) == 0, "violation_count": len(local)})
            continue

        if rtype == "monotone_warning" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append({"rule_id": rid, "type": rtype, "column": col, "passed": False, "violation_count": 1})
                continue
            order_col = str(rule.get("order_by", "row_id"))
            try:
                ocq = assert_col_or_table(order_col)
            except ValueError:
                ocq = "row_id"
            cur = conn.execute(f'SELECT "{ocq}" AS o, "{cq}" AS v FROM "{t}" ORDER BY CAST("{ocq}" AS REAL) ASC')
            vals: List[float] = []
            for rr in cur.fetchall():
                if rr["v"] is None:
                    continue
                try:
                    vals.append(float(rr["v"]))
                except (TypeError, ValueError):
                    continue
            local = []
            if len(vals) >= 3 and all(vals[i] < vals[i + 1] for i in range(len(vals) - 1)):
                local.append({"table": t, "rule_id": rid, "column": cq, "message": "monotone_warning: 性价比类列严格单调递增，缺少阶段性拐点"})
            violations.extend(local)
            summaries.append({"rule_id": rid, "type": rtype, "column": cq, "passed": len(local) == 0, "violation_count": len(local)})
            continue

        if rtype == "formula_has_value" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append({"rule_id": rid, "type": rtype, "column": col, "passed": False, "violation_count": 1})
                continue
            cur = conn.execute('SELECT 1 FROM _formula_registry WHERE table_name = ? AND column_name = ?', (t, cq))
            local = []
            if cur.fetchone():
                cur2 = conn.execute(f'SELECT row_id FROM "{t}" WHERE "{cq}" IS NULL')
                for rr in cur2.fetchall():
                    local.append({"table": t, "rule_id": rid, "row_id": rr["row_id"], "column": cq, "message": "formula_has_value: 公式列存在空值"})
            violations.extend(local)
            summaries.append({"rule_id": rid, "type": rtype, "column": cq, "passed": len(local) == 0, "violation_count": len(local)})
            continue

        if rtype == "not_empty_id" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append({"rule_id": rid, "type": rtype, "column": col, "passed": False, "violation_count": 1})
                continue
            cur = conn.execute(f"SELECT row_id FROM \"{t}\" WHERE \"{cq}\" IS NULL OR TRIM(CAST(\"{cq}\" AS TEXT))=''")
            local = []
            for rr in cur.fetchall():
                local.append({"table": t, "rule_id": rid, "row_id": rr["row_id"], "column": cq, "message": "not_empty_id: ID/等级列不允许空"})
            violations.extend(local)
            summaries.append({"rule_id": rid, "type": rtype, "column": cq, "passed": len(local) == 0, "violation_count": len(local)})
            continue

        if rtype == "resource_hourly" and col:
            try:
                cq = assert_col_or_table(col)
            except ValueError:
                violations.append({"table": t, "rule_id": rid, "message": f"非法列 {col}"})
                summaries.append({"rule_id": rid, "type": rtype, "column": col, "passed": False, "violation_count": 1})
                continue
            min_hourly = float(rule.get("min_hourly", 1.0))
            cur = conn.execute(f'SELECT row_id, "{cq}" FROM "{t}"')
            local = []
            for rr in cur.fetchall():
                val = rr[cq]
                if val is None:
                    continue
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    continue
                if fv < min_hourly:
                    local.append({"table": t, "rule_id": rid, "row_id": rr["row_id"], "column": cq, "message": f"resource_hourly: 小时产量 {fv} < 阈值 {min_hourly}"})
            violations.extend(local)
            summaries.append({"rule_id": rid, "type": rtype, "column": cq, "passed": len(local) == 0, "violation_count": len(local)})
            continue

    return {"violations": violations, "rule_summaries": summaries}


def build_validation_report(conn: sqlite3.Connection, filter_table: Optional[str] = None) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT table_name, validation_status FROM _table_registry ORDER BY table_name"
    )
    all_rows: List[Dict[str, Any]] = [dict(r) for r in cur.fetchall()]
    if filter_table:
        all_rows = [r for r in all_rows if r.get("table_name") == filter_table]
    warnings: List[str] = []
    per_table: Dict[str, str] = {}
    violations: List[Dict[str, Any]] = []
    rule_summaries: List[Dict[str, Any]] = []

    if filter_table and not all_rows:
        rep = {
            "passed": False,
            "warnings": [f"未知表 {filter_table}"],
            "tables": [],
            "per_table": {},
            "violations": [],
            "rule_summaries": [],
        }
        append_validation_history(conn, filter_table, rep)
        return rep

    for t in all_rows:
        name = str(t["table_name"])
        # 默认假设通过；若评估出违反或未挂规则，再降级
        per_table[name] = "ok"

    for t in all_rows:
        name = str(t["table_name"])
        part = _evaluate_rules_for_table(conn, name)
        violations.extend(part["violations"])
        for s in part.get("rule_summaries") or []:
            row = dict(s)
            row["table"] = name
            rule_summaries.append(row)
        # 没有任何规则被评估（rule_summaries 为空）→ 标记 warn 并提醒
        if not part.get("rule_summaries"):
            per_table[name] = "warn"
            warnings.append(f"表 {name} 未配置校验规则")

    if violations:
        warnings.append(f"规则违反 {len(violations)} 条")
        for v in violations:
            tn = str(v.get("table", ""))
            if tn in per_table:
                per_table[tn] = "warn"

    # 把 per_table 状态回写 _table_registry.validation_status，避免永远停留在 unknown
    for name, st in per_table.items():
        new_status = "passed" if st == "ok" else "warn"
        try:
            conn.execute(
                "UPDATE _table_registry SET validation_status = ? WHERE table_name = ?",
                (new_status, name),
            )
        except sqlite3.OperationalError:
            pass
    conn.commit()

    passed = len(warnings) == 0 and len(violations) == 0
    rep = {
        "passed": passed,
        "warnings": warnings,
        "tables": all_rows,
        "per_table": per_table,
        "violations": violations,
        "rule_summaries": rule_summaries,
    }
    append_validation_history(conn, filter_table, rep)
    return rep

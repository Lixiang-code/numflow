"""验证报告生成（/validate 与 Agent 工具复用）。"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

from app.services.cell_writes import assert_col_or_table


def append_validation_history(conn: sqlite3.Connection, table_name: Optional[str], report: Dict[str, Any]) -> None:
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
        st = str(t.get("validation_status") or "unknown")
        per_table[name] = "warn" if st == "unknown" else "ok"
        if st == "unknown":
            warnings.append(f"表 {name} 尚未验证")

    for t in all_rows:
        name = str(t["table_name"])
        part = _evaluate_rules_for_table(conn, name)
        violations.extend(part["violations"])
        for s in part.get("rule_summaries") or []:
            row = dict(s)
            row["table"] = name
            rule_summaries.append(row)

    if violations:
        warnings.append(f"规则违反 {len(violations)} 条")
        for v in violations:
            tn = str(v.get("table", ""))
            if tn in per_table:
                per_table[tn] = "warn"

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

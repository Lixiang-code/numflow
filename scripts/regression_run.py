#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Numflow 全真千问回归测试脚本 v2
用 requests + SSE iter_lines，read_timeout=600。

R1: maintain 模式 — "用一句话说明你能做什么"
R2: init    模式 — "我要做一个数值平衡（攻防+暴击）的卡牌养成 demo，请按当前步骤推进"

输出：
  scripts/regression_last.json  — 完整 JSON 结果
  docs/回归测试报告-2026-04-25.md — 中文回归报告

环境变量：
  NUMFLOW_BASE      默认 http://127.0.0.1:8000/api
  NUMFLOW_INVITE    默认 lixiang_B22jUD7F
  NUMFLOW_REPORT    报告路径
  NUMFLOW_ROUND     "first" | "second"
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

BASE    = os.environ.get("NUMFLOW_BASE",   "http://127.0.0.1:8000/api")
INVITE  = os.environ.get("NUMFLOW_INVITE", "lixiang_B22jUD7F")
REPORT_PATH = os.environ.get(
    "NUMFLOW_REPORT",
    "/www/wwwroot/numflow/docs/回归测试报告-2026-04-25.md",
)
JSON_PATH = "/www/wwwroot/numflow/scripts/regression_last.json"
ROUND = os.environ.get("NUMFLOW_ROUND", "first").strip().lower()

SSE_READ_TIMEOUT   = 600   # 千问三阶段实测 4-6 分钟
CONNECT_TIMEOUT    = 30
PIPELINE_STEPS_EXPECTED = 11


# ═══════════════════════════════════════════════════════════════
# HTTP helpers
# ═══════════════════════════════════════════════════════════════

def _req(
    sess: requests.Session,
    method: str,
    path: str,
    *,
    body: Optional[Dict] = None,
    project_id: Optional[int] = None,
    timeout: int = 60,
    params: Optional[Dict] = None,
) -> Tuple[int, Any]:
    url = BASE + path
    headers: Dict[str, str] = {}
    if project_id is not None:
        headers["X-Project-Id"] = str(project_id)
    try:
        resp = sess.request(
            method,
            url,
            json=body,
            headers=headers,
            params=params,
            timeout=(CONNECT_TIMEOUT, timeout),
        )
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, resp.text
    except requests.RequestException as e:
        return -1, repr(e)


def sse_chat(
    sess: requests.Session,
    project_id: int,
    message: str,
    mode: str = "maintain",
    strict_review: bool = False,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """POST /agent/chat，SSE 用 iter_lines 解析，read_timeout=600。
    返回 (events, error_msg)；即使连接中途断开也返回已收到的 events。"""
    url = BASE + "/agent/chat"
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "X-Project-Id": str(project_id),
    }
    body = {"message": message, "mode": mode, "strict_review": strict_review}
    events: List[Dict[str, Any]] = []
    exc_msg: Optional[str] = None

    try:
        with sess.post(
            url,
            json=body,
            headers=headers,
            stream=True,
            timeout=(CONNECT_TIMEOUT, SSE_READ_TIMEOUT),
        ) as resp:
            try:
                for raw_line in resp.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue
                    if raw_line.startswith("data: "):
                        data_str = raw_line[6:]
                        try:
                            events.append(json.loads(data_str))
                        except json.JSONDecodeError:
                            pass
            except Exception as inner_e:
                exc_msg = repr(inner_e)
                print(f"  SSE iter_lines error (partial): {exc_msg}", flush=True)
    except Exception as outer_e:
        exc_msg = repr(outer_e)
        print(f"  SSE connection error: {exc_msg}", flush=True)

    return events, exc_msg


# ═══════════════════════════════════════════════════════════════
# Spec / advance API 检查
# ═══════════════════════════════════════════════════════════════

REQUIRED_SPEC_FIELDS = (
    "goal", "inputs", "outputs", "required_tables", "acceptance", "agent_hint",
)


def check_spec_apis(sess: requests.Session, pid: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {"checks": [], "pass": True}

    def add(name: str, ok: bool, detail: Any = None):
        out["checks"].append({"name": name, "ok": bool(ok), "detail": detail})
        if not ok:
            out["pass"] = False

    c, o = _req(sess, "GET", "/pipeline/specs", project_id=pid)
    specs = (o or {}).get("specs") if isinstance(o, dict) else None
    add(
        "GET /pipeline/specs 返回 11 步",
        c == 200 and isinstance(specs, list) and len(specs) == PIPELINE_STEPS_EXPECTED,
        {"status": c, "count": len(specs) if isinstance(specs, list) else None},
    )

    c, o = _req(sess, "GET", "/pipeline/step/environment_global_readme/spec", project_id=pid)
    missing = (
        [f for f in REQUIRED_SPEC_FIELDS if f not in (o or {})]
        if isinstance(o, dict) else list(REQUIRED_SPEC_FIELDS)
    )
    add(
        "step/<id>/spec 含必要字段",
        c == 200 and not missing,
        {"status": c, "missing": list(missing)},
    )

    c, o = _req(sess, "GET", "/pipeline/step/environment_global_readme/readme", project_id=pid)
    src  = (o or {}).get("source") if isinstance(o, dict) else None
    text = (o or {}).get("text")   if isinstance(o, dict) else ""
    add(
        "未推进时 readme 回落 spec_template",
        c == 200 and src == "spec_template" and isinstance(text, str) and len(text) > 200,
        {"status": c, "source": src, "len": len(text or "")},
    )

    custom = "## 自定义 README\n回归脚本写入用于校验 PUT/GET 一致性。\n"
    cput, _ = _req(
        sess, "PUT", "/pipeline/step/base_attribute_framework/readme",
        project_id=pid, body={"text": custom},
    )
    cget, oget = _req(sess, "GET", "/pipeline/step/base_attribute_framework/readme", project_id=pid)
    got_text = (oget or {}).get("text")   if isinstance(oget, dict) else ""
    got_src  = (oget or {}).get("source") if isinstance(oget, dict) else None
    add(
        "PUT readme 后 GET 取回",
        cput == 200 and cget == 200 and got_text == custom and got_src == "user",
        {"put": cput, "get": cget, "source": got_src, "match": got_text == custom},
    )
    return out


def check_advance_dict_detail(sess: requests.Session, pid: int) -> Dict[str, Any]:
    c, o = _req(
        sess, "POST", "/pipeline/advance",
        project_id=pid,
        body={"step": "gameplay_attribute_scheme"},
    )
    detail = (o or {}).get("detail") if isinstance(o, dict) else None
    ok = (
        c == 400
        and isinstance(detail, dict)
        and detail.get("expected_step") == "environment_global_readme"
        and isinstance(detail.get("expected_goal"), str)
        and len(detail.get("expected_goal") or "") > 0
    )
    return {"ok": ok, "status": c, "detail": detail if isinstance(detail, dict) else o}


def check_advance_then_seed(sess: requests.Session, pid: int) -> Dict[str, Any]:
    cadv, oadv = _req(
        sess, "POST", "/pipeline/advance",
        project_id=pid,
        body={"step": "environment_global_readme"},
    )
    cget, oget = _req(sess, "GET", "/pipeline/step/environment_global_readme/readme", project_id=pid)
    txt = (oget or {}).get("text")   if isinstance(oget, dict) else ""
    src = (oget or {}).get("source") if isinstance(oget, dict) else None
    ok = (
        cadv == 200 and cget == 200
        and isinstance(txt, str) and len(txt) > 500
        and src in ("spec_template", "user")
    )
    return {
        "ok": ok, "advance_status": cadv,
        "readme_source": src, "readme_len": len(txt or ""),
    }


# ═══════════════════════════════════════════════════════════════
# Diagnostics
# ═══════════════════════════════════════════════════════════════

def fetch_diagnostics(sess: requests.Session, pid: Optional[int] = None) -> Dict[str, Any]:
    """GET /api/agent/diagnostics（含 project_id query 参数）。"""
    params = {"project_id": pid} if pid is not None else {}
    c, o = _req(sess, "GET", "/agent/diagnostics", params=params, timeout=30)
    return {"status": c, "body": o}


# ═══════════════════════════════════════════════════════════════
# SSE 分析
# ═══════════════════════════════════════════════════════════════

DESIGN_HEADERS = (
    "## 1. 我对用户需求的理解",
    "## 2. 我对游戏性的设计理解",
    "## 3. 我的最终设计",
)
REVIEW_FINAL_KW   = "## 最终操作方案"


def analyze_sse(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    bucket_text: Dict[str, List[str]] = {
        "route": [], "design": [], "review": [], "execute": [],
    }
    tool_calls:         List[Dict[str, Any]] = []
    tool_names:         List[str]            = []
    tool_results:       List[Dict[str, Any]] = []
    reviewer_verdicts:  List[Dict[str, Any]] = []
    blocked_cells_total = 0
    prompt_route_event: Optional[Dict[str, Any]] = None
    final_text = ""
    has_error  = False
    error_msgs: List[str] = []

    for ev in events:
        phase = ev.get("phase", "")
        typ   = ev.get("type",  "")

        if phase in bucket_text and typ == "token":
            bucket_text[phase].append(ev.get("text", ""))
        if typ == "prompt_route":
            prompt_route_event = ev
        if typ == "tool_call":
            tool_calls.append(ev)
            name = ev.get("name", "")
            if name and name not in tool_names:
                tool_names.append(name)
        if typ == "tool_result":
            tool_results.append(ev)
            preview = ev.get("preview", "") or ""
            try:
                pj = json.loads(preview)
                bc = pj.get("blocked_cells") or []
                if isinstance(bc, list):
                    blocked_cells_total += len(bc)
            except Exception:
                pass
        if typ == "reviewer_verdict":
            reviewer_verdicts.append(ev)
        if typ == "done":
            final_text = ev.get("full_text", "") or ""
        if typ == "error":
            has_error = True
            error_msgs.append(ev.get("message", ""))

    design_text  = "".join(bucket_text["design"])
    review_text  = "".join(bucket_text["review"])
    execute_text = "".join(bucket_text["execute"]) or final_text

    # design 三段命中
    design_hits = [h for h in DESIGN_HEADERS if h in design_text]
    design_three_phase_ok = len(design_hits) == 3

    # review 最终方案
    review_has_final_plan = REVIEW_FINAL_KW in review_text

    # prompt_route 解析
    pr: Dict[str, Any] = {}
    if prompt_route_event:
        pr = {
            "hit":          bool(prompt_route_event.get("hit")),
            "step_id":      prompt_route_event.get("step_id", ""),
            "rationale":    prompt_route_event.get("rationale", ""),
            "prompt":       prompt_route_event.get("prompt", ""),
            "prompt_len":   len(prompt_route_event.get("prompt", "") or ""),
        }

    # reviewer approve/reject 计数
    rv_approve = sum(1 for v in reviewer_verdicts if v.get("verdict") == "approve")
    rv_reject  = sum(1 for v in reviewer_verdicts if v.get("verdict") == "reject")

    return {
        "design_text":             design_text,
        "review_text":             review_text,
        "execute_text":            execute_text,
        "design_three_phase_hits": design_hits,
        "design_three_phase_ok":   design_three_phase_ok,
        "review_has_final_plan":   review_has_final_plan,
        "tool_call_count":         len(tool_calls),
        "tool_names":              tool_names,
        "tool_result_count":       len(tool_results),
        "blocked_cells_total":     blocked_cells_total,
        "reviewer_verdict_count":  len(reviewer_verdicts),
        "reviewer_approve":        rv_approve,
        "reviewer_reject":         rv_reject,
        "prompt_route":            pr,
        "final_text":              final_text,
        "has_error":               has_error,
        "error_msgs":              error_msgs,
        "char_design":             len(design_text),
        "char_review":             len(review_text),
        "char_execute":            len(execute_text),
        "total_events":            len(events),
    }


# ═══════════════════════════════════════════════════════════════
# 单用例运行
# ═══════════════════════════════════════════════════════════════

def run_case(
    sess: requests.Session,
    pid: int,
    case_id: str,
    mode: str,
    message: str,
    strict_review: bool = False,
) -> Dict[str, Any]:
    print(f"  [{case_id}] mode={mode} strict_review={strict_review}", flush=True)
    print(f"  [{case_id}] message={message[:80]}...", flush=True)
    t0 = time.time()
    events, exc_msg = sse_chat(sess, pid, message, mode=mode, strict_review=strict_review)

    elapsed = round(time.time() - t0, 1)
    metrics = analyze_sse(events)

    passed = (
        metrics["design_three_phase_ok"]
        and metrics["review_has_final_plan"]
        and not metrics["has_error"]
        and not exc_msg
    )

    result = {
        "case_id":              case_id,
        "mode":                 mode,
        "message":              message,
        "strict_review":        strict_review,
        "elapsed_s":            elapsed,
        "pass":                 passed,
        "event_count":          metrics["total_events"],
        "conn_error":           exc_msg,
        "error":                exc_msg or (metrics["error_msgs"][0] if metrics["error_msgs"] else None),
        **metrics,
    }
    print(
        f"  [{case_id}] done elapsed={elapsed}s pass={passed} "
        f"design3={metrics['design_three_phase_ok']} "
        f"review_final={metrics['review_has_final_plan']} "
        f"tool_calls={metrics['tool_call_count']} "
        f"tools={metrics['tool_names']}",
        flush=True,
    )
    return result


# ═══════════════════════════════════════════════════════════════
# 报告渲染
# ═══════════════════════════════════════════════════════════════

def _trunc(s: Any, n: int) -> str:
    if s is None:
        return ""
    s = str(s)
    return s[:n] + ("…" if len(s) > n else "")


def render_report(
    env_info: Dict[str, Any],
    spec_result: Dict[str, Any],
    advance_dict_result: Dict[str, Any],
    advance_seed_result: Dict[str, Any],
    cases: List[Dict[str, Any]],
    diagnostics: Dict[str, Any],
    overall_pass: bool,
    iteration_note: str = "",
) -> str:
    now = time.strftime("%Y-%m-%d %H:%M:%S CST")
    lines: List[str] = []

    lines += [
        "# Numflow 回归测试报告",
        "",
        f"> 生成时间：{now}  |  本轮：`{ROUND}`",
        "",
        "---",
        "",
        "## 一、摘要",
        "",
        "| 用例 | PASS/FAIL | 三阶段齐全 | 总用时(s) | 关键问题 |",
        "|---|---|---|---|---|",
    ]
    for c in cases:
        three_ok = (
            "✅ design+review+execute" if (c.get("design_three_phase_ok") and c.get("review_has_final_plan"))
            else "❌ " + ("design缺三段" if not c.get("design_three_phase_ok") else "review缺最终方案")
        )
        issue = c.get("error") or ("blocked_cells=" + str(c.get("blocked_cells_total")) if c.get("blocked_cells_total") else "—")
        lines.append(
            f"| {c['case_id']} ({c['mode']}) "
            f"| **{'PASS' if c['pass'] else 'FAIL'}** "
            f"| {three_ok} "
            f"| {c['elapsed_s']} "
            f"| {_trunc(issue, 80)} |"
        )
    lines += [
        "",
        f"**总判定：{'✅ PASS' if overall_pass else '❌ FAIL'}**",
        "",
        "---",
        "",
        "## 二、环境信息",
        "",
        f"- BASE URL：`{env_info.get('base')}`",
        f"- 模型：`{env_info.get('model', 'qwen3.6-plus')}`",
        f"- 用户：`{env_info.get('user')}`",
        f"- 项目 ID：`{env_info.get('pid')}`  slug：`{env_info.get('slug')}`",
        "",
        "### 诊断接口",
        "",
        f"```json\n{json.dumps(diagnostics, ensure_ascii=False, indent=2)}\n```",
        "",
        "---",
        "",
        "## 三、Spec / Advance API 检查",
        "",
        "| 检查项 | 通过 | 详情 |",
        "|---|---|---|",
    ]
    for ch in spec_result["checks"]:
        lines.append(
            f"| {ch['name']} "
            f"| {'✅' if ch['ok'] else '❌'} "
            f"| `{_trunc(json.dumps(ch['detail'], ensure_ascii=False), 200)}` |"
        )
    lines += [
        f"| 错序 advance detail 是 dict "
        f"| {'✅' if advance_dict_result['ok'] else '❌'} "
        f"| `{_trunc(json.dumps(advance_dict_result, ensure_ascii=False), 200)}` |",
        f"| 正序 advance + readme seed "
        f"| {'✅' if advance_seed_result['ok'] else '❌'} "
        f"| `{_trunc(json.dumps(advance_seed_result, ensure_ascii=False), 200)}` |",
        "",
        "---",
        "",
        "## 四、Agent SSE 用例指标",
        "",
        "| 指标 | " + " | ".join(c["case_id"] for c in cases) + " |",
        "|---|" + "|".join(["---"] * len(cases)) + "|",
    ]
    metric_rows = [
        ("mode",             "mode"),
        ("strict_review",    "strict_review"),
        ("总用时(s)",         "elapsed_s"),
        ("SSE 事件总数",      "event_count"),
        ("design 字符",       "char_design"),
        ("review 字符",       "char_review"),
        ("execute 字符",      "char_execute"),
        ("design 三段 OK",    "design_three_phase_ok"),
        ("design 三段命中",   "design_three_phase_hits"),
        ("review 最终方案",   "review_has_final_plan"),
        ("tool_call 次数",    "tool_call_count"),
        ("tool 名清单",       "tool_names"),
        ("blocked_cells",    "blocked_cells_total"),
        ("reviewer 总数",     "reviewer_verdict_count"),
        ("reviewer approve", "reviewer_approve"),
        ("reviewer reject",  "reviewer_reject"),
        ("有 error 事件",     "has_error"),
        ("PASS",             "pass"),
    ]
    for label, key in metric_rows:
        row = f"| {label}"
        for c in cases:
            val = c.get(key)
            if isinstance(val, list):
                val = ", ".join(str(v) for v in val) if val else "—"
            row += f" | {val}"
        lines.append(row + " |")

    lines += [
        "",
        "### 4.1 prompt_route 事件",
        "",
        "| 用例 | hit | step_id | prompt 长度 | rationale |",
        "|---|---|---|---|---|",
    ]
    for c in cases:
        pr = c.get("prompt_route") or {}
        lines.append(
            f"| {c['case_id']} "
            f"| {pr.get('hit')} "
            f"| `{pr.get('step_id', '')}` "
            f"| {pr.get('prompt_len', 0)} "
            f"| {_trunc(pr.get('rationale', ''), 160)} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 五、各用例细节",
        "",
    ]
    for c in cases:
        lines += [
            f"### {c['case_id']}（mode={c['mode']}）",
            "",
            f"**message：** {_trunc(c.get('message',''), 200)}",
            "",
            f"**elapsed_s：** {c['elapsed_s']}s  |  **PASS：** {c['pass']}",
            "",
            "#### design 节选（前 200 字）",
            "",
            "```text",
            _trunc(c.get("design_text", ""), 200),
            "```",
            "",
            "#### review 节选（前 200 字）",
            "",
            "```text",
            _trunc(c.get("review_text", ""), 200),
            "```",
            "",
            "#### execute 工具调用清单",
            "",
            f"共 **{c.get('tool_call_count', 0)}** 次，工具：{', '.join(c.get('tool_names', [])) or '（无）'}",
            "",
            f"blocked_cells 累计：{c.get('blocked_cells_total', 0)}",
            "",
        ]

    lines += [
        "---",
        "",
        "## 六、提示词效果指标",
        "",
    ]
    n = len(cases)
    cot_hit  = sum(1 for c in cases if c.get("design_three_phase_ok"))
    rev_hit  = sum(1 for c in cases if c.get("review_has_final_plan"))
    avg_tool = round(sum(c.get("tool_call_count", 0) for c in cases) / max(n, 1), 1)
    err_rate = round(sum(1 for c in cases if c.get("has_error")) / max(n, 1) * 100, 1)
    route_hit= sum(1 for c in cases if (c.get("prompt_route") or {}).get("hit"))
    lines += [
        f"- CoT 三段命中率：{cot_hit}/{n} = {round(cot_hit/n*100)}%",
        f"- 二次审核命中率（review 含「最终操作方案」）：{rev_hit}/{n} = {round(rev_hit/n*100)}%",
        f"- 工具调用平均次数：{avg_tool}",
        f"- 错误率：{err_rate}%",
        f"- prompt_route 命中率：{route_hit}/{n} = {round(route_hit/n*100)}%",
        "",
        "---",
        "",
        "## 七、发现的问题与建议",
        "",
    ]
    issues: List[str] = []
    for c in cases:
        if not c.get("design_three_phase_ok"):
            issues.append(f"- **{c['case_id']}** design 三段未齐全，命中：{c.get('design_three_phase_hits')}")
        if not c.get("review_has_final_plan"):
            issues.append(f"- **{c['case_id']}** review 缺少「最终操作方案」标题")
        if c.get("has_error"):
            issues.append(f"- **{c['case_id']}** 出现 error 事件：{c.get('error_msgs')}")
        if c.get("blocked_cells_total", 0) > 0:
            issues.append(f"- **{c['case_id']}** blocked_cells={c['blocked_cells_total']}，有单元格被保护拦截")
    if not issues:
        issues = ["- 本轮无严重问题发现。"]
    lines += issues
    lines += [
        "",
        "---",
        "",
        "## 八、下次迭代建议",
        "",
        "1. 若 CoT 三段命中率 < 100%，检查 system prompt `_DESIGN_SYSTEM_TAIL` 标题格式是否与检测关键字一致。",
        "2. 若 review 缺「最终操作方案」，检查 `_REVIEW_SYSTEM_TAIL` 是否明确要求该二级标题。",
        "3. R2（init）工具调用次数多时，可收窄 execute max_rounds 或拆分为两轮对话。",
        "4. blocked_cells > 0 需在 execute 后执行 get_protected_cells 梳理保护范围。",
        "5. prompt_route 未命中（hit=False）时说明任务偏离默认模板，可丰富 DEFAULT_STEP_PROMPTS。",
        "",
    ]

    if iteration_note:
        lines += [
            "---",
            "",
            "## 九、迭代后补充",
            "",
            iteration_note,
            "",
        ]

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════

def main() -> int:
    ts   = int(time.time())
    user = f"nf_reg_{ts}"
    pw   = f"NfReg_{ts}!x"

    sess = requests.Session()

    # ---- 注册 ----
    print(f"[setup] 注册用户 {user} ...", flush=True)
    c, o = _req(sess, "POST", "/auth/register",
                body={"username": user, "password": pw, "invite_code": INVITE})
    if c != 200:
        print(f"[setup] register failed {c} {o}", flush=True)
        sys.exit(1)
    print(f"[setup] 注册成功", flush=True)

    # ---- 创建项目 ----
    settings = {
        "core": {
            "title": f"回归{ts}",
            "formula": "subtraction",
            "level_max": 30,
            "notes": "回归脚本创建",
        },
        "game_systems": {"enabled": ["equipment", "gem"]},
        "attribute_system": {
            "core_attrs": ["攻击力", "防御力", "生命值", "暴击率"],
            "advanced": [],
        },
    }
    c, cr = _req(sess, "POST", "/projects",
                 body={"name": f"回归-{ts}", "settings": settings, "slug": f"regression-{ts}"})
    if c != 200:
        print(f"[setup] create project failed {c} {cr}", flush=True)
        sys.exit(1)
    pid  = int(cr["id"])
    slug = cr["slug"]
    print(f"[setup] 项目创建成功 pid={pid} slug={slug}", flush=True)

    # ---- Spec / advance API 检查 ----
    print("[spec] 检查 spec/advance API ...", flush=True)
    spec_result         = check_spec_apis(sess, pid)
    advance_dict_result = check_advance_dict_detail(sess, pid)
    advance_seed_result = check_advance_then_seed(sess, pid)
    print(
        f"[spec] spec={'PASS' if spec_result['pass'] else 'FAIL'} "
        f"advance_dict={'OK' if advance_dict_result['ok'] else 'FAIL'} "
        f"advance_seed={'OK' if advance_seed_result['ok'] else 'FAIL'}",
        flush=True,
    )

    # ---- 诊断接口 ----
    diagnostics = fetch_diagnostics(sess, pid)
    print(f"[diag] diagnostics={diagnostics}", flush=True)

    # ---- R1: maintain 模式 ----
    print("\n[R1] 开始 maintain 模式用例 ...", flush=True)
    r1 = run_case(
        sess, pid, "R1",
        mode="maintain",
        message="用一句话说明你能做什么",
        strict_review=False,
    )

    # ---- R2: init 模式 ----
    print("\n[R2] 开始 init 模式用例 ...", flush=True)
    r2 = run_case(
        sess, pid, "R2",
        mode="init",
        message="我要做一个数值平衡（攻防+暴击）的卡牌养成 demo，请按当前步骤推进",
        strict_review=False,
    )

    cases = [r1, r2]

    overall_pass = (
        spec_result["pass"]
        and advance_dict_result["ok"]
        and advance_seed_result["ok"]
        and all(c["pass"] for c in cases)
    )

    env_info = {
        "base":  BASE,
        "model": "qwen3.6-plus",
        "user":  user,
        "pid":   pid,
        "slug":  slug,
        "round": ROUND,
        "ts":    ts,
    }

    # ---- 输出 JSON ----
    result_json = {
        "env_info":             env_info,
        "overall_pass":         overall_pass,
        "diagnostics":          diagnostics,
        "spec_result":          spec_result,
        "advance_dict_result":  advance_dict_result,
        "advance_seed_result":  advance_seed_result,
        "cases": [
            {
                "case_id":                c["case_id"],
                "mode":                   c["mode"],
                "message":                c["message"],
                "strict_review":          c["strict_review"],
                "elapsed_s":              c["elapsed_s"],
                "pass":                   c["pass"],
                "event_count":            c["event_count"],
                "char_design":            c["char_design"],
                "char_review":            c["char_review"],
                "char_execute":           c["char_execute"],
                "design_three_phase_ok":  c["design_three_phase_ok"],
                "design_three_phase_hits":c["design_three_phase_hits"],
                "review_has_final_plan":  c["review_has_final_plan"],
                "tool_call_count":        c["tool_call_count"],
                "tool_names":             c["tool_names"],
                "tool_result_count":      c["tool_result_count"],
                "blocked_cells_total":    c["blocked_cells_total"],
                "reviewer_verdict_count": c["reviewer_verdict_count"],
                "reviewer_approve":       c["reviewer_approve"],
                "reviewer_reject":        c["reviewer_reject"],
                "has_error":              c["has_error"],
                "error":                  c.get("error"),
                "error_msgs":             c.get("error_msgs", []),
                "prompt_route":           c.get("prompt_route", {}),
                "design_preview":         c.get("design_text", "")[:400],
                "review_preview":         c.get("review_text", "")[:400],
                "execute_preview":        c.get("execute_text", "")[:400],
            }
            for c in cases
        ],
    }
    os.makedirs(os.path.dirname(JSON_PATH), exist_ok=True)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=2)
    print(f"\n[output] JSON → {JSON_PATH}", flush=True)

    # ---- 输出报告 ----
    iteration_note = ""
    if ROUND == "second":
        prev = ""
        if os.path.exists(REPORT_PATH):
            with open(REPORT_PATH, encoding="utf-8") as f:
                prev = f.read()
        iteration_note = (
            f"第二轮（迭代后）结果：PASS={overall_pass}；"
            f"用例：{[(c['case_id'], c['pass']) for c in cases]}。"
        )
        report_text = prev + "\n\n---\n\n" + render_report(
            env_info, spec_result, advance_dict_result, advance_seed_result,
            cases, diagnostics, overall_pass, iteration_note,
        )
    else:
        report_text = render_report(
            env_info, spec_result, advance_dict_result, advance_seed_result,
            cases, diagnostics, overall_pass,
        )

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"[output] 报告 → {REPORT_PATH}", flush=True)

    # ---- stdout 摘要 ----
    print(
        "\n" + json.dumps(
            {
                "round":        ROUND,
                "overall_pass": overall_pass,
                "cases": [
                    {
                        "id":           c["case_id"],
                        "pass":         c["pass"],
                        "elapsed_s":    c["elapsed_s"],
                        "design3":      c["design_three_phase_ok"],
                        "review_final": c["review_has_final_plan"],
                        "tool_calls":   c["tool_call_count"],
                        "tools":        c["tool_names"],
                        "error":        c.get("error"),
                        "route_hit":    (c.get("prompt_route") or {}).get("hit"),
                    }
                    for c in cases
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if overall_pass else 2


if __name__ == "__main__":
    sys.exit(main())

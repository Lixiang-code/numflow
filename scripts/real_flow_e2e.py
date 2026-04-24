#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实机 E2E：与 03 对齐；表名/列名中文；分玩法一表一系统（二级表）；一阶表注册示例公式
并在服务端 describe 中返回 column_formulas 供客户端表头悬停。

环境变量：NUMFLOW_LEVEL_MAX NUMFLOW_INVITE_CODE NUMFLOW_DATA_ONLY=1
"""
from __future__ import annotations

import http.cookiejar
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

BASE = "http://127.0.0.1:8000/api"
INVITE = os.environ.get("NUMFLOW_INVITE_CODE", "lixiang_B22jUD7F")
LEVEL_MAX = int(os.environ.get("NUMFLOW_LEVEL_MAX", "60"))
DATA_ONLY = os.environ.get("NUMFLOW_DATA_ONLY", "").lower() in ("1", "true", "yes")
WRITE_BATCH = 450

PIPELINE_STEPS = [
    "environment_global_readme",
    "base_attribute_framework",
    "gameplay_attribute_scheme",
    "gameplay_allocation_tables",
    "second_order_framework",
    "gameplay_attribute_tables",
    "cultivation_resource_design",
    "cultivation_resource_framework",
    "cultivation_allocation_tables",
    "cultivation_quant_tables",
    "gameplay_landing_tables",
]

SYS_KEYS = ("equipment", "mount", "gem", "dungeon")
SYS_ZH = {"equipment": "装备", "mount": "坐骑", "gem": "宝石", "dungeon": "副本"}
# 与 docs/02 主系统开放等级一致（未在 01 单独指定时）
OPEN_LEVEL_02 = {"equipment": 1, "mount": 30, "gem": 5, "dungeon": 10}
# 宝石：用「品阶链」为行轴，行数 18（3 合 1 链），不是角色 60 级一行
GEM_TIERS = 18
# 列名（全中文，与 /data 及公式 @表[列] 一致）
C = {
    "等级": "等级",
    "攻击力": "攻击力",
    "防御力": "防御力",
    "生命值": "生命值",
    "暴击率": "暴击率",
    "抗暴率": "抗暴率",
    "杀伤技能系数": "杀伤技能系数",
    "生存技能系数": "生存技能系数",
    "防御验算_公式": "防御验算_公式",
    "停留分钟": "停留分钟",
    "每分钟经验": "每分钟经验",
    "升级目标经验": "升级目标经验",
    "系统": "系统",
    "属性说明": "属性说明",
    "已覆盖全属性": "已覆盖全属性",
    "属性": "属性",
    "分配百分比": "分配百分比",
}

T流水 = "流水线步骤索引"
T一阶 = "一阶属性框架"
T节奏 = "等级与经验节奏"
T方案 = "玩法属性方案"
T分配 = "属性分配"
T二阶 = "二阶属性框架"
T资目 = "养成资源目录"
T资出 = "资源等级产出"
T资权 = "资源系统权重"


def t_玩法(zh: str) -> str:
    return f"{zh}系统_玩法属性"


def t_资可(zh: str) -> str:
    return f"{zh}系统_资源可用"


def t_落地(zh: str) -> str:
    return f"{zh}系统_落地"


# 4×5 分配%
ALLOC: Dict[str, Dict[str, float]] = {
    "equipment": {"攻击力": 22, "防御力": 18, "生命值": 25, "暴击率": 8, "抗暴率": 6},
    "mount": {"攻击力": 12, "防御力": 10, "生命值": 18, "暴击率": 5, "抗暴率": 4},
    "gem": {"攻击力": 8, "防御力": 6, "生命值": 5, "暴击率": 4, "抗暴率": 3.5},
    "dungeon": {"攻击力": 6, "防御力": 5, "生命值": 7, "暴击率": 3, "抗暴率": 2.5},
}


def req(
    method: str,
    path: str,
    cj: http.cookiejar.CookieJar,
    *,
    body: Optional[Dict] = None,
    project_id: Optional[int] = None,
) -> Tuple[int, Any]:
    url = BASE + path
    h: Dict[str, str] = {}
    if project_id is not None:
        h["X-Project-Id"] = str(project_id)
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        h["Content-Type"] = "application/json"
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    r = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with opener.open(r, timeout=600) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(raw) if raw else None
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw


def sse_agent_chat(
    cj: http.cookiejar.CookieJar, project_id: int, message: str, mode: str = "init"
) -> List[Dict[str, Any]]:
    url = BASE + "/agent/chat"
    b = json.dumps({"message": message, "mode": mode}, ensure_ascii=False).encode("utf-8")
    h = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "X-Project-Id": str(project_id),
    }
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    r = urllib.request.Request(url, data=b, headers=h, method="POST")
    out: List[Dict[str, Any]] = []
    with opener.open(r, timeout=300) as resp:
        buf = b""
        while True:
            chunk = resp.read(4096)
            if not chunk:
                break
            buf += chunk
            while b"\n\n" in buf:
                line, buf = buf.split(b"\n\n", 1)
                if not line.startswith(b"data: "):
                    continue
                try:
                    out.append(json.loads(line[6:].decode("utf-8")))
                except json.JSONDecodeError:
                    pass
    return out


def _batch_writes(
    cj: http.cookiejar.CookieJar,
    pid: int,
    name: str,
    updates: List[Dict[str, Any]],
) -> None:
    for i in range(0, len(updates), WRITE_BATCH):
        c, o = req(
            "POST",
            "/data/cells/write",
            cj,
            project_id=pid,
            body={"table_name": name, "updates": updates[i : i + WRITE_BATCH], "source_tag": "ai_generated"},
        )
        if c != 200:
            raise SystemExit(f"write {name} {i}: {c} {o}")


def _mk(
    cj: http.cookiejar.CookieJar,
    pid: int,
    name: str,
    cols: List[Tuple[str, str]],
    readme: str,
    purpose: str,
) -> None:
    c, o = req(
        "POST",
        "/data/tables",
        cj,
        project_id=pid,
        body={
            "table_name": name,
            "columns": [{"name": a, "sql_type": t} for a, t in cols],
            "readme": readme,
            "purpose": purpose,
        },
    )
    if c != 200:
        raise SystemExit(f"create {name} {c} {o}")


def build_curve() -> List[Dict[str, float]]:
    r: List[Dict[str, float]] = []
    for lv in range(1, LEVEL_MAX + 1):
        atk = 10.0 * (1.042 ** (lv - 1))
        dff = 0.5 * atk
        hp = 80.0 * (1.047 ** (lv - 1)) * (1.0 + 0.008 * lv)
        cr = min(0.45, 0.06 + lv * 0.0055)
        crr = min(0.4, cr * 0.88)
        skk = 1.2 * (1.02 ** (lv - 1))
        sks = 1.2 * (1.018 ** (lv - 1))
        r.append(
            {
                C["攻击力"]: atk,
                C["防御力"]: dff,
                C["生命值"]: hp,
                C["暴击率"]: cr,
                C["抗暴率"]: crr,
                C["杀伤技能系数"]: skk,
                C["生存技能系数"]: sks,
            }
        )
    return r


def w一阶(曲线: Sequence[Dict[str, float]]) -> List[Dict]:
    u: List[Dict] = []
    for lv in range(1, LEVEL_MAX + 1):
        b = 曲线[lv - 1]
        rid = f"L{lv}"
        for k, v in b.items():
            u.append({"row_id": rid, "column": k, "value": round(float(v), 6)})
        u.append({"row_id": rid, "column": C["等级"], "value": lv})
        u.append({"row_id": rid, "column": C["防御验算_公式"], "value": 0.0})
    return u


def w节奏() -> List[Dict]:
    u: List[Dict] = []
    for lv in range(1, LEVEL_MAX + 1):
        rid = f"L{lv}"
        if lv <= 15:
            m = max(2.0, 6.0 - lv * 0.2)
        elif lv <= 40:
            m = 3.0 + (lv - 15) * 0.12
        else:
            m = 5.0 + (lv - 40) * 0.1
        epm = 8.0 * (1.028**lv) * (1.0 + 0.01 * min(15, lv))
        ex = 50.0 * lv * (1.04 ** (lv * 0.4))
        u.extend(
            [
                {"row_id": rid, "column": C["等级"], "value": lv},
                {"row_id": rid, "column": C["停留分钟"], "value": round(m, 4)},
                {"row_id": rid, "column": C["每分钟经验"], "value": round(epm, 6)},
                {"row_id": rid, "column": C["升级目标经验"], "value": round(ex, 4)},
            ]
        )
    return u


def w二阶(曲线: Sequence[Dict[str, float]]) -> List[Dict]:
    cname = (C["攻击力"], C["防御力"], C["生命值"], C["暴击率"], C["抗暴率"])
    u: List[Dict] = []
    for lv in range(1, LEVEL_MAX + 1):
        rid = f"L{lv}"
        bl = 曲线[lv - 1]
        t_atk = t_de = t_hp = t_c = t_r = 0.0
        for _sk, m in ALLOC.items():
            t_atk += bl[C["攻击力"]] * m["攻击力"] / 100.0
            t_de += bl[C["防御力"]] * m["防御力"] / 100.0
            t_hp += bl[C["生命值"]] * m["生命值"] / 100.0
            t_c += bl[C["暴击率"]] * m["暴击率"] / 100.0
            t_r += bl[C["抗暴率"]] * m["抗暴率"] / 100.0
        u.extend(
            [
                {"row_id": rid, "column": C["等级"], "value": lv},
                {"row_id": rid, "column": cname[0] + "二阶", "value": round(t_atk, 6)},
                {"row_id": rid, "column": cname[1] + "二阶", "value": round(t_de, 6)},
                {"row_id": rid, "column": cname[2] + "二阶", "value": round(t_hp, 6)},
                {"row_id": rid, "column": cname[3] + "二阶", "value": round(t_c, 6)},
                {"row_id": rid, "column": cname[4] + "二阶", "value": round(t_r, 6)},
            ]
        )
    return u


def w分玩法(曲线: Sequence[Dict[str, float]], sk: str) -> List[Dict]:
    zh = SYS_ZH[sk]
    m = ALLOC[sk]
    u: List[Dict] = []
    for lv in range(1, LEVEL_MAX + 1):
        bl = 曲线[lv - 1]
        rid = f"L{lv}"
        u.append({"row_id": rid, "column": C["等级"], "value": lv})
        u.append({"row_id": rid, "column": C["系统"], "value": zh})
        u.append(
            {
                "row_id": rid,
                "column": C["攻击力"],
                "value": round(bl[C["攻击力"]] * m["攻击力"] / 100.0, 6),
            }
        )
        u.append(
            {
                "row_id": rid,
                "column": C["防御力"],
                "value": round(bl[C["防御力"]] * m["防御力"] / 100.0, 6),
            }
        )
        u.append(
            {
                "row_id": rid,
                "column": C["生命值"],
                "value": round(bl[C["生命值"]] * m["生命值"] / 100.0, 6),
            }
        )
        u.append(
            {
                "row_id": rid,
                "column": C["暴击率"],
                "value": round(bl[C["暴击率"]] * m["暴击率"] / 100.0, 6),
            }
        )
        u.append(
            {
                "row_id": rid,
                "column": C["抗暴率"],
                "value": round(bl[C["抗暴率"]] * m["抗暴率"] / 100.0, 6),
            }
        )
    return u


def cult_blobs(LEVEL: int) -> Dict[str, Any]:
    res: List[Tuple[str, str, str]] = [
        ("res_gold", "金币", "基础货币"),
        ("res_cry", "魔晶", "基础货币"),
        ("res_shard", "宝石碎片", "专项"),
        ("res_key", "副本钥匙", "专项"),
    ]
    u_cat: List[Dict] = []
    for key, disp, knd in res:
        rid = key
        u_cat.extend(
            [
                {"row_id": rid, "column": "资源键", "value": key},
                {"row_id": rid, "column": "资源名称", "value": disp},
                {"row_id": rid, "column": "资源类型", "value": knd},
            ]
        )
    u_out: List[Dict] = []
    for i, (key, _a, knd) in enumerate(res):
        for lv in range(1, LEVEL + 1):
            rr = f"{key}__L{lv}"
            if "基础" in knd:
                p = 0.25 * (1.025**lv) * (0.55 if i == 0 else 0.45)
            else:
                p = 0.09 * (lv**0.92)
            u_out.extend(
                [
                    {"row_id": rr, "column": C["等级"], "value": lv},
                    {"row_id": rr, "column": "资源", "value": key},
                    {"row_id": rr, "column": "每分钟产量", "value": round(p, 6)},
                ]
            )
    wmap = {"装备": 0.4, "坐骑": 0.3, "宝石": 0.2, "副本": 0.1}
    u_cas: List[Dict] = []
    for key, _a, knd in res:
        if "基础" not in knd:
            continue
        for wzh, wv in wmap.items():
            r2 = f"w_{key}_{wzh}"
            u_cas.extend(
                [
                    {"row_id": r2, "column": "资源", "value": key},
                    {"row_id": r2, "column": C["系统"], "value": wzh},
                    {"row_id": r2, "column": "权重", "value": wv},
                ]
            )
    wv_by = wmap
    u_q: List[Dict] = []
    for sk in SYS_KEYS:
        zh = SYS_ZH[sk]
        opl = OPEN_LEVEL_02[sk]
        for lv in range(1, LEVEL + 1):
            ridq = f"q_{sk}_L{lv}"
            wv = wv_by[zh]
            g = 0.3 * (1.025**lv) * wv
            u_q.extend(
                [
                    {"row_id": ridq, "column": C["等级"], "value": lv},
                    {"row_id": ridq, "column": C["系统"], "value": zh},
                    {
                        "row_id": ridq,
                        "column": "达开放等级",
                        "value": 1 if lv >= opl else 0,
                    },
                    {"row_id": ridq, "column": "可用金币_每分钟", "value": round(g, 6)},
                    {"row_id": ridq, "column": "可用魔晶_每分钟", "value": round(g * 0.32, 6)},
                ]
            )
    return {
        "cat": u_cat,
        "out": u_out,
        "w": u_cas,
        "q": u_q,
    }


def landing_by_system(
    system_key: str, level_max: int
) -> Tuple[List[Tuple[str, str]], List[Dict[str, Any]], str]:
    """
    各 *系统_落地* 行轴/列与 02 子系统说明对齐，不共用同构「标准等级+两列消耗」。
    宝石：以品阶链为行，非角色 1..N 一一对应行。
    """
    sk = system_key
    zh = SYS_ZH[sk]
    opl = OPEN_LEVEL_02[sk]

    def _e() -> Tuple[List, List, str]:
        cols: List[Tuple[str, str]] = [
            ("标准等级", "INTEGER"),
            ("装备等阶", "INTEGER"),
            ("主手攻击_投放比", "REAL"),
            ("铠甲防御_投放比", "REAL"),
            ("单件强化_建议金币", "REAL"),
            ("掉率_调节权重", "REAL"),
            ("多部位_细则摘要", "TEXT"),
        ]
        w: List[Dict] = []
        for lv in range(1, level_max + 1):
            tier = min(20, (lv + 9) // 10)
            rid = f"leq_{lv}"
            atk = round(0.32 + 0.011 * min(tier, 10), 4)
            dfn = round(0.3 + 0.009 * min(tier, 10), 4)
            w.extend(
                [
                    {"row_id": rid, "column": "标准等级", "value": lv},
                    {"row_id": rid, "column": "装备等阶", "value": tier},
                    {"row_id": rid, "column": "主手攻击_投放比", "value": atk},
                    {"row_id": rid, "column": "铠甲防御_投放比", "value": dfn},
                    {
                        "row_id": rid,
                        "column": "单件强化_建议金币",
                        "value": round(50 + lv * 2.1, 3),
                    },
                    {
                        "row_id": rid,
                        "column": "掉率_调节权重",
                        "value": round(0.2 + 0.0028 * lv, 4),
                    },
                    {
                        "row_id": rid,
                        "column": "多部位_细则摘要",
                        "value": "6部位:主/副/铠/下/鞋/饰，权重与02主系统一致;每10标级一等阶。",
                    },
                ]
            )
        rm = f"{zh}：轴=标准等级(1..{level_max})；等阶=每10标级一档，列含**属性投放**与强化消耗+掉权（E2E示例）。"
        return cols, w, rm

    def _m() -> Tuple[List, List, str]:
        cols2: List[Tuple[str, str]] = [
            ("标准等级", "INTEGER"),
            ("主系统_已开放", "INTEGER"),
            ("速度_外围加成", "REAL"),
            ("建议_培养_金币", "REAL"),
            ("一阶_生命_转化_说明", "TEXT"),
        ]
        w2: List[Dict] = []
        for lv in range(1, level_max + 1):
            rid = f"lmw_{lv}"
            open0 = 1 if lv >= opl else 0
            sp = (
                0.0
                if open0 == 0
                else round(0.05 + 0.0022 * (lv - opl), 4)
            )
            cst = 0.0 if open0 == 0 else round(80 + (lv - opl) * 1.5, 3)
            note = "未达开放(30)不生效" if open0 == 0 else f"同阶二阶/生命→坐骑外围({lv}标级)可对账"
            w2.extend(
                [
                    {"row_id": rid, "column": "标准等级", "value": lv},
                    {"row_id": rid, "column": "主系统_已开放", "value": open0},
                    {"row_id": rid, "column": "速度_外围加成", "value": sp},
                    {"row_id": rid, "column": "建议_培养_金币", "value": cst},
                    {"row_id": rid, "column": "一阶_生命_转化_说明", "value": note},
                ]
            )
        r = f"{zh}：轴=标准等级；{opl} 级后开放，列含**开放位**+外围+消耗（E2E）。"
        return cols2, w2, r

    def _g() -> Tuple[List, List, str]:
        # 行轴=品阶链，不铺 1..{level_max}
        cols3: List[Tuple[str, str]] = [
            ("品阶", "INTEGER"),
            ("三合一_规则", "TEXT"),
            ("解锁_标准等级_门槛", "INTEGER"),
            ("本阶_主属类型", "TEXT"),
            ("攻_投放_比", "REAL"),
            ("防_投放_比", "REAL"),
            ("体_投放_比", "REAL"),
            ("单格_金币", "REAL"),
            ("单格_宝石碎片", "REAL"),
        ]
        w3: List[Dict] = []
        types = ("攻击", "防御", "生命", "暴伤", "格挡", "穿透")
        for t in range(1, GEM_TIERS + 1):
            rid = f"lgm_{t}"
            ptype = types[(t - 1) % len(types)]
            a = round(0.33 + 0.012 * t, 4)
            b = round(0.3 + 0.01 * t, 4)
            c = round(max(0.0, 1.0 - a - b), 4)
            w3.extend(
                [
                    {"row_id": rid, "column": "品阶", "value": t},
                    {
                        "row_id": rid,
                        "column": "三合一_规则",
                        "value": "3个同品阶低→1个高1品" if t < GEM_TIERS else "满阶不合成",
                    },
                    {
                        "row_id": rid,
                        "column": "解锁_标准等级_门槛",
                        "value": min(
                            level_max, max(OPEN_LEVEL_02["gem"], 1 + (t - 1) * 2)
                        ),
                    },
                    {
                        "row_id": rid,
                        "column": "本阶_主属类型",
                        "value": f"池:{ptype}/辅随机",
                    },
                    {"row_id": rid, "column": "攻_投放_比", "value": a},
                    {"row_id": rid, "column": "防_投放_比", "value": b},
                    {"row_id": rid, "column": "体_投放_比", "value": c},
                    {"row_id": rid, "column": "单格_金币", "value": round(30 + t**1.7 * 6, 2)},
                    {
                        "row_id": rid,
                        "column": "单格_宝石碎片",
                        "value": round(2.0 + t**1.4, 2),
                    },
                ]
            )
        r2 = f"{zh}：轴=**品阶**(1..{GEM_TIERS})，三合一链；**非**{level_max} 行=角色标准等级 1:1。列含属性分配与双消耗。"
        return cols3, w3, r2

    def _d() -> Tuple[List, List, str]:
        cols4: List[Tuple[str, str]] = [
            ("标准等级", "INTEGER"),
            ("层_难度_系数", "REAL"),
            ("钥石_消耗", "INTEGER"),
            ("二阶_强度_参考", "REAL"),
            ("首通_产_基权", "REAL"),
            ("与一阶_二阶_对账", "TEXT"),
        ]
        w4: List[Dict] = []
        for lv in range(1, level_max + 1):
            rid = f"ldg_{lv}"
            open0 = 1 if lv >= opl else 0
            w4.extend(
                [
                    {"row_id": rid, "column": "标准等级", "value": lv},
                    {
                        "row_id": rid,
                        "column": "层_难度_系数",
                        "value": round(1.0 + 0.022 * (lv if open0 else 0), 3),
                    },
                    {
                        "row_id": rid,
                        "column": "钥石_消耗",
                        "value": 0 if not open0 else 1 + lv // 15,
                    },
                    {
                        "row_id": rid,
                        "column": "二阶_强度_参考",
                        "value": 0.0
                        if not open0
                        else round(100.0 * (1.08**lv) / 1e2, 3),
                    },
                    {
                        "row_id": rid,
                        "column": "首通_产_基权",
                        "value": 0.0
                        if not open0
                        else round(0.15 + 0.0015 * lv, 4),
                    },
                    {
                        "row_id": rid,
                        "column": "与一阶_二阶_对账",
                        "value": "未开放" if not open0 else f"对二阶攻击/二阶防与钥石;层{lv}",
                    },
                ]
            )
        r3 = f"{zh}：轴=标准等级；{opl} 级后开放。含难度/钥石/**强度**与一阶/二阶跨表对账列。"
        return cols4, w4, r3

    if sk == "equipment":
        return _e()
    if sk == "mount":
        return _m()
    if sk == "gem":
        return _g()
    if sk == "dungeon":
        return _d()
    raise KeyError(sk)


def _reg_demo_formula(cj: http.cookiejar.CookieJar, pid: int) -> None:
    c, o = req(
        "POST",
        "/compute/formulas/register",
        cj,
        project_id=pid,
        body={
            "table_name": T一阶,
            "column_name": C["防御验算_公式"],
            "formula": f"0.5*@{T一阶}[{C['攻击力']}]",
        },
    )
    if c != 200:
        raise SystemExit(f"register formula {c} {o}")
    c2, o2 = req(
        "POST",
        "/compute/formulas/execute",
        cj,
        project_id=pid,
        body={"table_name": T一阶, "column_name": C["防御验算_公式"], "level_column": C["等级"]},
    )
    if c2 != 200:
        raise SystemExit(f"execute formula {c2} {o2}")


def main() -> int:
    ts = int(time.time())
    user, pw = f"nf_real_{ts}", f"NfE2e_{ts}!x"
    cj = http.cookiejar.CookieJar()
    assert 200 == req("POST", "/auth/register", cj, body={"username": user, "password": pw, "invite_code": INVITE})[0]
    # 无 Cookie
    try:
        urllib.request.build_opener().open(
            urllib.request.Request(
                BASE + "/agent/chat",
                b'{"message":"x","mode":"maintain"}',
                headers={"Content-Type": "application/json", "X-Project-Id": "1"},
                method="POST",
            ),
            timeout=10,
        )
        c0 = 200
    except urllib.error.HTTPError as e:
        c0 = e.code
    assert c0 == 401, c0
    _, plist = req("GET", "/projects", cj)
    t0 = next((p for p in (plist or {}).get("projects", []) if p.get("is_template")), None)
    if t0:
        assert 403 == req("POST", "/pipeline/advance", cj, project_id=t0["id"], body={"step": PIPELINE_STEPS[0]})[0]
    stt = {
        "core": {"title": f"实机E2E_{ts}", "formula": "subtraction", "level_max": LEVEL_MAX, "notes": "中文表头/分表"},
        "game_systems": {"enabled": list(SYS_KEYS), "stubs": {k: {"开放等级": 1 + i} for i, k in enumerate(SYS_KEYS)}},
        "attribute_system": {
            "core_attrs": ["攻", "防", "血"],
            "advanced": ["技杀", "技生"],
        },
    }
    _, cr = req("POST", "/projects", cj, body={"name": f"实机E2E-{ts}", "settings": stt})
    pid, slug = int(cr["id"]), cr["slug"]
    greadme = (
        f"# 全局\n等级 1–{LEVEL_MAX}；一阶/二阶/分{len(SYS_KEYS)}玩法/资源/落地 已分表，列名均中文。"
        f"各 *系统_落地* 行轴/列**按 02 子系统区分**（装备=标级+等阶+投放；"
        f"宝石=**品阶链**；坐骑=标级+开放(≥30)+外围；"
        f"副本=标级+钥石/对账）—非同构 60/200 行「仅两列消耗」。\n"
        f"公式列「{C['防御验算_公式']}」=0.5×攻击力。\n"
    )
    assert 200 == req("PUT", "/meta/global-readme", cj, project_id=pid, body={"content": greadme})[0]
    曲线 = build_curve()
    u1 = w一阶(曲线)
    up = w节奏()
    u_sch: List[Dict] = []
    for sk in SYS_KEYS:
        zh = SYS_ZH[sk]
        d = f"doc_{sk}"
        u_sch.extend(
            [
                {"row_id": d, "column": C["系统"], "value": zh},
                {
                    "row_id": d,
                    "column": C["属性说明"],
                    "value": "、".join(["攻击力", "防御力", "生命", "暴击/抗暴", "双技能线"]),
                },
                {"row_id": d, "column": C["已覆盖全属性"], "value": 1},
            ]
        )
    u_all: List[Dict] = []
    for skey, m in ALLOC.items():
        for attr, p in m.items():
            rid = f"aa_{skey}_{attr}"
            u_all.extend(
                [
                    {"row_id": rid, "column": C["系统"], "value": SYS_ZH[skey]},
                    {"row_id": rid, "column": C["属性"], "value": attr},
                    {"row_id": rid, "column": C["分配百分比"], "value": p},
                ]
            )
    u2 = w二阶(曲线)
    cb = cult_blobs(LEVEL_MAX)

    def _steps(step: str) -> int:
        n = 0
        if step == "environment_global_readme":
            _mk(
                cj,
                pid,
                T流水,
                [("步骤标识", "TEXT"), ("阶段序号", "INTEGER")],
                "与流水线环节一一对应（格值可为技术 id）",
                "environment_global_readme",
            )
            w = []
            for o, s in enumerate(PIPELINE_STEPS, 1):
                w.extend(
                    [
                        {"row_id": f"idx_{s}", "column": "步骤标识", "value": s},
                        {"row_id": f"idx_{s}", "column": "阶段序号", "value": o},
                    ]
                )
            n = len(w)
            _batch_writes(cj, pid, T流水, w)
            return n
        if step == "base_attribute_framework":
            one_cols = [
                (C["等级"], "INTEGER"),
                (C["攻击力"], "REAL"),
                (C["防御力"], "REAL"),
                (C["生命值"], "REAL"),
                (C["暴击率"], "REAL"),
                (C["抗暴率"], "REAL"),
                (C["杀伤技能系数"], "REAL"),
                (C["生存技能系数"], "REAL"),
                (C["防御验算_公式"], "REAL"),
            ]
            _mk(cj, pid, T一阶, one_cols, "一阶全等级+公式列", "base_attribute_framework")
            _mk(
                cj,
                pid,
                T节奏,
                [
                    (C["等级"], "INTEGER"),
                    (C["停留分钟"], "REAL"),
                    (C["每分钟经验"], "REAL"),
                    (C["升级目标经验"], "REAL"),
                ],
                "停留/经验",
                "base_attribute_framework",
            )
            n = len(u1) + len(up)
            _batch_writes(cj, pid, T一阶, u1)
            _batch_writes(cj, pid, T节奏, up)
            _reg_demo_formula(cj, pid)
            return n
        if step == "gameplay_attribute_scheme":
            _mk(
                cj,
                pid,
                T方案,
                [(C["系统"], "TEXT"), (C["属性说明"], "TEXT"), (C["已覆盖全属性"], "INTEGER")],
                "分玩法",
                "gameplay_attribute_scheme",
            )
            n = len(u_sch)
            _batch_writes(cj, pid, T方案, u_sch)
            return n
        if step == "gameplay_allocation_tables":
            _mk(
                cj,
                pid,
                T分配,
                [(C["系统"], "TEXT"), (C["属性"], "TEXT"), (C["分配百分比"], "REAL")],
                "可≠100%",
                "gameplay_allocation_tables",
            )
            n = len(u_all)
            _batch_writes(cj, pid, T分配, u_all)
            return n
        if step == "second_order_framework":
            c2 = [C["等级"]] + [f"{x}二阶" for x in ("攻击力", "防御力", "生命值", "暴击率", "抗暴率")]
            _mk(
                cj,
                pid,
                T二阶,
                [(a, "REAL" if a != C["等级"] else "INTEGER") for a in c2],
                "加总后二阶",
                "second_order_framework",
            )
            n = len(u2)
            _batch_writes(cj, pid, T二阶, u2)
            return n
        if step == "gameplay_attribute_tables":
            gc = [
                (C["等级"], "INTEGER"),
                (C["系统"], "TEXT"),
                (C["攻击力"], "REAL"),
                (C["防御力"], "REAL"),
                (C["生命值"], "REAL"),
                (C["暴击率"], "REAL"),
                (C["抗暴率"], "REAL"),
            ]
            tot = 0
            for sk in SYS_KEYS:
                name = t_玩法(SYS_ZH[sk])
                _mk(cj, pid, name, gc, f"{SYS_ZH[sk]} 单独二级表", "gameplay_attribute_tables")
                w = w分玩法(曲线, sk)
                tot += len(w)
                _batch_writes(cj, pid, name, w)
            return tot
        if step == "cultivation_resource_design":
            _mk(
                cj,
                pid,
                T资目,
                [("资源键", "TEXT"), ("资源名称", "TEXT"), ("资源类型", "TEXT")],
                "两基础币+专用品",
                "cultivation_resource_design",
            )
            n2 = len(cb["cat"])
            _batch_writes(cj, pid, T资目, cb["cat"])
            return n2
        if step == "cultivation_resource_framework":
            _mk(
                cj,
                pid,
                T资出,
                [(C["等级"], "INTEGER"), ("资源", "TEXT"), ("每分钟产量", "REAL")],
                "按资源×等级",
                "cultivation_resource_framework",
            )
            n2 = len(cb["out"])
            _batch_writes(cj, pid, T资出, cb["out"])
            return n2
        if step == "cultivation_allocation_tables":
            _mk(
                cj,
                pid,
                T资权,
                [("资源", "TEXT"), (C["系统"], "TEXT"), ("权重", "REAL")],
                "可≠100%",
                "cultivation_allocation_tables",
            )
            n2 = len(cb["w"])
            _batch_writes(cj, pid, T资权, cb["w"])
            return n2
        if step == "cultivation_quant_tables":
            qc = [
                (C["等级"], "INTEGER"),
                (C["系统"], "TEXT"),
                ("达开放等级", "INTEGER"),
                ("可用金币_每分钟", "REAL"),
                ("可用魔晶_每分钟", "REAL"),
            ]
            tot = 0
            for sk in SYS_KEYS:
                name = t_资可(SYS_ZH[sk])
                _mk(cj, pid, name, qc, f"{SYS_ZH[sk]} 单表", "cultivation_quant_tables")
                w3 = [x for x in cb["q"] if f"q_{sk}" in x["row_id"]]
                tot += len(w3)
                _batch_writes(cj, pid, name, w3)
            return tot
        if step == "gameplay_landing_tables":
            tot = 0
            for sk in SYS_KEYS:
                name = t_落地(SYS_ZH[sk])
                lcols, lrows, lreadme = landing_by_system(sk, LEVEL_MAX)
                _mk(
                    cj,
                    pid,
                    name,
                    lcols,
                    f"{lreadme}",
                    "gameplay_landing_tables",
                )
                tot += len(lrows)
                _batch_writes(cj, pid, name, lrows)
            return tot
        return 0

    assert 400 == req("POST", "/pipeline/advance", cj, project_id=pid, body={"step": "gameplay_attribute_scheme"})[0]
    tot_cells = 0
    sids: List[int] = []
    for s in PIPELINE_STEPS:
        tot_cells += _steps(s)
        a, b = req("POST", "/pipeline/advance", cj, project_id=pid, body={"step": s})
        if a != 200:
            print("adv", s, a, b, file=sys.stderr)
            return 1
        sp = (b or {}).get("snapshot") or {}
        if isinstance(sp, dict) and sp.get("snapshot_id") is not None:
            sids.append(int(sp["snapshot_id"]))
    st = req("GET", "/pipeline/status", cj, project_id=pid)[1]
    assert st.get("finished") is True
    assert 400 == req("POST", "/pipeline/advance", cj, project_id=pid, body={"step": PIPELINE_STEPS[0]})[0]
    cdesc, fdesc = req("GET", "/data/tables/" + urllib.parse.quote(T一阶, safe=""), cj, project_id=pid)
    assert cdesc == 200
    if not (fdesc or {}).get("column_formulas", {}).get(C["防御验算_公式"]):
        print("warn: 公式未出现在 describe", fdesc, file=sys.stderr)
    _, vr = req("POST", "/validate/run", cj, project_id=pid, body={})
    res = req(
        "POST", "/compute/algorithm-apis/call", cj, project_id=pid, body={"api_name": "echo_sum", "params": {"numbers": [1, 2, 3]}}
    )[1]
    assert (res or {}).get("result", {}).get("sum") == 6.0
    c5, r5 = req("GET", f"/data/cells/{urllib.parse.quote(T一阶)}/L5/{urllib.parse.quote(C['攻击力'])}", cj, project_id=pid)
    v5 = (r5 or {}).get("value") if c5 == 200 else None
    if v5 is not None:
        for _ in range(2):
            a, _b = req(
                "POST",
                "/data/cells/write",
                cj,
                project_id=pid,
                body={"table_name": T一阶, "updates": [{"row_id": "L5", "column": C["攻击力"], "value": v5}], "source_tag": "ai_generated"},
            )
            assert a == 200
    ntbl = len((req("GET", "/meta/tables", cj, project_id=pid)[1] or {}).get("tables", []))
    if not DATA_ONLY:
        u = BASE + "/agent/chat"
        b = json.dumps(
            {
                "message": f"read_table 表 {T一阶} limit 3；再 read {t_落地('装备')}",
                "mode": "init",
            },
            ensure_ascii=False,
        ).encode()
        h = {"Content-Type": "application/json", "X-Project-Id": str(pid), "Accept": "text/event-stream"}
        rr = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj)).open(urllib.request.Request(u, data=b, headers=h, method="POST"), timeout=60)
        rr.read(1000)
        rr.close()
        assert len((req("GET", "/meta/tables", cj, project_id=pid)[1] or {}).get("tables", [])) == ntbl
        ev1 = sse_agent_chat(
            cj,
            pid,
            f"对 {T一阶} 用 write_cells 将 L3 的 {C['生命值']} 设为原值*1.001；对 {t_落地('装备')} 的 README 用 update_table_readme 追加句「E2E」。",
            "init",
        )
        ev2 = sse_agent_chat(
            cj, pid, f"read_table {T一阶} 确认 L3 {C['生命值']}", "init"
        )
    else:
        ev1 = ev2 = []
    rct = 0
    for tnx in [T一阶, T二阶, t_玩法("装备"), T资出, t_资可("装备"), t_落地("装备")]:
        a, b = req("GET", f"/data/tables/{urllib.parse.quote(tnx, safe='')}/rows?limit=5000", cj, project_id=pid)
        if a == 200 and isinstance(b, dict):
            rct += len((b.get("rows") or []))
    out = {
        "user": user,
        "project_id": pid,
        "slug": slug,
        "level_max": LEVEL_MAX,
        "cells": tot_cells,
        "row_sample_count": rct,
        "formula_desc_ok": bool((fdesc or {}).get("column_formulas", {}).get(C["防御验算_公式"])),
        "snaps": len(sids),
    }
    if not DATA_ONLY:
        out["a1"] = [e.get("type") for e in ev1]
        out["a2"] = [e.get("type") for e in ev2]
    print(json.dumps(out, ensure_ascii=False, indent=2))
    if tot_cells < 4000:
        print("warn: cells", tot_cells, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

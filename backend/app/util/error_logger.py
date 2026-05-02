"""Agent 错误监控日志 — 写入文件便于排查。"""

from __future__ import annotations

import json
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

from app.config import DATA_DIR

# 日志目录
LOG_DIR = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 当前日志文件路径
def _log_file() -> Path:
    return LOG_DIR / f"agent_errors_{time.strftime('%Y%m%d')}.log"


def log_agent_error(
    *,
    step_id: str = "",
    session_id: Optional[int] = None,
    phase: str = "",
    round_i: int = 0,
    error_type: str = "unknown",
    error_msg: str = "",
    exc: Optional[Exception] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """记录 Agent 错误到日志文件。"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    
    entry = {
        "ts": ts,
        "step_id": step_id,
        "session_id": session_id,
        "phase": phase,
        "round": round_i,
        "error_type": error_type,
        "error_msg": error_msg[:500],
    }
    
    if exc:
        entry["exception"] = repr(exc)[:500]
        entry["traceback"] = traceback.format_exc()[:2000]
    
    if context:
        # 截取关键上下文，避免日志过大
        ctx = {}
        for k, v in context.items():
            if isinstance(v, str):
                ctx[k] = v[:300]
            elif isinstance(v, (int, float, bool)):
                ctx[k] = v
            else:
                ctx[k] = str(v)[:300]
        entry["context"] = ctx
    
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    
    try:
        with open(_log_file(), "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass  # 日志写入失败不应影响主流程


def log_api_call(
    *,
    step_id: str = "",
    session_id: Optional[int] = None,
    phase: str = "",
    model: str = "",
    attempt: int = 1,
    success: bool = True,
    latency_ms: int = 0,
    error_msg: str = "",
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
) -> None:
    """记录 API 调用统计（成功/失败/延迟）。"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    
    entry = {
        "ts": ts,
        "type": "api_call",
        "step_id": step_id,
        "session_id": session_id,
        "phase": phase,
        "model": model,
        "attempt": attempt,
        "success": success,
        "latency_ms": latency_ms,
    }
    
    if error_msg:
        entry["error_msg"] = error_msg[:300]
    if prompt_tokens:
        entry["prompt_tokens"] = prompt_tokens
    if completion_tokens:
        entry["completion_tokens"] = completion_tokens
    
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    
    try:
        with open(_log_file(), "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

# 千问（DashScope）— Agent 接入说明（精简版）

本文从《游戏数值系统 AI 化自动开发》中粘贴的阿里云百炼文档里，**只保留与本项目 Agent 后端集成相关的条目**：密钥、模型名、调用端点、请求/响应要点、上下文缓存。

---

## 1. API Key（务必用环境变量）

| 项 | 说明 |
|----|------|
| 控制台创建 | [阿里云百炼 — 密钥管理](https://bailian.console.aliyun.com/?tab=model#/api-key) |
| 环境变量名 | `DASHSCOPE_API_KEY` |
| HTTP 用法 | 请求头 `Authorization: Bearer <DASHSCOPE_API_KEY>` |

**你当前计划使用的密钥**（从主文档提取；请尽快只保留在 `.env` / 密钥管理器中）：

- **集成代码中禁止写死**：在服务器 `.env` 或进程环境中配置 `DASHSCOPE_API_KEY`，由 FastAPI 启动时加载（与 `04-前后端框架与部署` 中 LLM Key 约定一致）。
- **安全**：若该文件或主文档会进入共享仓库或网盘，请在百炼控制台 **轮换 API Key**，并删除一切明文副本。

**附录（已移除明文密钥）**：请在服务器 `backend/.env` 或进程环境中配置 `DASHSCOPE_API_KEY`；若密钥曾出现在共享仓库中，请在百炼控制台轮换。

---

## 2. 目标模型

| 配置项 | 值 |
|--------|-----|
| `model` | `qwen3.6-plus` |
| 系列说明 | 千问 Plus 档；北京地域模型列表见 [模型列表](https://help.aliyun.com/zh/model-studio/models) |
| API 参考 | [千问 API 参考](https://help.aliyun.com/zh/model-studio/qwen-api-reference/) |

**注意（来自粘贴文档）**：除 `qwen3.6-max-preview` 外，Qwen3.6 / Qwen3.5 部分能力走 **多模态接口**；若把多模态模型误接到纯文本 Chat 路径可能报 `url error`。本项目 Agent 以 **文本 + OpenAI 兼容 Chat Completions** 为主时，以你当前选用的 **`qwen3.6-plus` + compatible-mode** 官方示例为准；若后续换 VL/Omni，需改 endpoint 与 `messages` 结构。

---

## 3. 调用方式（推荐：OpenAI 兼容 + Python）

### 3.1 端点（北京地域）

| 用途 | URL |
|------|-----|
| OpenAI 兼容 Chat Completions | `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| 单路径（SDK 里常写作 base + 方法路径） | `POST .../v1/chat/completions` |

新加坡地域将 host 换为 `dashscope-intl.aliyuncs.com`（**北京与新加坡的 API Key 不通用**，需分别创建）。

### 3.2 Python（`openai` SDK，与 Agent 后端同栈）

```python
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ["DASHSCOPE_API_KEY"],
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

completion = client.chat.completions.create(
    model="qwen3.6-plus",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "你是谁？"},
    ],
)
text = completion.choices[0].message.content
```

依赖：`pip install -U openai`

### 3.3 Agent 流式输出（对接前端 SSE）

在 `chat.completions.create` 中增加 `stream=True`，按 chunk 解析 `choices[0].delta.content` 并写入 SSE（与 `04` 中「Agent 流式输出使用 SSE」一致）。官方流式说明：[流式输出](https://help.aliyun.com/zh/model-studio/stream)。

---

## 4. 关键消息结构（来 / 去）

### 4.1 请求（Chat Completions）

核心 JSON 字段：

| 字段 | 含义 |
|------|------|
| `model` | 固定为 `qwen3.6-plus`（或你后续更换的百炼模型 id） |
| `messages` | 数组；每项含 `role`：`system` \| `user` \| `assistant`，`content` 为字符串（或多段 content 块，见缓存节） |
| `stream` | 可选；`true` 时响应为分块流 |
| `temperature` 等 | 可选；按 Agent 需要收敛或发散 |

典型 `messages` 形状：

```json
[
  {"role": "system", "content": "系统提示词 / 角色与工具约束摘要"},
  {"role": "user", "content": "用户自然语言或结构化任务"}
]
```

多轮时在数组末尾交替追加 `user` / `assistant`。

### 4.2 响应（非流式简化）

| 路径 | 含义 |
|------|------|
| `choices[0].message.role` | 一般为 `assistant` |
| `choices[0].message.content` | 模型正文 |
| `choices[0].finish_reason` | 如 `stop`；若异常可能为 `length` 等 |
| `usage.prompt_tokens` | 输入 token 数 |
| `usage.completion_tokens` | 输出 token 数 |
| `usage.total_tokens` | 合计 |
| `model` / `id` | 实际使用的模型与请求 id |

示例（结构与粘贴文档一致，数值为示意）：

```json
{
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "……模型回复文本……"
      },
      "finish_reason": "stop",
      "index": 0
    }
  ],
  "usage": {
    "prompt_tokens": 26,
    "completion_tokens": 66,
    "total_tokens": 92
  },
  "model": "qwen3.6-plus",
  "id": "chatcmpl-……"
}
```

### 4.3 与 Agent 工具的衔接（逻辑上）

- **去**：你把 `06` 中的系统提示 + 本轮用户指令 +（可选）工具返回摘要，拼进 `messages`。
- **来**：解析 `content`；若实现 Function Calling，则解析 `tool_calls` 再循环调用工具（详见百炼 [Function Calling](https://help.aliyun.com/zh/model-studio/qwen-function-calling)）。

---

## 5. 上下文缓存（Context Cache）

用于 **多轮对话或重复长前缀**（如固定系统提示 + 项目 README）时降延迟与成本。

### 5.1 两种模式（互斥，单次请求只用一种）

| 项目 | 显式缓存 | 隐式缓存 |
|------|----------|----------|
| 配置 | 在 `messages` 的 content 块上加 `"cache_control": {"type": "ephemeral"}` | 无需配置，自动前缀匹配 |
| 最少 Token | 1024 | 256 |
| 有效期 | 5 分钟（命中后重置） | 不定期，系统回收冷数据 |
| 创建缓存的输入计费 | 标准输入单价的 **125%** | **100%**（无额外创建费） |
| 命中缓存的输入计费 | 标准输入单价的 **10%** | **20%** |

### 5.2 显式缓存用法要点

- 在需缓存的文本块上使用 `cache_control`；从该标记 **向前** 最多回溯 **20 个** `content` 块尝试命中。
- **单次请求最多 4 个**缓存标记。
- **未命中**：从 `messages` 开头到该标记形成新缓存块；创建发生在 **模型响应之后**，故第二次请求再依赖命中更稳妥。
- **命中**：取最长公共前缀块，有效期重置为 5 分钟。

**OpenAI 兼容下带结构的 system 示例（多段 text + cache）**：

```json
{
  "role": "system",
  "content": [
    {
      "type": "text",
      "text": "<超长固定上下文，需 >1024 tokens 才满足显式缓存下限>",
      "cache_control": {"type": "ephemeral"}
    }
  ]
}
```

### 5.3 用量字段（便于日志与计费核对）

在支持缓存的调用中，`usage` 可能包含（名称以 OpenAI 兼容为准，实际以响应为准）：

- `prompt_tokens_details.cached_tokens` — 命中缓存的 token 数  
- `prompt_tokens_details.cache_creation_input_tokens` — 创建缓存消耗的输入 token（部分 SDK 字段名略有差异，如 `cache_creation_input_tokens` / `cache_read_input_tokens`）

**支持显式缓存的模型（中国内地摘录）**：含 **`qwen3.6-plus`**（千问 Plus 行），完整列表见原粘贴文档「支持的模型」小节或官方更新页。

### 5.4 其他

- **Responses API** 另有 **Session 缓存** 路径，与 Chat Completions 不同；若未来迁移可参考：[Session 缓存](https://help.aliyun.com/zh/model-studio/compatibility-with-openai-responses-api#example-session-cache)。

---

## 6. Numflow 后端自检（已实现）

| 方法 | 路径 | 作用 |
|------|------|------|
| `GET` | `/api/agent/diagnostics` | 是否已配置 `DASHSCOPE_API_KEY`、当前模型名（不发起外呼） |
| `POST` | `/api/agent/diagnostics/run` | 内置中文提示词：短对话验证连通性；两轮相同长 `system`（`cache_control: ephemeral`）对比 `usage`，返回 `cache_summary` 与完整 `cache_rounds` |

密钥从 `backend/.env` 或 systemd `EnvironmentFile` 加载；**勿将 `.env` 提交仓库**。

---

## 7. 建议的后续整理

将《游戏数值系统 AI 化自动开发.md》中从「账号设置」起的整段阿里云教程删除，仅保留一行链接指向本文或官方文档，避免重复与密钥扩散。

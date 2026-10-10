# Feishu LLM Bot

通过自己的飞书机器人，使用本机 **Claude Code 或 TraeX** 的会话历史和工具。
支持单用户私聊、图片、进度卡、串行任务、取消/恢复和独立结果交付。

Python 管理消息和任务，Node MCP 提供受管工具；Claude/TraeX 使用已有本机登录。
每次任务从指定会话分叉执行，成功后延续新的会话历史。

0.3.1 增加有界分析、阶段成果保存和部分完成展示。Libra 查询返回精简指标与缺数状态，
达到查询或阅读预算后收尾；Claude 历史过大时从已确认摘要开始新会话。

## 安装和启动

需要 Python 3.11+、Node.js 20+，以及已登录的 Claude 或 TraeX。Linux 已验证；
macOS 提供 POSIX/launchd 实现但尚未实机验收，Windows 请使用 WSL2。

```bash
git clone https://github.com/Frodo20/feishu-llm-bot.git
cd feishu-llm-bot
python3 scripts/install.py

.venv/bin/feishu-bot init \
  --backend traex \
  --agent-command traex \
  --session YOUR_SESSION_UUID \
  --cwd "$HOME/workspace" \
  --owner ou_YOUR_OPEN_ID \
  --app-id cli_YOUR_APP_ID \
  --state-dir "$HOME/.local/state/my-feishu-bot"

.venv/bin/feishu-bot doctor --config "$HOME/.local/state/my-feishu-bot/runtime.json"
.venv/bin/feishu-bot probe --config "$HOME/.local/state/my-feishu-bot/runtime.json"
.venv/bin/feishu-bot run --config "$HOME/.local/state/my-feishu-bot/runtime.json"
```

使用 Claude 时将 backend 和 agent-command 改为 `claude`；也可指定 `claude-w` 等包装器。
需要交互式选择模型的包装器必须加 `--model MODEL`。没有已有会话时，可将 `--session`
改为 `--new-session`。工作目录必须存在；App Secret 通过隐藏输入提供。

`probe` 使用隔离目录验证真实工具调用和会话继承，不连接飞书。`run` 才会启动机器人服务。
飞书权限、用户 open_id、凭据文件、常驻服务和可选集成的完整设置见
[安装与接入指南](PORTABLE_DEPLOYMENT.md)。

## 运维

- `status`：只读查询当前会话基点、任务与投递状态。
- `watch-worker`：本地跟随当前执行会话；空闲时等待下一项任务。
- `service-files`：生成本实例的 systemd 或 launchd 服务配置。
- 飞书控制：`/status`、`/queue`、`/cancel`、`/continue`、`/result`。

新实例默认提供通用工具；documents 和 Libra 需要显式启用及各自的 CLI 登录。
实例凭据、数据库和任务记录放在仓库之外。不要把实际 runtime.json、.env、会话或成果提交到 Git。

## 设计和验证

- [技术方案](TECHNICAL_DESIGN.md)
- [部署指南](PORTABLE_DEPLOYMENT.md)
- [开发约定](AGENTS.md)
- [0.3.1 改进与验证](RELIABILITY_0_3_1.md)

0.3.0 基线通过 393 项 Python 测试、Ruff、Node MCP 验证，以及 Claude/TraeX 真实工具调用
和历史继承验证。MCP SDK 锁定 1.32.1。目标用户仍需确认自己的飞书应用权限和首条消息收发。

开发验证：

```bash
python3 scripts/install.py --dev
PYTHONPATH=src .venv/bin/pytest -o addopts='' -q
.venv/bin/ruff check src tests scripts
node node-channel/worker-smoke-test.mjs
```

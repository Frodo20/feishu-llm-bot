# Feishu LLM Bot：迁移安装与 Claude / TraeX 接入

这个版本让每个用户在自己的机器上，将自己的飞书机器人连接到本机已有的 Claude Code
或 TraeX 会话。飞书负责输入和展示，原 CLI 负责模型登录和工具执行，bot 管理任务、
进度和结果。支持单用户私聊、单任务串行执行；不同用户使用各自的机器人和状态目录。

## 1. 准备

- Linux，或 macOS。Linux 的 systemd 与普通进程运行方式都可使用；macOS 使用普通进程
  和 launchd。原生 Windows 请使用 WSL2。macOS 的服务文件已实现，尚未在 macOS 实机验收。
- Python 3.11+（含 venv/ensurepip 或已安装 virtualenv）、Node.js 20+（含 npm），以及可下载依赖的网络。
- 已登录、能够在目标工作目录完成一次对话的 `claude` / `claude-w`，或 `traex` / `traecli`。
  两种后端只需安装其中一种。TraeX 需要提供 app-server 的 thread/start、thread/fork、
  turn/start 和结构化通知接口；本版实测 TraeX 0.208.1。
- 飞书自建应用的 App ID、App Secret，以及允许使用该 bot 的用户在**此应用下**的 open_id。
  用户 open_id 以 `ou_` 开头，不是手机号、员工 ID 或聊天 ID。可以从开放平台 API 调试台、
  通讯录 API（需相应权限）或该应用收到的消息事件 sender 字段获得。

在飞书开放平台启用机器人能力，使用“长连接接收事件”，订阅 `im.message.receive_v1`；
启用卡片交互时订阅 `card.action.trigger`。授予读取发给机器人的单聊消息、机器人发送消息、
读取消息资源（图片）、消息表情读写的权限；常用 scope 包括
`im:message.p2p_msg:readonly`、`im:message:send_as_bot`、`im:resource`、
`im:message.reactions:read`、`im:message.reactions:write_only`，也可使用租户已批准的上位权限。
发布应用版本并把使用者加入可用范围。长连接不需要公网回调服务器。

## 2. 安装代码

解压迁移包后进入目录：

```bash
tar -xzf feishu-llm-bot-0.3.0.tar.gz
cd feishu-llm-bot
python3 scripts/install.py
```

安装脚本创建本目录的 `.venv`，安装 Python 和 Node 依赖。它不会安装或修改全局模型 CLI、
不会修改全局权限/hook，也不会启动飞书接收器。若系统 Python 低于 3.11，请改用
`python3.11 scripts/install.py`。开发者可加 `--dev` 安装测试依赖。

迁移包仅包含代码、测试和通用文档；不包含原用户凭据、数据库、会话、任务成果或虚拟环境。
另一个机器上的会话 UUID 必须属于那台机器的 CLI 登录环境。

## 3. 初始化一个用户的 bot

Claude 示例（替换 `ou_...`、`cli_...` 和会话 UUID）：

```bash
.venv/bin/feishu-bot init \
  --backend claude \
  --agent-command claude \
  --session YOUR_CLAUDE_SESSION_UUID \
  --cwd "$HOME/workspace" \
  --owner ou_YOUR_OPEN_ID \
  --app-id cli_YOUR_APP_ID \
  --state-dir "$HOME/.local/state/my-feishu-bot"
```

TraeX 使用同样的入口：

```bash
.venv/bin/feishu-bot init \
  --backend traex \
  --agent-command traex \
  --session YOUR_TRAEX_SESSION_UUID \
  --cwd "$HOME/workspace" \
  --owner ou_YOUR_OPEN_ID \
  --app-id cli_YOUR_APP_ID \
  --state-dir "$HOME/.local/state/my-feishu-bot"
```

App Secret 通过终端隐藏输入。无人值守初始化可从进程环境 `FEISHU_APP_SECRET` 读取，
或用 `--credentials-file /private/credentials.json` 提供当前用户所有、权限为 0600 的
`{"app_id":"...","app_secret":"..."}` 文件。不要把 Secret 写进命令参数或仓库。

工作目录必须已存在。`--session` 指定本机已有会话；也可以改用 `--new-session`。
通常不必指定模型，使用 CLI 的有效默认配置；需要固定模型时加 `--model MODEL`。
**使用会弹出模型选择菜单的包装器（例如 claude-w）时必须传 --model**；TraeX 可另加
`--model-provider PROVIDER`。`--name` 设置卡片中的助手名称。

初始化会保存当前 PATH，以及已设置的 TRAE_HOME、TRAECLI_HOME、CLAUDE_CONFIG_DIR。
包装器依赖的 node、claude 等命令也必须在 PATH 中；可以用 `--path '完整的搜索路径'`
或 `--node-command /absolute/path/to/node` 显式指定。换机器必须重新 init，不能直接复用
旧机器的 runtime.json 中的绝对路径。已有非空状态目录会拒绝覆盖。

**会话连接的具体含义**：bot 从指定会话继承历史，每次任务建立独立分支，成功后以新分支
作为下一次基点。原对话可以保持打开；飞书的新回复在飞书卡片和 bot 的 worker 记录中，
不会自动追加到原 TUI。空闲时没有常驻模型进程，接收服务一直在线。
`status` 输出最新基点；`watch-worker` 可以连续观察后续任务。

## 4. 检查、验证和启动

```bash
.venv/bin/feishu-bot doctor --config "$HOME/.local/state/my-feishu-bot/runtime.json"
.venv/bin/feishu-bot probe --config "$HOME/.local/state/my-feishu-bot/runtime.json"
.venv/bin/feishu-bot run --config "$HOME/.local/state/my-feishu-bot/runtime.json"
```

`doctor` 检查配置、凭据文件权限、可执行文件、MCP 依赖及运行方式；不调用模型或飞书 API。
`probe` 会调用两轮真实模型：在临时目录执行一次输出随机标记的命令，再分叉会话检查历史
继承。它只 fork 指定基点，使用独立数据库、工作目录和输出记录，不启动 gateway/sender，
不发送飞书消息。其成功结果说明模型、工具与续接链路可用；飞书租户权限需要启动后实测。

`run` 启动 gateway、orchestrator 和 sender；Ctrl-C 会停止入口并收尾当前 worker。
上线后发送一条私聊并检查进度卡与最终答案，再试 `/status`、`/queue`、`/result`。
有当前任务时可用 `/cancel`；`/continue` 遵循已有操作核验规则。

```bash
.venv/bin/feishu-bot status --config "$HOME/.local/state/my-feishu-bot/runtime.json"
.venv/bin/watch-worker --config "$HOME/.local/state/my-feishu-bot/runtime.json"
```

## 5. 开机/登录后常驻

先退出前台的 `run`，生成适用于当前操作系统的服务文件：

```bash
.venv/bin/feishu-bot service-files \
  --config "$HOME/.local/state/my-feishu-bot/runtime.json" \
  --output ./generated-services
```

Linux 会输出 `feishu-bot-<实例摘要>.service`。把输出的**具体文件**复制到
`~/.config/systemd/user/`，执行 `systemctl --user daemon-reload`，再用
`systemctl --user enable --now 文件名.service` 启用。需要退出登录后继续运行时，按机器政策
为该用户开启 `loginctl enable-linger`。服务日志通过 `journalctl --user -u 文件名.service` 查看。

macOS 会输出同名 `.plist`。复制到 `~/Library/LaunchAgents/`，使用
`launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/文件名.plist` 加载。
launchd 用户服务在用户登录后运行，机器睡眠时无法保持消息接收。

每个实例使用自己的摘要服务名、数据库和锁。相同 OS 用户下，同一飞书 App ID 的两个新版
接收器会被锁阻止；其他机器或旧版本接收器不能被本机锁发现，应先停用原接收器再切换。

## 6. 可选能力、权限和运行边界

新实例默认提供 `reply`、`read_image`、`operations`、`read_artifact`、`run`。
不要求安装 bytedcli、Libra、Forge 或任何特定公司的工具。

- `--integration documents`：启用结构化文档搜索、读取和创建；需要目标用户已登录的
  bytedcli。可在 runtime.json 设置 `bytedcli_command`，飞书 bot 的 App Secret 不代替该登录。
- `--integration libra`：启用 Libra 结构化读取；需要已登录的 libra-cli，可设置 `libra_cli_command`。
- 其他工具可由模型通过本机 CLI 使用，受其权限和任务预算约束。

Claude 沿用认证 worker 的自动工具授权，工具调用先检查 attempt 身份，工作权限属于当前
OS 用户。TraeX 默认 `--worker-access workspace`：原生工具使用工作目录沙箱，需要额外授权
的原生命令会被拒绝并反馈模型；显式 `--worker-access full` 时允许本次有效任务的原生越界
命令。**两种设置下，受管 MCP run 都以宿主 OS 用户执行**，workspace 不是整个 bot 的安全沙箱。
只把 bot 配给这个 OS 账户的可信使用者。

Linux 有用户 systemd 时，默认用独立 cgroup 承载 worker；`--runner process` 采用 POSIX
session 守护器，macOS 与无用户 systemd 的 Linux 默认使用它。后者能清理普通子进程及
受管命令组，但不能保证清理主动 setsid/daemonize 逃出 session 的程序，也不提供 cgroup
资源上限。需要这些保证时使用 Linux systemd。

TraeX 的原生工具 hook 在版本间存在差异，因此该后端在执行前保守标记可能有副作用；
失败任务不会自动从头重放。受管操作仍有身份校验、成功回执复用及 unknown 核验规则。
原生工具的软期限依赖模型遵循提示，硬期限和取消由宿主停止整个 worker。

## 7. 升级和排障

- 无法启动模型：检查 `doctor`、CLI 登录、包装器的完整 PATH、是否缺少 --model。
- TraeX 协议错误：查看私有 `tasks/<任务>/<尝试>/backend-error.json`；升级/更换 CLI 后重跑 probe。
- 能收到消息但工具失败：看本次操作回执、独立 CLI 的登录和工具是否启用。
- 无法看到原 TUI 的新消息：这是分叉执行语义，使用 watch-worker 或 status 给出的新会话。
- 初始化或切换 Claude/TraeX：使用新的状态目录。后端绑定会阻止把 Claude UUID 当 TraeX 会话。

更新代码前停止该实例并确认 worker 已退出。相同机器、相同后端可保留原状态目录；
不要复制正在运行的 bot.sqlite3 主文件作备份，必须使用 SQLite backup API，并一致保留
attachments 和 tasks。给另一个用户部署时使用新的凭据、owner、会话和空状态目录。
不要把旧用户的任务库、会话和成果随迁移包分发。

设计见 [TECHNICAL_DESIGN.md](TECHNICAL_DESIGN.md)，详细验收见发布说明。

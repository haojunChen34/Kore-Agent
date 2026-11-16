# KoreAgent Next

KoreAgent Next 是一个面向单机单用户的 Personal Agent 桌面应用。它把对话、工作区文件、个人行动与目标、邮箱、飞书信息、网页研究和定时任务放在一个界面中。Python Host 负责模型调用、工具执行、连接器同步和 SQLite 持久化，Electron + React 负责桌面界面。

项目强调可确认、可追踪和可恢复：写文件、发邮件、修改飞书内容等动作会先展示预览并等待审批；运行记录保存工具调用、结果、Token 用量、错误和检查点，失败或结果未知时可以继续、核实或恢复。

## 功能

- 对话：普通模式和计划模式、工具调用、文件处理、交付物、审批、中断和检查点恢复。
- 工作台：今日摘要、今日行动、后续行动、活跃目标、待处理通知、外部信息和网页监控更新。
- 信息中心：邮箱、飞书日历、飞书任务和云文档的收件箱；详情、附件、已读/完成和与 Agent 讨论。
- 统一搜索：搜索会话、连接器项目、长期记忆、目标/任务和工作区文件，并打开详情或来源。
- 研究中心：研究会话、引用、版本、Markdown 导出，以及主题或固定 URL 的网页监控。
- 执行中心：运行记录、任务与计划、自动化、审批、外部操作和交付物。
- 长期记忆：待确认、活跃和归档记忆，详情、搜索、冲突、版本、回滚、维护、检查和导出。
- 设置和连接器健康：模型、上下文窗口、运行预算、工作区、隐私、通知、技能、同步、Token 和错误中心。

## 快速安装和启动

要求 Python 3.11+（Windows 脚本优先使用 3.12）、Node.js 20+ 和 npm，以及一个兼容的模型 API。

先下载项目并进入项目目录：

```powershell
git clone https://github.com/haojunChen34/Kore-Agent.git
cd Kore-Agent
```

也可以从 GitHub 下载 ZIP 并解压，在解压后的项目目录打开终端。当前提供源码运行方式，尚未提供独立安装包。

Windows 一键启动：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

脚本会创建虚拟环境、安装依赖、从 `backend/.env.example` 创建 `backend/.env`，再启动 Host 和 Electron。数据库、附件、交付物和设置目录会在首次启动时自动创建。

手动启动 Host：

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
.\.venv\Scripts\python.exe -m kore_next.server
```

另开终端启动桌面端：

```powershell
cd desktop
npm ci
npm run electron:dev
```

Host 默认地址是 `http://127.0.0.1:8765`，健康检查为 `/health`，RPC 地址为 `ws://127.0.0.1:8765/rpc`。本地数据默认位于 `backend/.koreagent-next/`，工作区默认位于 `backend/workspace/`。API Key、授权码、Token、数据库和运行数据不应提交到 GitHub。

第一次使用时，在“设置”配置默认模型、各模型的上下文窗口和运行预算。支持 OpenAI、DeepSeek、Qwen、Anthropic 以及兼容 OpenAI API 的服务；也可以在 `backend/.env` 中设置 `KORENEXT_PROVIDER`、`KORENEXT_MODEL`、`KORENEXT_BASE_URL` 和 `KORENEXT_API_KEY`。需要邮箱时在“信息中心 → 收件箱”配置 IMAP/SMTP 应用专用授权码；需要飞书时在 `backend/.env` 填写 App ID、App Secret 和回调地址，重启 Host 后点击“飞书 OAuth”。

## 使用示例

整理工作区并生成报告：

```text
整理工作区中的项目文档，先列文件，再总结，最后生成 report.md。
```

创建长期目标：

```text
我想在未来三个月准备求职，长期目标是完成算法面试准备。请创建三个阶段性行动、一个学习和复盘任务计划，并在每周日晚上八点提醒我复盘。
```

系统会先展示规划预览；勾选需要的内容并确认后才保存。外部写入动作仍需单独预览和审批。

创建网页监控：在“研究中心 → 监控主题”填写主题、频率和来源 URL。没有 URL 时，Agent 会先提供候选网站供确认。运行后可查看新增标题、链接、摘要、变化、来源可信度和重要性。

## 项目结构

```text
backend/       Python Host、RPC、模型客户端、连接器和 SQLite 数据层
desktop/       Electron + React 桌面端
scripts/       Windows 启动脚本
docs/          功能和连接器说明
evals/         Agent 测评数据集
backend/workspace/ 默认本地工作区（首次启动时创建）
```

更多页面和操作见 [docs/FEATURES.md](docs/FEATURES.md)，邮箱和飞书配置见 [docs/CONNECTORS.md](docs/CONNECTORS.md)。

## License

本项目采用 [MIT License](LICENSE)。

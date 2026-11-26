# 贡献指南

感谢你对 KoreAgent Next 的兴趣。项目目前处于 Personal Agent MVP 阶段，欢迎提交 Bug 修复、文档改进和小范围功能改动。

## 开始之前

1. Fork 仓库并创建功能分支。
2. 按 README 完成本地环境安装。
3. 阅读相关模块和文档，确认改动边界。

## 本地检查

后端在隔离临时目录执行，不需要真实模型、邮箱或飞书配置。从项目根目录运行（先按README创建后端虚拟环境）：

```powershell
cd backend
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install "ruff==0.17.6"
.\.venv\Scripts\ruff.exe check kore_next
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

前端构建与恢复交互检查：

```powershell
cd desktop
npm ci
npm run build
npx playwright-core install chromium
npm run dev -- --host 127.0.0.1 --port 15186 --strictPort
```

保持上述隔离Vite服务运行，另开终端进入 `desktop`：

```powershell
npm run test:recovery
```

完成后在Vite终端按Ctrl+C停止。默认测试地址为 `http://127.0.0.1:15186`，可通过 `VITE_TEST_URL` 设置；默认使用Playwright安装的Chromium。也可设置 `EDGE_PATH` 为本机已有Chromium兼容浏览器可执行文件的绝对路径，以跳过浏览器下载。测试挂载真实运行记录组件并替换RPC，不访问日常Host或发起外部操作。测试文件不被生产入口引用。

Linux/macOS同样支持上述npm命令；后端使用虚拟环境中的 `python` 运行相同命令。发布包中的6个后端测试文件共64项，浏览器脚本包含10项场景检查；它们与四套322条综合场景是不同层级。完整综合场景需按[测评说明](evals/README.md)准备各项前置、模型、真实账号与人工协助，不由这些回归命令代替。

## 提交规范

- 每个提交尽量只解决一个问题。
- 提交信息使用清晰的动词，例如 “fix: prevent duplicate notifications”。
- 不要提交 API key、.env、SQLite 数据库、虚拟环境、node_modules 或构建产物。
- 用户可见行为、配置项或数据迁移变化需要同步更新 README 和 CHANGELOG。

## Pull Request

PR 描述应说明问题、实现方式、验证命令和已知限制。涉及界面变化时，请附截图或简短录屏。涉及权限、网络、模型调用或个人数据时，请明确说明数据流和默认行为。

维护者会重点检查：

- 是否保持本地数据和 API key 的边界。
- 是否保留审批、权限和错误处理路径。
- 是否同步更新了用户文档和版本说明。
- 是否避免把运行时目录或敏感信息写入仓库。


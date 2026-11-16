# 贡献指南

感谢你对 KoreAgent Next 的兴趣。项目目前处于 Personal Agent MVP 阶段，欢迎提交 Bug 修复、文档改进和小范围功能改动。

## 开始之前

1. Fork 仓库并创建功能分支。
2. 按 README 完成本地环境安装。
3. 阅读相关模块和文档，确认改动边界。

## 本地检查

提交前至少运行：

~~~powershell
cd backend
.\.venv\Scripts\python.exe -m pip install "ruff>=0.7,<1"
.\.venv\Scripts\ruff.exe check kore_next
.\.venv\Scripts\python.exe -m compileall -q kore_next

cd ..\desktop
npm run typecheck
npm run build
~~~
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


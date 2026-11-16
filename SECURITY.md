# 安全策略


## 报告安全问题

请使用 GitHub 仓库的 Security → Advisories → Report a vulnerability 提交私密报告，并尽量提供：

- 受影响的版本或提交。
- 可重复的步骤或最小复现。
- 影响范围和可能的数据暴露。
- 建议的修复方向（如果有）。

如果仓库尚未启用 Private vulnerability reporting，请先通过维护者提供的私密渠道联系维护者，不要把完整复现发布到公开 Issue。

## 安全边界

- 默认 Host 只监听 127.0.0.1。
- API key 应通过桌面设置、环境变量或操作系统密钥存储提供，不要提交到 Git。
- KORENEXT_HTTP_ALLOW_PRIVATE_NETWORK=false 是默认值。
- 不要运行不可信的 MCP Server 或在宿主机模式下授予不必要的权限。


# `starsrc/pkg` 构建、测试与多模块发版手册

## 多模块与目录布局

本仓库承载跨业务仓（`Aether`、`bowl` 等）复用的无业务耦合 Go 基础设施库，各子目录均为独立 Go 模块：

- [`observability`](../../observability/go.mod)（`github.com/starsrc/pkg/observability`）：结构化日志（`slogx`，含脱敏与 ConnectRPC 访问日志拦截器）、OpenTelemetry 追踪与 Prometheus 指标运行时（`telemetry`）、管理面路由守卫及 Fx 装配模块（`obsfx`）。
- [`tx`](../../tx/go.mod)（`github.com/starsrc/pkg/tx`）：泛型事务执行器（`Transactor[Q]`）、PostgreSQL `40001`/`40P01` 指数退避重试及 `pgxpool` 连接池配置与 Fx 生命周期装配（零耦合 `observability`）。
- [`serverx`](../../serverx/go.mod)（`github.com/starsrc/pkg/serverx`）：ConnectRPC 多端口 HTTP/1.1 + `h2c` 服务运行时生命周期、标准洋葱拦截器链组装与 gRPC `ErrorInfo` 状态转换辅助（依赖远端已发布的 `github.com/starsrc/pkg/observability`）。

## 多人协作与模块隔离红线（无 `go.work` / 无 `replace`）

- **禁止本地 `go.work` 与 `replace`**：本仓库及下游业务仓一律不配置跨模块本地 `go.work` 或 `replace` 指令，所有跨模块依赖（含 `serverx` 依赖 `observability`，以及 `Aether`/`bowl` 依赖 `starsrc/pkg/*`）均通过 GitHub 远端已发布的子目录语义化 Tag 解析（依赖宿主机 `GOPRIVATE=github.com/starsrc/*`）。
- **子目录独立 Tag 发版规范**：
  - 变更 `observability` 或 `tx` 后，推送 `main` 并打对应子目录 Tag（如 `git tag observability/v0.1.1 && git push origin observability/v0.1.1`）。
  - 若 `serverx` 需同步消费新版 `observability`，须待 `observability/vX.Y.Z` Tag 推送后，执行 `go -C serverx get github.com/starsrc/pkg/observability@vX.Y.Z && go -C serverx mod tidy`，验证通过后再推送并打 `serverx/vX.Y.Z` Tag。

## 提交前守门

[`.githooks/pre-commit`](../../.githooks/pre-commit) 对暂存的 Go 文件执行 `gofmt -s` 检查，新克隆仓库先运行一次 `git config core.hooksPath .githooks` 启用。

## 单元测试与静态检查

一律通过根目录 [`Taskfile.yml`](../../Taskfile.yml)（内置 `GOWORK=off`）或显式带 `GOWORK=off -C <module>` 执行：

- `task test`（或 `GOWORK=off go -C <module> test ./...`）
- `task test:cover`
- `task lint`
- `task tidy`

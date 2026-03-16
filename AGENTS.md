## 项目目标
1. 把当前项目 `autumn` 从 Go 改写成 Rust。
2. 新代码在 `autumn-rs`。
3. 底层是 `stream layer`，负责分布式文件存储与恢复。
4. 上层是 `partition layer`，负责 table 管理与有序 KV 存储。

## 长任务执行规则（参考 effective harness 思路）
1. 每次开始任务前，必须先读取仓库根目录 `claude-progress.txt` 与 `feature_list.json`。
2. 在真正编码前，先输出本任务的两份清单：
   - 已实现的 feature/task
   - 未实现的 feature/task
3. 清单输出后才能开始编码。
4. 编码完成后必须更新 `claude-progress.txt` 中的任务状态，状态值只允许两种：
   - `completed`
   - `not_completed`
5. 如果任务中断、阻塞或验证失败，状态必须写成 `not_completed`。
6. 如果功能、测试、验证都完成，状态写成 `completed`。
7. 采用外置记忆三件套管理长任务上下文：
   - `feature_list.json`：记录 feature 列表、验收标准、完成状态
   - `claude-progress.txt`：记录当前进度、阻塞点、下一步
   - `git`：所有阶段性结果必须可回滚、可追溯
8. `feature_list.json` 作为需求账本，任务开始后需求描述、验收步骤、测试标准不可随意改写；只允许更新完成状态字段（如 `passes` 或等价状态位）。
9. 每次会话收尾必须完成交接闭环：
   - 提交本阶段代码（commit）
   - 更新 `claude-progress.txt` 与 `feature_list.json` 的状态
   - 确保工作区状态可继续（无破坏性中间态，下一会话可直接接手）
10. 每个 feature 必须按固定流程推进：
    - 定义 feature（目标/边界/验收）
    - 开发实现
    - 执行测试验证
    - 更新 `autumn-rs/README.md`（手动测试或使用说明）
    - 提交 git commit，作为该 feature 的完成点
11. `autumn-rs/README.md` 必须持续维护，确保人工手动验证步骤始终可执行。

## claude-progress.txt 约定
1. 文件位置：仓库根目录 `claude-progress.txt`。
2. 文件中必须包含 `TaskStatus` 字段。
3. `TaskStatus` 只能是 `completed` 或 `not_completed`，禁止其他值。
4. 推荐结构示例：
```txt
Date: 2026-03-16
TaskStatus: not_completed
Task scope: ...
Current summary: ...
Main gaps: ...
Next steps: ...
```

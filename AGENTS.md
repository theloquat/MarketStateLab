# Market State Lab 开发索引

市场状态实验室的入口是 `scripts/contest.sh`。先读 README，再按任务定位：
- 数据与研究接口在 `backend/marketlab/`，用 `pytest.ini` 运行 `backend/tests/`。
- Flink 窗口、模型推断和 Kafka 连线在 `services/flink-features/`。认证和路由在 `services/gateway/`。
- UI 入口在 `frontend/src/main.tsx`，访问 `/lab`。OpenAPI 由服务提供。
- `docs/release/` 给出当前设计和用法，`docs/contest/` 保存实测证据。旧日期报告用于比较，以报告标注的条件及版本为准。

`data/` 下的正式数据包、`.quantos-contest/` 和 `deploy/contest/secrets.env` 都是本机输入或运行状态。自动化测试使用替身模型或无费用工具。变更 Kafka 主题、Flink 算子 UID、状态描述符或 Compose 卷之前，核对已有数据与 Savepoint 的兼容性。

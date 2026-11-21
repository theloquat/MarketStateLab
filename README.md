# 市场状态实验室 · Market State Lab

分布式金融市场状态监控与智能投研平台。系统把多资产行情、宏观和文本资料归一，使用 Kafka 与 Flink 计算日频特征、识别状态并产生预警。浏览器可查看模型、数据质量、研究批注和原始证据。

## 本机启动

需要 macOS 或 Linux、Python 3.12+、Docker Engine 和 Docker Compose 2.24.4+。Docker 建议分配至少 8 GB 内存。首次构建需要访问依赖仓库。

1. 把主办方提供的原始 ZIP 放在 `data/` 下。
2. 在仓库根目录运行 `./scripts/contest.sh start`。它生成本机凭证、按需构建镜像并启动整套 Compose。
3. 运行 `./scripts/contest.sh credentials` 查看本机登录信息，打开 [实验室](http://127.0.0.1:18080/lab)。
4. 首次进入全景页时导入数据。完成后进入「模型与特征」训练冻结 HMM，再在「历史回放」运行 Kafka/Flink 回放。预警附带可追溯的原始行和模型版本。

停止用 `./scripts/contest.sh stop`，它保存 Flink Savepoint。状态和日志使用 `status`、`logs` 子命令。API 定义在 [OpenAPI](http://127.0.0.1:18080/openapi.json)，交互文档在 [Swagger](http://127.0.0.1:18080/docs)。

## 三项任务与代码入口

| 任务 | 代码入口 | 验收点 |
| --- | --- | --- |
| 多源数据接入与存储 | `backend/marketlab/ingest.py`、`storage.py` | 原始 ZIP、统一观察、来源和质量 |
| 实时计算与特征 | `services/flink-features/`、`backend/marketlab/modeling.py`、`batching.py` | 日窗口、冻结 HMM/GMM、批流一致性、恢复 |
| 系统与可视化 | `services/gateway/`、`frontend/`、`backend/marketlab/api.py` | 认证、实时状态、预警、监控与证据 |

- [使用入门](docs/使用入门.md) · [使用与验收](docs/release/使用与验收.md) · [答辩速读](docs/release/答辩速读.md)
- [架构与接口](docs/release/架构与接口.md) · [数据与模型卡](docs/release/数据与模型卡.md)
- [性能与测试](docs/release/性能与测试报告.md) · [原始验证报告](docs/contest/) · [架构决策](docs/adr/)
- [产品及技术方案 PDF](output/pdf/MarketStateLab-产品及技术方案.pdf) · [视频脚本](docs/release/视频脚本与验收清单.md)

## 开发验证

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-contest.lock
.venv/bin/python -m pytest --cov=backend/marketlab --cov-fail-under=30
npm --prefix frontend ci
npm --prefix frontend test
npm --prefix frontend run build
./scripts/contest.sh build
.venv/bin/python scripts/verify_clean_contest.py
```

官方市场数据截至 2026-04-30，用于历史回放。压测报告中保留了端到端尾延迟和持续吞吐未达标或未验证的项。研究工具可离线运行，模型 API 仅在配置且主动提交规划时调用。

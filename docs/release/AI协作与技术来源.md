# AI协作、复核与技术来源

用户负责产品目标、使用验收、反馈与研究判断；AI协助调研、实现、测试、故障注入、性能定位和材料制作。系统围绕官方数据及三项任务交付。

## 可核对的过程

- 从数据原件、单位和发布时间审计开始，未知的匿名宏观身份、新闻时区和发布时间不由模型补写。
- 每个模型保存训练截止、特征版本、PCA、参数、命名和产物哈希。状态概率、当前压力描述和未来下跌预测分开评价。
- 保留失败压力报告、完整性失败运行、失败付费调用和负消融结果。根据测量迭代窗口微批、outbox物化隔离、冻结推断、独立发布进程和网络缓冲，没有只挑最漂亮的一次运行。
- 合成单测用于验证因果性、边界、权限和数学；真实官方数据用于批流、容错与多存储核对。两类证据不互相冒充。
- 本机真实DeepSeek联调合计2次、1600 tokens；失败也计入预算。之后工程测试使用已有结果或无费用工具，不自动重复收费请求。
- 用户先验收，再实际录制或提交。本项目没有代发帖子、上传代码、真实资金下单或自动提交比赛材料。

## 核心技术来源

| 技术 | 原项目 / 说明 |
|---|---|
| Flink | https://flink.apache.org/ ，状态、日窗口、Checkpoint/Savepoint与Kafka连接器 |
| Kafka | https://kafka.apache.org/ ，持久事件传输；业务幂等不等于跨库严格exactly-once |
| GMM / PCA | https://scikit-learn.org/stable/ ，训练期拟合与冻结变换 |
| HMM | https://hmmlearn.readthedocs.io/ ，拟合统计参数；在线推断为独立实现的冻结前向过滤 |
| Undertow / Caffeine / Jedis | https://undertow.io/ 、https://github.com/ben-manes/caffeine 、https://github.com/redis/jedis |
| FastAPI / Strawberry | https://fastapi.tiangolo.com/ 、https://strawberry.rocks/ |
| orjson | https://github.com/ijl/orjson ，仅用于传输，原内容哈希规则保持不变 |
| React / Recharts | https://react.dev/ 、https://recharts.org/ |
| DeepSeek | https://api-docs.deepseek.com/ ，受控JSON规划、思考模式显式关闭与预算留痕 |
| 原始外部PDF | https://home.treasury.gov/system/files/221/TBACCharge1Q22020.pdf ，31页原件与哈希归档，发布时间未知 |

项目自身代码的MIT许可不覆盖第三方软件或官方数据。依赖保留其原许可；MinIO采用原项目AGPLv3版本，源码构建入口及固定发布版本在 `deploy/contest/Dockerfile.minio`。赛事原始数据不包含在源码分发包，使用者须自行提供获授权原件。

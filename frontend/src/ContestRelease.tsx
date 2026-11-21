import { useEffect, useState } from "react";
type Data = Record<string, any>;
type Call = (path: string, body?: unknown) => Promise<any>;
const labels: Record<string, string> = {
  proposed: "待批准",
  approved: "已批准",
  running: "执行中",
  review: "待复核",
  accepted: "已接纳",
  rejected: "已拒绝",
  cancelled: "已取消",
  failed: "失败",
};
function Json({ value }: { value: unknown }) {
  return <pre className="lab-json">{JSON.stringify(value, null, 2)}</pre>;
}
function useAction() {
  const [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  return {
    error,
    busy,
    run: async (fn: () => Promise<void>) => {
      setError("");
      setBusy(true);
      try {
        await fn();
      } catch (e) {
        setError(String(e));
      } finally {
        setBusy(false);
      }
    },
  };
}
function Feedback({ error, busy }: { error: string; busy: boolean }) {
  return (
    <>
      {error && (
        <p role="alert" className="lab-error">
          {error}
        </p>
      )}
      {busy && <p role="status">正在提交，请稍候…</p>}
    </>
  );
}
export function ModelWorkbench({
  call,
  models,
  refresh,
  role,
}: {
  call: Call;
  models: Data[];
  refresh: () => Promise<void>;
  role: string;
}) {
  const [method, setMethod] = useState("hmm"),
    [reduction, setReduction] = useState("pca95"),
    [components, setComponents] = useState(3),
    [trainEnd, setTrainEnd] = useState("2022-12-31"),
    [selected, setSelected] = useState<string[]>([]),
    [feature, setFeature] = useState<Data | null>(null),
    [analyses, setAnalyses] = useState<Data[]>([]),
    [registry, setRegistry] = useState<Data>({}),
    [message, setMessage] = useState("");
  const a = useAction();
  const load = async () => {
    setAnalyses((await call("/analyses")).items);
    setRegistry(await call("/registry"));
  };
  useEffect(() => {
    load().catch((e) =>
      a.run(async () => {
        throw e;
      }),
    );
  }, []);
  return (
    <>
      <section className="lab-panel">
        <h2>冻结模型与特征版本</h2>
        <p>
          训练只使用截止日之前的数据。HMM采用逐日因果过滤；历史运行始终绑定原版本。模型状态是统计划分，不代表未来收益。
        </p>
        <div className="lab-tool-grid">
          <label>
            方法
            <select value={method} onChange={(e) => setMethod(e.target.value)}>
              <option value="hmm">HMM · 时序状态</option>
              <option value="gmm">GMM · 混合分布</option>
            </select>
          </label>
          <label>
            状态数
            <input
              type="number"
              min={2}
              max={4}
              value={components}
              onChange={(e) => setComponents(+e.target.value)}
            />
          </label>
          <label>
            降维
            <select
              value={reduction}
              onChange={(e) => setReduction(e.target.value)}
            >
              <option value="pca95">PCA · 保留95%训练方差</option>
              <option value="none">原始特征</option>
            </select>
          </label>
          <label>
            训练截止
            <input
              type="date"
              value={trainEnd}
              onChange={(e) => setTrainEnd(e.target.value)}
            />
          </label>
        </div>
        <button
          className="lab-button"
          disabled={a.busy || role === "viewer"}
          onClick={() =>
            a.run(async () => {
              const r = await call("/models", {
                method,
                components,
                reduction,
                train_end: trainEnd,
              });
              setMessage(
                "训练已排队：" + r.task_id + "；可在总览查看任务结果。",
              );
              await refresh();
            })
          }
        >
          训练新版本
        </button>
        <Feedback {...a} />
        {message && <p role="status">{message}</p>}
      </section>
      <section className="lab-panel">
        <h2>注册表</h2>
        <p>
          当前新运行默认版本：
          {registry.aliases?.find((x: Data) => x.alias === "active")
            ?.model_id || "未指定，请在回放中手动选择"}
        </p>
        <div className="lab-model-grid">
          {models.map((m) => (
            <article className="lab-model-card" key={m.id}>
              <label>
                <input
                  type="checkbox"
                  checked={selected.includes(m.id)}
                  onChange={(e) =>
                    setSelected((s) =>
                      e.target.checked
                        ? [...s, m.id].slice(-4)
                        : s.filter((v) => v !== m.id),
                    )
                  }
                />{" "}
                比较此版本
              </label>
              <h3>
                {m.method?.toLowerCase().includes("hmm") ? "HMM" : "GMM"} ·{" "}
                {m.components || 3} 状态
              </h3>
              <code>{m.id}</code>
              <p>
                训练截止 {m.train_end} · {m.reduction || "none"}
              </p>
              <button
                onClick={() =>
                  a.run(async () => setFeature(await call("/features/" + m.id)))
                }
              >
                检查特征与投影
              </button>{" "}
              <button
                disabled={role === "viewer" || a.busy}
                onClick={() =>
                  a.run(async () => {
                    await call("/models/active", { model_id: m.id });
                    await load();
                  })
                }
              >
                设为新运行默认
              </button>
            </article>
          ))}
        </div>
        <button
          className="lab-button"
          disabled={!selected.length || role === "viewer" || a.busy}
          onClick={() =>
            a.run(async () => {
              const r = await call("/evaluations", { model_ids: selected });
              setMessage("评价已排队：" + r.task_id);
              await refresh();
            })
          }
        >
          评价所选版本（最多4个）
        </button>{" "}
        <button onClick={() => a.run(load)}>刷新评价</button>
        {feature && (
          <details open>
            <summary>特征定义 / PCA</summary>
            <Json value={feature} />
          </details>
        )}
        {analyses.map((r, i) => (
          <EvaluationCard key={r.id || i} report={r} />
        ))}
      </section>
    </>
  );
}
const tools: Record<string, string> = {
  quality_audit: "数据质量检查",
  model_compare: "冻结模型比较",
  warning_sweep: "验证区间阈值扫描",
  event_evidence: "事件材料与反证",
  feature_ablation: "价格 / 文本同窗消融",
  macro_lag_sensitivity: "匿名宏观滞后敏感性",
};
export function ResearchWorkbench({
  call,
  models,
  role,
}: {
  call: Call;
  models: Data[];
  role: string;
}) {
  const [items, setItems] = useState<Data[]>([]),
    [tool, setTool] = useState("quality_audit"),
    [hypothesis, setHypothesis] = useState(
      "核对数据缺口、来源与发布时间，判断哪些特征具备研究条件。",
    ),
    [day, setDay] = useState("2020-03-16"),
    [ids, setIds] = useState<string[]>([]);
  const a = useAction();
  const load = async () => setItems((await call("/research")).items);
  useEffect(() => {
    load().catch((e) =>
      a.run(async () => {
        throw e;
      }),
    );
    const t = setInterval(() => load().catch(() => {}), 5000);
    return () => clearInterval(t);
  }, []);
  return (
    <>
      <AgentPlanner call={call} role={role} reload={load} />
      <section className="lab-panel">
        <h2>提出假设 → 批准 → 工具验证 → 人工复核</h2>
        <p>
          每项研究保留输入、版本、结果和反证。工具不会下单，也不会自动花费模型
          API 费用。
        </p>
        <div className="lab-tool-grid">
          <label>
            研究工具
            <select value={tool} onChange={(e) => setTool(e.target.value)}>
              {Object.entries(tools).map(([k, v]) => (
                <option value={k} key={k}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <label>
            案例日期
            <input
              type="date"
              value={day}
              onChange={(e) => setDay(e.target.value)}
            />
          </label>
        </div>
        <label>
          研究问题
          <textarea
            rows={3}
            value={hypothesis}
            maxLength={3000}
            onChange={(e) => setHypothesis(e.target.value)}
          />
        </label>
        <div>
          {models.map((m) => (
            <label key={m.id} style={{ display: "inline-block", margin: 12 }}>
              <input
                type="checkbox"
                checked={ids.includes(m.id)}
                onChange={(e) =>
                  setIds((s) =>
                    e.target.checked
                      ? [...s, m.id].slice(-4)
                      : s.filter((x) => x !== m.id),
                  )
                }
              />
              {m.method} {m.id.slice(0, 8)}
            </label>
          ))}
        </div>
        <button
          className="lab-button"
          disabled={a.busy || role === "viewer" || hypothesis.length < 3}
          onClick={() =>
            a.run(async () => {
              await call("/research", {
                tool,
                hypothesis,
                day,
                model_ids: ids,
              });
              await load();
            })
          }
        >
          创建待批准研究
        </button>
        <Feedback {...a} />
      </section>
      <section className="lab-panel">
        <h2>研究队列</h2>
        {!items.length && <p>尚无研究，先提交一个可验证的问题。</p>}
        {items.map((r) => (
          <article className="lab-research-item" key={r.id}>
            <h3>
              {tools[r.spec.tool] || r.spec.tool} ·{" "}
              {labels[r.status] || r.status}
            </h3>
            <p>{r.spec.hypothesis}</p>
            <small>
              {r.owner} · {r.created_at} · {r.id}
            </small>
            <div>
              {(r.status === "proposed"
                ? ["approved", "rejected", "cancelled"]
                : r.status === "review"
                  ? ["accepted", "rejected"]
                  : r.status === "approved"
                    ? ["cancelled"]
                    : []
              ).map((s) => (
                <button
                  disabled={role === "viewer" || a.busy}
                  key={s}
                  onClick={() =>
                    a.run(async () => {
                      await call("/research/" + r.id + "/decision", {
                        status: s,
                        note: "用户在研究工作台复核",
                      });
                      await load();
                    })
                  }
                >
                  {
                    {
                      approved: "批准研究",
                      accepted: "接纳结论",
                      rejected: "拒绝结论",
                      cancelled: "取消研究",
                    }[s]
                  }
                </button>
              ))}
              {r.status === "approved" && (
                <button
                  className="lab-button"
                  disabled={role === "viewer" || a.busy}
                  onClick={() =>
                    a.run(async () => {
                      await call("/research/" + r.id + "/execute", {});
                      await load();
                    })
                  }
                >
                  执行已批准工具
                </button>
              )}
            </div>
            <details>
              <summary>输入与结果证据</summary>
              <Json value={{ input: r.spec, result: r.result }} />
            </details>
          </article>
        ))}
      </section>
    </>
  );
}
export function OperationsWorkbench({ call }: { call: Call }) {
  const [data, setData] = useState<Data | null>(null),
    [events, setEvents] = useState<Data[]>([]),
    [socket, setSocket] = useState("正在连接"),
    [connection, setConnection] = useState(0);
  const a = useAction();
  useEffect(() => {
    const load = () =>
      call("/monitor")
        .then(setData)
        .catch((e) =>
          a.run(async () => {
            throw e;
          }),
        );
    load();
    const t = setInterval(load, 10000);
    const ws = new WebSocket(
      `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/api/lab/events`,
    );
    ws.onopen = () => setSocket("已连接");
    ws.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data);
        setEvents((v) =>
          d.event_id && v.some((old) => old.event_id === d.event_id)
            ? v
            : [d, ...v].slice(0, 20),
        );
      } catch {
        setSocket("收到无效消息");
      }
    };
    ws.onclose = () => setSocket("连接已关闭，点击下方按钮重连");
    ws.onerror = () => setSocket("连接异常");
    return () => {
      clearInterval(t);
      ws.close();
    };
  }, [connection]);
  return (
    <>
      <section className="lab-panel">
        <h2>运行监控</h2>
        <p>
          历史业务时间与处理时钟分别记录；组件可用不等于性能或业务正确性已达标。
        </p>
        <Feedback {...a} />
        {data ? (
          <>
            <div className="lab-stats">
              {Object.entries(data.components || {}).map(([name, ready]) => (
                <section key={name}>
                  <span>{name}</span>
                  <strong>{ready ? "就绪" : "待恢复"}</strong>
                  <small>{ready ? "连接检查通过" : "请检查组件日志"}</small>
                </section>
              ))}
            </div>
            <div className="lab-stats compact">
              {[
                ["outbox", "实时投递"],
                ["ch_outbox", "历史物化"],
              ].map(([key, title]) => {
                const rows = data.outboxes?.[key] || [];
                const pending = rows
                  .filter((r: Data) => !r.sent)
                  .reduce((sum: number, r: Data) => sum + r.n, 0);
                return (
                  <section key={key}>
                    <span>{title}待处理</span>
                    <strong>{pending.toLocaleString()}</strong>
                    <small>计数来自持久化投递记录</small>
                  </section>
                );
              })}
            </div>
            <h3>消费服务心跳 · UTC</h3>
            {(data.workers || []).map((w: Data) => (
              <p key={w.role}>
                {w.role} · {w.updated_at}
              </p>
            ))}
            <details>
              <summary>组件原始诊断</summary>
              <Json value={data} />
            </details>
            <h3>可下载验证证据</h3>
            <p>
              这些报告保留测试边界与失败结果。组件就绪不代表压力指标全部通过。
            </p>
            {(data.evidence || []).length ? (
              <details>
                <summary>{data.evidence.length} 份报告，展开下载</summary>
                <ul>
                  {data.evidence.map((r: Data) => (
                    <li key={r.name}>
                      <a
                        href={"/api/lab/reports/" + encodeURIComponent(r.name)}
                      >
                        {r.name}
                      </a>{" "}
                      · {Math.ceil(r.bytes / 1024)} KB
                    </li>
                  ))}
                </ul>
              </details>
            ) : (
              <p>当前部署尚无发布证据。</p>
            )}
          </>
        ) : (
          <p>正在获取组件状态…</p>
        )}
      </section>
      <section className="lab-panel">
        <h2>实时事件 · {socket}</h2>
        <button
          className="lab-button secondary"
          onClick={() => setConnection((v) => v + 1)}
        >
          重新连接事件流
        </button>
        <p>
          推送已提交的状态 / 预警事件；没有新回放时保持安静。此处不伪造行情。
        </p>
        {events.map((e, i) => (
          <details key={e.event_id || i}>
            <summary>
              {e.type || e.kind || "事件"} ·{" "}
              {e.payload?.day || e.transport || ""}
            </summary>
            <Json value={e} />
          </details>
        ))}
      </section>
    </>
  );
}
export function PdfWorkbench({ call, role }: { call: Call; role: string }) {
  const [file, setFile] = useState<File | null>(null),
    [url, setUrl] = useState(""),
    [published, setPublished] = useState(""),
    [receipt, setReceipt] = useState<Data | null>(null),
    [pages, setPages] = useState<Data[]>([]);
  const a = useAction();
  return (
    <section className="lab-panel">
      <h2>原始 PDF 与逐页证据</h2>
      <p>
        保留原件、SHA-256、来源及页码。填写的发布时间属于待核实声明，不能直接成为历史模型输入。
      </p>
      <div className="lab-tool-grid">
        <label>
          PDF（10MB以内）
          <input
            type="file"
            accept="application/pdf,.pdf"
            onChange={(e) => setFile(e.target.files?.[0] || null)}
          />
        </label>
        <label>
          来源网址
          <input
            type="url"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
          />
        </label>
        <label>
          声明发布时间（可空）
          <input
            value={published}
            placeholder="未知则留空"
            onChange={(e) => setPublished(e.target.value)}
          />
        </label>
      </div>
      <button
        className="lab-button"
        disabled={!file || a.busy || role === "viewer"}
        onClick={() =>
          a.run(async () => {
            const f = new FormData();
            f.append("file", file!);
            f.append("source_url", url);
            f.append("published_at", published);
            const response = await fetch("/api/lab/pdf", {
              method: "POST",
              body: f,
            });
            const r = await response.json();
            if (!response.ok) throw new Error(r.detail || "上传失败");
            setReceipt(r);
            setPages((await call("/pdf/" + r.id + "/pages")).items);
          })
        }
      >
        归档与提取
      </button>
      <Feedback {...a} />
      {receipt && (
        <>
          <p>
            已归档 {receipt.pages} 页 ·{" "}
            <a
              href={"/api/lab/pdf/" + receipt.id + "/original"}
              target="_blank"
              rel="noreferrer"
            >
              查看原件
            </a>
          </p>
          <code>{receipt.sha256}</code>
          {pages.map((p) => (
            <details key={p.page}>
              <summary>第 {p.page} 页</summary>
              <pre className="lab-json">
                {p.text || "本页未提取到文本（可能为扫描页）"}
              </pre>
            </details>
          ))}
        </>
      )}
    </section>
  );
}

function AgentPlanner({
  call,
  role,
  reload,
}: {
  call: Call;
  role: string;
  reload: () => Promise<void>;
}) {
  const [config, setConfig] = useState<Data | null>(null),
    [question, setQuestion] = useState(
      "检查2020年3月市场压力变化，并列出能否支持该判断的证据与反证。",
    ),
    [message, setMessage] = useState("");
  const a = useAction();
  useEffect(() => {
    if (role === "admin")
      call("/admin/llm")
        .then(setConfig)
        .catch((e) =>
          a.run(async () => {
            throw e;
          }),
        );
  }, [role]);
  return (
    <section className="lab-panel">
      <h2>Agent 研究规划</h2>
      <p>
        仅主动点击会调用一次模型。规划结果进入待批准队列，模型无法执行任意代码或下单。每日调用和
        token 预留预算在发出请求前扣记，失败也不自动重试。
      </p>
      {role === "admin" && config && (
        <details>
          <summary>
            模型连接与预算 · {config.configured ? "已配置" : "尚未配置"} · 今日{" "}
            {config.calls_today} 次
          </summary>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const f = new FormData(e.currentTarget);
              a.run(async () => {
                setConfig(
                  await call("/admin/llm", {
                    base_url: f.get("base_url"),
                    model: f.get("model"),
                    api_key: f.get("api_key"),
                    max_calls_per_day: Number(f.get("max_calls_per_day")),
                    token_budget_per_day: Number(f.get("token_budget_per_day")),
                    token_parameter: f.get("token_parameter"),
                  }),
                );
                setMessage("配置已保存，尚未调用模型。");
              });
            }}
          >
            <div className="lab-tool-grid">
              <label>
                API 根地址
                <input
                  name="base_url"
                  type="url"
                  defaultValue={config.base_url}
                  required
                />
              </label>
              <label>
                模型名称
                <input name="model" defaultValue={config.model} required />
              </label>
              <label>
                API Key
                <input
                  name="api_key"
                  type="password"
                  autoComplete="new-password"
                  placeholder={
                    config.configured ? "留空保留原密钥" : "填写服务商密钥"
                  }
                />
              </label>
              <label>
                每日最多调用
                <input
                  name="max_calls_per_day"
                  type="number"
                  min={1}
                  max={10}
                  defaultValue={config.max_calls_per_day}
                />
              </label>
              <label>
                每日 token 预留预算
                <input
                  name="token_budget_per_day"
                  type="number"
                  min={1000}
                  max={100000}
                  defaultValue={config.token_budget_per_day}
                />
              </label>
              <label>
                输出参数
                <select
                  name="token_parameter"
                  defaultValue={config.token_parameter}
                >
                  <option value="max_tokens">max_tokens（DeepSeek）</option>
                  <option value="max_completion_tokens">
                    max_completion_tokens
                  </option>
                </select>
              </label>
            </div>
            <button disabled={a.busy}>保存配置（不调用）</button>
          </form>
          <Json
            value={{
              reserved_tokens_today: config.reserved_tokens_today,
              history: config.history,
            }}
          />
        </details>
      )}
      <label>
        让 Agent 规划一个可检验的问题
        <textarea
          rows={3}
          value={question}
          maxLength={1500}
          onChange={(e) => setQuestion(e.target.value)}
        />
      </label>
      <button
        className="lab-button"
        disabled={role === "viewer" || a.busy}
        onClick={() =>
          a.run(async () => {
            const r = await call("/agent/plan", { question });
            setMessage(
              "规划已排队：" +
                r.task_id +
                "。任务完成后会出现在研究队列；请查看总览任务结果。",
            );
            await reload();
          })
        }
      >
        调用模型规划一次
      </button>
      <Feedback {...a} />
      {message && <p role="status">{message}</p>}
    </section>
  );
}

function EvaluationCard({ report }: { report: Data }) {
  const rows = Object.entries(report.models || {})
    .map(([id, m]: [string, any]) => ({
      id,
      method: m.method?.toLowerCase().includes("hmm") ? "HMM" : "GMM",
      metrics: m.stages?.heldout,
    }))
    .filter((r) => r.metrics);
  const pct = (v: number) =>
    Number.isFinite(v) ? (v * 100).toFixed(1) + "%" : "—";
  return (
    <article className="lab-research-item">
      <h3>时间留出评价 · {report.id?.slice(0, 8)}</h3>
      <p>
        从 {report.test_start} 起评分。
        {report.reused_holdout
          ? "这段历史此前已查看，属于重复历史检验。"
          : "本次记录为首次检验。"}
      </p>
      <p>
        目标：未来十次标普收盘中至少出现一次相对当前收盘下跌
        5%。这不是状态真值，也不是盈利证明。
      </p>
      <div className="lab-table-wrap">
        <table>
          <thead>
            <tr>
              <th>模型</th>
              <th>样本</th>
              <th>精确率</th>
              <th>召回率</th>
              <th>F1</th>
              <th>状态切换率</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id}>
                <td>
                  {r.method} {r.id.slice(0, 8)}
                </td>
                <td>{r.metrics.evaluated_snapshots}</td>
                <td>{pct(r.metrics.precision)}</td>
                <td>{pct(r.metrics.recall)}</td>
                <td>{r.metrics.f1.toFixed(3)}</td>
                <td>{pct(r.metrics.state_switch_rate)}</td>
              </tr>
            ))}
            {report.fixed_price_rule_heldout && (
              <tr>
                <td>固定价格规则</td>
                <td>{report.fixed_price_rule_heldout.evaluated_snapshots}</td>
                <td>{pct(report.fixed_price_rule_heldout.precision)}</td>
                <td>{pct(report.fixed_price_rule_heldout.recall)}</td>
                <td>{report.fixed_price_rule_heldout.f1.toFixed(3)}</td>
                <td>—</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <p>
        按这项未来下跌目标，当前复杂模型尚未展示稳定优势；当期压力状态与提前预测应分别评估。
      </p>
      <details>
        <summary>查看状态画像、假设与完整计算结果</summary>
        <Json value={report} />
      </details>
    </article>
  );
}

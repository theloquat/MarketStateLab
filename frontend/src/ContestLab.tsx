import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  Database,
  Play,
  RotateCcw,
  Search,
  ShieldCheck,
  AlertTriangle,
  ChevronRight,
  Layers3,
} from "lucide-react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
} from "recharts";
import "./contest.css";
import { useContestEvents } from "./useContestEvents";
import { useContestSession } from "./ContestSession";
import {
  ModelWorkbench,
  ResearchWorkbench,
  OperationsWorkbench,
  PdfWorkbench,
} from "./ContestRelease";

type Data = Record<string, any>;
async function request(path: string, body?: unknown, signal?: AbortSignal) {
  const r = await fetch(
    "/api/lab" + path,
    body === undefined
      ? { signal }
      : {
          signal,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  if (r.status === 401)
    window.dispatchEvent(new Event("quantos-session-expired"));
  const value = await r.json();
  if (!r.ok)
    throw new Error(
      typeof value.detail === "string"
        ? value.detail
        : JSON.stringify(value.detail),
    );
  return value;
}
const number = (v: unknown, d = 2) =>
  typeof v === "number"
    ? v.toLocaleString("zh-CN", { maximumFractionDigits: d })
    : "—";
const status = (s: string) =>
  ({
    queued: "等待中",
    running: "处理中",
    completed: "已完成",
    failed: "失败",
    interrupted: "已中断，可重新发起",
    ready: "可用",
    missing: "缺失",
    importing: "导入中",
    paused: "已暂停",
    published: "输入已发送",
    acknowledged: "已确认",
    open: "待处理",
    dismissed: "已忽略",
    publishing: "发送中",
    publish_failed: "发送失败",
    features_completed: "特征已完成",
    pause_requested: "等待安全暂停",
    failed_integrity: "完整性失败（已隔离）",
  })[s] || s;
export default function ContestLab() {
  const session = useContestSession();
  const [connectionError, setConnectionError] = useState("");
  const [summary, setSummary] = useState<Data>({
    datasets: [],
    series: [],
    models: [],
    runs: [],
    tasks: [],
    sources: [],
  });
  const [page, setPage] = useState("overview"),
    [error, setError] = useState(""),
    [busy, setBusy] = useState("");
  const [run, setRun] = useState(""),
    [results, setResults] = useState<Data>({ items: [], alerts: [] }),
    [model, setModel] = useState("");
  const [start, setStart] = useState("2023-01-01"),
    [end, setEnd] = useState("2026-05-02"),
    [threshold, setThreshold] = useState(0.75),
    [inputRate, setInputRate] = useState(1000),
    [cooldown, setCooldown] = useState(5);
  const [selected, setSelected] = useState<Data | null>(null),
    [evidence, setEvidence] = useState<Data | null>(null);
  const [query, setQuery] = useState(""),
    [kind, setKind] = useState("news"),
    [docs, setDocs] = useState<Data[]>([]),
    [doc, setDoc] = useState<Data | null>(null);
  const [streams, setStreams] = useState<Data>({ available: false, runs: [] }),
    [stream, setStream] = useState("");
  const [resultEngine, setResultEngine] = useState("local");
  const liveStatus = useContestEvents(stream, resultEngine === "stream", {
    state: (value) =>
      setResults((previous) => ({
        ...previous,
        items: Array.from(
          new Map(
            [...previous.items, value].map((row: Data) => [row.day, row]),
          ).values(),
        ).sort((a: Data, b: Data) => a.day.localeCompare(b.day)),
      })),
    alert: (value) =>
      setResults((previous) => ({
        ...previous,
        alerts: previous.alerts.some((row: Data) => row.id === value.id)
          ? previous.alerts
          : [
              ...previous.alerts,
              {
                id: value.id,
                run_id: stream,
                day: value.result.day,
                payload: value,
                disposition: "open",
                note: "",
              },
            ],
      })),
    reconnect: async (signal) => {
      const value = await request("/stream/" + stream, undefined, signal);
      if (signal.aborted) return;
      setResults((previous) => ({
        ...previous,
        items: Array.from(
          new Map(
            [
              ...previous.items,
              ...value.items.map((row: Data) => row.payload),
            ].map((row: Data) => [row.day, row]),
          ).values(),
        ).sort((a: Data, b: Data) => a.day.localeCompare(b.day)),
        alerts: Array.from(
          new Map(
            [...previous.alerts, ...value.alerts].map((row: Data) => [
              row.id,
              row,
            ]),
          ).values(),
        ),
      }));
    },
  });
  const [historyPage, setHistoryPage] = useState(0);
  const [trainEnd, setTrainEnd] = useState("2022-12-31");
  const [resultRevision, setResultRevision] = useState(0);
  useEffect(() => {
    if (!stream || resultEngine !== "stream") return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const progress = async () => {
      try {
        const row = await request(
          "/stream/" + stream + "/progress",
          undefined,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        setStreams((previous: Data) => ({
          ...previous,
          runs: [...previous.runs.filter((r: Data) => r.id !== row.id), row],
        }));
        if (!["features_completed", "failed_integrity"].includes(row.status))
          timer = setTimeout(progress, 2000);
      } catch (error) {
        if (!controller.signal.aborted) timer = setTimeout(progress, 5000);
      }
    };
    progress();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [stream, resultEngine, resultRevision]);
  const runCursor = summary.runs.find((r: Data) => r.id === run)?.cursor;
  useEffect(() => {
    if (
      summary.server_time &&
      typeof performance.mark === "function" &&
      !performance.getEntriesByName("quantos-lab-data-ready").length
    ) {
      performance.mark("quantos-lab-data-ready");
      document.documentElement.dataset.quantosReadyMs = performance
        .getEntriesByName("quantos-lab-data-ready")[0]
        .startTime.toFixed(3);
    }
  }, [summary.server_time]);
  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/graphql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: "{ dashboard }" }),
      });
      if (response.status === 401) {
        window.dispatchEvent(new Event("quantos-session-expired"));
        return;
      }
      const graph = await response.json();
      if (!response.ok || graph.errors)
        throw new Error(graph.errors?.[0]?.message || "工作区查询失败");
      const value = graph.data.dashboard;
      setSummary(value);
      setConnectionError("");
      setModel((m) => m || value.active_model || value.models[0]?.id || "");
    } catch (e) {
      setConnectionError(String(e));
    }
  }, []);
  useEffect(() => {
    document.title = "市场状态实验室";
    refresh();
    const timer = setInterval(refresh, 3000);
    return () => clearInterval(timer);
  }, [refresh]);
  useEffect(() => {
    if (
      (resultEngine === "local" && !run) ||
      (resultEngine === "stream" && !stream)
    )
      return;
    let canceled = false;
    const path =
      resultEngine === "stream"
        ? "/stream/" + stream
        : `/runs/${run}/results?limit=5000`;
    request(path)
      .then((v) => {
        if (canceled) return;
        if (resultEngine === "stream")
          setResults((previous) => ({
            ...previous,
            items: Array.from(
              new Map(
                [...previous.items, ...v.items.map((r: Data) => r.payload)].map(
                  (r: Data) => [r.day, r],
                ),
              ).values(),
            ).sort((a: Data, b: Data) => a.day.localeCompare(b.day)),
            alerts: Array.from(
              new Map(
                [...previous.alerts, ...v.alerts].map((r: Data) => [r.id, r]),
              ).values(),
            ),
          }));
        else setResults(v);
      })
      .catch((e) => {
        if (!canceled) setError(String(e));
      });
    return () => {
      canceled = true;
    };
  }, [run, runCursor, resultRevision, resultEngine, stream]);
  useEffect(() => {
    setHistoryPage(0);
  }, [run, stream, resultEngine]);
  const act = async (name: string, fn: () => Promise<any>) => {
    if (
      session.role === "viewer" &&
      [
        "import",
        "train",
        "create",
        "advance",
        "stream",
        "ack",
        "pause-stream",
        "resume-stream",
      ].includes(name)
    ) {
      setError("当前为只读观察员，请切换研究员或管理员执行操作");
      return;
    }
    setBusy(name);
    setError("");
    try {
      await fn();
      await refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
      setResultRevision((v) => v + 1);
    }
  };
  const dataset = summary.datasets[0],
    active = summary.runs.find((r: Data) => r.id === run);
  const tasksRunning = summary.tasks.some((t: Data) =>
    ["queued", "running"].includes(t.status),
  );
  const selectedStream = streams.runs.find((r: Data) => r.id === stream);
  const latest = results.items.at(-1);
  const chartRows = useMemo(() => {
    if (!results.items.length) return [];
    const byDay = new Map<string, Data>(
      results.items.map((r: Data) => [r.day, r]),
    );
    const first = Date.parse(results.items[0].day + "T00:00:00Z");
    const last = Date.parse(results.items.at(-1).day + "T00:00:00Z");
    const rows = [];
    for (let t = first; t <= last; t += 86400000) {
      const day = new Date(t).toISOString().slice(0, 10),
        row = byDay.get(day);
      rows.push({
        day,
        high_pressure_probability: row?.probabilities?.["高压力"] ?? null,
      });
    }
    return rows;
  }, [results.items]);
  const explore = async (row: Data) => {
    setSelected(row);
    setEvidence(null);
  };
  const viewEvidence = async (id: string) => {
    try {
      setEvidence(await request("/evidence/" + id));
    } catch (e) {
      setError(String(e));
    }
  };
  const loadStreams = async () => {
    try {
      setStreams(await request("/stream"));
    } catch (e) {
      setError(String(e));
    }
  };
  return (
    <div className="lab-shell">
      <aside className="lab-sidebar">
        <a href="/lab" className="lab-brand">
          <Layers3 />
          <div>
            市场状态实验室<span>MARKET STATE LAB</span>
          </div>
        </a>
        <p className="lab-subtitle">市场状态实验室</p>
        <nav>
          {[
            ["overview", "市场全景", Activity],
            ["replay", "历史回放", Play],
            ["data", "数据与质量", Database],
            ["evidence", "文本证据", Search],
            ["models", "模型与特征", Activity],
            ["research", "研究与案例", Search],
            ["ops", "运行监控", Database],
          ].map(([id, name, Icon]: any) => (
            <button
              key={id}
              className={page === id ? "active" : ""}
              onClick={() => setPage(id)}
            >
              <Icon size={20} />
              {name}
              <ChevronRight size={16} />
            </button>
          ))}
        </nav>
        <div className="lab-sidebar-bottom">
          <ShieldCheck size={22} />
          <strong>历史数据研究工作区</strong>
          <p>回放使用已归档的历史数据。</p>
        </div>
      </aside>
      <main className="lab-main">
        <header className="lab-topbar">
          <span>
            市场状态实验室 /{" "}
            {
              (
                {
                  overview: "市场全景",
                  replay: "历史回放",
                  data: "数据与质量",
                  evidence: "文本证据",
                  models: "模型与特征",
                  research: "研究与案例",
                  ops: "运行监控",
                } as Data
              )[page]
            }
          </span>
          {resultEngine === "stream" && (
            <span className="lab-tag" role="status">
              {liveStatus}
            </span>
          )}
          <span className="lab-tag">
            {!summary.deployment
              ? "正在连接工作区"
              : summary.deployment === "distributed"
                ? "分布式部署"
                : "离线参照部署"}
          </span>
        </header>
        <div className="lab-content">
          <div className="lab-heading">
            <div>
              <p className="lab-eyebrow">MULTI-ASSET RESEARCH</p>
              <h1>
                {
                  (
                    {
                      overview: "看清市场状态，追溯每个结论",
                      replay: "让历史按你的节奏重现",
                      data: "研究从可信的数据开始",
                      evidence: "回到原始材料，核对判断",
                      models: "比较模型，检查每一项特征",
                      research: "把假设变成可复现的研究",
                      ops: "观察系统运行与恢复证据",
                    } as Data
                  )[page]
                }
              </h1>
              <p>行情与收益率 → 因果特征 → 冻结模型 → 可配置预警 → 原始证据</p>
            </div>
            <button
              onClick={() => {
                setError("");
                setResultRevision((v) => v + 1);
                refresh();
              }}
              className="lab-button secondary"
            >
              <RotateCcw size={17} />
              刷新
            </button>
          </div>
          {(error || connectionError) && (
            <div className="lab-error" role="alert">
              {error || connectionError}
            </div>
          )}
          <div className="lab-notice">
            <AlertTriangle size={20} />
            <div>
              <strong>这是历史回放，不是当前市场行情</strong>
              <p>
                官方资产观测截至
                2026-04-30。日期缺少精确发布时间时，采用“日期后第2个自然日 UTC
                可用”的公开假设；此假设不构成历史可得性认证。
              </p>
            </div>
          </div>
          {page === "models" && (
            <ModelWorkbench
              call={request}
              models={summary.models}
              refresh={refresh}
              role={session.role}
            />
          )}
          {page === "research" && (
            <ResearchWorkbench
              call={request}
              models={summary.models}
              role={session.role}
            />
          )}
          {page === "ops" && <OperationsWorkbench call={request} />}
          {page === "overview" && (
            <>
              <div className="lab-stats">
                {[
                  [
                    "已接入资产",
                    dataset?.series_count,
                    "股票、商品、外汇、收益率等",
                  ],
                  [
                    "真实市场观测",
                    dataset?.observation_count,
                    "来源、原始行和版本可追溯",
                  ],
                  [
                    "已导入辅助记录",
                    dataset?.document_count,
                    "文本、事件与匿名指标，未进入主模型",
                  ],
                  [
                    "已归档模型",
                    summary.models.length,
                    "训练截止后冻结标准化和状态命名",
                  ],
                ].map(([title, value, detail]) => (
                  <section key={String(title)}>
                    <span>{title}</span>
                    <strong>{number(value, 0)}</strong>
                    <small>{detail}</small>
                  </section>
                ))}
              </div>
              <section className="lab-panel">
                <div className="lab-panel-title">
                  <h2>开始一次市场研究</h2>
                  <span>三个连续步骤</span>
                </div>
                <div className="lab-start-grid">
                  <div>
                    <b>01</b>
                    <h3>检查数据底座</h3>
                    <p>查看覆盖、缺口、单位和来源。未知内容保留未知。</p>
                    <button
                      disabled={!!busy || tasksRunning}
                      className="lab-button secondary"
                      onClick={() =>
                        act("import", () => request("/import", {}))
                      }
                    >
                      {dataset ? "校验并继续导入" : "导入官方数据"}
                    </button>
                  </div>
                  <div>
                    <b>02</b>
                    <h3>建立冻结基线</h3>
                    <p>
                      固定8个核心资产、16项特征。训练期之后的数据不参与拟合。
                    </p>
                    <label>
                      训练截止日期
                      <input
                        type="date"
                        value={trainEnd}
                        onChange={(e) => setTrainEnd(e.target.value)}
                      />
                    </label>
                    <button
                      disabled={
                        !dataset ||
                        dataset.status !== "ready" ||
                        !!busy ||
                        tasksRunning
                      }
                      className="lab-button secondary"
                      onClick={() =>
                        act("train", () =>
                          request("/train", { train_end: trainEnd }),
                        )
                      }
                    >
                      训练并归档 GMM
                    </button>
                  </div>
                  <div>
                    <b>03</b>
                    <h3>回放与查证</h3>
                    <p>观察状态变化与预警，并检查同一时期的原始材料。</p>
                    <button
                      className="lab-button"
                      onClick={() => setPage("replay")}
                    >
                      进入历史回放 <ChevronRight size={16} />
                    </button>
                  </div>
                </div>
              </section>
              <section className="lab-panel">
                <h2>后台工作记录</h2>
                {summary.tasks.length ? (
                  <div className="lab-table">
                    <table>
                      <thead>
                        <tr>
                          <th>任务</th>
                          <th>状态</th>
                          <th>更新时间</th>
                          <th>说明</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary.tasks.map((t: Data) => (
                          <tr key={t.id}>
                            <td>
                              {(
                                {
                                  official_import: "官方数据导入",
                                  train_gmm: "冻结模型训练",
                                  replay: "推进历史回放",
                                  distributed_replay: "真实分布式回放",
                                } as Data
                              )[t.kind] || t.kind}
                            </td>
                            <td>{status(t.status)}</td>
                            <td>{t.updated_at?.slice(0, 19)} UTC</td>
                            <td>{t.detail.error || t.detail.id || "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="lab-empty">
                    尚无后台任务。先检查已导入数据或发起导入。
                  </p>
                )}
              </section>
            </>
          )}
          {page === "replay" && (
            <>
              <section className="lab-panel">
                <h2>配置与运行</h2>
                <div className="lab-form">
                  <label>
                    冻结模型
                    <select
                      value={model}
                      onChange={(e) => setModel(e.target.value)}
                    >
                      <option value="">请选择模型</option>
                      {summary.models.map((m: Data) => (
                        <option key={m.id} value={m.id}>
                          {m.id.slice(0, 8)} · 训练截止 {m.train_end}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    开始日期
                    <input
                      type="date"
                      value={start}
                      onChange={(e) => setStart(e.target.value)}
                    />
                  </label>
                  <label>
                    结束日期
                    <input
                      type="date"
                      value={end}
                      onChange={(e) => setEnd(e.target.value)}
                    />
                  </label>
                  <label>
                    高压力预警概率
                    <input
                      type="number"
                      min="0.5"
                      max="1"
                      step="0.05"
                      value={threshold}
                      onChange={(e) => setThreshold(Number(e.target.value))}
                    />
                  </label>
                  <label>
                    预警冷却天数
                    <input
                      type="number"
                      min="0"
                      max="90"
                      step="1"
                      value={cooldown}
                      onChange={(e) => setCooldown(Number(e.target.value))}
                    />
                  </label>
                  <label>
                    分布式输入速度
                    <select
                      value={inputRate}
                      onChange={(e) => setInputRate(Number(e.target.value))}
                    >
                      <option value={1000}>每秒约 1,000 条 · 便于观察</option>
                      <option value={5000}>每秒约 5,000 条 · 加速回放</option>
                      <option value={0}>最大速度 · 压力测试</option>
                    </select>
                  </label>
                </div>
                <div className="lab-actions">
                  <button
                    className="lab-button"
                    disabled={!model || !!busy || tasksRunning}
                    onClick={() =>
                      act("create", async () => {
                        const value = await request("/runs", {
                          model_id: model,
                          start,
                          end,
                          warning_threshold: threshold,
                          cooldown_days: cooldown,
                        });
                        setRun(value.id);
                        setResultRevision((v) => v + 1);
                      })
                    }
                  >
                    建立离线参照回放
                  </button>
                  <button
                    className="lab-button secondary"
                    onClick={() => {
                      loadStreams();
                    }}
                  >
                    检查分布式连接
                  </button>
                  {streams.ready && (
                    <button
                      className="lab-button secondary"
                      disabled={!model || !!busy || tasksRunning}
                      onClick={() =>
                        act("stream", async () => {
                          const created = await request("/stream", {
                            rate_per_second: inputRate,
                            cooldown_days: cooldown,
                            model_id: model,
                            start,
                            end,
                            warning_threshold: threshold,
                          });
                          setResults({ items: [], alerts: [] });
                          setSelected(null);
                          setStream(created.run_id);
                          setResultEngine("stream");
                          await loadStreams();
                        })
                      }
                    >
                      启动真实 Kafka / Flink 回放
                    </button>
                  )}
                </div>
                <p className="lab-muted">
                  离线参照用于批流对照；分布式回放由 Kafka、Flink
                  和独立服务执行。两种路径分别留档。输入速度只改变处理节奏，不改变历史日期或模型结果。
                  HMM
                  会先读取历史进行预热，首次状态出现前可通过输入与特征进度检查运行。
                </p>
              </section>
              <section className="lab-panel">
                <div className="lab-panel-title">
                  <h2>离线参照运行</h2>
                  <select
                    aria-label="离线参照运行"
                    value={run}
                    onChange={(e) => {
                      setRun(e.target.value);
                      setSelected(null);
                      setResultEngine("local");
                      setResultRevision((v) => v + 1);
                    }}
                  >
                    <option value="">选择已有回放</option>
                    {summary.runs.map((r: Data) => (
                      <option key={r.id} value={r.id}>
                        {r.id.slice(0, 8)} · {status(r.status)} · {r.cursor}/
                        {r.total}
                      </option>
                    ))}
                  </select>
                </div>
                {active ? (
                  <>
                    <div className="lab-actions">
                      <span>
                        已推进 {active.cursor} / {active.total} 个有效快照 ·{" "}
                        {status(active.status)}
                      </span>
                      {[1, 20, 5000].map((n) => (
                        <button
                          key={n}
                          className="lab-button secondary"
                          disabled={
                            !!busy ||
                            tasksRunning ||
                            active.cursor >= active.total
                          }
                          onClick={() =>
                            act("advance", () =>
                              request(`/runs/${run}/advance`, { steps: n }),
                            )
                          }
                        >
                          {n === 5000 ? "推进至结束" : `推进 ${n} 个快照`}
                        </button>
                      ))}
                    </div>
                    <p className="lab-muted">
                      每次推进均落盘，完成当前批次后暂停。关闭页面不丢失已完成进度。
                    </p>
                  </>
                ) : (
                  <p className="lab-empty">建立或选择回放后开始观察。</p>
                )}
              </section>
              {latest && (
                <section className="lab-panel">
                  <div className="lab-panel-title">
                    <h2>
                      截至 {latest.day} 的重建状态 ·{" "}
                      {resultEngine === "stream" ? "Kafka / Flink" : "离线参照"}
                    </h2>
                    <span className="lab-tag">
                      {latest.evaluation === "in_sample"
                        ? "样本内展示"
                        : "冻结模型前向评分（历史）"}
                    </span>
                  </div>
                  <p className="lab-muted">
                    归档模型 {latest.model_id?.slice(0, 8)} · 运行{" "}
                    {(resultEngine === "stream" ? stream : run).slice(0, 8)}。
                    上方配置仅影响新建运行，已有结果保持原版本。
                  </p>
                  <div className="lab-stats compact">
                    <section>
                      <span>模型状态</span>
                      <strong>{latest.state}</strong>
                      <small>
                        最大状态概率 {number(latest.probability * 100)}%
                      </small>
                    </section>
                    <section>
                      <span>规则基线</span>
                      <strong>{latest.rule_baseline}</strong>
                      <small>同一特征的固定压力阈值</small>
                    </section>
                    <section>
                      <span>预警案例</span>
                      <strong>{results.alerts.length}</strong>
                      <small>触发后按规则冷却</small>
                    </section>
                  </div>
                  <p className="lab-muted">
                    状态概率描述模型分类把握，不表示未来涨跌概率。预警效果尚需独立评估。
                  </p>
                  <ResponsiveContainer width="100%" height={290}>
                    <LineChart data={chartRows}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e4e8ee" />
                      <XAxis dataKey="day" minTickGap={80} />
                      <YAxis
                        domain={[0, 1]}
                        tickFormatter={(v) => `${v * 100}%`}
                      />
                      <Tooltip />
                      <ReferenceLine
                        y={threshold}
                        stroke="#d18c4c"
                        strokeDasharray="4 4"
                      />
                      <Line
                        dataKey="high_pressure_probability"
                        name="高压力状态概率"
                        connectNulls={false}
                        stroke="#5d63bb"
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                  <div className="lab-state-strip">
                    {results.items.slice(-120).map((r: Data) => (
                      <button
                        key={r.day}
                        className={
                          "lab-state-cell " +
                          (r.state === "高压力"
                            ? "high"
                            : r.state === "低压力"
                              ? "low"
                              : "medium")
                        }
                        title={`${r.day} ${r.state}`}
                        aria-label={`${r.day} ${r.state} 查看证据`}
                        onClick={() => explore(r)}
                      />
                    ))}
                  </div>
                  <p className="lab-muted">
                    最近120个有效快照：绿色为低压力，灰蓝为中等压力，红色为高压力。按时间从左到右、逐行排列，点击可追溯来源。
                  </p>
                </section>
              )}
              {results.items.length > 0 && (
                <section className="lab-panel">
                  <div className="lab-panel-title">
                    <h2>完整状态记录</h2>
                    <span>{results.items.length} 个快照</span>
                  </div>
                  <div className="lab-table">
                    <table>
                      <thead>
                        <tr>
                          <th>决策日期</th>
                          <th>模型状态</th>
                          <th>概率</th>
                          <th>规则基线</th>
                          <th>来源</th>
                        </tr>
                      </thead>
                      <tbody>
                        {results.items
                          .slice(historyPage * 30, historyPage * 30 + 30)
                          .map((r: Data) => (
                            <tr key={r.day}>
                              <td>{r.day}</td>
                              <td>{r.state}</td>
                              <td>{number(r.probability * 100)}%</td>
                              <td>{r.rule_baseline}</td>
                              <td>
                                <button
                                  className="lab-link"
                                  onClick={() => explore(r)}
                                >
                                  查看证据
                                </button>
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="lab-actions">
                    <button
                      className="lab-button secondary"
                      disabled={historyPage === 0}
                      onClick={() => setHistoryPage((p) => p - 1)}
                    >
                      上一页
                    </button>
                    <span>
                      第 {historyPage + 1} /{" "}
                      {Math.ceil(results.items.length / 30)} 页
                    </span>
                    <button
                      className="lab-button secondary"
                      disabled={(historyPage + 1) * 30 >= results.items.length}
                      onClick={() => setHistoryPage((p) => p + 1)}
                    >
                      下一页
                    </button>
                  </div>
                </section>
              )}
              <section className="lab-panel">
                <h2>预警与证据</h2>
                {results.alerts.length ? (
                  <div className="lab-table">
                    <table>
                      <thead>
                        <tr>
                          <th>回放时点</th>
                          <th>规则</th>
                          <th>概率</th>
                          <th>处理状态</th>
                          <th>操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {results.alerts.map((a: Data) => (
                          <tr key={a.id}>
                            <td>{a.day}</td>
                            <td>
                              高压力 ≥ {number(a.payload.threshold * 100)}%
                            </td>
                            <td>
                              {number(a.payload.result.probability * 100)}%
                            </td>
                            <td>{status(a.disposition)}</td>
                            <td>
                              <button
                                className="lab-link"
                                onClick={() => explore(a.payload.result)}
                              >
                                查看证据
                              </button>
                              <button
                                className="lab-link"
                                onClick={() =>
                                  act("ack", () =>
                                    request(
                                      (resultEngine === "stream"
                                        ? "/stream-alerts/"
                                        : "/alerts/") + a.id,
                                      {
                                        status: "acknowledged",
                                        note: "通过本地界面确认已读",
                                      },
                                    ),
                                  )
                                }
                              >
                                确认已读
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="lab-empty">
                    当前区间尚无预警。可以查看状态历史，或调整预警阈值建立新的回放。
                  </p>
                )}
              </section>
              <section className="lab-panel">
                <div className="lab-panel-title">
                  <h2>真实分布式运行</h2>
                  <button
                    className="lab-button secondary"
                    onClick={loadStreams}
                  >
                    刷新运行记录
                  </button>
                </div>
                {streams.components && (
                  <p className="lab-muted">
                    {Object.entries(streams.components)
                      .map(([key, ok]) => `${key}: ${ok ? "就绪" : "未就绪"}`)
                      .join(" · ")}
                  </p>
                )}
                {selectedStream?.status === "failed_integrity" && (
                  <div className="lab-error">
                    该运行完整性失败，已隔离保留用于排错。请选择完整运行，或建立新的回放。
                  </div>
                )}
                {streams.available ? (
                  <>
                    <select
                      aria-label="分布式运行"
                      value={stream}
                      onChange={(e) => {
                        setResults({ items: [], alerts: [] });
                        setStream(e.target.value);
                      }}
                    >
                      <option value="">选择分布式运行</option>
                      {streams.runs.map((r: Data) => (
                        <option key={r.id} value={r.id}>
                          {r.id.slice(0, 8)} · {status(r.status)} · 输入{" "}
                          {r.sent} / 特征 {r.processed}
                        </option>
                      ))}
                    </select>
                    <button
                      className="lab-button secondary"
                      disabled={
                        !stream ||
                        !!busy ||
                        selectedStream?.status !== "publishing"
                      }
                      onClick={() =>
                        act("pause-stream", () =>
                          request("/stream/" + stream + "/pause", {}),
                        )
                      }
                    >
                      暂停发布
                    </button>
                    <button
                      className="lab-button secondary"
                      disabled={
                        !stream ||
                        !!busy ||
                        tasksRunning ||
                        !["paused", "publish_failed"].includes(
                          selectedStream?.status,
                        )
                      }
                      onClick={() =>
                        act("resume-stream", () =>
                          request("/stream/" + stream + "/resume", {}),
                        )
                      }
                    >
                      从断点继续
                    </button>
                    <button
                      className="lab-button secondary"
                      disabled={!stream}
                      onClick={() =>
                        act("stream-results", async () => {
                          setSelected(null);
                          setResultEngine("stream");
                        })
                      }
                    >
                      读取分布式结果
                    </button>
                  </>
                ) : (
                  <p className="lab-muted">
                    点击检查连接；未连接时仍可运行离线参照，分布式验收需启动完整栈。
                  </p>
                )}
              </section>
            </>
          )}
          {page === "data" && (
            <>
              <section className="lab-panel">
                <h2>数据版本与处理口径</h2>
                {dataset ? (
                  <>
                    <p className="lab-mono">
                      版本 {dataset.id}
                      <br />
                      原包 SHA256 {dataset.archive_sha256}
                    </p>
                    {dataset.limitations.map((s: string) => (
                      <p key={s} className="lab-muted">
                        {s}
                      </p>
                    ))}
                  </>
                ) : (
                  <p>尚未导入。</p>
                )}
              </section>
              <section className="lab-panel">
                <h2>资产目录与内部质量</h2>
                <div className="lab-table">
                  <table>
                    <thead>
                      <tr>
                        <th>资产 / 类别</th>
                        <th>字段 / 单位</th>
                        <th>覆盖范围</th>
                        <th>观测数</th>
                        <th>需注意</th>
                      </tr>
                    </thead>
                    <tbody>
                      {summary.series.map((s: Data) => (
                        <tr key={s.id}>
                          <td>
                            <strong>{s.name}</strong>
                            <small>
                              {s.id} · {s.asset_class}
                            </small>
                          </td>
                          <td>
                            {s.quote_kind}
                            <small>{s.unit || "—"}</small>
                          </td>
                          <td>
                            {s.start || "—"}
                            <small>{s.end || status(s.status)}</small>
                          </td>
                          <td>{number(s.count, 0)}</td>
                          <td>
                            {s.status === "missing"
                              ? "官方缺失"
                              : `${s.quality.negative_count} 条负值保留 · ${s.quality.bad_rows.length} 条隔离`}
                            <small>{s.notes}</small>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
              <section className="lab-panel">
                <h2>辅助数据导入</h2>
                <div className="lab-table">
                  <table>
                    <thead>
                      <tr>
                        <th>来源</th>
                        <th>状态</th>
                        <th>记录数</th>
                        <th>质量说明</th>
                      </tr>
                    </thead>
                    <tbody>
                      {summary.sources.map((s: Data) => (
                        <tr key={s.path}>
                          <td>{s.path.split("/").at(-1)}</td>
                          <td>{status(s.status)}</td>
                          <td>{number(s.count, 0)}</td>
                          <td>{s.detail}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            </>
          )}
          {page === "evidence" && (
            <PdfWorkbench call={request} role={session.role} />
          )}
          {page === "evidence" && (
            <section className="lab-panel">
              <h2>检索已导入材料</h2>
              <p className="lab-muted">
                按标题检索；原始文本只作为材料展示，不执行其中的 HTML
                或指令。记录日期不等于已确认的历史发布时间。
              </p>
              <form
                className="lab-form"
                onSubmit={(e) => {
                  e.preventDefault();
                  act("search", async () =>
                    setDocs(
                      (
                        await request(
                          "/documents?q=" +
                            encodeURIComponent(query) +
                            "&kind=" +
                            kind,
                        )
                      ).items,
                    ),
                  );
                }}
              >
                <label>
                  数据类别
                  <select
                    value={kind}
                    onChange={(e) => setKind(e.target.value)}
                  >
                    {[
                      ["news", "新闻"],
                      ["calendar", "经济日历"],
                      ["anonymous_macro", "匿名指标"],
                      ["fund_summary", "资金摘要"],
                      ["social_posthoc", "事后采集推文"],
                      ["wechat", "公众号"],
                      ["turnover", "商品期货总成交额"],
                    ].map(([k, n]) => (
                      <option key={k} value={k}>
                        {n}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  标题关键词
                  <input
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="例如：美联储"
                  />
                </label>
                <button className="lab-button" disabled={!!busy}>
                  检索
                </button>
              </form>
              <div className="lab-docs">
                {docs.map((d) => (
                  <button
                    key={d.id}
                    onClick={() =>
                      act("doc", async () =>
                        setDoc(await request("/documents/" + d.id)),
                      )
                    }
                  >
                    <span>
                      {d.day || "日期未知"} · {d.trust}
                    </span>
                    <strong>{d.title}</strong>
                    <small>
                      {d.source} / {d.source_row}
                    </small>
                  </button>
                ))}
              </div>
              {doc && (
                <article className="lab-document">
                  <h3>{doc.title}</h3>
                  <p>
                    {doc.day} · {doc.trust}
                  </p>
                  <pre>{doc.body}</pre>
                </article>
              )}
            </section>
          )}
          {selected && (
            <section className="lab-panel">
              <div className="lab-panel-title">
                <h2>
                  {selected.day} · {selected.state} · 证据链
                </h2>
                <button
                  className="lab-link"
                  onClick={() => {
                    setSelected(null);
                    setEvidence(null);
                  }}
                >
                  关闭
                </button>
              </div>
              <p>
                模型 {selected.model_id} · 数据 {selected.dataset_id}
              </p>
              <div className="lab-table">
                <table>
                  <thead>
                    <tr>
                      <th>资产</th>
                      <th>原始观测日期</th>
                      <th>假设可用日期</th>
                      <th>值</th>
                      <th>原始行</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selected.evidence.map((e: Data) => (
                      <tr key={e.event_id}>
                        <td>
                          {summary.series.find(
                            (s: Data) => s.id === e.series_id,
                          )?.name || e.series_id}
                        </td>
                        <td>{e.observation_date}</td>
                        <td>{e.available_date}</td>
                        <td>{number(e.value, 6)}</td>
                        <td>
                          <button
                            className="lab-link"
                            onClick={() => viewEvidence(e.event_id)}
                          >
                            查看第 {e.source_row} 行
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <details>
                <summary>查看模型输入与特征贡献</summary>
                <pre>
                  {JSON.stringify(
                    {
                      features: selected.features,
                      contributions: selected.contributions,
                    },
                    null,
                    2,
                  )}
                </pre>
              </details>
              {evidence && (
                <div className="lab-document">
                  <h3>原始观测凭证</h3>
                  <pre>{JSON.stringify(evidence, null, 2)}</pre>
                </div>
              )}
            </section>
          )}
        </div>
      </main>
    </div>
  );
}

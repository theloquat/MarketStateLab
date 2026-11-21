import React from "react";
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  waitFor,
  fireEvent,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import ContestSession from "./ContestSession";
import ContestLab from "./ContestLab";
import {
  ModelWorkbench,
  ResearchWorkbench,
  PdfWorkbench,
  OperationsWorkbench,
} from "./ContestRelease";
const model = {
  id: "a".repeat(24),
  method: "hmm",
  components: 3,
  train_end: "2022-12-31",
  reduction: "pca95",
};
const dataset = {
  id: "fixture-only",
  archive_sha256: "synthetic-fixture",
  series_count: 1,
  observation_count: 21,
  document_count: 1,
  limitations: ["测试专用资料"],
};
const summary = {
  datasets: [dataset],
  series: [
    {
      id: "s",
      name: "测试资产",
      asset_class: "测试",
      status: "ready",
      count: 21,
      quality: { negative_count: 0, bad_rows: [] },
    },
  ],
  sources: [],
  models: [model],
  runs: [{ id: "run", status: "completed", cursor: 1, total: 1, spec: {} }],
  tasks: [],
  deployment: "distributed",
};
let loggedIn = false;
let calls: { path: string; body: any }[] = [];
const answer = (value: any, status = 200) =>
  Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    json: async () => value,
  });
function fetchMock(path: any, options: any = {}) {
  const body =
    options.body && typeof options.body === "string"
      ? JSON.parse(options.body)
      : options.body;
  calls.push({ path: String(path), body });
  if (path === "/auth/me")
    return answer(
      loggedIn ? { username: "viewer", role: "viewer" } : { detail: "expired" },
      loggedIn ? 200 : 401,
    );
  if (path === "/auth/login") {
    loggedIn = true;
    return answer({ username: "viewer", role: "viewer" });
  }
  if (path === "/auth/logout") {
    loggedIn = false;
    return answer({ status: "logged_out" });
  }
  if (path === "/graphql") return answer({ data: { dashboard: summary } });
  if (String(path).endsWith("/registry")) return answer({ aliases: [] });
  if (String(path).endsWith("/analyses")) return answer({ items: [] });
  if (String(path).endsWith("/research")) return answer({ items: [] });
  if (String(path).includes("/documents?"))
    return answer({
      items: [
        { id: "doc", title: "测试材料", kind: "news", day: "2020-03-16" },
      ],
    });
  if (String(path).endsWith("/documents/doc"))
    return answer({
      title: "测试材料",
      body: "<script>不得执行的材料</script>",
      raw_json: "{}",
      trust: "C",
    });
  if (String(path).endsWith("/monitor"))
    return answer({ ready: true, components: { mysql: true }, outboxes: {} });
  if (String(path).endsWith("/stream"))
    return answer({
      available: true,
      ready: true,
      runs: [],
      components: { mysql: true },
    });
  if (String(path).includes("/results"))
    return answer({
      items: [
        {
          day: "2023-01-04",
          state: "高压力",
          probability: 0.9,
          probabilities: { 高压力: 0.9 },
          features: {},
          evidence: [],
          contributions: [],
        },
      ],
      alerts: [],
    });
  return answer({ items: [] });
}
beforeEach(() => {
  loggedIn = false;
  calls = [];
  vi.stubGlobal("fetch", vi.fn(fetchMock));
  vi.stubGlobal(
    "WebSocket",
    class {
      onopen: any;
      onmessage: any;
      onclose: any;
      onerror: any;
      close() {}
    },
  );
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("critical contest workflows", () => {
  it("keeps login state, blocks a viewer mutation, navigates all research surfaces and logs out", async () => {
    const user = userEvent.setup();
    render(
      <ContestSession>
        <ContestLab />
      </ContestSession>,
    );
    await screen.findByRole("heading", { name: "登录市场状态实验室" });
    await user.type(screen.getByLabelText("密码"), "synthetic-test-password");
    await user.click(screen.getByRole("button", { name: "登录工作区" }));
    await screen.findByText("测试资产").catch(() => {});
    await user.click(screen.getByRole("button", { name: "校验并继续导入" }));
    await screen.findByText(/当前为只读观察员/);
    expect(calls.some((c) => c.path === "/api/lab/import")).toBe(false);
    await user.click(screen.getByRole("button", { name: "历史回放" }));
    await user.selectOptions(screen.getByLabelText("离线参照运行"), "run");
    await screen.findByText(/截至 2023-01-04/);
    await user.click(screen.getByRole("button", { name: "检查分布式连接" }));
    await screen.findByRole("button", { name: "启动真实 Kafka / Flink 回放" });
    await user.click(screen.getByRole("button", { name: "数据与质量" }));
    await screen.findByText("测试资产");
    await user.click(screen.getByRole("button", { name: "文本证据" }));
    await user.click(screen.getByRole("button", { name: "检索" }));
    await screen.findByText("测试材料");
    await user.click(screen.getByRole("button", { name: "模型与特征" }));
    await screen.findByRole("heading", { name: "注册表" });
    expect(
      (screen.getByRole("button", { name: "训练新版本" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "研究与案例" }));
    await screen.findByRole("heading", { name: "Agent 研究规划" });
    expect(
      (
        screen.getByRole("button", {
          name: "调用模型规划一次",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "运行监控" }));
    await screen.findByText(/"mysql": true/);
    await user.click(screen.getByRole("button", { name: "退出" }));
    await screen.findByRole("heading", { name: "登录市场状态实验室" });
  });
  it("expires the session without persisting a bearer token in browser storage", async () => {
    loggedIn = true;
    render(
      <ContestSession>
        <div>private screen</div>
      </ContestSession>,
    );
    await screen.findByText("private screen");
    fireEvent(window, new Event("quantos-session-expired"));
    await screen.findByRole("heading", { name: "登录市场状态实验室" });
    expect(localStorage.length).toBe(0);
  });
  it("submits a frozen training specification and displays metadata without paid model calls", async () => {
    const call = vi.fn(async (path: string, body?: any) =>
      path === "/registry"
        ? { aliases: [] }
        : path === "/analyses"
          ? { items: [] }
          : path.startsWith("/features/")
            ? { items: [{ key: "feature1", definition: "测试定义" }] }
            : { task_id: "task" },
    );
    const refresh = vi.fn(async () => {});
    const user = userEvent.setup();
    render(
      <ModelWorkbench
        call={call}
        models={[model]}
        refresh={refresh}
        role="researcher"
      />,
    );
    await user.click(screen.getByRole("button", { name: "训练新版本" }));
    await waitFor(() =>
      expect(call).toHaveBeenCalledWith("/models", {
        method: "hmm",
        components: 3,
        reduction: "pca95",
        train_end: "2022-12-31",
      }),
    );
    await user.click(screen.getByRole("button", { name: "检查特征与投影" }));
    await screen.findByText(/测试定义/);
    expect(call.mock.calls.some(([p]) => p.includes("agent"))).toBe(false);
  });
  it("requires an approved research record before showing the execution action", async () => {
    let status = "proposed";
    const call = vi.fn(async (path: string, body?: any) => {
      if (path === "/research")
        return {
          items: [
            {
              id: "r1",
              owner: "fixture",
              status,
              spec: { tool: "quality_audit", hypothesis: "检查数据完整性" },
              result: {},
            },
          ],
        };
      if (path.endsWith("/decision")) {
        status = body.status;
        return { status };
      }
      return { task_id: "tool-task" };
    });
    const user = userEvent.setup();
    render(
      <ResearchWorkbench call={call} models={[model]} role="researcher" />,
    );
    await screen.findByText("数据质量检查 · 待批准");
    expect(screen.queryByRole("button", { name: "执行已批准工具" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "批准研究" }));
    await screen.findByRole("button", { name: "执行已批准工具" });
    await user.click(screen.getByRole("button", { name: "执行已批准工具" }));
    await waitFor(() =>
      expect(call).toHaveBeenCalledWith("/research/r1/execute", {}),
    );
    expect(call.mock.calls.filter(([p]) => p === "/agent/plan")).toHaveLength(
      0,
    );
  });
  it("keeps PDF submission disabled without a file and renders monitor failure clearly", async () => {
    const call = vi.fn(async () => {
      throw new Error("fixture backend unavailable");
    });
    render(
      <>
        <PdfWorkbench call={call} role="viewer" />
        <OperationsWorkbench call={call} />
      </>,
    );
    expect(
      (screen.getByRole("button", { name: "归档与提取" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    await screen.findByRole("alert");
  });
});

it("separates pending deliveries from completed history and offers real evidence downloads", async () => {
  const call = vi.fn(async () => ({
    components: { kafka: true, flink: false },
    outboxes: {
      outbox: [
        { sent: 1, n: 9000 },
        { sent: 0, n: 7 },
      ],
      ch_outbox: [{ sent: 1, n: 12000 }],
    },
    workers: [{ role: "features", updated_at: "2026-11-08T11:00:00" }],
    evidence: [{ name: "capacity.json", bytes: 1024 }],
  }));
  render(<OperationsWorkbench call={call} />);
  await screen.findByText("实时投递待处理");
  expect(screen.getByText("7")).toBeTruthy();
  expect(screen.getByText("0")).toBeTruthy();
  expect(screen.queryByText("9,000")).toBeNull();
  fireEvent.click(screen.getByText(/份报告，展开下载/));
  expect(
    screen.getByRole("link", { name: "capacity.json" }).getAttribute("href"),
  ).toBe("/api/lab/reports/capacity.json");
  expect(screen.getByText("待恢复")).toBeTruthy();
});

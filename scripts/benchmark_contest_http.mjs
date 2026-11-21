// Built-in Node HTTP client, six worker threads, 120 distinct signed test users.
// Source secrets are read locally and never included in stdout or report files.
import {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} from "node:worker_threads";
import { readFileSync, writeFileSync } from "node:fs";
import crypto from "node:crypto";
import http from "node:http";
import { performance } from "node:perf_hooks";
let paths = workerData?.paths || [];
function bearer(secret, user) {
  const now = Math.floor(Date.now() / 1000);
  const header = Buffer.from(
    JSON.stringify({ alg: "HS256", typ: "JWT" }),
  ).toString("base64url");
  const payload = Buffer.from(
    JSON.stringify({
      sub: user,
      role: "viewer",
      iat: now,
      exp: now + 300,
      jti: crypto.randomBytes(16).toString("hex"),
      iss: "quantos",
      aud: "market-lab",
    }),
  ).toString("base64url");
  const body = header + "." + payload;
  return (
    body +
    "." +
    crypto.createHmac("sha256", secret).update(body).digest("base64url")
  );
}
function quantiles(values) {
  values.sort((a, b) => a - b);
  return Object.fromEntries(
    [50, 95, 99].map((p) => [
      `p${p}_ms`,
      values[
        Math.min(values.length - 1, Math.floor(((values.length - 1) * p) / 100))
      ] ?? null,
    ]),
  );
}
if (isMainThread) {
  const config = Object.fromEntries(
    readFileSync(
      new URL("../deploy/contest/secrets.env", import.meta.url),
      "utf8",
    )
      .split("\n")
      .filter((x) => x.includes("="))
      .map((x) => {
        let i = x.indexOf("=");
        return [x.slice(0, i), x.slice(i + 1)];
      }),
  );
  const discovery = await fetch("http://127.0.0.1:18080/api/lab/stream", {
    headers: {
      Authorization:
        "Bearer " + bearer(config.CONTEST_JWT_SECRET, "benchmark-discovery"),
    },
  });
  if (!discovery.ok) throw new Error("Cannot discover benchmark runs");
  const completed = (await discovery.json()).runs
    .filter((r) => r.status === "features_completed")
    .sort((a, b) => b.payload.created_at.localeCompare(a.payload.created_at));
  if (!completed.length)
    throw new Error("Complete a distributed replay before benchmarking");
  const run = completed[0];
  paths = [
    "/api/features/latest/" + run.id,
    "/api/models/" + run.payload.model_id,
    "/api/alerts/" + run.id,
    "/graphql",
  ];
  const start = Date.now() + 1500;
  const seconds = Number(process.env.BENCH_SECONDS || 20);
  const stagger = process.env.BENCH_STAGGER === "1";
  const workers = Array.from(
    { length: 6 },
    (_, i) =>
      new Promise((resolve, reject) => {
        let w = new Worker(new URL(import.meta.url), {
          workerData: {
            paths,
            first: i * 20,
            start,
            seconds,
            stagger,
            jwt: config.CONTEST_JWT_SECRET,
          },
        });
        w.on("message", resolve);
        w.on("error", reject);
        w.on("exit", (c) => {
          if (c) reject(new Error("worker exited " + c));
        });
      }),
  );
  const pieces = await Promise.all(workers);
  const all = pieces.flatMap((x) => x.samples);
  const errors = pieces.flatMap((x) => x.errors);
  let perPath = {};
  for (let p of paths)
    perPath[p] = quantiles(all.filter((x) => x.path === p).map((x) => x.ms));
  const result = {
    created_at: new Date().toISOString(),
    client:
      "Node built-in http / 6 worker threads / keepalive / body read and JSON validated",
    users: 120,
    request_pattern: `120 authenticated users, ${stagger ? "uniformly staggered arrivals" : "synchronized burst"}, one request each second for ${seconds} seconds`,
    successful_requests: all.length,
    errors,
    endpoint_latency: perPath,
    client_latency: quantiles(all.map((x) => x.ms)),
    gateway_duration: quantiles(
      all.map((x) => x.server).filter(Number.isFinite),
    ),
    auth: quantiles(all.map((x) => x.auth).filter(Number.isFinite)),
    revocation: quantiles(all.map((x) => x.revocation).filter(Number.isFinite)),
    queue: quantiles(all.map((x) => x.queue).filter(Number.isFinite)),
    effective_requests_per_second: all.length / seconds,
    raw_pipeline_throughput_measured: false,
    paid_calls: 0,
  };
  writeFileSync(
    new URL(
      "../docs/contest/performance-http-node-" +
        new Date()
          .toISOString()
          .replace(/[-:]/g, "")
          .replace("T", "-")
          .slice(0, 15) +
        ".json",
      import.meta.url,
    ),
    JSON.stringify(result, null, 2) + "\n",
  );
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
  if (errors.length || all.length !== 120 * seconds) process.exitCode = 1;
} else {
  const agent = new http.Agent({ keepAlive: true, maxSockets: 30 });
  const samples = [],
    errors = [];
  const tokens = Array.from({ length: 20 }, (_, i) =>
    bearer(workerData.jwt, "node-capacity-" + (workerData.first + i)),
  );
  function request(path, t) {
    return new Promise((resolve) => {
      const started = performance.now();
      const body =
        path === "/graphql" ? JSON.stringify({ query: "{ dashboard }" }) : null;
      const req = http.request(
        {
          hostname: "127.0.0.1",
          port: 18080,
          path,
          method: body ? "POST" : "GET",
          agent,
          headers: {
            Authorization: "Bearer " + t,
            ...(body
              ? {
                  "Content-Type": "application/json",
                  "Content-Length": Buffer.byteLength(body),
                }
              : {}),
          },
        },
        (res) => {
          let data = [];
          res.on("data", (d) => data.push(d));
          res.on("end", () => {
            try {
              const value = JSON.parse(Buffer.concat(data));
              if (res.statusCode !== 200 || value.errors)
                throw new Error("HTTP " + res.statusCode);
              samples.push({
                path,
                ms: performance.now() - started,
                server: Number(res.headers["server-timing"]?.split("=")[1]),
                auth: Number(res.headers["x-auth-ms"]),
                revocation: Number(res.headers["x-revocation-ms"]),
                queue: Number(res.headers["x-queue-ms"]),
              });
            } catch (e) {
              errors.push({ path, error: String(e), status: res.statusCode });
            }
            resolve();
          });
        },
      );
      req.setTimeout(10000, () => req.destroy(new Error("timeout")));
      req.on("error", (e) => {
        errors.push({ path, error: e.message });
        resolve();
      });
      if (body) req.write(body);
      req.end();
    });
  }
  const wait = (ms) => new Promise((r) => setTimeout(r, Math.max(0, ms)));
  await wait(workerData.start - Date.now());
  await Promise.all(
    Array.from({ length: 20 }, async (_, i) => {
      for (let j = 0; j < workerData.seconds; j++) {
        await wait(
          workerData.start +
            j * 1000 +
            (workerData.stagger ? ((workerData.first + i) * 1000) / 120 : 0) -
            Date.now(),
        );
        await request(
          paths[(workerData.first + i + j) % paths.length],
          tokens[i],
        );
      }
    }),
  );
  agent.destroy();
  parentPort.postMessage({ samples, errors });
}

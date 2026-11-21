import { useEffect, useRef, useState } from "react";
type Data = Record<string, any>;
export function useContestEvents(
  runId: string,
  enabled: boolean,
  handlers: {
    state: (value: Data) => void;
    alert: (value: Data) => void;
    reconnect: (signal: AbortSignal) => Promise<void>;
  },
) {
  const activeRun = useRef(runId);
  activeRun.current = runId;
  const callbacks = useRef(handlers);
  callbacks.current = handlers;
  const [status, setStatus] = useState("未连接");
  useEffect(() => {
    if (!enabled || !runId) {
      setStatus("未连接");
      return;
    }
    let stopped = false,
      socket: WebSocket | undefined,
      timer: ReturnType<typeof setTimeout> | undefined,
      delay = 1000;
    const controller = new AbortController();
    const seen = new Set<string>();
    const connect = () => {
      if (stopped || activeRun.current !== runId) return;
      setStatus("正在连接实时更新");
      socket = new WebSocket(
        `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/api/lab/events`,
      );
      socket.onopen = () => {
        if (stopped || activeRun.current !== runId) return;
        delay = 1000;
        setStatus("回放实时更新已连接");
        callbacks.current.reconnect(controller.signal).catch(() => {
          if (!stopped) setStatus("已连接，历史补齐失败，可手动读取");
        });
      };
      socket.onmessage = (event) => {
        if (stopped || activeRun.current !== runId) return;
        let packet: Data;
        try {
          packet = JSON.parse(event.data);
        } catch {
          return;
        }
        const value = packet.payload;
        if (!value || typeof value !== "object") return;
        const belongs =
          packet.type === "state"
            ? value.run_id === runId
            : packet.type === "alert" && value.result?.run_id === runId;
        if (!belongs) return;
        if (packet.event_id) {
          if (seen.has(packet.event_id)) return;
          seen.add(packet.event_id);
          if (seen.size > 2048) seen.delete(seen.values().next().value!);
        }
        if (packet.type === "state") callbacks.current.state(value);
        else callbacks.current.alert(value);
      };
      socket.onclose = () => {
        if (stopped || activeRun.current !== runId) return;
        setStatus("连接中断，正在恢复");
        timer = setTimeout(connect, delay);
        delay = Math.min(delay * 2, 15000);
      };
    };
    connect();
    return () => {
      stopped = true;
      controller.abort();
      if (timer) clearTimeout(timer);
      socket?.close();
    };
  }, [runId, enabled]);
  return status;
}

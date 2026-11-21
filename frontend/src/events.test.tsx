import { renderHook, act, cleanup, waitFor } from "@testing-library/react";
import { afterEach, it, expect, vi } from "vitest";
import { useContestEvents } from "./useContestEvents";
class Socket {
  static instances: Socket[] = [];
  onopen: any;
  onmessage: any;
  onclose: any;
  closed = false;
  constructor() {
    Socket.instances.push(this);
  }
  close() {
    this.closed = true;
  }
}
afterEach(() => {
  cleanup();
  Socket.instances = [];
  vi.unstubAllGlobals();
  vi.useRealTimers();
});
it("applies only selected-run committed events, deduplicates replay and closes on unmount", async () => {
  vi.stubGlobal("WebSocket", Socket);
  const state = vi.fn(),
    alert = vi.fn(),
    reconnect = vi.fn(async () => {});
  const result = renderHook(() =>
    useContestEvents("run", true, { state, alert, reconnect }),
  );
  const socket = Socket.instances[0];
  act(() => socket.onopen());
  await waitFor(() => expect(reconnect).toHaveBeenCalledTimes(1));
  const send = (packet: any) =>
    act(() => socket.onmessage({ data: JSON.stringify(packet) }));
  send({ type: "state", event_id: "a", payload: { run_id: "other" } });
  send({
    type: "state",
    event_id: "b",
    payload: { run_id: "run", day: "2020-01-01" },
  });
  send({
    type: "state",
    event_id: "b",
    payload: { run_id: "run", day: "2020-01-01" },
  });
  send({
    type: "alert",
    event_id: "c",
    payload: { id: "warning", result: { run_id: "run" } },
  });
  expect(state).toHaveBeenCalledTimes(1);
  expect(alert).toHaveBeenCalledTimes(1);
  result.unmount();
  expect(socket.closed).toBe(true);
});
it("reconnects after disconnection and asks REST to fill the missed interval", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", Socket);
  const reconnect = vi.fn(async () => {});
  const result = renderHook(() =>
    useContestEvents("run", true, {
      state: () => {},
      alert: () => {},
      reconnect,
    }),
  );
  await act(async () => Socket.instances[0].onopen());
  act(() => Socket.instances[0].onclose());
  await act(async () => vi.advanceTimersByTime(1000));
  expect(Socket.instances).toHaveLength(2);
  await act(async () => Socket.instances[1].onopen());
  expect(reconnect).toHaveBeenCalledTimes(2);
  result.unmount();
});

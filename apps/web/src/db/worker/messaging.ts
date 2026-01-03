import type { WorkerResponse } from "../client/protocol";

export function reply(msg: WorkerResponse): void {
  self.postMessage(msg);
}

export function log(message: string): void {
  reply({ type: "event", event: { type: "log", message } });
}

export function fail(requestId: string, e: unknown): void {
  const err = e instanceof Error ? e : new Error(String(e));
  reply({
    type: "response",
    requestId,
    ok: false,
    error: { message: err.message, stack: err.stack },
  });
}

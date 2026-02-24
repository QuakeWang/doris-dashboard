import { afterEach, describe, expect, it, vi } from "vitest";
import { DbClient } from "./dbClient";

class MockWorker {
  static instances: MockWorker[] = [];

  public onmessage: ((event: MessageEvent) => void) | null = null;
  public terminateCalls = 0;

  constructor(_scriptURL: string | URL, _options?: WorkerOptions) {
    MockWorker.instances.push(this);
  }

  addEventListener(_type: string, _listener: EventListenerOrEventListenerObject): void {}

  postMessage(_message: unknown): void {}

  terminate(): void {
    this.terminateCalls += 1;
  }
}

describe("DbClient.dispose", () => {
  afterEach(() => {
    MockWorker.instances = [];
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("rejects pending requests when disposed", async () => {
    vi.stubGlobal("Worker", MockWorker as unknown as typeof Worker);
    const client = new DbClient();

    const pending = client.queryTopSql("dataset-1", 10, {});
    client.dispose();

    await expect(pending).rejects.toThrow("DbClient has been disposed");
  });

  it("throws on new requests after dispose", async () => {
    vi.stubGlobal("Worker", MockWorker as unknown as typeof Worker);
    const client = new DbClient();
    client.dispose();

    await expect(client.queryOverview("dataset-1", {})).rejects.toThrow(
      "DbClient has been disposed"
    );
  });

  it("is idempotent and terminates worker once", () => {
    vi.stubGlobal("Worker", MockWorker as unknown as typeof Worker);
    const client = new DbClient();
    const worker = MockWorker.instances[0];
    expect(worker).toBeTruthy();

    client.dispose();
    client.dispose();

    expect(worker?.terminateCalls).toBe(1);
  });
});

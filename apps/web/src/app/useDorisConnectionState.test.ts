import { describe, expect, it } from "vitest";
import { formatDorisLabel, parseDorisSessionInfo } from "./useDorisConnectionState";

describe("parseDorisSessionInfo", () => {
  it("returns null for empty and malformed payloads", () => {
    expect(parseDorisSessionInfo(null)).toBeNull();
    expect(parseDorisSessionInfo("")).toBeNull();
    expect(parseDorisSessionInfo("{")).toBeNull();
  });

  it("returns null when required fields are missing", () => {
    expect(parseDorisSessionInfo(JSON.stringify({ port: 9030, user: "root" }))).toBeNull();
    expect(parseDorisSessionInfo(JSON.stringify({ host: "127.0.0.1", port: 9030 }))).toBeNull();
  });

  it("returns null when port is out of range", () => {
    expect(
      parseDorisSessionInfo(JSON.stringify({ host: "127.0.0.1", port: 0, user: "root" }))
    ).toBeNull();
    expect(
      parseDorisSessionInfo(JSON.stringify({ host: "127.0.0.1", port: 65536, user: "root" }))
    ).toBeNull();
  });

  it("parses a valid session payload", () => {
    expect(
      parseDorisSessionInfo(JSON.stringify({ host: "127.0.0.1", port: "9030", user: "root" }))
    ).toEqual({
      host: "127.0.0.1",
      port: 9030,
      user: "root",
    });
  });
});

describe("formatDorisLabel", () => {
  it("formats configured label and fallback", () => {
    expect(formatDorisLabel(null)).toBe("-");
    expect(
      formatDorisLabel({
        host: "127.0.0.1",
        port: 9030,
        user: "root",
        password: "pwd",
      })
    ).toBe("root@127.0.0.1:9030");
  });
});

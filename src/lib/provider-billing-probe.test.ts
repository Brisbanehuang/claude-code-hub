import { describe, expect, it, vi } from "vitest";
import {
  buildSub2ApiBillingProbeUrl,
  issueProviderBillingProbeToken,
  parseProviderBillingSnapshot,
  probeProviderBillingByIds,
  type ProviderBillingProbeConnection,
  type ProviderBillingSnapshot,
  validateProviderBillingProbeBindings,
  validateProviderBillingProbeToken,
} from "./provider-billing-probe";

const NOW = new Date("2026-07-24T12:01:00.000Z");
const TOKEN_SECRET = "test-provider-billing-probe-secret";

function provider(
  overrides: Partial<ProviderBillingProbeConnection> = {}
): ProviderBillingProbeConnection {
  return {
    id: 80,
    name: "Lyclaude-special",
    url: "https://upstream.example/v1",
    key: "sk-upstream-secret",
    isEnabled: true,
    proxyUrl: null,
    proxyFallbackToDirect: false,
    customHeaders: { "x-tenant": "tenant-a" },
    ...overrides,
  };
}

function billingPayload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    object: "sub2api.key_billing",
    schema_version: 1,
    billing_scope: "token",
    group_rate_multiplier: 0.08,
    resolved_rate_multiplier: 0.08,
    peak_rate_enabled: false,
    effective_rate_multiplier: 0.08,
    observed_at: "2026-07-24T12:00:00Z",
    ...overrides,
  };
}

function encodePayload(payload: Record<string, unknown>): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(payload));
}

function snapshot(overrides: Partial<ProviderBillingSnapshot> = {}): ProviderBillingSnapshot {
  return {
    object: "sub2api.key_billing",
    schemaVersion: 1,
    billingScope: "token",
    groupRateMultiplier: 0.08,
    resolvedRateMultiplier: 0.08,
    peakRateEnabled: false,
    observedEffectiveRateMultiplier: 0.08,
    currentEffectiveRateMultiplier: 0.08,
    observedAt: "2026-07-24T12:00:00.000Z",
    ...overrides,
  };
}

function jsonResponse(payload: Record<string, unknown>, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

describe("provider billing probe URL", () => {
  it.each([
    ["https://upstream.example", "https://upstream.example/v1/sub2api/billing"],
    ["https://upstream.example/v1", "https://upstream.example/v1/sub2api/billing"],
    ["https://upstream.example/openai", "https://upstream.example/openai/v1/sub2api/billing"],
    [
      "https://upstream.example/openai/v2?tenant=a#stale",
      "https://upstream.example/openai/v2/sub2api/billing?tenant=a",
    ],
  ])("builds %s", (base, expected) => {
    expect(buildSub2ApiBillingProbeUrl(base)).toBe(expected);
  });
});

describe("provider billing response validation", () => {
  it("validates the required schema and returns the current peak multiplier", () => {
    const parsed = parseProviderBillingSnapshot(
      encodePayload(
        billingPayload({
          peak_rate_enabled: true,
          peak_start: "12:00",
          peak_end: "13:00",
          peak_rate_multiplier: 2,
          applied_peak_multiplier: 1,
          timezone: "UTC",
          observed_at: "2026-07-24T11:59:00Z",
        })
      ),
      { now: NOW }
    );

    expect(parsed.observedEffectiveRateMultiplier).toBe(0.08);
    expect(parsed.currentEffectiveRateMultiplier).toBe(0.16);
  });

  it("accepts and discards unknown upstream fields", () => {
    const parsed = parseProviderBillingSnapshot(
      encodePayload(billingPayload({ future_extension: { secret: "must-not-propagate" } })),
      { now: NOW }
    );

    expect(parsed.currentEffectiveRateMultiplier).toBe(0.08);
    expect(parsed).not.toHaveProperty("future_extension");
    expect(JSON.stringify(parsed)).not.toContain("must-not-propagate");
  });

  it("accepts a single-digit peak hour", () => {
    const parsed = parseProviderBillingSnapshot(
      encodePayload(
        billingPayload({
          peak_rate_enabled: true,
          peak_start: "1:00",
          peak_end: "2:00",
          peak_rate_multiplier: 2,
          applied_peak_multiplier: 2,
          timezone: "UTC",
          effective_rate_multiplier: 0.16,
          observed_at: "2026-07-24T01:30:00Z",
        })
      ),
      { now: new Date("2026-07-24T01:31:00Z") }
    );
    expect(parsed.currentEffectiveRateMultiplier).toBe(0.16);
  });

  it.each([
    ["inconsistent resolved", { resolved_rate_multiplier: 0.09 }],
    ["stale observation", { observed_at: "2026-07-24T11:40:00Z" }],
    ["future observation", { observed_at: "2026-07-24T12:07:00Z" }],
    ["invalid RFC3339 date", { observed_at: "2026-02-30T12:00:00Z" }],
  ])("rejects %s", (_label, overrides) => {
    expect(() =>
      parseProviderBillingSnapshot(encodePayload(billingPayload(overrides)), { now: NOW })
    ).toThrow();
  });
});

describe("provider billing probe token", () => {
  it("binds the provider connection without exposing its identity in the payload", () => {
    const connection = provider();
    const issued = issueProviderBillingProbeToken({
      provider: connection,
      snapshot: snapshot(),
      now: NOW,
      secret: TOKEN_SECRET,
    });
    const payloadJson = Buffer.from(issued.token.split(".")[1], "base64url").toString("utf8");
    expect(payloadJson).not.toContain(connection.url);
    expect(payloadJson).not.toContain(connection.key);
    expect(payloadJson).not.toContain("x-tenant");

    expect(
      validateProviderBillingProbeToken({
        provider: connection,
        probeToken: issued.token,
        now: new Date(NOW.getTime() + 1_000),
        secret: TOKEN_SECRET,
      })
    ).toMatchObject({ ok: true });
    expect(
      validateProviderBillingProbeToken({
        provider: { ...connection, key: "sk-rotated" },
        probeToken: issued.token,
        now: new Date(NOW.getTime() + 1_000),
        secret: TOKEN_SECRET,
      })
    ).toEqual({ ok: false, reason: "identity_changed" });
  });

  it("rejects expired tokens and invalid binding sets", () => {
    const connection = provider();
    const issued = issueProviderBillingProbeToken({
      provider: connection,
      snapshot: snapshot(),
      now: NOW,
      ttlSeconds: 1,
      secret: TOKEN_SECRET,
    });
    expect(
      validateProviderBillingProbeToken({
        provider: connection,
        probeToken: issued.token,
        now: new Date(NOW.getTime() + 1_000),
        secret: TOKEN_SECRET,
      })
    ).toEqual({ ok: false, reason: "expired" });
    expect(
      validateProviderBillingProbeBindings({
        bindings: [
          { providerId: connection.id, probeToken: issued.token },
          { providerId: connection.id, probeToken: issued.token },
        ],
        providerIds: [connection.id],
        providers: [connection],
        now: NOW,
        secret: TOKEN_SECRET,
      })
    ).toEqual({ ok: false, providerId: connection.id, reason: "duplicate_provider" });
  });
});

describe("provider billing network probe", () => {
  it("uses stored credentials and returns only the sanitized current multiplier", async () => {
    const connection = provider();
    const fetchImpl = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      expect(String(url)).toBe("https://upstream.example/v1/sub2api/billing");
      expect(init?.method).toBe("GET");
      expect(init?.cache).toBe("no-store");
      expect(init?.redirect).toBe("manual");
      const headers = new Headers(init?.headers);
      expect(headers.get("authorization")).toBe(`Bearer ${connection.key}`);
      expect(headers.get("accept")).toBe("application/json");
      expect(headers.get("x-tenant")).toBe("tenant-a");
      return jsonResponse(billingPayload());
    });

    const result = await probeProviderBillingByIds([connection.id], {
      fetchImpl: fetchImpl as typeof fetch,
      loadProviders: async () => [connection],
      now: () => NOW,
      tokenSecret: TOKEN_SECRET,
      getProxyConfig: async () => null,
    });

    expect(result.results[0]).toMatchObject({
      providerId: connection.id,
      providerName: connection.name,
      status: "ok",
      effectiveRateMultiplier: 0.08,
      observedAt: "2026-07-24T12:00:00.000Z",
      probeExpiresAt: "2026-07-24T12:16:00.000Z",
    });
    expect(JSON.stringify(result)).not.toContain(connection.url);
    expect(JSON.stringify(result)).not.toContain(connection.key);
    expect(JSON.stringify(result)).not.toContain("x-tenant");
  });

  it("refuses stored custom headers that try to replace Authorization", async () => {
    const fetchImpl = vi.fn();
    const result = await probeProviderBillingByIds([80], {
      fetchImpl: fetchImpl as typeof fetch,
      loadProviders: async () => [
        provider({ customHeaders: { Authorization: "Bearer attacker-controlled" } }),
      ],
      now: () => NOW,
      tokenSecret: TOKEN_SECRET,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({
      status: "invalid_response",
      errorCode: "invalid_custom_headers",
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("rejects a response body larger than 64 KiB without returning it", async () => {
    const upstreamBody = "x".repeat(64 * 1024 + 1);
    const result = await probeProviderBillingByIds([80], {
      fetchImpl: vi.fn(
        async () =>
          new Response(upstreamBody, {
            headers: { "content-type": "application/json" },
          })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider()],
      now: () => NOW,
      tokenSecret: TOKEN_SECRET,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toEqual({
      providerId: 80,
      providerName: "Lyclaude-special",
      status: "invalid_response",
      errorCode: "response_too_large",
      httpStatus: 200,
    });
    expect(JSON.stringify(result)).not.toContain(upstreamBody.slice(0, 100));
  });

  it.each([
    [404, "unsupported"],
    [405, "unsupported"],
    [401, "unauthorized"],
    [403, "unauthorized"],
    [500, "http_error"],
  ] as const)("isolates HTTP %s as %s", async (status, expectedStatus) => {
    const result = await probeProviderBillingByIds([80], {
      fetchImpl: vi.fn(async () => jsonResponse({}, status)) as unknown as typeof fetch,
      loadProviders: async () => [provider()],
      now: () => NOW,
      tokenSecret: TOKEN_SECRET,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({ status: expectedStatus, httpStatus: status });
  });

  it("keeps proxy ownership until the response body is consumed", async () => {
    let closeBody!: () => void;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encodePayload(billingPayload()));
        closeBody = () => controller.close();
      },
    });
    const releaseProxyConfig = vi.fn();
    const pending = probeProviderBillingByIds([80], {
      fetchImpl: vi.fn(
        async () => new Response(stream, { headers: { "content-type": "application/json" } })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider({ proxyUrl: "http://proxy.example:8080" })],
      now: () => NOW,
      tokenSecret: TOKEN_SECRET,
      getProxyConfig: async () =>
        ({
          agent: {} as never,
          fallbackToDirect: false,
          proxyUrl: "http://proxy.example:8080",
          http2Enabled: false,
          cacheKey: "proxy-key",
          dispatcherId: "dispatcher-1",
        }) as never,
      releaseProxyConfig,
    });

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(releaseProxyConfig).not.toHaveBeenCalled();
    closeBody();
    await expect(pending).resolves.toMatchObject({ results: [{ status: "ok" }] });
    expect(releaseProxyConfig).toHaveBeenCalledTimes(1);
  });

  it("times out a stalled body and releases the proxy", async () => {
    const releaseProxyConfig = vi.fn();
    const result = await probeProviderBillingByIds([80], {
      fetchImpl: vi.fn(
        async () =>
          new Response(new ReadableStream<Uint8Array>(), {
            headers: { "content-type": "application/json" },
          })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider({ proxyUrl: "http://proxy.example:8080" })],
      now: () => NOW,
      tokenSecret: TOKEN_SECRET,
      timeoutMs: 10,
      getProxyConfig: async () =>
        ({
          agent: {} as never,
          fallbackToDirect: false,
          proxyUrl: "http://proxy.example:8080",
          http2Enabled: false,
          cacheKey: "proxy-key",
          dispatcherId: "dispatcher-1",
        }) as never,
      releaseProxyConfig,
    });
    expect(result.results[0]).toMatchObject({ status: "timeout", errorCode: "request_timeout" });
    expect(releaseProxyConfig).toHaveBeenCalledTimes(1);
  });

  it("limits one batch to four concurrent upstream requests", async () => {
    let active = 0;
    let maxActive = 0;
    const fetchImpl = vi.fn(async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return jsonResponse(billingPayload());
    });
    const connections = Array.from({ length: 12 }, (_, index) =>
      provider({ id: index + 1, name: `Provider ${index + 1}` })
    );
    const result = await probeProviderBillingByIds(
      connections.map((connection) => connection.id),
      {
        fetchImpl: fetchImpl as typeof fetch,
        loadProviders: async () => connections,
        now: () => NOW,
        tokenSecret: TOKEN_SECRET,
        getProxyConfig: async () => null,
      }
    );
    expect(result.results).toHaveLength(12);
    expect(maxActive).toBe(4);
  });
});

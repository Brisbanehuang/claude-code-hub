import { describe, expect, it, vi } from "vitest";
import {
  buildProviderUsageProbeUrl,
  parseProviderUsageResponse,
  probeProviderUsageByIds,
  type ProviderUsageProbeConnection,
} from "./provider-usage-probe";

const NOW = new Date("2026-07-25T12:00:00.000Z");

function provider(
  overrides: Partial<ProviderUsageProbeConnection> = {}
): ProviderUsageProbeConnection {
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

function encode(payload: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(payload));
}

function jsonResponse(
  payload: unknown,
  status = 200,
  headers: Record<string, string> = {}
): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers },
  });
}

describe("provider usage probe URL", () => {
  it.each<[string, string]>([
    ["https://upstream.example", "https://upstream.example/v1/usage"],
    ["https://upstream.example/v1", "https://upstream.example/v1/usage"],
    ["https://upstream.example/openai", "https://upstream.example/openai/v1/usage"],
    [
      "https://upstream.example/openai/v2?tenant=a#old",
      "https://upstream.example/openai/v2/usage?tenant=a",
    ],
  ])("builds %s", (base, expected) => {
    expect(buildProviderUsageProbeUrl(base)).toBe(expected);
  });
});

describe("provider usage response validation", () => {
  const validCases: Array<
    [
      Record<string, unknown>,
      string | null,
      string | null,
      "quota_limited" | "unrestricted",
      boolean,
    ]
  > = [
    [
      { mode: "unrestricted", remaining: 12.34, unit: "USD", isValid: true },
      "12.34",
      "USD",
      "unrestricted",
      true,
    ],
    [
      { mode: "quota_limited", quota: { remaining: "001.2300", unit: "USD" }, is_active: false },
      "1.23",
      "USD",
      "quota_limited",
      false,
    ],
    [
      { mode: "unrestricted", balance: "8e-3", unit: "USD", isValid: true },
      "0.008",
      "USD",
      "unrestricted",
      true,
    ],
    [
      { mode: "unrestricted", remaining: -0.5, unit: "USD", isValid: true },
      "-0.5",
      "USD",
      "unrestricted",
      true,
    ],
    [{ remaining: 13.31, unit: "USD" }, "13.31", "USD", "unrestricted", true],
    [
      { mode: "quota_limited", rate_limits: [{ window: "1d", remaining: 50 }], isValid: true },
      null,
      null,
      "quota_limited",
      true,
    ],
  ];
  it.each(validCases)(
    "normalizes supported response %#",
    (payload, remaining, unit, mode, isActive) => {
      expect(parseProviderUsageResponse(encode(payload), NOW)).toEqual({
        remaining,
        unit,
        mode,
        isActive,
        observedAt: NOW.toISOString(),
      });
    }
  );

  it("drops unknown fields and upstream timestamps", () => {
    const result = parseProviderUsageResponse(
      encode({
        mode: "unrestricted",
        remaining: 1,
        unit: "USD",
        isValid: true,
        observedAt: "1999-01-01T00:00:00Z",
        secret: "must-not-propagate",
      }),
      NOW
    );
    expect(result.observedAt).toBe(NOW.toISOString());
    expect(JSON.stringify(result)).not.toContain("must-not-propagate");
    expect(result).not.toHaveProperty("secret");
  });

  it.each([
    { mode: "other", remaining: 1, unit: "USD", isValid: true },
    { mode: "unrestricted", remaining: "NaN", unit: "USD", isValid: true },
    { mode: "unrestricted", remaining: 1, unit: "", isValid: true },
    { remaining: null, unit: "USD" },
    { mode: "unrestricted", remaining: 1, unit: "USD", isValid: "yes" },
  ])("rejects invalid response %#", (payload) => {
    expect(() => parseProviderUsageResponse(encode(payload), NOW)).toThrow();
  });
});

describe("provider usage network probe", () => {
  it("uses the stored connection and returns only the usage summary", async () => {
    const connection = provider();
    const fetchImpl = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      expect(String(url)).toBe("https://upstream.example/v1/usage");
      expect(init?.method).toBe("GET");
      expect(init?.cache).toBe("no-store");
      expect(init?.redirect).toBe("manual");
      const headers = new Headers(init?.headers);
      expect(headers.get("authorization")).toBe(`Bearer ${connection.key}`);
      expect(headers.get("x-tenant")).toBe("tenant-a");
      return jsonResponse({
        mode: "unrestricted",
        remaining: 13.31,
        unit: "USD",
        isValid: true,
        secret: "body-secret",
      });
    });
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: fetchImpl as typeof fetch,
      loadProviders: async () => [connection],
      now: () => NOW,
      getProxyConfig: async () => null,
    });
    expect(result).toEqual({
      results: [
        {
          providerId: 80,
          providerName: connection.name,
          status: "ok",
          remaining: "13.31",
          unit: "USD",
          mode: "unrestricted",
          isActive: true,
          observedAt: NOW.toISOString(),
        },
      ],
    });
    const serialized = JSON.stringify(result);
    expect(serialized).not.toContain(connection.url);
    expect(serialized).not.toContain(connection.key);
    expect(serialized).not.toContain("x-tenant");
    expect(serialized).not.toContain("body-secret");
  });

  it.each([
    [404, "unsupported", "usage_endpoint_unsupported"],
    [405, "unsupported", "usage_endpoint_unsupported"],
    [401, "unauthorized", "upstream_unauthorized"],
    [403, "unauthorized", "upstream_unauthorized"],
    [302, "invalid_response", "redirect_not_allowed"],
    [500, "http_error", "upstream_http_error"],
  ] as const)("isolates HTTP %s as %s", async (status, expectedStatus, errorCode) => {
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: vi.fn(async () => jsonResponse({}, status)) as unknown as typeof fetch,
      loadProviders: async () => [provider()],
      now: () => NOW,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({
      status: expectedStatus,
      errorCode,
      httpStatus: status,
    });
  });

  it("rejects responses larger than 64 KiB without returning their body", async () => {
    const body = "x".repeat(64 * 1024 + 1);
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: vi.fn(
        async () => new Response(body, { headers: { "content-type": "application/json" } })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider()],
      now: () => NOW,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({
      status: "invalid_response",
      errorCode: "response_too_large",
    });
    expect(JSON.stringify(result)).not.toContain(body.slice(0, 100));
  });

  it("rejects oversized content-length before reading the body", async () => {
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: vi.fn(
        async () =>
          new Response("{}", {
            headers: {
              "content-type": "application/json",
              "content-length": String(64 * 1024 + 1),
            },
          })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider()],
      now: () => NOW,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({
      status: "invalid_response",
      errorCode: "response_too_large",
    });
  });

  it.each([
    ["text/html", "<html>login</html>", "response_not_json"],
    ["application/json", "{broken", "invalid_json"],
  ])("rejects unsafe %s responses", async (contentType, body, errorCode) => {
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: vi.fn(
        async () => new Response(body, { headers: { "content-type": contentType } })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider()],
      now: () => NOW,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({ status: "invalid_response", errorCode });
    expect(JSON.stringify(result)).not.toContain(body);
  });

  it("rejects a custom Authorization header before making an upstream request", async () => {
    const connection = provider({ customHeaders: { Authorization: "Bearer attacker-value" } });
    const fetchImpl = vi.fn();
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: fetchImpl as typeof fetch,
      loadProviders: async () => [connection],
      now: () => NOW,
      getProxyConfig: async () => null,
    });
    expect(result.results[0]).toMatchObject({
      status: "invalid_response",
      errorCode: "invalid_custom_headers",
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("falls back from a failed proxy exactly once and releases its agent once", async () => {
    const releaseProxyConfig = vi.fn();
    const fetchImpl = vi
      .fn()
      .mockRejectedValueOnce(new Error("proxy unavailable"))
      .mockResolvedValueOnce(jsonResponse({ mode: "unrestricted", remaining: 2, unit: "USD" }));
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: fetchImpl as typeof fetch,
      loadProviders: async () => [
        provider({ proxyUrl: "http://proxy.example:8080", proxyFallbackToDirect: true }),
      ],
      now: () => NOW,
      getProxyConfig: async () =>
        ({
          agent: {} as never,
          fallbackToDirect: true,
          proxyUrl: "http://proxy.example:8080",
          http2Enabled: false,
          cacheKey: "proxy-key",
          dispatcherId: "dispatcher-1",
        }) as never,
      releaseProxyConfig,
    });
    expect(result.results[0]).toMatchObject({ status: "ok", remaining: "2" });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(releaseProxyConfig).toHaveBeenCalledOnce();
  });

  it("times out a stalled response body and releases the proxy", async () => {
    const releaseProxyConfig = vi.fn();
    const result = await probeProviderUsageByIds([80], {
      fetchImpl: vi.fn(
        async () =>
          new Response(new ReadableStream<Uint8Array>(), {
            headers: { "content-type": "application/json" },
          })
      ) as unknown as typeof fetch,
      loadProviders: async () => [provider({ proxyUrl: "http://proxy.example:8080" })],
      now: () => NOW,
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
    expect(releaseProxyConfig).toHaveBeenCalledOnce();
  });

  it("limits each batch to four concurrent upstream requests", async () => {
    let active = 0;
    let maxActive = 0;
    const fetchImpl = vi.fn(async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return jsonResponse({ mode: "unrestricted", remaining: 1, unit: "USD", isValid: true });
    });
    const providers = Array.from({ length: 12 }, (_, index) =>
      provider({ id: index + 1, name: `Provider ${index + 1}` })
    );
    const result = await probeProviderUsageByIds(
      providers.map((item) => item.id),
      {
        fetchImpl: fetchImpl as typeof fetch,
        loadProviders: async () => providers,
        now: () => NOW,
        getProxyConfig: async () => null,
      }
    );
    expect(result.results).toHaveLength(12);
    expect(maxActive).toBe(4);
  });

  it("rejects duplicate ids and isolates missing or disabled providers", async () => {
    await expect(
      probeProviderUsageByIds([80, 80], { loadProviders: async () => [] })
    ).rejects.toThrow(/unique/);
    const fetchImpl = vi.fn();
    const result = await probeProviderUsageByIds([80, 81], {
      fetchImpl: fetchImpl as typeof fetch,
      loadProviders: async () => [provider({ id: 81, isEnabled: false })],
    });
    expect(result.results).toEqual([
      {
        providerId: 80,
        providerName: "Provider #80",
        status: "invalid_response",
        errorCode: "provider_not_found",
      },
      {
        providerId: 81,
        providerName: "Lyclaude-special",
        status: "provider_disabled",
        errorCode: "provider_disabled",
      },
    ]);
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});

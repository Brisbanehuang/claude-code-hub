import "server-only";

import { normalizeCustomHeadersRecord } from "@/lib/custom-headers";
import {
  getGlobalAgentPool,
  getProxyAgentForProvider,
  type ProxyConfigWithCacheKey,
} from "@/lib/proxy-agent";
import { validateProviderUrlForConnectivity } from "@/lib/validation/provider-url";
import { findAllProvidersFresh } from "@/repository/provider";
import type { Provider } from "@/types/provider";

const USAGE_ENDPOINT = "/v1/usage";
const USAGE_PROBE_TIMEOUT_MS = 10_000;
const USAGE_PROBE_MAX_BODY_BYTES = 64 * 1024;
const USAGE_PROBE_CONCURRENCY = 4;

export const PROVIDER_USAGE_PROBE_STATUSES = [
  "ok",
  "unsupported",
  "unauthorized",
  "timeout",
  "http_error",
  "invalid_response",
  "provider_disabled",
] as const;

export type ProviderUsageProbeStatus = (typeof PROVIDER_USAGE_PROBE_STATUSES)[number];
export type ProviderUsageMode = "quota_limited" | "unrestricted";

export type ProviderUsageProbeResult =
  | {
      providerId: number;
      providerName: string;
      status: "ok";
      remaining: string | null;
      unit: string | null;
      mode: ProviderUsageMode;
      isActive: boolean;
      observedAt: string;
    }
  | {
      providerId: number;
      providerName: string;
      status: Exclude<ProviderUsageProbeStatus, "ok">;
      errorCode: string;
      httpStatus?: number;
    };

export type ProviderUsageProbeConnection = Pick<
  Provider,
  | "id"
  | "name"
  | "url"
  | "key"
  | "isEnabled"
  | "proxyUrl"
  | "proxyFallbackToDirect"
  | "customHeaders"
>;

interface ProviderUsageProbeDependencies {
  fetchImpl?: typeof fetch;
  loadProviders?: () => Promise<ProviderUsageProbeConnection[]>;
  now?: () => Date;
  timeoutMs?: number;
  getProxyConfig?: (
    provider: ProviderUsageProbeConnection,
    targetUrl: string
  ) => Promise<ProxyConfigWithCacheKey | null>;
  releaseProxyConfig?: (config: ProxyConfigWithCacheKey) => void;
}

class ProbeFailure extends Error {
  constructor(
    readonly status: Exclude<ProviderUsageProbeStatus, "ok">,
    readonly errorCode: string,
    readonly httpStatus?: number
  ) {
    super(errorCode);
  }
}

export function buildProviderUsageProbeUrl(baseUrl: string): string {
  const validation = validateProviderUrlForConnectivity(baseUrl);
  if (!validation.valid) throw new ProbeFailure("invalid_response", "invalid_provider_url");

  const parsed = new URL(validation.normalizedUrl);
  let path = parsed.pathname.replace(/\/+$/, "");
  if (!path.endsWith(USAGE_ENDPOINT)) {
    path += hasVersionSuffix(path) ? "/usage" : USAGE_ENDPOINT;
  }
  parsed.pathname = path;
  parsed.hash = "";
  return parsed.toString();
}

function hasVersionSuffix(path: string): boolean {
  const lastSegment = path.replace(/\/+$/, "").split("/").at(-1)?.toLowerCase() ?? "";
  return /^v\d+(?:\.\d+|(?:alpha|beta|preview).*)?$/.test(lastSegment);
}

function canonicalDecimal(value: unknown): string {
  const source =
    typeof value === "number"
      ? Number.isFinite(value)
        ? String(value)
        : ""
      : typeof value === "string"
        ? value.trim()
        : "";
  if (!source || source.length > 128) {
    throw new ProbeFailure("invalid_response", "invalid_remaining");
  }
  const match = /^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:e([+-]?\d+))?$/i.exec(source);
  if (!match) throw new ProbeFailure("invalid_response", "invalid_remaining");
  const exponent = Number(match[5] ?? "0");
  if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > 100) {
    throw new ProbeFailure("invalid_response", "invalid_remaining");
  }
  const integer = match[2] ?? "0";
  const fraction = match[3] ?? match[4] ?? "";
  const digits = `${integer}${fraction}`.replace(/^0+(?=\d)/, "");
  const scale = fraction.length - exponent;
  let output: string;
  if (scale <= 0) {
    output = digits + "0".repeat(-scale);
  } else if (digits.length <= scale) {
    output = `0.${"0".repeat(scale - digits.length)}${digits}`;
  } else {
    output = `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
  }
  if (output.includes(".")) output = output.replace(/0+$/, "").replace(/\.$/, "");
  output = output.replace(/^0+(?=\d)/, "") || "0";
  if (/^0(?:\.0*)?$/.test(output)) return "0";
  const canonical = match[1] === "-" ? `-${output}` : output;
  if (canonical.length > 128) throw new ProbeFailure("invalid_response", "invalid_remaining");
  return canonical;
}

export function parseProviderUsageResponse(
  body: Uint8Array,
  observedAt: Date
): Omit<
  Extract<ProviderUsageProbeResult, { status: "ok" }>,
  "providerId" | "providerName" | "status"
> {
  let json: unknown;
  try {
    json = JSON.parse(Buffer.from(body).toString("utf8"));
  } catch {
    throw new ProbeFailure("invalid_response", "invalid_json");
  }
  if (!json || typeof json !== "object" || Array.isArray(json)) {
    throw new ProbeFailure("invalid_response", "unexpected_usage_schema");
  }
  const data = json as Record<string, unknown>;
  const quota =
    data.quota && typeof data.quota === "object" && !Array.isArray(data.quota)
      ? (data.quota as Record<string, unknown>)
      : null;
  const hasLegacyBalance =
    (data.remaining !== undefined && data.remaining !== null) ||
    (data.balance !== undefined && data.balance !== null) ||
    (quota?.remaining !== undefined && quota.remaining !== null);
  const mode =
    data.mode === undefined && hasLegacyBalance
      ? quota || Array.isArray(data.rate_limits)
        ? "quota_limited"
        : "unrestricted"
      : data.mode;
  if (mode !== "quota_limited" && mode !== "unrestricted") {
    throw new ProbeFailure("invalid_response", "unexpected_usage_mode");
  }
  const remaining = data.remaining ?? quota?.remaining ?? data.balance;
  const unit = data.unit ?? quota?.unit;
  const isActive = data.is_active ?? data.isValid;
  if (isActive !== undefined && typeof isActive !== "boolean") {
    throw new ProbeFailure("invalid_response", "invalid_usage_active_state");
  }
  if (!Number.isFinite(observedAt.getTime())) {
    throw new ProbeFailure("invalid_response", "invalid_observed_at");
  }
  if (remaining === undefined || remaining === null) {
    return {
      remaining: null,
      unit: null,
      mode,
      isActive: isActive ?? true,
      observedAt: observedAt.toISOString(),
    };
  }
  if (typeof unit !== "string" || !unit.trim() || unit.trim().length > 32) {
    throw new ProbeFailure("invalid_response", "invalid_usage_unit");
  }
  return {
    remaining: canonicalDecimal(remaining),
    unit: unit.trim(),
    mode,
    isActive: isActive ?? true,
    observedAt: observedAt.toISOString(),
  };
}

async function readBoundedBody(response: Response, signal: AbortSignal): Promise<Uint8Array> {
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > USAGE_PROBE_MAX_BODY_BYTES) {
    await cancelResponseBody(response);
    throw new ProbeFailure("invalid_response", "response_too_large", response.status);
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  let aborted = false;
  const abort = () => {
    aborted = true;
    void reader.cancel();
  };
  signal.addEventListener("abort", abort, { once: true });
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > USAGE_PROBE_MAX_BODY_BYTES) {
        await reader.cancel();
        throw new ProbeFailure("invalid_response", "response_too_large", response.status);
      }
      chunks.push(value);
    }
  } finally {
    signal.removeEventListener("abort", abort);
    reader.releaseLock();
  }
  if (aborted) throw new ProbeFailure("timeout", "request_timeout");
  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

async function cancelResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // The response is already closed or owned by the transport.
  }
}

async function fetchWithProviderConnection(
  provider: ProviderUsageProbeConnection,
  url: string,
  headers: Headers,
  signal: AbortSignal,
  dependencies: ProviderUsageProbeDependencies
): Promise<{ response: Response; release: () => void }> {
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const getProxyConfig = dependencies.getProxyConfig ?? getProxyAgentForProvider;
  const releaseProxyConfig =
    dependencies.releaseProxyConfig ??
    ((config: ProxyConfigWithCacheKey) => {
      getGlobalAgentPool().releaseAgent(config.cacheKey, config.dispatcherId);
    });
  let proxyConfig: ProxyConfigWithCacheKey | null = null;
  try {
    proxyConfig = await getProxyConfig(provider, url);
  } catch {
    throw new ProbeFailure("invalid_response", "invalid_proxy_configuration");
  }
  let released = false;
  const release = () => {
    if (proxyConfig && !released) {
      released = true;
      releaseProxyConfig(proxyConfig);
    }
  };
  const init: RequestInit & { dispatcher?: unknown } = {
    method: "GET",
    headers,
    cache: "no-store",
    redirect: "manual",
    signal,
  };
  if (proxyConfig) init.dispatcher = proxyConfig.agent;
  try {
    try {
      return { response: await fetchImpl(url, init), release };
    } catch (error) {
      const canFallback = Boolean(proxyConfig?.fallbackToDirect) && !signal.aborted;
      release();
      if (!canFallback) throw error;
      return {
        response: await fetchImpl(url, {
          method: "GET",
          headers,
          cache: "no-store",
          redirect: "manual",
          signal,
        }),
        release: () => {},
      };
    }
  } catch (error) {
    release();
    throw error;
  }
}

async function probeOneProvider(
  provider: ProviderUsageProbeConnection,
  dependencies: ProviderUsageProbeDependencies
): Promise<ProviderUsageProbeResult> {
  if (!provider.isEnabled) {
    return {
      providerId: provider.id,
      providerName: provider.name,
      status: "provider_disabled",
      errorCode: "provider_disabled",
    };
  }
  try {
    if (!provider.key) throw new ProbeFailure("invalid_response", "missing_api_key");
    const url = buildProviderUsageProbeUrl(provider.url);
    const customHeaders = provider.customHeaders
      ? normalizeCustomHeadersRecord(provider.customHeaders)
      : { ok: true as const, value: null };
    if (!customHeaders.ok) throw new ProbeFailure("invalid_response", "invalid_custom_headers");
    const headers = new Headers(customHeaders.value ?? {});
    headers.set("Accept", "application/json");
    headers.set("Authorization", `Bearer ${provider.key}`);

    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      dependencies.timeoutMs ?? USAGE_PROBE_TIMEOUT_MS
    );
    let release = () => {};
    try {
      const fetched = await fetchWithProviderConnection(
        provider,
        url,
        headers,
        controller.signal,
        dependencies
      );
      const response = fetched.response;
      release = fetched.release;
      if (response.status === 404 || response.status === 405) {
        await cancelResponseBody(response);
        throw new ProbeFailure("unsupported", "usage_endpoint_unsupported", response.status);
      }
      if (response.status === 401 || response.status === 403) {
        await cancelResponseBody(response);
        throw new ProbeFailure("unauthorized", "upstream_unauthorized", response.status);
      }
      if (response.status >= 300 && response.status < 400) {
        await cancelResponseBody(response);
        throw new ProbeFailure("invalid_response", "redirect_not_allowed", response.status);
      }
      if (response.status < 200 || response.status >= 300) {
        await cancelResponseBody(response);
        throw new ProbeFailure("http_error", "upstream_http_error", response.status);
      }
      if (!/json/i.test(response.headers.get("content-type") ?? "")) {
        await cancelResponseBody(response);
        throw new ProbeFailure("invalid_response", "response_not_json", response.status);
      }
      const body = await readBoundedBody(response, controller.signal);
      const usage = parseProviderUsageResponse(body, dependencies.now?.() ?? new Date());
      return { providerId: provider.id, providerName: provider.name, status: "ok", ...usage };
    } catch (error) {
      if (controller.signal.aborted) throw new ProbeFailure("timeout", "request_timeout");
      if (error instanceof ProbeFailure) throw error;
      throw new ProbeFailure("http_error", "request_failed");
    } finally {
      clearTimeout(timeout);
      release();
    }
  } catch (error) {
    const failure =
      error instanceof ProbeFailure
        ? error
        : new ProbeFailure("invalid_response", "probe_internal_error");
    return {
      providerId: provider.id,
      providerName: provider.name,
      status: failure.status,
      errorCode: failure.errorCode,
      ...(failure.httpStatus === undefined ? {} : { httpStatus: failure.httpStatus }),
    };
  }
}

async function mapWithConcurrency<T, R>(
  values: T[],
  concurrency: number,
  worker: (value: T) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(values.length);
  let nextIndex = 0;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () => {
      while (true) {
        const index = nextIndex++;
        if (index >= values.length) return;
        results[index] = await worker(values[index]);
      }
    })
  );
  return results;
}

export async function probeProviderUsageByIds(
  providerIds: number[],
  dependencies: ProviderUsageProbeDependencies = {}
): Promise<{ results: ProviderUsageProbeResult[] }> {
  const uniqueProviderIds = [...new Set(providerIds)];
  if (
    uniqueProviderIds.length !== providerIds.length ||
    uniqueProviderIds.length < 1 ||
    uniqueProviderIds.length > 20
  ) {
    throw new Error("providerIds must contain between 1 and 20 unique provider ids");
  }
  const loadProviders = dependencies.loadProviders ?? findAllProvidersFresh;
  const providers = await loadProviders();
  const providersById = new Map(providers.map((provider) => [provider.id, provider]));
  const results = await mapWithConcurrency(
    providerIds,
    USAGE_PROBE_CONCURRENCY,
    async (providerId) => {
      const provider = providersById.get(providerId);
      if (!provider) {
        return {
          providerId,
          providerName: `Provider #${providerId}`,
          status: "invalid_response" as const,
          errorCode: "provider_not_found",
        };
      }
      return probeOneProvider(provider, dependencies);
    }
  );
  return { results };
}

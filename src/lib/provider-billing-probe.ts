import "server-only";

import { createHmac, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import { getEnvConfig } from "@/lib/config/env.schema";
import { normalizeCustomHeadersRecord } from "@/lib/custom-headers";
import {
  getGlobalAgentPool,
  getProxyAgentForProvider,
  type ProxyConfigWithCacheKey,
} from "@/lib/proxy-agent";
import { validateProviderUrlForConnectivity } from "@/lib/validation/provider-url";
import { findAllProvidersFresh } from "@/repository/provider";
import type { Provider } from "@/types/provider";

const BILLING_ENDPOINT = "/v1/sub2api/billing";
const BILLING_PROBE_TIMEOUT_MS = 10_000;
const BILLING_PROBE_MAX_BODY_BYTES = 64 * 1024;
const BILLING_PROBE_CONCURRENCY = 4;
const BILLING_PROBE_TOKEN_TTL_SECONDS = 15 * 60;
const BILLING_PROBE_TOKEN_DOMAIN = "cch:provider-billing-probe:v1";
const BILLING_PROBE_TOKEN_PREFIX = "pbp1";
const BILLING_MULTIPLIER_EPSILON = 1e-9;

export const PROVIDER_BILLING_PROBE_STATUSES = [
  "ok",
  "unsupported",
  "unauthorized",
  "timeout",
  "http_error",
  "invalid_response",
  "provider_disabled",
] as const;

export type ProviderBillingProbeStatus = (typeof PROVIDER_BILLING_PROBE_STATUSES)[number];

export interface ProviderBillingSnapshot {
  object: "sub2api.key_billing";
  schemaVersion: 1;
  billingScope: "token";
  groupRateMultiplier: number;
  userRateMultiplier?: number;
  resolvedRateMultiplier: number;
  peakRateEnabled: boolean;
  peakStart?: string;
  peakEnd?: string;
  peakRateMultiplier?: number;
  appliedPeakMultiplier?: number;
  timezone?: string;
  observedEffectiveRateMultiplier: number;
  currentEffectiveRateMultiplier: number;
  observedAt: string;
}

export interface ProviderBillingProbeBinding {
  providerId: number;
  probeToken: string;
}

export type ProviderBillingProbeResult =
  | {
      providerId: number;
      providerName: string;
      status: "ok";
      effectiveRateMultiplier: number;
      observedAt: string;
      probeToken: string;
      probeExpiresAt: string;
    }
  | {
      providerId: number;
      providerName: string;
      status: Exclude<ProviderBillingProbeStatus, "ok">;
      errorCode: string;
      httpStatus?: number;
    };

export type ProviderBillingProbeConnection = Pick<
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

interface ProviderBillingProbeTokenPayload {
  version: 1;
  providerId: number;
  issuedAt: number;
  expiresAt: number;
  snapshot: ProviderBillingSnapshot;
}

export type ProviderBillingProbeTokenValidation =
  | { ok: true; payload: ProviderBillingProbeTokenPayload }
  | {
      ok: false;
      reason:
        | "invalid_token"
        | "provider_mismatch"
        | "identity_changed"
        | "expired"
        | "secret_unavailable";
    };

type ProviderBillingProbeTokenFailureReason = Extract<
  ProviderBillingProbeTokenValidation,
  { ok: false }
>["reason"];

export type ProviderBillingProbeBindingsValidation =
  | {
      ok: true;
      providersById: Map<number, ProviderBillingProbeConnection>;
    }
  | {
      ok: false;
      providerId?: number;
      reason:
        | "duplicate_provider"
        | "provider_not_in_batch"
        | "provider_not_found"
        | "provider_disabled"
        | ProviderBillingProbeTokenFailureReason;
    };

interface ProviderBillingProbeDependencies {
  fetchImpl?: typeof fetch;
  loadProviders?: () => Promise<ProviderBillingProbeConnection[]>;
  now?: () => Date;
  tokenSecret?: string;
  timeoutMs?: number;
  getProxyConfig?: (
    provider: ProviderBillingProbeConnection,
    targetUrl: string
  ) => Promise<ProxyConfigWithCacheKey | null>;
  releaseProxyConfig?: (config: ProxyConfigWithCacheKey) => void;
}

const UpstreamBillingResponseSchema = z.object({
  object: z.literal("sub2api.key_billing"),
  schema_version: z.literal(1),
  billing_scope: z.literal("token"),
  group_rate_multiplier: z.number(),
  user_rate_multiplier: z.number().nullable().optional(),
  resolved_rate_multiplier: z.number(),
  peak_rate_enabled: z.boolean(),
  peak_start: z.string().nullable().optional(),
  peak_end: z.string().nullable().optional(),
  peak_rate_multiplier: z.number().nullable().optional(),
  applied_peak_multiplier: z.number().nullable().optional(),
  effective_rate_multiplier: z.number(),
  timezone: z.string().nullable().optional(),
  observed_at: z.string(),
});

const ProviderBillingSnapshotSchema = z
  .object({
    object: z.literal("sub2api.key_billing"),
    schemaVersion: z.literal(1),
    billingScope: z.literal("token"),
    groupRateMultiplier: z.number().nonnegative(),
    userRateMultiplier: z.number().nonnegative().optional(),
    resolvedRateMultiplier: z.number().nonnegative(),
    peakRateEnabled: z.boolean(),
    peakStart: z.string().optional(),
    peakEnd: z.string().optional(),
    peakRateMultiplier: z.number().nonnegative().optional(),
    appliedPeakMultiplier: z.number().nonnegative().optional(),
    timezone: z.string().optional(),
    observedEffectiveRateMultiplier: z.number().nonnegative(),
    currentEffectiveRateMultiplier: z.number().nonnegative(),
    observedAt: z.string(),
  })
  .strict();

const ProviderBillingProbeTokenPayloadSchema = z
  .object({
    version: z.literal(1),
    providerId: z.number().int().positive(),
    issuedAt: z.number().int().nonnegative(),
    expiresAt: z.number().int().positive(),
    snapshot: ProviderBillingSnapshotSchema,
  })
  .strict();

class ProbeFailure extends Error {
  constructor(
    readonly status: Exclude<ProviderBillingProbeStatus, "ok">,
    readonly errorCode: string,
    readonly httpStatus?: number
  ) {
    super(errorCode);
  }
}

function base64UrlEncode(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

function base64UrlDecode(value: string): string {
  return Buffer.from(value, "base64url").toString("utf8");
}

function getTokenSecret(override?: string): string | null {
  if (override) return override;
  const env = getEnvConfig();
  return env.CSRF_SECRET ?? env.ADMIN_TOKEN ?? null;
}

function normalizedConnectionIdentity(provider: ProviderBillingProbeConnection): string {
  const normalizedHeaders = Object.entries(provider.customHeaders ?? {})
    .map(([name, value]) => [name.toLowerCase(), value] as const)
    .sort(([left], [right]) => left.localeCompare(right));

  return JSON.stringify({
    providerId: provider.id,
    url: provider.url,
    key: provider.key,
    proxyUrl: provider.proxyUrl,
    proxyFallbackToDirect: provider.proxyFallbackToDirect,
    customHeaders: normalizedHeaders,
  });
}

function signTokenPayload(
  encodedPayload: string,
  provider: ProviderBillingProbeConnection,
  secret: string
): Buffer {
  return createHmac("sha256", secret)
    .update(BILLING_PROBE_TOKEN_DOMAIN)
    .update("\0")
    .update(encodedPayload)
    .update("\0")
    .update(normalizedConnectionIdentity(provider))
    .digest();
}

export function issueProviderBillingProbeToken(input: {
  provider: ProviderBillingProbeConnection;
  snapshot: ProviderBillingSnapshot;
  now?: Date;
  ttlSeconds?: number;
  secret?: string;
}): { token: string; expiresAt: Date } {
  const secret = getTokenSecret(input.secret);
  if (!secret) {
    throw new ProbeFailure("invalid_response", "probe_token_secret_unavailable");
  }
  const now = input.now ?? new Date();
  const ttlSeconds = input.ttlSeconds ?? BILLING_PROBE_TOKEN_TTL_SECONDS;
  const issuedAt = Math.floor(now.getTime() / 1000);
  const expiresAtSeconds = issuedAt + ttlSeconds;
  const payload: ProviderBillingProbeTokenPayload = {
    version: 1,
    providerId: input.provider.id,
    issuedAt,
    expiresAt: expiresAtSeconds,
    snapshot: input.snapshot,
  };
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signature = signTokenPayload(encodedPayload, input.provider, secret).toString("base64url");
  return {
    token: `${BILLING_PROBE_TOKEN_PREFIX}.${encodedPayload}.${signature}`,
    expiresAt: new Date(expiresAtSeconds * 1000),
  };
}

export function validateProviderBillingProbeToken(input: {
  provider: ProviderBillingProbeConnection;
  probeToken: string;
  now?: Date;
  secret?: string;
}): ProviderBillingProbeTokenValidation {
  const secret = getTokenSecret(input.secret);
  if (!secret) return { ok: false, reason: "secret_unavailable" };
  if (input.probeToken.length > 16_384) return { ok: false, reason: "invalid_token" };

  const parts = input.probeToken.split(".");
  if (parts.length !== 3 || parts[0] !== BILLING_PROBE_TOKEN_PREFIX) {
    return { ok: false, reason: "invalid_token" };
  }

  let receivedSignature: Buffer;
  try {
    receivedSignature = Buffer.from(parts[2], "base64url");
  } catch {
    return { ok: false, reason: "invalid_token" };
  }
  const expectedSignature = signTokenPayload(parts[1], input.provider, secret);
  if (
    receivedSignature.length !== expectedSignature.length ||
    !timingSafeEqual(receivedSignature, expectedSignature)
  ) {
    return { ok: false, reason: "identity_changed" };
  }

  let payload: ProviderBillingProbeTokenPayload;
  try {
    payload = ProviderBillingProbeTokenPayloadSchema.parse(JSON.parse(base64UrlDecode(parts[1])));
  } catch {
    return { ok: false, reason: "invalid_token" };
  }
  if (payload.providerId !== input.provider.id) {
    return { ok: false, reason: "provider_mismatch" };
  }
  const nowSeconds = Math.floor((input.now ?? new Date()).getTime() / 1000);
  if (payload.expiresAt <= nowSeconds || payload.expiresAt <= payload.issuedAt) {
    return { ok: false, reason: "expired" };
  }
  return { ok: true, payload };
}

export function validateProviderBillingProbeBindings(input: {
  bindings: ProviderBillingProbeBinding[];
  providerIds: number[];
  providers: ProviderBillingProbeConnection[];
  now?: Date;
  secret?: string;
}): ProviderBillingProbeBindingsValidation {
  const providerIdSet = new Set(input.providerIds);
  const providersById = new Map(input.providers.map((provider) => [provider.id, provider]));
  const seen = new Set<number>();
  const boundProviders = new Map<number, ProviderBillingProbeConnection>();

  for (const binding of input.bindings) {
    if (seen.has(binding.providerId)) {
      return { ok: false, providerId: binding.providerId, reason: "duplicate_provider" };
    }
    seen.add(binding.providerId);
    if (!providerIdSet.has(binding.providerId)) {
      return { ok: false, providerId: binding.providerId, reason: "provider_not_in_batch" };
    }
    const provider = providersById.get(binding.providerId);
    if (!provider) {
      return { ok: false, providerId: binding.providerId, reason: "provider_not_found" };
    }
    if (!provider.isEnabled) {
      return { ok: false, providerId: binding.providerId, reason: "provider_disabled" };
    }
    const token = validateProviderBillingProbeToken({
      provider,
      probeToken: binding.probeToken,
      now: input.now,
      secret: input.secret,
    });
    if (!token.ok) {
      return { ok: false, providerId: binding.providerId, reason: token.reason };
    }
    boundProviders.set(binding.providerId, provider);
  }
  return { ok: true, providersById: boundProviders };
}

export function buildSub2ApiBillingProbeUrl(baseUrl: string): string {
  const validation = validateProviderUrlForConnectivity(baseUrl);
  if (!validation.valid) {
    throw new ProbeFailure("invalid_response", "invalid_provider_url");
  }

  const parsed = new URL(validation.normalizedUrl);
  const endpoint = BILLING_ENDPOINT;
  const relative = endpoint.slice("/v1".length);
  let path = parsed.pathname.replace(/\/+$/, "");
  if (!path.endsWith(endpoint) && !path.endsWith(relative)) {
    path += hasVersionSuffix(path) ? relative : endpoint;
  }
  parsed.pathname = path;
  parsed.hash = "";
  return parsed.toString();
}

function hasVersionSuffix(path: string): boolean {
  const lastSegment = path.replace(/\/+$/, "").split("/").at(-1)?.toLowerCase() ?? "";
  return /^v\d+(?:\.\d+|(?:alpha|beta|preview).*)?$/.test(lastSegment);
}

function isNonNegativeFinite(value: number): boolean {
  return Number.isFinite(value) && value >= 0;
}

function equalMultiplier(left: number, right: number): boolean {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
  const scale = Math.max(1, Math.abs(left), Math.abs(right));
  return Math.abs(left - right) <= BILLING_MULTIPLIER_EPSILON * scale;
}

function parseMinuteOfDay(value: string): number | null {
  const match = /^(\d|[01]\d|2[0-3]):([0-5]\d)$/.exec(value);
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
}

function minuteOfDayInTimezone(date: Date, timezone: string): number | null {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).formatToParts(date);
    const hour = Number(parts.find((part) => part.type === "hour")?.value);
    const minute = Number(parts.find((part) => part.type === "minute")?.value);
    return Number.isInteger(hour) && Number.isInteger(minute) ? hour * 60 + minute : null;
  } catch {
    return null;
  }
}

function parseStrictRfc3339(value: string): Date | null {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/.exec(
      value
    );
  if (!match) return null;
  const [year, month, day, hour, minute, second] = match.slice(1, 7).map((part) => Number(part));
  const check = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  if (
    check.getUTCFullYear() !== year ||
    check.getUTCMonth() !== month - 1 ||
    check.getUTCDate() !== day ||
    check.getUTCHours() !== hour ||
    check.getUTCMinutes() !== minute ||
    check.getUTCSeconds() !== second
  ) {
    return null;
  }
  const zone = match[7];
  if (zone !== "Z" && (Number(zone.slice(1, 3)) > 23 || Number(zone.slice(4, 6)) > 59)) {
    return null;
  }
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}

function canonicalMultiplierNumber(value: number): number {
  return Number(value.toPrecision(15));
}

export function parseProviderBillingSnapshot(
  body: Uint8Array,
  options: { now?: Date; maxObservedAgeMs?: number; maxFutureSkewMs?: number } = {}
): ProviderBillingSnapshot {
  let json: unknown;
  try {
    json = JSON.parse(Buffer.from(body).toString("utf8"));
  } catch {
    throw new ProbeFailure("invalid_response", "invalid_json");
  }
  const parsed = UpstreamBillingResponseSchema.safeParse(json);
  if (!parsed.success) {
    throw new ProbeFailure("invalid_response", "unexpected_billing_schema");
  }
  const data = parsed.data;
  const requiredMultipliers = [
    data.group_rate_multiplier,
    data.resolved_rate_multiplier,
    data.effective_rate_multiplier,
  ];
  if (
    requiredMultipliers.some((value) => !isNonNegativeFinite(value)) ||
    (data.user_rate_multiplier != null && !isNonNegativeFinite(data.user_rate_multiplier))
  ) {
    throw new ProbeFailure("invalid_response", "invalid_billing_multiplier");
  }

  const expectedResolved = data.user_rate_multiplier ?? data.group_rate_multiplier;
  if (!equalMultiplier(data.resolved_rate_multiplier, expectedResolved)) {
    throw new ProbeFailure("invalid_response", "inconsistent_resolved_multiplier");
  }

  const observedAt = parseStrictRfc3339(data.observed_at);
  if (!observedAt) {
    throw new ProbeFailure("invalid_response", "invalid_observed_at");
  }
  const now = options.now ?? new Date();
  if (!Number.isFinite(now.getTime())) {
    throw new ProbeFailure("invalid_response", "invalid_current_time");
  }
  const ageMs = now.getTime() - observedAt.getTime();
  if (ageMs > (options.maxObservedAgeMs ?? 15 * 60 * 1000)) {
    throw new ProbeFailure("invalid_response", "observed_at_expired");
  }
  if (ageMs < -(options.maxFutureSkewMs ?? 5 * 60 * 1000)) {
    throw new ProbeFailure("invalid_response", "observed_at_in_future");
  }

  let expectedAppliedPeak = 1;
  if (data.peak_rate_enabled) {
    if (
      !data.peak_start ||
      !data.peak_end ||
      !data.timezone ||
      data.peak_rate_multiplier == null ||
      data.applied_peak_multiplier == null ||
      !isNonNegativeFinite(data.peak_rate_multiplier) ||
      !isNonNegativeFinite(data.applied_peak_multiplier)
    ) {
      throw new ProbeFailure("invalid_response", "incomplete_peak_billing");
    }
    const start = parseMinuteOfDay(data.peak_start);
    const end = parseMinuteOfDay(data.peak_end);
    const observedMinute = minuteOfDayInTimezone(observedAt, data.timezone);
    if (start === null || end === null || observedMinute === null || start >= end) {
      throw new ProbeFailure("invalid_response", "invalid_peak_billing");
    }
    expectedAppliedPeak =
      observedMinute >= start && observedMinute < end ? data.peak_rate_multiplier : 1;
    if (!equalMultiplier(data.applied_peak_multiplier, expectedAppliedPeak)) {
      throw new ProbeFailure("invalid_response", "inconsistent_applied_peak_multiplier");
    }
  } else if (
    data.applied_peak_multiplier != null &&
    !equalMultiplier(data.applied_peak_multiplier, 1)
  ) {
    throw new ProbeFailure("invalid_response", "inconsistent_applied_peak_multiplier");
  }

  if (
    !equalMultiplier(
      data.effective_rate_multiplier,
      data.resolved_rate_multiplier * expectedAppliedPeak
    )
  ) {
    throw new ProbeFailure("invalid_response", "inconsistent_effective_multiplier");
  }

  let currentAppliedPeak = 1;
  if (data.peak_rate_enabled) {
    const start = parseMinuteOfDay(data.peak_start!);
    const end = parseMinuteOfDay(data.peak_end!);
    const currentMinute = minuteOfDayInTimezone(now, data.timezone!);
    if (start === null || end === null || currentMinute === null || start >= end) {
      throw new ProbeFailure("invalid_response", "invalid_peak_billing");
    }
    currentAppliedPeak =
      currentMinute >= start && currentMinute < end ? data.peak_rate_multiplier! : 1;
  }
  const currentEffectiveRateMultiplier = canonicalMultiplierNumber(
    data.resolved_rate_multiplier * currentAppliedPeak
  );
  if (!isNonNegativeFinite(currentEffectiveRateMultiplier)) {
    throw new ProbeFailure("invalid_response", "invalid_current_effective_multiplier");
  }

  return {
    object: data.object,
    schemaVersion: data.schema_version,
    billingScope: data.billing_scope,
    groupRateMultiplier: data.group_rate_multiplier,
    ...(data.user_rate_multiplier == null ? {} : { userRateMultiplier: data.user_rate_multiplier }),
    resolvedRateMultiplier: data.resolved_rate_multiplier,
    peakRateEnabled: data.peak_rate_enabled,
    ...(data.peak_start == null ? {} : { peakStart: data.peak_start }),
    ...(data.peak_end == null ? {} : { peakEnd: data.peak_end }),
    ...(data.peak_rate_multiplier == null ? {} : { peakRateMultiplier: data.peak_rate_multiplier }),
    ...(data.applied_peak_multiplier == null
      ? {}
      : { appliedPeakMultiplier: data.applied_peak_multiplier }),
    ...(data.timezone == null ? {} : { timezone: data.timezone }),
    observedEffectiveRateMultiplier: canonicalMultiplierNumber(data.effective_rate_multiplier),
    currentEffectiveRateMultiplier,
    observedAt: observedAt.toISOString(),
  };
}

async function readBoundedBody(response: Response, signal: AbortSignal): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  let aborted = signal.aborted;
  const abortBody = () => {
    aborted = true;
    void reader.cancel(signal.reason).catch(() => {});
  };
  if (signal.aborted) abortBody();
  else signal.addEventListener("abort", abortBody, { once: true });
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!value) continue;
      length += value.byteLength;
      if (length > BILLING_PROBE_MAX_BODY_BYTES) {
        await reader.cancel();
        throw new ProbeFailure("invalid_response", "response_too_large", response.status);
      }
      chunks.push(value);
    }
  } finally {
    signal.removeEventListener("abort", abortBody);
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
  provider: ProviderBillingProbeConnection,
  url: string,
  headers: Headers,
  signal: AbortSignal,
  dependencies: ProviderBillingProbeDependencies
): Promise<{ response: Response; release: () => void }> {
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const getProxyConfig = dependencies.getProxyConfig ?? getProxyAgentForProvider;
  const releaseProxyConfig =
    dependencies.releaseProxyConfig ??
    ((config: ProxyConfigWithCacheKey) => {
      getGlobalAgentPool().releaseAgent(config.cacheKey, config.dispatcherId);
    });
  let proxyConfig: ProxyConfigWithCacheKey | null = null;

  let released = false;
  const release = () => {
    if (proxyConfig && !released) {
      released = true;
      releaseProxyConfig(proxyConfig);
    }
  };

  try {
    proxyConfig = await getProxyConfig(provider, url);
  } catch {
    throw new ProbeFailure("invalid_response", "invalid_proxy_configuration");
  }
  try {
    const init: RequestInit & { dispatcher?: unknown } = {
      method: "GET",
      headers,
      cache: "no-store",
      redirect: "manual",
      signal,
    };
    if (proxyConfig) init.dispatcher = proxyConfig.agent;
    try {
      const response = await fetchImpl(url, init);
      return { response, release };
    } catch (error) {
      const canFallback = Boolean(proxyConfig?.fallbackToDirect) && !signal.aborted;
      release();
      if (!canFallback) throw error;
      const response = await fetchImpl(url, {
        method: "GET",
        headers,
        cache: "no-store",
        redirect: "manual",
        signal,
      });
      return { response, release: () => {} };
    }
  } catch (error) {
    release();
    throw error;
  }
}

async function probeOneProvider(
  provider: ProviderBillingProbeConnection,
  dependencies: ProviderBillingProbeDependencies
): Promise<ProviderBillingProbeResult> {
  if (!provider.isEnabled) {
    return {
      providerId: provider.id,
      providerName: provider.name,
      status: "provider_disabled",
      errorCode: "provider_disabled",
    };
  }

  try {
    if (!provider.key) {
      throw new ProbeFailure("invalid_response", "missing_api_key");
    }
    const url = buildSub2ApiBillingProbeUrl(provider.url);
    const customHeaders = provider.customHeaders
      ? normalizeCustomHeadersRecord(provider.customHeaders)
      : { ok: true as const, value: null };
    if (!customHeaders.ok) {
      throw new ProbeFailure("invalid_response", "invalid_custom_headers");
    }
    const headers = new Headers(customHeaders.value ?? {});
    headers.set("Accept", "application/json");
    headers.set("Authorization", `Bearer ${provider.key}`);

    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      dependencies.timeoutMs ?? BILLING_PROBE_TIMEOUT_MS
    );
    let response: Response | undefined;
    let release = () => {};
    try {
      const fetched = await fetchWithProviderConnection(
        provider,
        url,
        headers,
        controller.signal,
        dependencies
      );
      response = fetched.response;
      release = fetched.release;

      if (response.status === 404 || response.status === 405) {
        await cancelResponseBody(response);
        throw new ProbeFailure("unsupported", "billing_endpoint_unsupported", response.status);
      }
      if (response.status === 401 || response.status === 403) {
        await cancelResponseBody(response);
        throw new ProbeFailure("unauthorized", "upstream_unauthorized", response.status);
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
      const completedAt = dependencies.now?.() ?? new Date();
      const snapshot = parseProviderBillingSnapshot(body, { now: completedAt });
      const issued = issueProviderBillingProbeToken({
        provider,
        snapshot,
        now: completedAt,
        secret: dependencies.tokenSecret,
      });
      return {
        providerId: provider.id,
        providerName: provider.name,
        status: "ok",
        effectiveRateMultiplier: snapshot.currentEffectiveRateMultiplier,
        observedAt: snapshot.observedAt,
        probeToken: issued.token,
        probeExpiresAt: issued.expiresAt.toISOString(),
      };
    } catch (error) {
      if (controller.signal.aborted) {
        throw new ProbeFailure("timeout", "request_timeout");
      }
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

export async function probeProviderBillingByIds(
  providerIds: number[],
  dependencies: ProviderBillingProbeDependencies = {}
): Promise<{ results: ProviderBillingProbeResult[] }> {
  const uniqueProviderIds = [...new Set(providerIds)];
  if (uniqueProviderIds.length < 1 || uniqueProviderIds.length > 20) {
    throw new Error("providerIds must contain between 1 and 20 unique provider ids");
  }
  const loadProviders = dependencies.loadProviders ?? findAllProvidersFresh;
  const allProviders = await loadProviders();
  const providersById = new Map(allProviders.map((provider) => [provider.id, provider]));
  const requested = uniqueProviderIds.map(
    (providerId): ProviderBillingProbeConnection =>
      providersById.get(providerId) ?? {
        id: providerId,
        name: `Provider #${providerId}`,
        url: "",
        key: "",
        isEnabled: false,
        proxyUrl: null,
        proxyFallbackToDirect: false,
        customHeaders: null,
      }
  );
  const results = await mapWithConcurrency<
    ProviderBillingProbeConnection,
    ProviderBillingProbeResult
  >(requested, BILLING_PROBE_CONCURRENCY, async (provider) => {
    if (providersById.has(provider.id)) return probeOneProvider(provider, dependencies);
    return {
      providerId: provider.id,
      providerName: provider.name,
      status: "invalid_response",
      errorCode: "provider_not_found",
    };
  });
  return { results };
}

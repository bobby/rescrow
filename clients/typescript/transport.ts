/** Bounded Fetch transport; no redirects or inferred endpoint discovery. */
export type ClientOptions = {
  authorityGeneration?: string;
  requireProfile?: "simulation" | "local" | "regional" | "enterprise-mr";
  attemptTimeoutMs?: number;
  overallTimeoutMs?: number;
  maxAttempts?: number;
};
const uuid7 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
export function newId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let time = BigInt(Date.now());
  for (let i = 5; i >= 0; i--) {
    bytes[i] = Number(time & 255n);
    time >>= 8n;
  }
  bytes[6] = (bytes[6]! & 15) | 0x70;
  bytes[8] = (bytes[8]! & 63) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join(
    "",
  );
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export function pathId(value: string): string {
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(value))
    throw new Error("Invalid object ID");
  return value === "." ? "~Lg" : value === ".." ? "~Li4" : value;
}
export class ProblemError extends Error {
  readonly problem: Record<string, unknown>;
  constructor(problem: Record<string, unknown>) {
    super(String(problem.detail ?? "Remote request failed"));
    this.problem = problem;
  }
}
const profiles = ["simulation", "local", "regional", "enterprise-mr"];
function unknown(key?: string): ProblemError {
  return new ProblemError({
    type: "urn:rescrow:problem:deadline_exceeded",
    category: "unknown_outcome",
    code: "deadline_exceeded",
    status: 503,
    detail:
      "Outcome unconfirmed; inspect or retry only the identical identity and request.",
    request_id: "client",
    transaction_id: key ?? null,
    retryable: true,
  });
}
export class Transport {
  readonly endpoints: string[];
  readonly token: string;
  readonly options: Required<Omit<ClientOptions, "authorityGeneration">>;
  generation?: string;
  constructor(
    endpoints: string | string[],
    token: string,
    options: ClientOptions = {},
  ) {
    this.endpoints =
      typeof endpoints === "string" ? [endpoints] : [...endpoints];
    this.token = token;
    this.generation = options.authorityGeneration;
    this.options = {
      requireProfile: options.requireProfile ?? "simulation",
      attemptTimeoutMs: options.attemptTimeoutMs ?? 5000,
      overallTimeoutMs: options.overallTimeoutMs ?? 30000,
      maxAttempts: options.maxAttempts ?? 8,
    };
    if (
      this.endpoints.length < 1 ||
      this.endpoints.length > 8 ||
      !token ||
      token.length > 16384 ||
      /[\x00-\x1f\x7f]/.test(token)
    )
      throw new Error("Invalid endpoints or bearer");
    if (this.generation !== undefined && !uuid7.test(this.generation))
      throw new Error("Invalid authority generation");
    if (this.endpoints.length > 1 && !this.generation)
      throw new Error(
        "Alternate endpoints require a pinned authority generation",
      );
    this.endpoints = this.endpoints.map((endpoint) => {
      const url = new URL(endpoint);
      const host = url.hostname;
      const loopback =
        host === "localhost" ||
        host === "[::1]" ||
        /^127(?:\.\d{1,3}){3}$/.test(host);
      if (
        (url.protocol !== "https:" &&
          !(url.protocol === "http:" && loopback)) ||
        url.username ||
        url.password ||
        url.pathname !== "/" ||
        url.search ||
        url.hash
      )
        throw new Error("HTTPS origin or loopback HTTP origin required");
      return url.origin;
    });
    const o = this.options;
    if (
      !(
        o.attemptTimeoutMs > 0 &&
        o.attemptTimeoutMs <= o.overallTimeoutMs &&
        o.overallTimeoutMs <= 300000 &&
        Number.isInteger(o.maxAttempts) &&
        o.maxAttempts >= 1 &&
        o.maxAttempts <= 32 &&
        profiles.includes(o.requireProfile)
      )
    )
      throw new Error("Invalid retry bounds or profile");
  }
  protected async request(
    method: string,
    path: string,
    body?: unknown,
    key?: string,
    query?: Record<string, string | number>,
    transactionId?: string,
  ): Promise<unknown> {
    const identity = key ?? transactionId;
    if (key !== undefined && !uuid7.test(key))
      throw new Error("Idempotency-Key must be a canonical UUIDv7");
    // Capture the exact body once; every attempt uses these bytes and this key.
    const payload = body === undefined ? undefined : JSON.stringify(body);
    if (query)
      path +=
        "?" +
        new URLSearchParams(
          Object.entries(query).map(([k, v]) => [k, String(v)]),
        ).toString();
    const deadline = performance.now() + this.options.overallTimeoutMs;
    let last = unknown(identity);
    for (let attempt = 0; attempt < this.options.maxAttempts; attempt++) {
      const remaining = deadline - performance.now();
      if (remaining <= 0) break;
      let delay = 0;
      const controller = new AbortController();
      const timer = setTimeout(
        () => controller.abort(),
        Math.min(this.options.attemptTimeoutMs, remaining),
      );
      try {
        const headers: Record<string, string> = {
          Authorization: `Bearer ${this.token}`,
          "Require-Durability-Profile": this.options.requireProfile,
          Accept: "application/json",
        };
        if (this.generation)
          headers["Require-Authority-Generation"] = this.generation;
        if (payload !== undefined) headers["Content-Type"] = "application/json";
        if (key) headers["Idempotency-Key"] = key;
        const response = await fetch(
          this.endpoints[attempt % this.endpoints.length] + path,
          {
            method,
            body: payload,
            headers,
            redirect: "error",
            signal: controller.signal,
          },
        );
        const generation = response.headers.get("Authority-Generation");
        if (!generation || !uuid7.test(generation)) throw unknown(identity);
        if (this.generation && generation !== this.generation)
          throw new ProblemError({
            code: "conflict",
            detail: "Endpoint belongs to a different authority generation.",
            retryable: false,
            transaction_id: identity ?? null,
          });
        this.generation = generation;
        const reader = response.body?.getReader();
        if (!reader) throw unknown(identity);
        const chunks: Uint8Array[] = [];
        let size = 0;
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            size += value.length;
            if (size > 16 * 1024 * 1024) throw unknown(identity);
            chunks.push(value);
          }
        } finally {
          await reader.cancel();
        }
        const bytes = new Uint8Array(size);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.length;
        }
        const value = JSON.parse(
          new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        );
        if ([200, 201, 202].includes(response.status)) {
          if (
            value.metadata.authority_generation !== generation ||
            !profiles.includes(value.metadata.durability_profile) ||
            profiles.indexOf(value.metadata.durability_profile) <
              profiles.indexOf(this.options.requireProfile)
          )
            throw unknown(identity);
          if (response.status !== 202) return value;
          last = unknown(identity);
        } else {
          if (value.status !== response.status) throw unknown(identity);
          if (value.transaction_id == null && identity !== undefined)
            value.transaction_id = identity;
          last = new ProblemError(value);
          if (![429, 503].includes(response.status) || value.retryable !== true)
            throw last;
        }
        const after = response.headers.get("Retry-After") ?? "0";
        delay = /^\d+$/.test(after) ? Number(after) * 1000 : 0;
      } catch (error) {
        if (
          error instanceof ProblemError &&
          !["deadline_exceeded", "unknown_outcome"].includes(
            String(error.problem.code),
          )
        )
          throw error;
        last = error instanceof ProblemError ? error : unknown(identity);
      } finally {
        clearTimeout(timer);
      }
      if (attempt + 1 === this.options.maxAttempts) break;
      const random = crypto.getRandomValues(new Uint32Array(1))[0]!;
      delay = Math.max(delay, 50 + (random % (50 << Math.min(attempt, 4))));
      if (delay >= deadline - performance.now()) break;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
    throw last;
  }
}

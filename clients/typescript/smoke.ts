/** Real-server lifecycle and replay; also the TypeScript usage example. */
import { Client, newId, ProblemError } from "./rescrow_client.ts";
// Node supplies process; the generated library itself uses browser-standard APIs.
declare const process: { env: Record<string, string | undefined> };
const client = new Client(
  process.env.RESCROW_ENDPOINT!,
  process.env.RESCROW_TOKEN!,
  { authorityGeneration: process.env.RESCROW_GENERATION! },
);
const resource = "typescript-credits";
await client.create_resource(resource, {
  lower: "0",
  upper: "100",
  initial: "60",
  unit: "credits",
  scale: 0,
});
const key = newId(); // Persist key and intent before I/O. Never replace on timeout.
const request = {
  operations: [{ operation_id: "op", resource_id: resource, delta: "-40" }],
  hold_ms: null,
  context: {},
};
const first = await client.prepare(request, key);
if (
  JSON.stringify((await client.prepare(request, key)).data) !==
  JSON.stringify(first.data)
)
  throw new Error("Replay changed");
const actuals = { actuals: [{ operation_id: "op", delta: "-25" }] };
await client.commit(key, actuals);
await client.commit(key, actuals);
if ((await client.get_resource(resource)).data.observation.quantity !== "35")
  throw new Error("Wrong quantity");
try {
  await client.commit(key, { actuals: [{ operation_id: "op", delta: "-24" }] });
  throw new Error("Changed actuals accepted");
} catch (error) {
  if (!(error instanceof ProblemError) || error.problem.code !== "conflict")
    throw error;
}
if (!(await client.list_reservations()).data.items.length)
  throw new Error("Missing reservations");
console.log(
  "TypeScript lifecycle, stable key, partial settlement and conflict passed",
);

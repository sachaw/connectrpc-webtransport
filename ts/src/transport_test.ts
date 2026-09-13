import { assertEquals, assertRejects } from "@std/assert";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import {
  createWebTransportServer,
  createWebTransportTransport,
  type LinkState,
} from "./mod.ts";
import type { Echo } from "../testing/echo_pb.ts";
import { echo, EchoService, seen } from "../testing/echo_service.ts";
import { memorySessionPair } from "../testing/memory.ts";

const serve = createWebTransportServer({
  routes: (r) => r.service(EchoService, echo),
});

function session(): WebTransport {
  const pair = memorySessionPair();
  void serve(pair.server);
  return pair.client;
}

Deno.test("unary declares content-length and speaks Connect", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: session() }),
  );
  assertEquals((await client.unary({ text: "hi" })).text, "hi");
  assertEquals(seen!.requestMethod, "POST");
  assertEquals(seen!.requestHeader.get("content-type"), "application/proto");
  assertEquals(seen!.requestHeader.get("connect-protocol-version"), "1");
  assertEquals(seen!.requestHeader.get("content-length"), "4");
});

Deno.test("unary error carries the Connect code", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: session() }),
  );
  const err = await assertRejects(
    () => client.fail({ text: "x" }),
    ConnectError,
  );
  assertEquals(err.code, Code.NotFound);
  assertEquals(err.rawMessage, "no such thing");
});

Deno.test("an unknown path is Unimplemented", async () => {
  const empty = createWebTransportServer({ routes: () => {} });
  const pair = memorySessionPair();
  void empty(pair.server);
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: pair.client }),
  );
  const err = await assertRejects(
    () => client.unary({ text: "x" }),
    ConnectError,
  );
  assertEquals(err.code, Code.Unimplemented);
});

Deno.test("server stream declares content-length and yields trailers", async () => {
  const transport = createWebTransportTransport({ session: session() });
  const res = await transport.stream(
    EchoService.method.serverStream,
    undefined,
    undefined,
    undefined,
    (async function* () {
      yield { text: "s" };
    })(),
  );
  assertEquals((await Array.fromAsync(res.message)).map((m) => m.text), [
    "s",
    "s",
    "s",
  ]);
  assertEquals(res.trailer.get("x-echo-trailer"), "yes");
  assertEquals(seen!.requestHeader.get("content-length"), "8");
});

Deno.test("bidi streams both ways as a chunked request", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: session() }),
  );
  const texts: string[] = [];
  for await (
    const m of client.bidi((async function* () {
      yield { text: "a" };
      yield { text: "b" };
    })())
  ) texts.push(m.text);
  assertEquals(texts, ["a", "b"]);
  assertEquals(seen!.requestHeader.has("content-length"), false);
  assertEquals(seen!.requestHeader.get("transfer-encoding"), "chunked");
});

Deno.test("a failing bidi input surfaces on the call, not as an unhandled rejection", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: session() }),
  );
  const err = await assertRejects(async () => {
    for await (
      const _ of client.bidi((async function* () {
        yield { text: "a" };
        throw new Error("input broke");
      })())
    ) { /* drain */ }
  }, ConnectError);
  assertEquals(err.rawMessage, "input broke");
});

Deno.test("a session that cannot open is Unavailable", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({
      session: () => Promise.reject(new TypeError("Failed to fetch")),
      backoff: { initialMs: 1, maxMs: 1 },
    }),
  );
  const err = await assertRejects(
    () => client.unary({ text: "x" }),
    ConnectError,
  );
  assertEquals(err.code, Code.Unavailable);
});

Deno.test("deadline aborts a call the server never answers", async () => {
  const stuck = createWebTransportServer({
    routes: (router) =>
      router.service(EchoService, { unary: () => new Promise<Echo>(() => {}) }),
  });
  const pair = memorySessionPair();
  void stuck(pair.server);
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: pair.client }),
  );
  const err = await assertRejects(
    () => client.unary({ text: "x" }, { timeoutMs: 20 }),
    ConnectError,
  );
  assertEquals(err.code, Code.DeadlineExceeded);
});

Deno.test("factory sessions open lazily, share one dial, back off, and reconnect", async () => {
  const states: LinkState[] = [];
  let opens = 0;
  let pair = memorySessionPair();
  void serve(pair.server);
  const transport = createWebTransportTransport({
    session: () => {
      opens += 1;
      if (opens < 3) return Promise.reject(new Error("refused"));
      return Promise.resolve(pair.client);
    },
    backoff: { initialMs: 1, maxMs: 2 },
    onState: (s) => states.push(s),
  });
  const client = createClient(EchoService, transport);
  assertEquals(opens, 0);

  await Promise.all([
    assertRejects(() => client.unary({ text: "1" })),
    assertRejects(() => client.unary({ text: "2" })),
  ]);
  assertEquals(opens, 1);
  await assertRejects(() => client.unary({ text: "3" }));
  assertEquals((await client.unary({ text: "4" })).text, "4");
  assertEquals(opens, 3);
  assertEquals(states.map((s) => [s.phase, s.failures]), [
    ["connecting", 1],
    ["connecting", 2],
    ["connected", 0],
  ]);

  pair.client.close();
  await pair.client.closed;
  pair = memorySessionPair();
  void serve(pair.server);
  assertEquals((await client.unary({ text: "5" })).text, "5");
  assertEquals(opens, 4);
  assertEquals(states.slice(3).map((s) => s.phase), [
    "reconnecting",
    "connected",
  ]);
});

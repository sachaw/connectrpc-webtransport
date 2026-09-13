import { assert, assertEquals, assertRejects } from "@std/assert";
import {
  Code,
  ConnectError,
  createClient,
  createContextKey,
  createContextValues,
} from "@connectrpc/connect";
import {
  compressionGzip,
  createWebTransportServer,
  createWebTransportTransport,
  type LinkState,
} from "../src/mod.ts";
import type { Echo } from "./support/echo_pb.ts";
import { echo, EchoService, seen } from "./support/echo_service.ts";
import { memorySessionPair } from "./support/memory.ts";

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

Deno.test("aborting a call aborts the handler's signal", async () => {
  const aborted = Promise.withResolvers<void>();
  const hanging = createWebTransportServer({
    routes: (router) =>
      router.service(EchoService, {
        async *serverStream(req: Echo, ctx) {
          yield { text: req.text };
          await new Promise<void>((resolve) =>
            ctx.signal.addEventListener("abort", () => resolve())
          );
          aborted.resolve();
        },
      }),
  });
  const pair = memorySessionPair();
  void hanging(pair.server);
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: pair.client }),
  );
  const controller = new AbortController();
  for await (
    const m of client.serverStream({ text: "x" }, { signal: controller.signal })
  ) {
    assertEquals(m.text, "x");
    controller.abort();
    break;
  }
  await aborted.promise;
});

Deno.test("the signal closes the session and stops redialling", async () => {
  const pair = memorySessionPair();
  void serve(pair.server);
  const controller = new AbortController();
  const states: LinkState[] = [];
  let opens = 0;
  const client = createClient(
    EchoService,
    createWebTransportTransport({
      session: () => {
        opens += 1;
        return Promise.resolve(pair.client);
      },
      signal: controller.signal,
      onState: (s) => states.push(s),
    }),
  );
  assertEquals((await client.unary({ text: "1" })).text, "1");
  controller.abort();
  await pair.client.closed;
  const err = await assertRejects(
    () => client.unary({ text: "2" }),
    ConnectError,
  );
  assertEquals(err.code, Code.Canceled);
  assertEquals(opens, 1);
  assertEquals(states.map((s) => s.phase), ["connected"]);
});

Deno.test("context values reach every call on a session", async () => {
  const peer = createContextKey("nobody");
  const identified = createWebTransportServer({
    contextValues: () => createContextValues().set(peer, "node-7"),
    routes: (router) =>
      router.service(EchoService, {
        unary(req: Echo, ctx) {
          ctx.responseHeader.set("x-peer", ctx.values.get(peer));
          return { text: req.text };
        },
      }),
  });
  const pair = memorySessionPair();
  void identified(pair.server);
  const transport = createWebTransportTransport({ session: pair.client });
  const res = await transport.unary(
    EchoService.method.unary,
    undefined,
    undefined,
    undefined,
    { text: "x" },
  );
  assertEquals(res.header.get("x-peer"), "node-7");
});

Deno.test("compression both ways", async () => {
  const compressed = createWebTransportServer({
    acceptCompression: [compressionGzip],
    compressMinBytes: 0,
    routes: (router) => router.service(EchoService, echo),
  });
  const pair = memorySessionPair();
  void compressed(pair.server);
  const client = createClient(
    EchoService,
    createWebTransportTransport({
      session: pair.client,
      acceptCompression: [compressionGzip],
      sendCompression: compressionGzip,
      compressMinBytes: 0,
    }),
  );
  const text = "x".repeat(10_000);
  assertEquals((await client.unary({ text })).text, text);
  assertEquals(seen!.requestHeader.get("content-encoding"), "gzip");
  assert(Number(seen!.requestHeader.get("content-length")) < 1000);
  const texts: string[] = [];
  for await (const m of client.serverStream({ text })) texts.push(m.text);
  assertEquals(texts, [text, text, text]);
  assertEquals(seen!.requestHeader.get("connect-content-encoding"), "gzip");
});

Deno.test("sendOrder sets the stream's send priority", async () => {
  const pair = memorySessionPair();
  void serve(pair.server);
  const client = createClient(
    EchoService,
    createWebTransportTransport({
      session: pair.client,
      sendOrder: (method) => method.name === "Unary" ? 10 : 1,
    }),
  );
  await client.unary({ text: "x" });
  for await (const _ of client.serverStream({ text: "x" })) { /* drain */ }
  assertEquals(pair.sendOrders, [10, 1]);
});

Deno.test("onSession sees sessions open and close", async () => {
  const events: string[] = [];
  const observed = createWebTransportServer({
    routes: (router) => router.service(EchoService, echo),
    onSession: (_, event) => events.push(event),
  });
  const pair = memorySessionPair();
  void observed(pair.server);
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: pair.client }),
  );
  await client.unary({ text: "x" });
  pair.client.close();
  await pair.client.closed;
  await new Promise((r) => setTimeout(r, 0));
  assertEquals(events, ["open", "close"]);
});

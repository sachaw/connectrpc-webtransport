import { assertEquals, assertRejects } from "@std/assert";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { create, toBinary } from "@bufbuild/protobuf";
import { createWebTransportTransport, type LinkState } from "./mod.ts";
import { EchoSchema, EchoService } from "../testing/echo_pb.ts";
import {
  collect,
  envelope,
  envelopes,
  fakeSession,
  type Handler,
} from "../testing/fake.ts";

const END = 0b10;
const encode = (text: string) =>
  toBinary(EchoSchema, create(EchoSchema, { text }));

const echo: Handler = async (req) => {
  switch (req.path) {
    case "/echo.EchoService/Unary":
      return {
        status: 200,
        headers: req.headers,
        body: [await collect(req.body)],
      };
    case "/echo.EchoService/Fail":
      return {
        status: 404,
        headers: { "content-type": "application/json" },
        body: [
          new TextEncoder().encode(
            '{"code":"not_found","message":"no such thing"}',
          ),
        ],
      };
    case "/echo.EchoService/ServerStream": {
      const [message] = await Array.fromAsync(envelopes(req.body));
      return {
        status: 200,
        headers: { "content-type": req.headers.get("content-type")! },
        body: [
          envelope(0, message),
          envelope(0, message),
          envelope(0, message),
          envelope(END, '{"metadata":{"x-trailer":["yes"]}}'),
        ],
      };
    }
    case "/echo.EchoService/Bidi":
      return {
        status: 200,
        headers: { "content-type": req.headers.get("content-type")! },
        body: (async function* () {
          for await (const message of envelopes(req.body)) {
            yield envelope(0, message);
          }
          yield envelope(END, "{}");
        })(),
      };
    default:
      throw new Error(`unexpected path ${req.path}`);
  }
};

Deno.test("unary declares content-length and speaks Connect", async () => {
  let seen: Headers | undefined;
  let seenMethod: string | undefined;
  const session = fakeSession((req) => {
    seen = req.headers;
    seenMethod = req.method;
    return echo(req);
  });
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session }),
  );
  const res = await client.unary({ text: "hi" });
  assertEquals(res.text, "hi");
  assertEquals(seenMethod, "POST");
  assertEquals(seen!.get("content-type"), "application/proto");
  assertEquals(seen!.get("connect-protocol-version"), "1");
  assertEquals(seen!.get("content-length"), String(encode("hi").byteLength));
});

Deno.test("unary error carries the Connect code", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: fakeSession(echo) }),
  );
  const err = await assertRejects(
    () => client.fail({ text: "x" }),
    ConnectError,
  );
  assertEquals(err.code, Code.NotFound);
  assertEquals(err.rawMessage, "no such thing");
});

Deno.test("server stream declares content-length and yields trailers", async () => {
  let seen: Headers | undefined;
  const session = fakeSession((req) => {
    seen = req.headers;
    return echo(req);
  });
  const transport = createWebTransportTransport({ session });
  const res = await transport.stream(
    EchoService.method.serverStream,
    undefined,
    undefined,
    undefined,
    (async function* () {
      yield { text: "s" };
    })(),
  );
  const texts = (await Array.fromAsync(res.message)).map((m) => m.text);
  assertEquals(texts, ["s", "s", "s"]);
  assertEquals(res.trailer.get("x-trailer"), "yes");
  assertEquals(
    seen!.get("content-length"),
    String(envelope(0, encode("s")).byteLength),
  );
});

Deno.test("bidi streams both ways as a chunked request", async () => {
  let seen: Headers | undefined;
  const session = fakeSession((req) => {
    seen = req.headers;
    return echo(req);
  });
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session }),
  );
  const texts: string[] = [];
  for await (
    const m of client.bidi((async function* () {
      yield { text: "a" };
      yield { text: "b" };
    })())
  ) texts.push(m.text);
  assertEquals(texts, ["a", "b"]);
  assertEquals(seen!.has("content-length"), false);
  assertEquals(seen!.get("transfer-encoding"), "chunked");
});

Deno.test("a failing bidi input surfaces on the call, not as an unhandled rejection", async () => {
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session: fakeSession(echo) }),
  );
  const boom = new Error("input broke");
  const err = await assertRejects(async () => {
    for await (
      const _ of client.bidi((async function* () {
        yield { text: "a" };
        throw boom;
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
  const session = fakeSession(() => new Promise(() => {}));
  const client = createClient(
    EchoService,
    createWebTransportTransport({ session }),
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
  let session = fakeSession(echo);
  const transport = createWebTransportTransport({
    session: () => {
      opens += 1;
      if (opens < 3) return Promise.reject(new Error("refused"));
      return Promise.resolve(session);
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

  session.closeNow();
  await session.closed;
  session = fakeSession(echo);
  assertEquals((await client.unary({ text: "5" })).text, "5");
  assertEquals(opens, 4);
  assertEquals(states.slice(3).map((s) => s.phase), [
    "reconnecting",
    "connected",
  ]);
});

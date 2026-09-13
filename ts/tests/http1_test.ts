import { assertEquals, assertRejects } from "@std/assert";
import { Code, ConnectError } from "@connectrpc/connect";
import { Http1Reader, MAX_HEAD_BYTES } from "../src/http1.ts";

const encoder = new TextEncoder();

function reader(...chunks: (string | Uint8Array)[]): Http1Reader {
  const bytes = chunks.map((c) =>
    typeof c === "string" ? encoder.encode(c) : c
  );
  return new Http1Reader(ReadableStream.from(bytes).getReader());
}

async function collect(body: AsyncIterable<Uint8Array>): Promise<string> {
  let out = "";
  for await (const chunk of body) out += new TextDecoder().decode(chunk);
  return out;
}

Deno.test("head split across chunks, then a content-length body", async () => {
  const r = reader(
    "POST /x HTTP/1.1\r\ncontent-len",
    "gth: 5\r\nx: a\r\nx: b\r\n\r\nhel",
    "lo",
  );
  const head = (await r.head())!;
  assertEquals(head.startLine, "POST /x HTTP/1.1");
  assertEquals(head.headers.get("x"), "a, b");
  assertEquals(await collect(r.body(head.headers, "request")), "hello");
});

Deno.test("chunked body with extensions and trailers", async () => {
  const r = reader(
    "HTTP/1.1 200 \r\ntransfer-encoding: chunked\r\n\r\n3;ext=1\r\nabc\r\n2\r\nde\r\n0\r\nx-t: 1\r\n\r\n",
  );
  const head = (await r.head())!;
  assertEquals(await collect(r.body(head.headers, "response")), "abcde");
});

Deno.test("no bytes at all is null", async () => {
  assertEquals(await reader().head(), null);
});

const rejected: [string, string, Code][] = [
  [
    "header without a colon",
    "POST /x HTTP/1.1\r\ngarbage\r\n\r\n",
    Code.Internal,
  ],
  [
    "header with an invalid name",
    "POST /x HTTP/1.1\r\nbad name: 1\r\n\r\n",
    Code.Internal,
  ],
  [
    "stream ending inside the head",
    "POST /x HTTP/1.1\r\ncontent-length: 5\r\n",
    Code.Unavailable,
  ],
];
for (const [name, input, code] of rejected) {
  Deno.test(`rejects ${name}`, async () => {
    const err = await assertRejects(() => reader(input).head(), ConnectError);
    assertEquals(err.code, code);
  });
}

Deno.test("rejects an oversized head", async () => {
  const err = await assertRejects(
    () =>
      reader("POST /x HTTP/1.1\r\n", "x: " + "a".repeat(MAX_HEAD_BYTES + 1))
        .head(),
    ConnectError,
  );
  assertEquals(err.code, Code.ResourceExhausted);
});

const badBodies: [string, string, Code][] = [
  [
    "a non-numeric content-length",
    "content-length: abc\r\n\r\n",
    Code.Internal,
  ],
  ["a negative content-length", "content-length: -1\r\n\r\n", Code.Internal],
  [
    "a short content-length body",
    "content-length: 5\r\n\r\nhel",
    Code.Unavailable,
  ],
  [
    "a negative chunk size",
    "transfer-encoding: chunked\r\n\r\n-5\r\nhello\r\n0\r\n\r\n",
    Code.Internal,
  ],
  [
    "a non-hex chunk size",
    "transfer-encoding: chunked\r\n\r\nzz\r\n",
    Code.Internal,
  ],
  [
    "a truncated chunked body",
    "transfer-encoding: chunked\r\n\r\n5\r\nhel",
    Code.Unavailable,
  ],
];
for (const [name, input, code] of badBodies) {
  Deno.test(`rejects ${name}`, async () => {
    const r = reader("POST /x HTTP/1.1\r\n" + input);
    const head = (await r.head())!;
    const err = await assertRejects(
      () => collect(r.body(head.headers, "request")),
      ConnectError,
    );
    assertEquals(err.code, code);
  });
}

Deno.test("random bytes never produce anything but a head or a ConnectError", async () => {
  for (let i = 0; i < 500; i++) {
    const bytes = crypto.getRandomValues(
      new Uint8Array(1 + Math.floor(Math.random() * 512)),
    );
    if (Math.random() < 0.5 && bytes.byteLength > 4) {
      bytes.set(
        encoder.encode("\r\n\r\n"),
        Math.floor(Math.random() * (bytes.byteLength - 4)),
      );
    }
    const r = reader(bytes);
    try {
      const head = await r.head();
      if (head) await collect(r.body(head.headers, "request"));
    } catch (e) {
      if (!(e instanceof ConnectError)) throw e;
    }
  }
});

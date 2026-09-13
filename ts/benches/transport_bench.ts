// The TypeScript client against the TypeScript server over in-memory streams.

import { createClient } from "@connectrpc/connect";
import {
  createWebTransportServer,
  createWebTransportTransport,
} from "../src/mod.ts";
import { echo, EchoService } from "../tests/support/echo_service.ts";
import { memorySessionPair } from "../tests/support/memory.ts";

const serve = createWebTransportServer({
  routes: (r) => r.service(EchoService, echo),
});
const pair = memorySessionPair();
void serve(pair.server);
const client = createClient(
  EchoService,
  createWebTransportTransport({ session: pair.client }),
);
const text = "x".repeat(100);
const big = "x".repeat(64 * 1024);

Deno.bench("unary, 100 B", async () => {
  await client.unary({ text });
});

Deno.bench("unary x16 concurrent, 100 B", async () => {
  await Promise.all(Array.from({ length: 16 }, () => client.unary({ text })));
});

Deno.bench("bidi, 16 x 64 KiB", async () => {
  for await (
    const _ of client.bidi((async function* () {
      for (let i = 0; i < 16; i++) yield { text: big };
    })())
  ) { /* drain */ }
});

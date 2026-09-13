// Driven by the crate's `conformance` test.

import { assertEquals, assertRejects } from "@std/assert";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createWebTransportTransport } from "../src/mod.ts";
import { EchoService } from "./support/echo_pb.ts";

const port = Deno.env.get("ECHO_PORT");
const hash = Deno.env.get("ECHO_HASH");

Deno.test(
  { name: "conformance against the Rust server", ignore: !port },
  async (t) => {
    const transport = createWebTransportTransport({
      session: async () => {
        const wt = new WebTransport(`https://127.0.0.1:${port}/echo`, {
          serverCertificateHashes: [{
            algorithm: "sha-256",
            value: new Uint8Array(hash!.split(":").map((b) => parseInt(b, 16))),
          }],
        });
        await wt.ready;
        return wt;
      },
    });
    const client = createClient(EchoService, transport);

    await t.step("unary", async () => {
      assertEquals((await client.unary({ text: "hello" })).text, "hello");
    });

    await t.step("unary error", async () => {
      const err = await assertRejects(
        () => client.fail({ text: "x" }),
        ConnectError,
      );
      assertEquals(err.code, Code.NotFound);
      assertEquals(err.rawMessage, "no such thing");
    });

    await t.step("server stream with trailer", async () => {
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
    });

    await t.step("bidi", async () => {
      const texts: string[] = [];
      for await (
        const m of client.bidi((async function* () {
          yield { text: "a" };
          yield { text: "b" };
        })())
      ) texts.push(m.text);
      assertEquals(texts, ["a", "b"]);
    });
  },
);

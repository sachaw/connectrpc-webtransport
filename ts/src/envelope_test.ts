import { assert, assertEquals, assertRejects } from "@std/assert";
import { createAsyncIterable } from "@connectrpc/connect/protocol";
import { splitEnvelopes } from "./envelope.ts";

function enveloped(flags: number, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + data.byteLength);
  out[0] = flags;
  new DataView(out.buffer).setUint32(1, data.byteLength);
  out.set(data, 5);
  return out;
}

async function all<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of it) out.push(x);
  return out;
}

Deno.test("envelopes split at any chunk boundary, the empty one too", async () => {
  const a = enveloped(0, Uint8Array.of(1, 2, 3));
  const b = enveloped(2, new Uint8Array());
  const c = enveloped(0, Uint8Array.of(9));
  const bytes = new Uint8Array([...a, ...b, ...c]);
  for (let size = 1; size <= bytes.length; size++) {
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < bytes.length; i += size) {
      chunks.push(bytes.subarray(i, i + size));
    }
    const got = await all(splitEnvelopes(1 << 20)(createAsyncIterable(chunks)));
    assertEquals(got.map((e) => [e.flags, [...e.data]]), [[0, [1, 2, 3]], [
      2,
      [],
    ], [0, [9]]]);
  }
});

Deno.test("an envelope cut short, or larger than allowed, is refused", async () => {
  const a = enveloped(0, Uint8Array.of(1, 2, 3));
  await assertRejects(() =>
    all(splitEnvelopes(1 << 20)(createAsyncIterable([a.subarray(0, 6)])))
  );
  await assertRejects(() => all(splitEnvelopes(2)(createAsyncIterable([a]))));
});

Deno.test("a message of megabytes in small chunks splits in linear time", async () => {
  const data = new Uint8Array(16 << 20);
  const bytes = enveloped(0, data);
  const chunks: Uint8Array[] = [];
  for (let i = 0; i < bytes.length; i += 1200) {
    chunks.push(bytes.subarray(i, i + 1200));
  }
  const t0 = performance.now();
  const got = await all(splitEnvelopes(32 << 20)(createAsyncIterable(chunks)));
  const ms = performance.now() - t0;
  assertEquals(got[0].data.byteLength, 16 << 20);
  assert(ms < 250, `${ms.toFixed(0)} ms for ${chunks.length} chunks`);
});

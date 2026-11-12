import { Code, ConnectError } from "@connectrpc/connect";
import type { EnvelopedMessage } from "@connectrpc/connect/protocol";

/**
 * The envelopes of a body, in linear time however finely it is chunked: a
 * WebTransport stream reads a message of megabytes as thousands of chunks,
 * which Connect's own splitter rescans in full for each one.
 */
export function splitEnvelopes(readMaxBytes: number) {
  return async function* (
    chunks: AsyncIterable<Uint8Array>,
  ): AsyncIterable<EnvelopedMessage> {
    const head = new Uint8Array(5);
    let headAt = 0;
    let env: EnvelopedMessage | undefined;
    let at = 0;
    for await (let chunk of chunks) {
      while (chunk.byteLength > 0) {
        if (!env) {
          const n = Math.min(5 - headAt, chunk.byteLength);
          head.set(chunk.subarray(0, n), headAt);
          headAt += n;
          chunk = chunk.subarray(n);
          if (headAt < 5) break;
          const length = new DataView(head.buffer).getUint32(1);
          if (length > readMaxBytes) {
            throw new ConnectError(
              `message size ${length} is larger than configured readMaxBytes ${readMaxBytes}`,
              Code.ResourceExhausted,
            );
          }
          env = { flags: head[0], data: new Uint8Array(length) };
          at = 0;
          headAt = 0;
        }
        const n = Math.min(env.data.byteLength - at, chunk.byteLength);
        env.data.set(chunk.subarray(0, n), at);
        at += n;
        chunk = chunk.subarray(n);
        if (at === env.data.byteLength) {
          yield env;
          env = undefined;
        }
      }
    }
    if (env || headAt > 0) {
      throw new ConnectError(
        "protocol error: incomplete envelope",
        Code.InvalidArgument,
      );
    }
  };
}

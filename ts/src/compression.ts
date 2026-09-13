import { Code, ConnectError } from "@connectrpc/connect";
import type { Compression } from "@connectrpc/connect/protocol";

/** gzip via the runtime's `CompressionStream`. */
export const compressionGzip: Compression = {
  name: "gzip",
  compress: (bytes) => run(new CompressionStream("gzip"), bytes, Infinity),
  decompress: (bytes, readMaxBytes) =>
    run(new DecompressionStream("gzip"), bytes, readMaxBytes),
};

async function run(
  stream: GenericTransformStream,
  bytes: Uint8Array,
  readMaxBytes: number,
): Promise<Uint8Array<ArrayBuffer>> {
  const chunks: Uint8Array[] = [];
  let total = 0;
  await ReadableStream.from([bytes]).pipeThrough(stream).pipeTo(
    new WritableStream({
      write(chunk) {
        total += chunk.byteLength;
        if (total > readMaxBytes) {
          throw new ConnectError(
            `message is larger than configured readMaxBytes ${readMaxBytes} after decompression`,
            Code.ResourceExhausted,
          );
        }
        chunks.push(chunk);
      },
    }),
  );
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

import { Code, ConnectError } from "@connectrpc/connect";

export const MAX_HEAD_BYTES = 1 << 20;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const LAST_CHUNK = encoder.encode("0\r\n\r\n");

export function encodeHead(startLine: string, headers: Headers): Uint8Array {
  let head = `${startLine}\r\n`;
  for (const [name, value] of headers) head += `${name}: ${value}\r\n`;
  return encoder.encode(head + "\r\n");
}

export function encodeChunk(data: Uint8Array): Uint8Array {
  const size = encoder.encode(`${data.byteLength.toString(16)}\r\n`);
  const out = new Uint8Array(size.byteLength + data.byteLength + 2);
  out.set(size);
  out.set(data, size.byteLength);
  out.set([13, 10], size.byteLength + data.byteLength);
  return out;
}

export interface Head {
  startLine: string;
  headers: Headers;
}

/** Reads one HTTP/1.1 message off a byte stream. */
export class Http1Reader {
  #reader: ReadableStreamDefaultReader<Uint8Array>;
  #buffered: Uint8Array = new Uint8Array(0);

  constructor(reader: ReadableStreamDefaultReader<Uint8Array>) {
    this.#reader = reader;
  }

  cancel(reason?: unknown): Promise<void> {
    return this.#reader.cancel(reason);
  }

  async #fill(): Promise<boolean> {
    const { value, done } = await this.#reader.read();
    if (done) return false;
    this.#buffered = concat(this.#buffered, value);
    return true;
  }

  /** Resolves null if the stream ends before any byte arrives. */
  async head(): Promise<Head | null> {
    let end: number;
    while ((end = indexOfHeadEnd(this.#buffered)) < 0) {
      if (this.#buffered.byteLength > MAX_HEAD_BYTES) {
        throw new ConnectError(
          "head exceeds the size limit",
          Code.ResourceExhausted,
        );
      }
      if (!(await this.#fill())) {
        if (this.#buffered.byteLength === 0) return null;
        throw new ConnectError(
          "stream ended inside the head",
          Code.Unavailable,
        );
      }
    }
    const [startLine, ...lines] = decoder.decode(
      this.#buffered.subarray(0, end),
    ).split("\r\n");
    this.#buffered = this.#buffered.subarray(end + 4);
    const headers = new Headers();
    for (const line of lines) {
      const colon = line.indexOf(":");
      headers.append(line.slice(0, colon).trim(), line.slice(colon + 1).trim());
    }
    return { startLine, headers };
  }

  /** The body the head describes; without a length or chunking it is empty for requests and runs to the end for responses. */
  body(
    headers: Headers,
    kind: "request" | "response",
  ): AsyncIterable<Uint8Array> {
    if (headers.get("transfer-encoding")?.toLowerCase() === "chunked") {
      return this.#chunked();
    }
    const length = headers.get("content-length");
    if (length !== null) return this.#take(Number(length));
    return kind === "response" ? this.#take(Infinity) : this.#take(0);
  }

  async *#take(limit: number): AsyncIterable<Uint8Array> {
    while (limit > 0) {
      if (this.#buffered.byteLength === 0 && !(await this.#fill())) {
        if (limit === Infinity) return;
        throw new ConnectError(
          "stream ended inside the body",
          Code.Unavailable,
        );
      }
      const chunk = this.#buffered.subarray(
        0,
        Math.min(this.#buffered.byteLength, limit),
      );
      this.#buffered = this.#buffered.subarray(chunk.byteLength);
      limit -= chunk.byteLength;
      if (chunk.byteLength > 0) yield chunk;
    }
  }

  async #line(): Promise<string> {
    let i: number;
    while ((i = this.#buffered.indexOf(10)) < 0) {
      if (!(await this.#fill())) {
        throw new ConnectError(
          "stream ended inside a chunked body",
          Code.Unavailable,
        );
      }
    }
    const text = decoder.decode(this.#buffered.subarray(0, i)).trim();
    this.#buffered = this.#buffered.subarray(i + 1);
    return text;
  }

  // RFC 9112 §7.1; trailers are dropped (Connect does not use them).
  async *#chunked(): AsyncIterable<Uint8Array> {
    for (;;) {
      const size = parseInt(await this.#line(), 16);
      if (!Number.isInteger(size)) {
        throw new ConnectError("invalid chunk size", Code.Internal);
      }
      if (size === 0) break;
      yield* this.#take(size);
      await this.#line();
    }
    while ((await this.#line()) !== "");
  }
}

function indexOfHeadEnd(buf: Uint8Array): number {
  for (let i = 0; i + 3 < buf.byteLength; i++) {
    if (
      buf[i] === 13 && buf[i + 1] === 10 && buf[i + 2] === 13 &&
      buf[i + 3] === 10
    ) return i;
  }
  return -1;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a);
  out.set(b, a.byteLength);
  return out;
}

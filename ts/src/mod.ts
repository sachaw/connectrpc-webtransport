/**
 * A {@link https://connectrpc.com | Connect} `Transport` over WebTransport, giving browsers client-streaming and bidi RPCs.
 *
 * ```ts
 * import { createClient } from "@connectrpc/connect";
 * import { createWebTransportTransport } from "@sachaw/connectrpc-webtransport";
 *
 * const transport = createWebTransportTransport({
 *   session: async () => {
 *     const wt = new WebTransport("https://node.example:4433/connect");
 *     await wt.ready;
 *     return wt;
 *   },
 * });
 * const client = createClient(MyService, transport);
 * ```
 *
 * @module
 */

export { createWebTransportTransport } from "./transport.ts";
export type {
  LinkPhase,
  LinkState,
  WebTransportTransportOptions,
} from "./transport.ts";
export { createWebTransportServer } from "./server.ts";
export type {
  WebTransportServer,
  WebTransportServerOptions,
} from "./server.ts";

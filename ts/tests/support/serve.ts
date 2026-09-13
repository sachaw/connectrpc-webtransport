// Serves the echo service on a QUIC listener and prints its port.

import { createWebTransportServer } from "../../src/mod.ts";
import { echo, EchoService } from "./echo_service.ts";

const serve = createWebTransportServer({
  routes: (r) => r.service(EchoService, echo),
});
const endpoint = new Deno.QuicEndpoint({ hostname: "127.0.0.1", port: 0 });
const listener = endpoint.listen({
  cert: Deno.env.get("ECHO_CERT")!,
  key: Deno.env.get("ECHO_KEY")!,
  alpnProtocols: ["h3"],
  // denoland/deno#36822.
  maxConcurrentBidirectionalStreams: 1 << 20,
});
console.log(endpoint.addr.port);
for await (const incoming of listener) {
  void Deno.upgradeWebTransport(await incoming.accept()).then(serve);
}

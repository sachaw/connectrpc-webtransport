import { Code, ConnectError, type HandlerContext } from "@connectrpc/connect";
import { type Echo, EchoService } from "./echo_pb.ts";

/** The most recent handler context, for asserting what reached the server. */
export let seen: HandlerContext | undefined;

export const echo = {
  unary(req: Echo, ctx: HandlerContext) {
    seen = ctx;
    return { text: req.text };
  },
  fail() {
    throw new ConnectError("no such thing", Code.NotFound);
  },
  async *serverStream(req: Echo, ctx: HandlerContext) {
    seen = ctx;
    ctx.responseTrailer.set("x-echo-trailer", "yes");
    for (let i = 0; i < 3; i++) yield { text: req.text };
  },
  async *bidi(reqs: AsyncIterable<Echo>, ctx: HandlerContext) {
    seen = ctx;
    for await (const req of reqs) yield { text: req.text };
  },
};

export { EchoService };

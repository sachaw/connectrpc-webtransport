# Connect over WebTransport

One WebTransport bidirectional stream carries one HTTP/1.1 exchange
([RFC 9112](https://www.rfc-editor.org/rfc/rfc9112)): a request, then a
response. The body is the Connect body for the call, exactly as over HTTP.

```text
POST /pkg.Service/Method HTTP/1.1
content-type: application/proto
content-length: 42

<body>
```
```text
HTTP/1.1 200 OK
content-type: application/proto

<body>
```

## Message framing

- **Request body**: `content-length` when known (unary and server-streaming
  calls), otherwise `transfer-encoding: chunked`; neither means no body. The
  server never waits for the stream's FIN, so a client that cannot send one
  still works.
- **Response body**: `content-length` or `transfer-encoding: chunked`; a
  response also ends at FIN.
- `Host` is not required. Heads are bounded (400 KiB server, 1 MiB client)
  and the server times out a head not received within 30 s.

The server may stop reading the request stream (`STOP_SENDING`) once it has
what it needs. A client must treat that as "read the response", not as a
failure.

## Trailers

Not used: Connect carries the unary status in the head and the streaming
status in the end-of-stream envelope.

Codecs, compression, timeouts and errors are Connect's own and ride in
headers and bodies unchanged.

## Session

The session URL's path is a deployment choice; the server accepts any. Browsers
require the server certificate to be publicly trusted or pinned with
`serverCertificateHashes`, and a pinned certificate must be valid for at most
14 days.

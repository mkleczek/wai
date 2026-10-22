# Warp

Warp is a server library for HTTP/1.x and HTTP/2 based WAI(Web Application Interface in Haskell). For more information, see [Warp](http://www.aosabook.org/en/posa/warp.html).

## HTTP/1.1 operation streams

`Network.Wai.Handler.Warp.Streaming.runHTTP1` and `runHTTP1Socket` run
`Network.Wai.Streaming.Application` with shared `streaming-io` resources:

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

import Control.Concurrent.Resource.Acquisition (newContext)
import GHC.Event (getSystemTimerManager)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Network.Wai.Handler.Warp (defaultSettings)
import qualified Network.Wai.Handler.Warp.Streaming as Warp
import qualified Network.Wai.Streaming as Streaming
import Network.HTTP.Types (status200)

main = do
    acquisition <- newContext <$> getSystemTimerManager
    resources <- newBufferPool acquisition PoolSettings
        { bufferSize = positiveInt @16384
        , ordinaryBufferCount = positiveInt @63
        , reserveBufferCount = positiveInt @1
        , setupBufferCount = positiveInt @8
        }
    Warp.runHTTP1 defaultSettings resources $ \_ exchange ->
        Streaming.withBody exchange $ \source ->
            Streaming.respond exchange status200 [] (Streaming.forward source)
```

`PoolSettings` specifies a block size and positive counts for ordinary,
reserved and setup blocks. This example allocates 64 body blocks (63 ordinary
plus one reserve) and eight setup blocks, each 16 KiB.

The example requires a direct dependency on `resource-admission` for
`newContext`. The application supplies GHC's shared timer manager; the context
has no shutdown action. `newBufferPool` allocates storage retained by its
resources and outstanding requests; the garbage collector reclaims unreachable
storage. Applications own stopping, cancelling and joining their users. Retained
resources remain usable after a startup callback returns or throws. Buffer loans
and access capabilities still expire when their `withRequestBuffers` scope ends.

The handler receives a streaming request and an opaque exchange. Use
`requestMetadata` for ordinary WAI metadata and `requestBuffers` for the shared
`BufferAccess` capability. Owned WAI request-body reads are unavailable.

Warp admits one primary buffer after request activity begins, before parsing
HTTP metadata. Every request, including GET and preflight failures, keeps that
primary until its application and response finish. Idle keep-alive connections
hold no body-pool lease. `reserveBufferCount` protects capacity from primary
admission so an admitted request can borrow one additional buffer. Applications
pass the same capability to downstream consumers such as a database operation.
A third overlapping or pending loan fails promptly.

Primary-buffer admission uses Warp's configured connection timeout. If it
expires before admission, Warp sends `503 Service Unavailable` with
`Warp-Error: request-buffer-timeout`, `Content-Length: 0`, and `Connection: close`,
then closes the connection without calling the application. The fixed response
needs no body-pool buffer. Sending
has a fresh connection-timeout window; successful sends are followed by a
best-effort drain of at most 16 KiB with a 250 ms deadline before normal transport
close. Its separate, initialized 1 KiB scratch lets ordinary unread request bytes
be discarded without waiting for the exhausted pool, reducing TCP resets that
could hide the response. Large or continuing uploads can exceed those limits.
External cancellation and timeouts after admission keep their existing behavior;
they do not generate an overload response. Application execution still pauses
the connection timeout, and this does not add a deadline to later buffer loans.

`withBody` selects the request source once without acquiring another buffer.
A source exposes `withChunk source callback`: its bytes are borrowed only while
that callback runs, and an empty chunk means EOF. Copy bytes before retaining
them or handing them to asynchronous consumers. Input loans return locally when
the callback finishes and no unread bytes remain; parser read-ahead retains its
loan until consumed. `forward` completes each sink call inside the source
callback before pulling again.

`respond` consumes each byte chunk synchronously and needs no writable HTTP
output buffer. Applications can respond before selecting a body. Sources and
sinks are exclusive, cannot escape their scopes, and blocked operations are
cancelled and joined before input detaches.

The new runner supports HTTP/1.1 only, with no HTTP/2 or raw upgrades. It uses
the existing Warp parsers, response framing, worker lifecycle and settings.
A transport provides optional `connRecvInto`; its blocking callback owns
readiness and any bidirectional TLS progress, and may not retain the supplied
pointer. Sending consumes borrowed plaintext completely before returning.
Transport callbacks must be cancellable; scope exit waits until they stop.

An idle connection reads one byte to detect request activity. Header receives
then use the admitted request buffer; only metadata bytes are copied into GC
storage, bounded by Warp's total-header setting. Body bytes read alongside the
last header retain their buffer loan. Receives are capped at 16 KiB even with a
larger configured buffer. Unread bodies follow `settingsMaximumBodyFlush`.
At request completion, at most one 16 KiB pipeline prefix is copied before its
loan returns, so the following request can obtain its own primary admission.
PROXY protocol control metadata has its separate 108-byte bound.

The ordinary WAI API remains available. Integration-only `withBodyBuffer`
activates explicitly supplied storage for low-level integration;
`respondResponse` preserves existing WAI response middleware and materialized
responses through the exchange's one-shot gate. Compatibility Builder/file
scratch is allocated only when used and freed before the next idle request.
Neither path installs request storage in connection fields.

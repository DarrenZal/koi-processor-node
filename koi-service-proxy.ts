/**
 * KOI service proxy (R26 fix).
 *
 * The query API previously exposed a generic `proxyToService` that forwarded
 * `req.originalUrl` verbatim to the internal services (coordinator :8005,
 * event-bridge :8100, bge :8090, pipeline-metadata :8002). nginx selected its
 * location on the dot-resolved path but forwarded the raw request URI, so a
 * crafted path could escape the mount and reach any internal path on those
 * services. Those generic mounts are removed.
 *
 * The one client-facing consumer was `kb_rid_lookup` in the published
 * regen-koi-mcp, which calls `/api/koi/rid-lookup/<encodeURIComponent(rid)>`.
 * It is preserved here as a dedicated GET handler that is pinned to the
 * `/api/koi/rid-lookup/` space on the upstream and cannot be steered elsewhere.
 */

import type express from 'express';

export const RID_LOOKUP_PREFIX = '/api/koi/rid-lookup/';

// Match the prefix plus a non-empty remainder. The remainder is a single RID,
// percent-encoded by the caller (encodeURIComponent escapes '/', so a RID never
// introduces a raw path separator here).
export const RID_LOOKUP_ROUTE = /^\/api\/koi\/rid-lookup\/(.+)$/;

export interface RidLookupOptions {
  /** Upstream base URL. Default: the pipeline-metadata API on loopback. */
  upstreamBase?: string;
  /** Per-request timeout in ms. */
  timeoutMs?: number;
}

export function createRidLookupHandler(opts: RidLookupOptions = {}) {
  const upstreamBase = (opts.upstreamBase ?? 'http://localhost:8002').replace(/\/+$/, '');
  const timeoutMs = opts.timeoutMs ?? 10_000;

  return async (req: express.Request, res: express.Response) => {
    // req.path keeps percent-encoding (Node/Express do not decode the pathname),
    // so the remainder is exactly the caller's encodeURIComponent(rid).
    if (!req.path.startsWith(RID_LOOKUP_PREFIX)) {
      return res.status(404).json({ error: 'Not found' });
    }
    const rawRid = req.path.slice(RID_LOOKUP_PREFIX.length);
    if (!rawRid) {
      return res.status(400).json({ error: 'Missing RID' });
    }

    let rid: string;
    try {
      rid = decodeURIComponent(rawRid);
    } catch {
      return res.status(400).json({ error: 'Malformed RID' });
    }

    // Re-encode as a single path segment. encodeURIComponent escapes '/', so no
    // amount of traversal inside the RID can create a path separator upstream.
    const target = new URL(upstreamBase + RID_LOOKUP_PREFIX + encodeURIComponent(rid));

    // Forward only the two query parameters the upstream endpoint reads.
    const qIndex = req.originalUrl.indexOf('?');
    if (qIndex !== -1) {
      const incoming = new URLSearchParams(req.originalUrl.slice(qIndex + 1));
      const out = new URLSearchParams();
      const includeChunks = incoming.get('include_chunks');
      if (includeChunks !== null) out.set('include_chunks', includeChunks);
      const limit = incoming.get('limit');
      if (limit !== null) out.set('limit', limit);
      const qs = out.toString();
      if (qs) target.search = qs;
    }

    // Defense in depth: the target must never leave the rid-lookup space.
    if (!target.pathname.startsWith(RID_LOOKUP_PREFIX)) {
      return res.status(400).json({ error: 'Invalid RID path' });
    }

    // Forward no client headers except Authorization. The upstream validates the
    // bearer session token to decide whether private rows are visible; everything
    // else (Host, Cookie, Content-Length, ...) is dropped.
    const headers: Record<string, string> = { Accept: 'application/json' };
    const auth = req.headers['authorization'];
    if (typeof auth === 'string' && auth) {
      headers['Authorization'] = auth;
    }

    try {
      const response = await fetch(target, {
        method: 'GET',
        headers,
        signal: AbortSignal.timeout(timeoutMs),
      });
      const data = await response.text();
      res.status(response.status);
      const contentType = response.headers.get('content-type');
      if (contentType) {
        res.set('Content-Type', contentType);
      }
      res.send(data);
    } catch (error) {
      const isTimeout =
        error instanceof Error &&
        (error.name === 'TimeoutError' || error.name === 'AbortError');
      console.error(`rid-lookup proxy error for ${req.path}:`, error);
      res.status(isTimeout ? 504 : 503).json({
        error: isTimeout ? 'Upstream timeout' : 'Service unavailable',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  };
}

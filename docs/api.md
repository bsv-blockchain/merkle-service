# Merkle Service API Reference

Base URL: `http://localhost:8080` (default; configurable via `API_PORT`)

## POST /watch

Register a transaction for merkle proof callbacks. When the merkle proof becomes available, the service delivers it to the specified callback URL.

### Request

| Field         | Type   | Required | Description                                      |
|---------------|--------|----------|--------------------------------------------------|
| `txid`        | string | Yes      | Transaction ID; 64-character hexadecimal string. |
| `callbackUrl` | string | Yes      | HTTP or HTTPS URL to receive the proof callback. |

### Responses

**200 OK** -- registration accepted:

```json
{
  "status": "ok",
  "message": "registration successful"
}
```

**400 Bad Request** -- validation failure (examples):

```json
{ "error": "txid is required" }
{ "error": "invalid txid format: must be a 64-character hex string" }
{ "error": "callbackUrl is required" }
{ "error": "invalid callbackUrl: must be a valid HTTP/HTTPS URL" }
{ "error": "invalid request body" }
```

**500 Internal Server Error** -- storage failure:

```json
{ "error": "internal server error" }
```

### curl examples

Register a transaction:

```bash
curl -X POST http://localhost:8080/watch \
  -H 'Content-Type: application/json' \
  -d '{
    "txid": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
    "callbackUrl": "https://example.com/callback"
  }'
```

Expected output:

```json
{"status":"ok","message":"registration successful"}
```

Invalid txid (too short):

```bash
curl -X POST http://localhost:8080/watch \
  -H 'Content-Type: application/json' \
  -d '{"txid": "abc123", "callbackUrl": "https://example.com/callback"}'
```

Expected output:

```json
{"error":"invalid txid format: must be a 64-character hex string"}
```

---

## GET /health

Returns service health status including Aerospike connectivity.

### Responses

**200 OK** -- all dependencies healthy:

```json
{
  "status": "healthy",
  "details": {
    "aerospike": "connected"
  }
}
```

**503 Service Unavailable** -- one or more dependencies unhealthy:

```json
{
  "status": "unhealthy",
  "details": {
    "aerospike": "connection refused"
  }
}
```

### curl example

```bash
curl http://localhost:8080/health
```

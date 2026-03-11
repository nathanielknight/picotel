# Build a Minimal OTLP-Compatible Tracing Module for Python

## Goal

Build a single-module Python tracing library (~300 lines) that emits OpenTelemetry-compatible traces without depending on the `opentelemetry-sdk` or any OTel packages. The only external dependency allowed is `httpx` (assumed already installed). Everything else uses the Python standard library.

The module must use **explicit context passing** — no contextvars, no thread-locals, no global state. The tracer and spans are passed as regular function arguments or via FastAPI's dependency injection. A developer reading the code should be able to follow span parentage by reading function signatures alone.

## Architecture

The module is a single file, `tracing.py`, that exposes:

- `Span` — a frozen dataclass representing a completed or in-progress span
- `Tracer` — the main entry point; creates spans, manages export
- `ConsoleExporter` — pretty-prints spans as indented JSON to stderr
- `HTTPExporter` — batches and POSTs spans as OTLP JSON to a configurable endpoint in a background thread
- `TraceparentHeader` — parses and generates W3C `traceparent` headers

## Data Model

### Span

```python
@dataclass
class Span:
    trace_id: str          # 32 hex chars
    span_id: str           # 16 hex chars
    parent_span_id: str | None
    name: str
    start_time_ns: int     # unix epoch nanoseconds
    end_time_ns: int | None
    status: str            # "UNSET", "OK", "ERROR"
    attributes: dict[str, str | int | float | bool]
    events: list[dict]     # each event: {"name": str, "timestamp_ns": int, "attributes": dict}
```

Generate `trace_id` with `secrets.token_hex(16)` and `span_id` with `secrets.token_hex(8)`. Timestamps use `time.time_ns()`.

### TraceparentHeader

Implement W3C Trace Context Level 2 parsing and generation for the `traceparent` header. Format: `00-{trace_id}-{parent_id}-{trace_flags}`.

```python
@dataclass(frozen=True)
class TraceparentHeader:
    trace_id: str       # 32 hex chars
    parent_id: str      # 16 hex chars
    trace_flags: int    # typically 0x01 for sampled

    @classmethod
    def parse(cls, header: str) -> "TraceparentHeader | None":
        """Parse a traceparent header string. Return None if malformed."""
        ...

    def encode(self) -> str:
        """Render as a traceparent header value string."""
        return f"00-{self.trace_id}-{self.parent_id}-{self.trace_flags:02x}"
```

Validation rules:
- Exactly 4 dash-separated fields
- Version must be `"00"`
- `trace_id` must be 32 hex chars, not all zeros
- `parent_id` must be 16 hex chars, not all zeros
- `trace_flags` must be 2 hex chars
- If any check fails, return `None` (do not raise)

## Tracer

The `Tracer` class holds configuration and a list of exporters. It is **not** a singleton — you instantiate it explicitly and pass it around.

```python
class Tracer:
    def __init__(
        self,
        service_name: str,
        exporters: list[Exporter],
        default_attributes: dict[str, str | int | float | bool] | None = None,
    ):
        ...
```

### `span()` context manager

The primary API for creating spans. Returns a context manager that yields a `Span`. On exit, it sets `end_time_ns` and sends the span to all exporters.

```python
@contextmanager
def span(
    self,
    name: str,
    parent: Span | TraceparentHeader | None = None,
    attributes: dict | None = None,
) -> Generator[Span, None, None]:
    ...
```

Parent resolution:
- If `parent` is a `Span`, inherit its `trace_id` and use its `span_id` as `parent_span_id`
- If `parent` is a `TraceparentHeader`, inherit its `trace_id` and use its `parent_id` as `parent_span_id`
- If `parent` is `None`, start a new trace (generate new `trace_id`, `parent_span_id` is `None`)

On `__exit__`:
- If an exception occurred, set `status = "ERROR"` and append an event named `"exception"` with attributes `exception.type`, `exception.message`, and `exception.stacktrace`
- Otherwise, set `status = "OK"`
- Set `end_time_ns`
- Call `self._export(span)` which sends the span to every registered exporter
- **Re-raise the exception** — the tracer must never swallow errors

### `add_event()`

Allow adding events to an in-progress span:

```python
def add_event(self, span: Span, name: str, attributes: dict | None = None) -> None:
    span.events.append({
        "name": name,
        "timestamp_ns": time.time_ns(),
        "attributes": attributes or {},
    })
```

### Generating a `traceparent` for outgoing requests

Provide a helper to get the `traceparent` header value from a span, for propagating context to downstream HTTP calls:

```python
def traceparent_for(self, span: Span) -> str:
    return f"00-{span.trace_id}-{span.span_id}-01"
```

## Exporters

### Protocol

```python
class Exporter(Protocol):
    def export(self, span: Span) -> None: ...
    def shutdown(self) -> None: ...
```

### ConsoleExporter

Writes to stderr. Pretty-prints each completed span as indented JSON. Include the span name, duration in milliseconds, trace/span/parent IDs, status, attributes, and events.

```python
class ConsoleExporter:
    def export(self, span: Span) -> None:
        duration_ms = (span.end_time_ns - span.start_time_ns) / 1_000_000
        record = {
            "timestamp": datetime.fromtimestamp(span.start_time_ns / 1e9, tz=timezone.utc).isoformat(),
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "parent_span_id": span.parent_span_id,
            "name": span.name,
            "duration_ms": round(duration_ms, 2),
            "status": span.status,
            "attributes": span.attributes,
            "events": span.events if span.events else None,
        }
        # Drop None values for cleaner output
        record = {k: v for k, v in record.items() if v is not None}
        print(json.dumps(record, indent=2, default=str), file=sys.stderr)

    def shutdown(self) -> None:
        pass
```

### HTTPExporter

Batches spans and POSTs them as OTLP/HTTP JSON to a configurable endpoint (default: `http://localhost:4318/v1/traces`). Runs a **daemon background thread** that flushes every N seconds or when the batch reaches a size limit.

```python
class HTTPExporter:
    def __init__(
        self,
        endpoint: str = "http://localhost:4318/v1/traces",
        batch_size: int = 64,
        flush_interval_seconds: float = 5.0,
        timeout_seconds: float = 10.0,
        headers: dict[str, str] | None = None,
    ):
        ...
```

Implementation requirements:
- Use a `queue.Queue` internally. `export()` puts spans on the queue (never blocks the caller).
- A daemon thread runs a loop: sleep for `flush_interval_seconds`, then drain the queue and POST.
- Also flush whenever the queue reaches `batch_size`.
- Use `httpx.Client` (sync) for the POST. The client should be created once and reused.
- Set `Content-Type: application/json`.
- On HTTP errors, log a warning to stderr and **drop the batch** — do not retry. This is tracing, not critical data.
- `shutdown()` does a final flush and joins the thread with a timeout.

### OTLP JSON format

The POST body must conform to the OTLP/HTTP JSON traces format. Here is the structure:

```json
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "my-service" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "tracing.py", "version": "0.1.0" },
          "spans": [
            {
              "traceId": "<32 hex chars>",
              "spanId": "<16 hex chars>",
              "parentSpanId": "<16 hex chars or empty string>",
              "name": "span name",
              "kind": 1,
              "startTimeUnixNano": "<string of integer>",
              "endTimeUnixNano": "<string of integer>",
              "attributes": [
                { "key": "attr_name", "value": { "stringValue": "val" } }
              ],
              "events": [
                {
                  "name": "exception",
                  "timeUnixNano": "<string of integer>",
                  "attributes": [...]
                }
              ],
              "status": {
                "code": 1
              }
            }
          ]
        }
      ]
    }
  ]
}
```

Notes on the format:
- `kind` 1 = INTERNAL, 2 = SERVER, 3 = CLIENT. Default to 1 (INTERNAL), but the `span()` context manager should accept an optional `kind` parameter.
- Nanosecond timestamps are encoded as **strings**, not integers.
- `parentSpanId` is an empty string `""` if there is no parent, not null.
- Status codes: 0 = UNSET, 1 = OK, 2 = ERROR.
- Attribute values are typed objects. Infer the type:
  - `str` → `{"stringValue": val}`
  - `int` → `{"intValue": str(val)}` (also string-encoded)
  - `float` → `{"doubleValue": val}`
  - `bool` → `{"boolValue": val}`
- Group all spans in a single flush into one `resourceSpans` entry (they share the same service).
- Include `default_attributes` from the `Tracer` in the resource attributes. Always include `service.name`.

Write a private `_serialize_batch(self, spans: list[Span]) -> dict` method on `HTTPExporter` (or as a module-level function) that converts a list of `Span` dataclasses into this JSON structure.

## FastAPI Integration Example

Show this pattern in the module's docstring or a companion README. The tracer is created at app startup and injected via `Depends()`:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.tracer = Tracer(
        service_name="wavvest-aggregator",
        exporters=[
            ConsoleExporter(),
            HTTPExporter(endpoint="http://localhost:4318/v1/traces"),
        ],
    )
    yield
    app.state.tracer.shutdown()

app = FastAPI(lifespan=lifespan)

def get_tracer(request: Request) -> Tracer:
    return request.app.state.tracer

def get_request_span(request: Request, tracer: Tracer = Depends(get_tracer)) -> Span:
    """Extract traceparent from incoming request headers, create a root span."""
    tp = TraceparentHeader.parse(request.headers.get("traceparent", ""))
    # The middleware (below) actually manages the span lifecycle;
    # this dependency just provides access to the current span.
    return request.state.span
```

### Middleware

Write a simple ASGI middleware (or Starlette `BaseHTTPMiddleware`) that:

1. Parses the incoming `traceparent` header
2. Opens a root span for the request (name = `"{method} {path}"`, kind = SERVER)
3. Sets standard attributes: `http.method`, `http.route`, `http.status_code`, `http.url`
4. Stores the span on `request.state.span` so downstream dependencies can access it
5. On response, sets `http.status_code` and closes the span

```python
class TracingMiddleware:
    def __init__(self, app, tracer: Tracer):
        self.app = app
        self.tracer = tracer

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        # ... parse traceparent, create span, set on scope["state"], etc.
```

**Important:** Since we refuse to use contextvars, the middleware puts the span on `request.state`, and downstream code accesses it explicitly via the `Depends(get_request_span)` dependency. This is the whole point — span propagation is visible in function signatures.

## SQLAlchemy Integration Example

Show a helper that hooks into SQLAlchemy events to create child spans for queries:

```python
from sqlalchemy import event

def instrument_sqlalchemy(engine, tracer: Tracer, parent_span_fn):
    """
    parent_span_fn: a callable that returns the current parent Span,
    e.g. a closure over the request state.
    """
    @event.listens_for(engine, "before_cursor_execute")
    def before_execute(conn, cursor, statement, parameters, context, executemany):
        parent = parent_span_fn()
        if parent:
            span = tracer._start_span(
                name="db.query",
                parent=parent,
                attributes={"db.statement": statement, "db.system": "postgresql"},
            )
            conn.info["trace_span"] = span

    @event.listens_for(engine, "after_cursor_execute")
    def after_execute(conn, cursor, statement, parameters, context, executemany):
        span = conn.info.pop("trace_span", None)
        if span:
            tracer._finish_span(span)
```

This requires exposing `_start_span` and `_finish_span` as lower-level methods on `Tracer` in addition to the `span()` context manager, since the SA event API doesn't pair start/end in a `with` block. These can be public (maybe name them `start_span` / `finish_span`), just document that `span()` is preferred when possible.

## Configuration Pattern

Use environment variables with sensible defaults. The module should work with zero configuration for local development (console exporter only) and be configurable for staging/production:

| Env Var | Default | Description |
|---------|---------|-------------|
| `TRACE_SERVICE_NAME` | `"unknown-service"` | Service name in resource attributes |
| `TRACE_EXPORTER` | `"console"` | `"console"`, `"otlp"`, or `"console+otlp"` |
| `TRACE_OTLP_ENDPOINT` | `"http://localhost:4318/v1/traces"` | OTLP HTTP endpoint |
| `TRACE_OTLP_HEADERS` | `""` | Comma-separated `key=value` pairs for extra headers |
| `TRACE_BATCH_SIZE` | `"64"` | Flush after this many spans |
| `TRACE_FLUSH_INTERVAL` | `"5.0"` | Seconds between flushes |

Provide a convenience factory:

```python
def tracer_from_env() -> Tracer:
    """Build a Tracer from environment variables."""
    ...
```

## Testing

Include tests in a companion `test_tracing.py`. Use a `ListExporter` that appends spans to a list for assertions:

```python
class ListExporter:
    def __init__(self):
        self.spans: list[Span] = []
    def export(self, span: Span) -> None:
        self.spans.append(span)
    def shutdown(self) -> None:
        pass
```

Test cases to cover:

1. **Basic span creation and export** — verify trace_id, span_id, start/end times, status
2. **Parent-child relationship** — child span inherits `trace_id`, has correct `parent_span_id`
3. **Traceparent parsing** — valid header, malformed header (returns None), all-zeros trace_id (returns None)
4. **Traceparent generation** — `traceparent_for()` produces valid header from span
5. **Traceparent as parent** — span created with parsed traceparent inherits IDs correctly
6. **Error capture** — exception in `with tracer.span()` block sets status to ERROR, records exception event with type/message/stacktrace, and re-raises
7. **OTLP JSON serialization** — serialize a batch and validate the structure matches the spec (check nesting, string-encoded nanoseconds, typed attribute values)
8. **HTTP exporter batching** — mock the HTTP endpoint, verify spans arrive in OTLP format
9. **Console exporter output** — capture stderr and verify JSON structure
10. **Span events** — `add_event()` appends correctly
11. **Default attributes** — tracer's `default_attributes` appear in serialized resource
12. **start_span / finish_span** — the non-context-manager API works correctly

## Constraints

- **Single file** for the core module (`tracing.py`). Integration helpers (FastAPI middleware, SQLAlchemy hooks) can be in the same file or a separate `tracing_integrations.py` — your call, but keep it to two files maximum.
- **No dependencies beyond `httpx` and the standard library.**
- **No global state, no contextvars, no thread-locals.** The `Tracer` instance is the only state, and it's passed explicitly.
- **No classes with `__init_subclass__` magic, metaclasses, or descriptors.** Keep it boring.
- **Type hints throughout.** Target Python 3.11+ (use `X | Y` union syntax, not `Optional`).
- The background flush thread in `HTTPExporter` must be a **daemon thread** so it doesn't prevent process exit.
- All public functions and classes need docstrings.
- Use `from __future__ import annotations` for forward reference support.
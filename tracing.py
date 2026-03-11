"""
picotel — a minimal OTLP-compatible tracing module for Python.

Uses explicit context passing (no contextvars, no thread-locals, no global
state).  Spans are ordinary dataclasses; the Tracer instance is the only
shared object, passed as a regular argument or via FastAPI Depends().

FastAPI integration example
----------------------------
    from contextlib import asynccontextmanager
    from fastapi import FastAPI, Request, Depends

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.tracer = Tracer(
            service_name="my-service",
            exporters=[ConsoleExporter(), HTTPExporter()],
        )
        yield
        app.state.tracer.shutdown()

    app = FastAPI(lifespan=lifespan)

    def get_tracer(request: Request) -> Tracer:
        return request.app.state.tracer

    def get_request_span(request: Request) -> Span:
        return request.state.span

    # Attach TracingMiddleware so every request gets a root span:
    #   app.add_middleware(TracingMiddleware, tracer=app.state.tracer)
    # (see TracingMiddleware below)
"""
from __future__ import annotations

import json
import queue
import secrets
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Generator, Protocol


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Span:
    """A single tracing span.  Mutable so events can be appended in-place."""

    trace_id: str           # 32 hex chars
    span_id: str            # 16 hex chars
    parent_span_id: str | None
    name: str
    start_time_ns: int      # unix epoch nanoseconds
    end_time_ns: int | None
    status: str             # "UNSET" | "OK" | "ERROR"
    attributes: dict[str, str | int | float | bool]
    events: list[dict]      # {"name": str, "timestamp_ns": int, "attributes": dict}
    kind: int = 1           # 1=INTERNAL, 2=SERVER, 3=CLIENT


# ---------------------------------------------------------------------------
# W3C traceparent
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TraceparentHeader:
    """W3C Trace Context Level 2 traceparent header."""

    trace_id: str       # 32 hex chars
    parent_id: str      # 16 hex chars
    trace_flags: int    # typically 0x01 for sampled

    @classmethod
    def parse(cls, header: str) -> TraceparentHeader | None:
        """Parse a traceparent header string.  Return None if malformed."""
        if not isinstance(header, str):
            return None
        parts = header.split("-")
        if len(parts) != 4:
            return None
        version, trace_id, parent_id, flags_str = parts
        if version != "00":
            return None
        if len(trace_id) != 32 or not _is_hex(trace_id) or trace_id == "0" * 32:
            return None
        if len(parent_id) != 16 or not _is_hex(parent_id) or parent_id == "0" * 16:
            return None
        if len(flags_str) != 2 or not _is_hex(flags_str):
            return None
        return cls(
            trace_id=trace_id,
            parent_id=parent_id,
            trace_flags=int(flags_str, 16),
        )

    def encode(self) -> str:
        """Render as a traceparent header value string."""
        return f"00-{self.trace_id}-{self.parent_id}-{self.trace_flags:02x}"


def _is_hex(s: str) -> bool:
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Exporter protocol
# ---------------------------------------------------------------------------


class Exporter(Protocol):
    """Protocol that every exporter must satisfy."""

    def export(self, span: Span) -> None: ...
    def shutdown(self) -> None: ...


# ---------------------------------------------------------------------------
# ConsoleExporter
# ---------------------------------------------------------------------------


class ConsoleExporter:
    """Pretty-prints completed spans as indented JSON to stderr."""

    def export(self, span: Span) -> None:
        """Write a single span to stderr as JSON."""
        duration_ms = (span.end_time_ns - span.start_time_ns) / 1_000_000
        record = {
            "timestamp": datetime.fromtimestamp(
                span.start_time_ns / 1e9, tz=timezone.utc
            ).isoformat(),
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "parent_span_id": span.parent_span_id,
            "name": span.name,
            "duration_ms": round(duration_ms, 2),
            "status": span.status,
            "attributes": span.attributes if span.attributes else None,
            "events": span.events if span.events else None,
        }
        record = {k: v for k, v in record.items() if v is not None}
        print(json.dumps(record, indent=2, default=str), file=sys.stderr)

    def shutdown(self) -> None:
        """No-op — ConsoleExporter has no background resources."""
        pass


# ---------------------------------------------------------------------------
# HTTPExporter
# ---------------------------------------------------------------------------


_STATUS_CODE = {"UNSET": 0, "OK": 1, "ERROR": 2}


def _attr_value(v: str | int | float | bool) -> dict:
    """Convert a Python attribute value to an OTLP typed attribute object."""
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    return {"stringValue": str(v)}


class HTTPExporter:
    """Batches spans and POSTs them as OTLP/HTTP JSON to a configurable endpoint.

    Runs a daemon background thread that flushes every *flush_interval_seconds*
    or whenever the internal queue reaches *batch_size* spans.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:4318/v1/traces",
        batch_size: int = 64,
        flush_interval_seconds: float = 5.0,
        timeout_seconds: float = 10.0,
        headers: dict[str, str] | None = None,
        service_name: str = "unknown-service",
        default_attributes: dict[str, str | int | float | bool] | None = None,
    ):
        """Create an HTTPExporter.

        Args:
            endpoint: OTLP HTTP traces endpoint.
            batch_size: Maximum spans per HTTP POST.
            flush_interval_seconds: How often the background thread flushes.
            timeout_seconds: HTTP request timeout.
            headers: Extra HTTP headers (e.g. authentication).
            service_name: Service name embedded in resource attributes.
            default_attributes: Extra resource attributes.
        """
        import httpx  # late import keeps the module importable without httpx

        self.endpoint = endpoint
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.service_name = service_name
        self.default_attributes: dict[str, str | int | float | bool] = default_attributes or {}

        self._queue: queue.Queue[Span] = queue.Queue()
        self._stop_event = threading.Event()

        extra_headers = {"Content-Type": "application/json"}
        if headers:
            extra_headers.update(headers)
        self._client = httpx.Client(
            headers=extra_headers,
            timeout=timeout_seconds,
        )

        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="picotel-http-exporter",
        )
        self._thread.start()

    def export(self, span: Span) -> None:
        """Enqueue a span for batched export.  Never blocks the caller."""
        self._queue.put_nowait(span)

    def shutdown(self) -> None:
        """Flush remaining spans and stop the background thread."""
        self._stop_event.set()
        self._thread.join(timeout=30)
        self._flush()
        self._client.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Background loop: flush on interval or when batch is full."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.flush_interval_seconds)
            self._flush()

    def _flush(self) -> None:
        """Drain the queue and POST in chunks of batch_size."""
        batch: list[Span] = []
        while True:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
            if len(batch) >= self.batch_size:
                self._post(batch)
                batch = []
        if batch:
            self._post(batch)

    def _post(self, spans: list[Span]) -> None:
        body = self._serialize_batch(spans)
        try:
            resp = self._client.post(self.endpoint, content=json.dumps(body))
            resp.raise_for_status()
        except Exception as exc:
            print(
                f"[picotel] WARNING: failed to export {len(spans)} spans: {exc}",
                file=sys.stderr,
            )

    def _serialize_batch(self, spans: list[Span]) -> dict:
        """Convert a list of Span dataclasses into the OTLP/HTTP JSON structure."""
        resource_attrs: dict[str, str | int | float | bool] = {
            "service.name": self.service_name,
            **self.default_attributes,
        }
        resource_attr_list = [
            {"key": k, "value": _attr_value(v)} for k, v in resource_attrs.items()
        ]

        otlp_spans = []
        for s in spans:
            span_attrs = [
                {"key": k, "value": _attr_value(v)} for k, v in s.attributes.items()
            ]
            span_events = [
                {
                    "name": e["name"],
                    "timeUnixNano": str(e["timestamp_ns"]),
                    "attributes": [
                        {"key": k, "value": _attr_value(v)}
                        for k, v in e.get("attributes", {}).items()
                    ],
                }
                for e in s.events
            ]
            otlp_spans.append(
                {
                    "traceId": s.trace_id,
                    "spanId": s.span_id,
                    "parentSpanId": s.parent_span_id or "",
                    "name": s.name,
                    "kind": s.kind,
                    "startTimeUnixNano": str(s.start_time_ns),
                    "endTimeUnixNano": str(s.end_time_ns),
                    "attributes": span_attrs,
                    "events": span_events,
                    "status": {"code": _STATUS_CODE.get(s.status, 0)},
                }
            )

        return {
            "resourceSpans": [
                {
                    "resource": {"attributes": resource_attr_list},
                    "scopeSpans": [
                        {
                            "scope": {"name": "tracing.py", "version": "0.1.0"},
                            "spans": otlp_spans,
                        }
                    ],
                }
            ]
        }


# ---------------------------------------------------------------------------
# Tracer
# ---------------------------------------------------------------------------


class Tracer:
    """Entry point for creating and exporting spans.

    Not a singleton — instantiate explicitly and pass it around.
    """

    def __init__(
        self,
        service_name: str,
        exporters: list[Exporter],
        default_attributes: dict[str, str | int | float | bool] | None = None,
    ):
        """Create a Tracer.

        Args:
            service_name: Embedded in resource attributes of every span batch.
            exporters: List of exporters (ConsoleExporter, HTTPExporter, etc.).
            default_attributes: Extra key/value pairs attached to resource attributes.
        """
        self.service_name = service_name
        self.exporters = exporters
        self.default_attributes: dict[str, str | int | float | bool] = default_attributes or {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @contextmanager
    def span(
        self,
        name: str,
        parent: Span | TraceparentHeader | None = None,
        attributes: dict | None = None,
        kind: int = 1,
    ) -> Generator[Span, None, None]:
        """Context manager that creates a span, yields it, then exports it.

        On exit:
        - Sets end_time_ns.
        - Sets status to "OK" or "ERROR".
        - Appends an "exception" event if an error occurred.
        - Exports to all registered exporters.
        - Re-raises any exception — never swallows errors.

        Args:
            name: Span name.
            parent: Parent Span, TraceparentHeader, or None for a root span.
            attributes: Initial span attributes.
            kind: Span kind (1=INTERNAL, 2=SERVER, 3=CLIENT).
        """
        s = self.start_span(name, parent=parent, attributes=attributes, kind=kind)
        exc_info = None
        try:
            yield s
        except Exception:
            exc_info = sys.exc_info()
            raise
        finally:
            self.finish_span(s, exc_info=exc_info)

    def start_span(
        self,
        name: str,
        parent: Span | TraceparentHeader | None = None,
        attributes: dict | None = None,
        kind: int = 1,
    ) -> Span:
        """Create and return a new in-progress span without a context manager.

        Useful for frameworks (e.g. SQLAlchemy events) where start/end don't
        happen inside a single ``with`` block.  Prefer :meth:`span` when possible.
        """
        trace_id, parent_span_id = self._resolve_parent(parent)
        return Span(
            trace_id=trace_id,
            span_id=secrets.token_hex(8),
            parent_span_id=parent_span_id,
            name=name,
            start_time_ns=time.time_ns(),
            end_time_ns=None,
            status="UNSET",
            attributes=dict(attributes) if attributes else {},
            events=[],
            kind=kind,
        )

    def finish_span(
        self,
        span: Span,
        exc_info: tuple | None = None,
    ) -> None:
        """Finish a span started with :meth:`start_span` and export it.

        Args:
            span: The span to finish.
            exc_info: Optional ``sys.exc_info()`` tuple if an error occurred.
        """
        span.end_time_ns = time.time_ns()
        if exc_info and exc_info[0] is not None:
            span.status = "ERROR"
            exc_type, exc_value, exc_tb = exc_info
            span.events.append(
                {
                    "name": "exception",
                    "timestamp_ns": span.end_time_ns,
                    "attributes": {
                        "exception.type": exc_type.__qualname__ if exc_type else "",
                        "exception.message": str(exc_value),
                        "exception.stacktrace": "".join(
                            traceback.format_exception(exc_type, exc_value, exc_tb)
                        ),
                    },
                }
            )
        else:
            span.status = "OK"
        self._export(span)

    def add_event(
        self,
        span: Span,
        name: str,
        attributes: dict | None = None,
    ) -> None:
        """Append an event to an in-progress span.

        Args:
            span: Target span.
            name: Event name.
            attributes: Optional key/value metadata.
        """
        span.events.append(
            {
                "name": name,
                "timestamp_ns": time.time_ns(),
                "attributes": attributes or {},
            }
        )

    def traceparent_for(self, span: Span) -> str:
        """Return a W3C traceparent header value for propagating to downstream calls.

        Args:
            span: The current span.
        """
        return f"00-{span.trace_id}-{span.span_id}-01"

    def shutdown(self) -> None:
        """Flush and shut down all registered exporters."""
        for exporter in self.exporters:
            exporter.shutdown()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_parent(
        self, parent: Span | TraceparentHeader | None
    ) -> tuple[str, str | None]:
        """Return (trace_id, parent_span_id) for a new span."""
        if isinstance(parent, Span):
            return parent.trace_id, parent.span_id
        if isinstance(parent, TraceparentHeader):
            return parent.trace_id, parent.parent_id
        return secrets.token_hex(16), None

    def _export(self, span: Span) -> None:
        for exporter in self.exporters:
            exporter.export(span)


# ---------------------------------------------------------------------------
# Environment-based factory
# ---------------------------------------------------------------------------


def tracer_from_env() -> Tracer:
    """Build a Tracer from environment variables.

    Supported variables:

    TRACE_SERVICE_NAME   Service name (default: "unknown-service")
    TRACE_EXPORTER       "console" | "otlp" | "console+otlp" (default: "console")
    TRACE_OTLP_ENDPOINT  OTLP endpoint (default: "http://localhost:4318/v1/traces")
    TRACE_OTLP_HEADERS   Comma-separated key=value pairs for extra HTTP headers
    TRACE_BATCH_SIZE     Spans per HTTP POST (default: 64)
    TRACE_FLUSH_INTERVAL Seconds between flushes (default: 5.0)
    """
    import os

    service_name = os.environ.get("TRACE_SERVICE_NAME", "unknown-service")
    exporter_mode = os.environ.get("TRACE_EXPORTER", "console").lower()
    endpoint = os.environ.get("TRACE_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")
    batch_size = int(os.environ.get("TRACE_BATCH_SIZE", "64"))
    flush_interval = float(os.environ.get("TRACE_FLUSH_INTERVAL", "5.0"))

    raw_headers = os.environ.get("TRACE_OTLP_HEADERS", "")
    headers: dict[str, str] = {}
    for pair in raw_headers.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            headers[k.strip()] = v.strip()

    exporters: list[Exporter] = []
    if "console" in exporter_mode:
        exporters.append(ConsoleExporter())
    if "otlp" in exporter_mode:
        exporters.append(
            HTTPExporter(
                endpoint=endpoint,
                batch_size=batch_size,
                flush_interval_seconds=flush_interval,
                headers=headers or None,
                service_name=service_name,
            )
        )

    return Tracer(service_name=service_name, exporters=exporters)


# ---------------------------------------------------------------------------
# FastAPI / ASGI middleware (optional integration)
# ---------------------------------------------------------------------------


class TracingMiddleware:
    """ASGI middleware that creates a root span for every HTTP request.

    Stores the span on ``scope["state"]`` / ``request.state.span`` so
    downstream FastAPI dependencies can access it explicitly.
    """

    def __init__(self, app, tracer: Tracer):
        """Args:
            app: The ASGI application to wrap.
            tracer: The Tracer instance created at startup.
        """
        self.app = app
        self.tracer = tracer

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        traceparent_raw = headers.get(b"traceparent", b"").decode("latin-1")
        parent = TraceparentHeader.parse(traceparent_raw)

        method = scope.get("method", "GET")
        path = scope.get("path", "/")
        attrs = {
            "http.method": method,
            "http.route": path,
            "http.url": path,
        }

        status_code_holder: list[int] = []

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
                status_code_holder.append(status_code)
                attrs["http.status_code"] = status_code
            await send(message)

        span = self.tracer.start_span(
            name=f"{method} {path}",
            parent=parent,
            attributes=attrs,
            kind=2,  # SERVER
        )
        if "state" not in scope:
            scope["state"] = {}
        scope["state"]["span"] = span

        exc_info = None
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            exc_info = sys.exc_info()
            raise
        finally:
            self.tracer.finish_span(span, exc_info=exc_info)

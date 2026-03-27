"""
picotel — a minimal OTLP-compatible tracing module for Python.

Uses explicit context passing (no contextvars, no thread-locals, no global
state).  Spans are ordinary dataclasses; the Tracer instance is the only
shared object, passed as a regular argument or via FastAPI Depends().

Example::

    from picotel import Tracer, ConsoleExporter

    tracer = Tracer("my-service", exporters=[ConsoleExporter()])

    with tracer.span("handle-request") as root:
        with root.subspan("fetch-data") as child:
            child.add_event("cache-miss", {"key": "user:42"})
            ...

    tracer.shutdown()
"""

from __future__ import annotations

try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("picotel")
except Exception:
    __version__ = "unknown"

__all__ = [
    "Span",
    "SpanKind",
    "ConsoleExporter",
    "HTTPExporter",
    "ResourceInfo",
    "Tracer",
    "tracer_from_env",
    "__version__",
]

import copy
import dataclasses
import json
import secrets
import sys
import time
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum
from types import TracebackType
from typing import Any, Protocol

# Type alias for the 3-tuple returned by sys.exc_info().
_ExcInfo = tuple[type[BaseException], BaseException, TracebackType]

# Type alias for span attribute values.
_AttrValue = str | int | float | bool
_Attributes = dict[str, _AttrValue]

# ---------------------------------------------------------------------------
# Span kind constants
# ---------------------------------------------------------------------------


class SpanKind(IntEnum):
    """Span kind classification per OpenTelemetry spec.

    Uses :class:`IntEnum` so values are plain ints in serialization
    but invalid literals are rejected at construction time.
    """

    INTERNAL = 1
    SERVER = 2
    CLIENT = 3


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Span:
    """A single tracing span.  Mutable so events can be appended in-place.

    Do not instantiate directly — use :meth:`Tracer.span` (context manager)
    or :meth:`Tracer.start_span` (manual) instead.  The tracer generates
    trace/span IDs, records timing, handles export, and sets the internal
    ``_tracer`` reference needed by :meth:`subspan` and :meth:`finish`.
    """

    trace_id: str  # 32 hex chars
    span_id: str  # 16 hex chars
    parent_span_id: str | None
    name: str
    start_time_ns: int  # unix epoch nanoseconds
    end_time_ns: int | None
    status: str  # "UNSET" | "OK" | "ERROR"
    attributes: _Attributes
    events: list[dict[str, Any]]
    kind: int = SpanKind.INTERNAL
    _tracer: Tracer | None = dataclasses.field(default=None, repr=False, compare=False)
    _finished: bool = dataclasses.field(default=False, repr=False, compare=False)

    def add_event(
        self,
        name: str,
        attributes: _Attributes | None = None,
    ) -> None:
        """Append a timestamped event to this span.

        Example::

            with tracer.span("process") as span:
                span.add_event("step-complete", {"step": "validate"})
                span.add_event("step-complete", {"step": "transform"})
        """
        self.events.append(
            {
                "name": name,
                "timestamp_ns": time.time_ns(),
                "attributes": copy.deepcopy(attributes) if attributes else {},
            }
        )

    def subspan(
        self,
        name: str,
        attributes: _Attributes | None = None,
        kind: int = SpanKind.INTERNAL,
    ) -> _SubspanContext:
        """Create a child span whose parent is this span.

        Example::

            with tracer.span("parent") as parent:
                with parent.subspan("child") as child:
                    child.add_event("starting")
                    ...
        """
        if self._tracer is None:
            raise RuntimeError(
                "Cannot create a subspan: this span has no associated "
                "Tracer. Use Tracer.span() or Tracer.start_span() to "
                "create spans."
            )
        return _SubspanContext(self._tracer, self, name, attributes, kind)

    def finish(
        self,
        exc_info: _ExcInfo | None = None,
    ) -> FinishedSpan:
        """Finish this span and export it.

        Raises ``RuntimeError`` if called more than once.

        Prefer :meth:`Tracer.span` (context manager) or :meth:`subspan`
        over manual start/finish.  This method exists for cases where
        start and end don't happen in the same ``with`` block (e.g.
        framework callbacks).

        Example::

            span = tracer.start_span("bg-task")
            try:
                do_work()
            except Exception:
                span.finish(exc_info=sys.exc_info())
                raise
            else:
                span.finish()
        """
        if self._tracer is None:
            raise RuntimeError(
                "Cannot finish span: no associated Tracer. "
                "Use Tracer.span() or Tracer.start_span() to "
                "create spans."
            )
        if self._finished:
            raise RuntimeError(
                "Cannot finish span: span has already been finished. "
                "Each span can only be finished once."
            )
        self._finished = True
        end_ns = time.time_ns()
        self.end_time_ns = end_ns
        if exc_info and exc_info[0] is not None:
            self.status = "ERROR"
            exc_type, exc_value, exc_tb = exc_info
            self.events.append(
                {
                    "name": "exception",
                    "timestamp_ns": end_ns,
                    "attributes": {
                        "exception.type": (exc_type.__qualname__ if exc_type else ""),
                        "exception.message": str(exc_value),
                        "exception.stacktrace": "".join(
                            traceback.format_exception(exc_type, exc_value, exc_tb)
                        ),
                    },
                }
            )
        elif self.status == "UNSET":
            self.status = "OK"
        finished = self._to_finished()
        self._tracer._export(finished)
        return finished

    def traceparent(self) -> str:
        """Return a W3C ``traceparent`` header value for this span.

        Use this when making outgoing HTTP calls to propagate the trace::

            with tracer.span("call-api") as span:
                requests.get(url, headers={"traceparent": span.traceparent()})
        """
        return f"00-{self.trace_id}-{self.span_id}-01"

    def _to_finished(self) -> FinishedSpan:
        """Create an immutable FinishedSpan from this span's current values.

        If ``end_time_ns`` is ``None``, sets it to the current time.
        Performs a deep copy of ``attributes`` and ``events`` so the
        ``FinishedSpan`` is truly independent of this mutable span.
        """
        end_ns = self.end_time_ns if self.end_time_ns is not None else time.time_ns()
        self.end_time_ns = end_ns
        return FinishedSpan(
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            name=self.name,
            start_time_ns=self.start_time_ns,
            end_time_ns=end_ns,
            status=self.status,
            attributes=copy.deepcopy(self.attributes),
            events=copy.deepcopy(self.events),
            kind=self.kind,
        )


@dataclass(frozen=True)
class FinishedSpan:
    """A completed, immutable snapshot of a span.

    Created by :meth:`Span.finish` — not by direct instantiation.
    All fields are guaranteed to be present, and ``end_time_ns`` is
    always set.
    """

    trace_id: str  # 32 hex chars
    span_id: str  # 16 hex chars
    parent_span_id: str | None
    name: str
    start_time_ns: int  # unix epoch nanoseconds
    end_time_ns: int
    status: str  # "OK" | "ERROR"
    attributes: _Attributes
    events: list[dict[str, Any]]
    kind: int = SpanKind.INTERNAL


# ---------------------------------------------------------------------------
# _SubspanContext
# ---------------------------------------------------------------------------


class _SubspanContext:
    """Context manager returned by :meth:`Span.subspan`.

    Creates a child span on ``__enter__`` and finishes it on ``__exit__``,
    mirroring the behaviour of :meth:`Tracer.span`.
    """

    def __init__(
        self,
        tracer: Tracer,
        parent: Span,
        name: str,
        attributes: _Attributes | None,
        kind: int,
    ) -> None:
        self._tracer = tracer
        self._parent = parent
        self._name = name
        self._attributes = attributes
        self._kind = kind
        self._span: Span | None = None

    def __enter__(self) -> Span:
        self._span = self._tracer.start_span(
            self._name,
            parent=self._parent,
            attributes=self._attributes,
            kind=self._kind,
        )
        return self._span

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        assert self._span is not None
        exc_info: _ExcInfo | None = None
        if exc_type is not None and exc_val is not None and exc_tb is not None:
            exc_info = (exc_type, exc_val, exc_tb)
        self._span.finish(exc_info=exc_info)


# ---------------------------------------------------------------------------
# W3C traceparent
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TraceparentHeader:
    """W3C Trace Context Level 2 ``traceparent`` header."""

    trace_id: str  # 32 hex chars
    parent_id: str  # 16 hex chars
    trace_flags: int  # typically 0x01 for sampled

    @classmethod
    def parse(cls, header: str) -> TraceparentHeader | None:
        """Parse a ``traceparent`` header string.

        Returns ``None`` if the header is malformed.
        """
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
        """Render as a ``traceparent`` header value string."""
        return f"00-{self.trace_id}-{self.parent_id}-{self.trace_flags:02x}"


def _narrow_exc_info() -> _ExcInfo | None:
    """Return ``sys.exc_info()`` narrowed to ``_ExcInfo`` or ``None``."""
    exc_type, exc_value, exc_tb = sys.exc_info()
    if exc_type is not None and exc_value is not None and exc_tb is not None:
        return (exc_type, exc_value, exc_tb)
    return None


_HEX_CHARS = set("0123456789abcdef")


def _is_hex(s: str) -> bool:
    return all(c in _HEX_CHARS for c in s)


# ---------------------------------------------------------------------------
# Exporter protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResourceInfo:
    """Immutable resource metadata passed to exporters with each span.

    Carries the ``service.name`` and any default attributes configured
    on the :class:`Tracer`.  Exporters receive this at export time
    rather than storing it as mutable instance state.
    """

    service_name: str
    default_attributes: _Attributes

    def __hash__(self) -> int:
        return hash((self.service_name, tuple(sorted(self.default_attributes.items()))))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ResourceInfo):
            return NotImplemented
        return (
            self.service_name == other.service_name
            and self.default_attributes == other.default_attributes
        )


class _Exporter(Protocol):
    """Protocol that every exporter must satisfy."""

    def export(self, span: FinishedSpan, resource: ResourceInfo) -> None: ...

    def shutdown(self) -> None: ...


# ---------------------------------------------------------------------------
# ConsoleExporter
# ---------------------------------------------------------------------------


class ConsoleExporter:
    """Prints completed spans as JSON to stderr."""

    def export(self, span: FinishedSpan, resource: ResourceInfo) -> None:
        """Write a single span to stderr as JSON."""
        duration_ms = (span.end_time_ns - span.start_time_ns) / 1_000_000
        # NOTE: We set empty collections to None so that they don't
        # get serialized.
        record: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(
                span.start_time_ns / 1e9, tz=UTC
            ).isoformat(),
            "service_name": resource.service_name,
            "resource_attributes": (
                resource.default_attributes if resource.default_attributes else None
            ),
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
        pass


# ---------------------------------------------------------------------------
# HTTPExporter
# ---------------------------------------------------------------------------


_STATUS_CODE = {"UNSET": 0, "OK": 1, "ERROR": 2}


def _attr_value(v: _AttrValue) -> dict[str, Any]:
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    return {"stringValue": str(v)}


class HTTPExporter:
    """Batches spans and POSTs them as OTLP/HTTP JSON.

    Runs a daemon background thread that flushes every
    *flush_interval_seconds* or whenever the internal queue reaches
    *batch_size* spans.

    The ``service_name`` and ``default_attributes`` are configured on
    the :class:`Tracer` and forwarded here at construction time. Use
    :func:`tracer_from_env` or construct the exporter via the
    ``Tracer`` to keep them in sync.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:4318/v1/traces",
        batch_size: int = 64,
        flush_interval_seconds: float = 5.0,
        timeout_seconds: float = 10.0,
        headers: dict[str, str] | None = None,
    ):
        import queue as _queue_mod
        import threading as _threading_mod

        try:
            import httpx
        except ImportError:
            raise ImportError(
                "HTTPExporter requires httpx. Install picotel[http] to include it."
            ) from None

        self._endpoint = endpoint
        self._batch_size = batch_size
        self._flush_interval_seconds = flush_interval_seconds
        self._queue_mod = _queue_mod

        self._queue: _queue_mod.Queue[tuple[FinishedSpan, ResourceInfo]] = (
            _queue_mod.Queue()
        )
        self._stop_event = _threading_mod.Event()
        self._lock = _threading_mod.Lock()

        extra_headers = {"Content-Type": "application/json"}
        if headers:
            extra_headers.update(headers)
        self._client = httpx.Client(
            headers=extra_headers,
            timeout=timeout_seconds,
        )

        self._thread = _threading_mod.Thread(
            target=self._run,
            daemon=True,
            name="picotel-http-exporter",
        )
        self._thread.start()

    def export(self, span: FinishedSpan, resource: ResourceInfo) -> None:
        """Enqueue a span for batched export.  Never blocks the caller."""
        self._queue.put_nowait((span, resource))

    def shutdown(self) -> None:
        """Flush remaining spans and stop the background thread."""
        self._stop_event.set()
        self._thread.join(timeout=30)
        with self._lock:
            if not self._thread.is_alive():
                # Thread exited cleanly — drain anything left in the queue.
                self._flush()
            self._client.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._flush_interval_seconds)
            with self._lock:
                self._flush()

    def _flush(self) -> None:
        batch: list[tuple[FinishedSpan, ResourceInfo]] = []
        while True:
            try:
                batch.append(self._queue.get_nowait())
            except self._queue_mod.Empty:
                break
            if len(batch) >= self._batch_size:
                self._post(batch)
                batch = []
        if batch:
            self._post(batch)

    def _post(self, items: list[tuple[FinishedSpan, ResourceInfo]]) -> None:
        body = self._serialize_batch(items)
        try:
            resp = self._client.post(self._endpoint, content=json.dumps(body))
            resp.raise_for_status()
        except Exception as exc:
            print(
                f"[picotel] WARNING: failed to export {len(items)} spans: {exc}",
                file=sys.stderr,
            )

    @staticmethod
    def _serialize_batch(
        items: list[tuple[FinishedSpan, ResourceInfo]],
    ) -> dict[str, Any]:
        # Group by resource so spans from different tracers get
        # separate resourceSpans entries.  In practice there is
        # usually only one resource per batch.
        by_resource: dict[ResourceInfo, list[FinishedSpan]] = {}
        for span, resource in items:
            if resource not in by_resource:
                by_resource[resource] = []
            by_resource[resource].append(span)

        resource_spans_list: list[dict[str, Any]] = []
        for resource, spans in by_resource.items():
            resource_spans_list.append(
                HTTPExporter._serialize_resource_spans(resource, spans)
            )
        return {"resourceSpans": resource_spans_list}

    @staticmethod
    def _serialize_resource_spans(
        resource: ResourceInfo, spans: list[FinishedSpan]
    ) -> dict[str, Any]:
        resource_attrs: _Attributes = {
            "service.name": resource.service_name,
            **resource.default_attributes,
        }
        resource_attr_list = [
            {"key": k, "value": _attr_value(v)} for k, v in resource_attrs.items()
        ]

        otlp_spans: list[dict[str, Any]] = []
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
            "resource": {"attributes": resource_attr_list},
            "scopeSpans": [
                {
                    "scope": {
                        "name": "picotel",
                        "version": __version__,
                    },
                    "spans": otlp_spans,
                }
            ],
        }


# ---------------------------------------------------------------------------
# Tracer
# ---------------------------------------------------------------------------


class Tracer:
    """Entry point for creating and exporting spans.

    Not a singleton — instantiate explicitly and pass it around.  However,
    because :class:`HTTPExporter` starts a background thread, you should
    typically create one ``Tracer`` instance for the lifetime of your
    application rather than creating them in a loop.

    The ``service_name`` and ``default_attributes`` are forwarded to any
    :class:`HTTPExporter` in *exporters* so the OTLP resource block is
    populated automatically.

    Example::

        from picotel import Tracer, ConsoleExporter, HTTPExporter

        tracer = Tracer(
            "my-service",
            exporters=[ConsoleExporter(), HTTPExporter()],
        )

        with tracer.span("handle-request") as span:
            span.add_event("starting")
            with span.subspan("db-query") as child:
                ...

        tracer.shutdown()
    """

    def __init__(
        self,
        service_name: str,
        exporters: list[_Exporter],
        default_attributes: _Attributes | None = None,
    ):
        self._service_name = service_name
        self._exporters = exporters
        self._default_attributes: _Attributes = default_attributes or {}
        self._resource = ResourceInfo(
            service_name=service_name,
            default_attributes=self._default_attributes,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @contextmanager
    def span(
        self,
        name: str,
        parent: Span | TraceparentHeader | None = None,
        attributes: _Attributes | None = None,
        kind: int = SpanKind.INTERNAL,
    ) -> Generator[Span, None, None]:
        """Context manager that creates, yields, and finishes a span.

        On normal exit the span gets status ``"OK"``.  If an exception
        propagates out, the span gets status ``"ERROR"`` with an
        ``exception`` event, and the exception is re-raised.

        Example::

            with tracer.span("fetch-user", attributes={"user_id": 42}) as s:
                user = db.get_user(42)
                s.add_event("fetched", {"name": user.name})
        """
        s = self.start_span(name, parent=parent, attributes=attributes, kind=kind)
        exc_info: _ExcInfo | None = None
        try:
            yield s
        except BaseException:
            exc_info = _narrow_exc_info()
            raise
        finally:
            s.finish(exc_info=exc_info)

    def start_span(
        self,
        name: str,
        parent: Span | TraceparentHeader | None = None,
        attributes: _Attributes | None = None,
        kind: int = SpanKind.INTERNAL,
    ) -> Span:
        """Create and return a new in-progress span.

        Useful when start and end don't happen in the same ``with``
        block (e.g. framework callbacks).  Call :meth:`Span.finish`
        when done.

        Example::

            span = tracer.start_span("background-job")
            try:
                process()
            except Exception:
                span.finish(exc_info=sys.exc_info())
                raise
            else:
                span.finish()
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
            _tracer=self,
        )

    def shutdown(self) -> None:
        """Flush and shut down all registered exporters."""
        for exporter in self._exporters:
            exporter.shutdown()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_parent(
        self, parent: Span | TraceparentHeader | None
    ) -> tuple[str, str | None]:
        if isinstance(parent, Span):
            return parent.trace_id, parent.span_id
        if isinstance(parent, TraceparentHeader):
            return parent.trace_id, parent.parent_id
        return secrets.token_hex(16), None

    def _export(self, span: FinishedSpan) -> None:
        for exporter in self._exporters:
            try:
                exporter.export(span, self._resource)
            except Exception as exc:
                print(
                    f"[picotel] WARNING: exporter {type(exporter).__name__}"
                    f" failed for span {span.name!r}: {exc}",
                    file=sys.stderr,
                )


# ---------------------------------------------------------------------------
# Environment-based factory
# ---------------------------------------------------------------------------


def tracer_from_env() -> Tracer:
    """Build a Tracer from standard OpenTelemetry environment variables.

    Uses the standard ``OTEL_*`` variable names where applicable, so
    configuration is portable between picotel and the official
    OpenTelemetry SDK.

    Supported variables:

    - ``OTEL_SERVICE_NAME`` — default ``"unknown-service"``
    - ``OTEL_TRACES_EXPORTER`` — ``"console"``, ``"otlp"``, or
      ``"console,otlp"`` (default ``"console"``)
    - ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` — default
      ``"http://localhost:4318/v1/traces"``
    - ``OTEL_EXPORTER_OTLP_HEADERS`` — comma-separated ``key=value``
      pairs
    - ``OTEL_BSP_MAX_EXPORT_BATCH_SIZE`` — spans per HTTP POST
      (default ``64``)
    - ``OTEL_BSP_SCHEDULE_DELAY_MILLIS`` — milliseconds between
      flushes (default ``5000``)
    """
    import os

    service_name = os.environ.get("OTEL_SERVICE_NAME", "unknown-service")
    exporter_mode = os.environ.get("OTEL_TRACES_EXPORTER", "console").lower()
    endpoint = os.environ.get(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://localhost:4318/v1/traces",
    )
    batch_size = int(os.environ.get("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "64"))
    flush_interval_ms = float(os.environ.get("OTEL_BSP_SCHEDULE_DELAY_MILLIS", "5000"))
    flush_interval = flush_interval_ms / 1000.0

    raw_headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "")
    headers: dict[str, str] = {}
    for pair in raw_headers.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            headers[k.strip()] = v.strip()

    exporters: list[_Exporter] = []
    if "console" in exporter_mode:
        exporters.append(ConsoleExporter())
    if "otlp" in exporter_mode:
        exporters.append(
            HTTPExporter(
                endpoint=endpoint,
                batch_size=batch_size,
                flush_interval_seconds=flush_interval,
                headers=headers or None,
            )
        )

    return Tracer(
        service_name=service_name,
        exporters=exporters,
    )

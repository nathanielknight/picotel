# AGENTS.md — picotel

## What is picotel?

picotel is a deliberately minimal OpenTelemetry-compatible tracing library for Python. It provides span creation, W3C traceparent propagation, and OTLP/HTTP JSON export without the complexity of the official OpenTelemetry SDK.

Key design principles:

- **Explicit context passing** — no `contextvars`, no thread-locals, no global state. The `Tracer` instance and `Span` objects are passed as regular arguments.
- **Minimal surface area** — the entire library is a single `picotel/__init__.py` file.
- **No required dependencies** — `httpx` is only needed for `HTTPExporter` (installed via `picotel[http]`).

## Project structure

```
picotel/
├── picotel/__init__.py   # The entire library (single module)
├── test_picotel.py       # All tests (pytest + hypothesis)
├── pyproject.toml        # Project metadata, tool config
├── mise.toml             # Task runner config (mise)
└── README.md
```

## Public API (`__all__`)

- **`Tracer`** — Entry point. Creates spans, manages exporters. Not a singleton; instantiate and pass explicitly.
- **`Span`** — A mutable in-progress span. The main object users interact with.
  - `span.add_event(name, attributes)` — append a timestamped event
  - `span.subspan(name, ...)` — create a child span (context manager)
  - `span.finish(exc_info=...)` — manually finish and export (raises `RuntimeError` if called twice)
  - `span.traceparent()` — W3C traceparent header for propagation
- **`ConsoleExporter`** — Prints spans as JSON to stderr.
- **`HTTPExporter`** — Batches and POSTs spans as OTLP/HTTP JSON. Service name and default attributes are forwarded from the `Tracer`.
- **`TracingMiddleware`** — ASGI middleware for FastAPI/Starlette.
- **`tracer_from_env()`** — Build a Tracer from standard `OTEL_*` environment variables.

Internal types that exist but are **not** in `__all__`:
- `FinishedSpan` — immutable frozen dataclass snapshot of a completed span (used by exporters). Not a subclass of `Span`.
- `TraceparentHeader` — W3C traceparent parsing/encoding
- `_Exporter` — protocol for custom exporters
- `_SubspanContext` — context manager returned by `Span.subspan()`

Tests import `FinishedSpan` and `TraceparentHeader` directly for low-level testing.

## Important: creating spans

Always create spans through the `Tracer` — never instantiate `Span` directly in application code:

```python
# Context manager (preferred)
with tracer.span("operation") as span:
    span.add_event("started")
    with span.subspan("sub-task") as child:
        ...

# Manual start/finish (for framework callbacks etc.)
span = tracer.start_span("operation")
try:
    process()
except Exception:
    span.finish(exc_info=sys.exc_info())
    raise
else:
    span.finish()
```

The `Tracer` generates IDs, records timing, handles export, and sets the internal `_tracer` reference that `Span.subspan()` and `Span.finish()` need. A manually constructed `Span` won't have an associated tracer and those methods will raise `RuntimeError`.

Tests that need a bare `Span` (e.g. to test `_to_finished()` or serialization) can construct one directly, but this is a testing convenience, not intended usage.

## Running tests and checks

The project uses `uv` for dependency management and `mise` as a task runner. Python 3.11.

```bash
# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Run formatter
uv run ruff format .

# Run type checker
uv run basedpyright

# Run everything (via mise)
mise run ci
```

Or individual mise tasks:

```bash
mise run tests
mise run lint
mise run format
mise run typecheck
mise run checks     # format + lint + typecheck
mise run ci         # tests + checks
```

## Testing conventions

- All tests live in `test_picotel.py` in the project root.
- Tests use `pytest` and `hypothesis` (property-based testing).
- A `ListExporter` helper collects exported spans in-memory for assertions.
- The `make_tracer()` helper creates a `Tracer` + `ListExporter` pair for tests.
- Tests are organized into numbered sections with comment headers.
- When adding tests, follow the existing style: group related tests under a comment header, use descriptive names, and add docstrings for non-obvious tests.

## Code style

- Formatted with `ruff format`, linted with `ruff check` (rules: E, F, I, UP).
- Type-checked with `basedpyright` in standard mode.
- `from __future__ import annotations` is used in all files.
- Line length limit is 88 characters (ruff default).
- All public API is listed in `__all__`.
- Docstrings use examples (``Example::`` blocks) rather than `Args:` blocks — type annotations are the source of truth for parameter types.
- Internal names use a leading underscore (`_tracer`, `_SubspanContext`, `_Exporter`, `_export`, etc.) to keep them out of pdoc output.

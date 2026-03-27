# picotel

Deliberately minimal Open-Telemetry compatible tracing.

This library lets you collect and export traces in an Open Telemtry compatible format
without all the machinery and complexity of the official Open Telemtry libraries.



## How to use it

1. Create a `Tracer` for your application with the relevant metadata.
1. Use `Tracer.span` as a context manager to create a root span.
1. Add events to a span (in places where you might therwise make log calls).
1. Use subspans for logically distinct subroutines.
1. The tracer will export your spans as they finish.


## Examples


```python
# FastAPI: trace each request, continuing the trace from an upstream traceparent header.
from fastapi import FastAPI, Depends, Request
from picotel import Tracer, SpanKind, TraceparentHeader, tracer_from_env

app = FastAPI()
tracer = tracer_from_env()  # reads OTEL_SERVICE_NAME, OTEL_TRACES_EXPORTER, etc.

def get_root_span(request: Request):
    """FastAPI dependency that creates a server span for the current request."""
    parent = TraceparentHeader.parse(request.headers.get("traceparent", ""))
    with tracer.span(
        f"{request.method} {request.url.path}",
        parent=parent,
        attributes={"http.method": request.method, "http.url": str(request.url)},
        kind=SpanKind.SERVER,
    ) as span:
        yield span

@app.get("/users/{user_id}")
def get_user(user_id: int, span=Depends(get_root_span)):
    span.add_event("fetching-user", {"user_id": user_id})
    user = ...  # fetch from database
    return user
```

You can also instrument SQLAlchemy:

```python
# SQLAlchemy: create a subspan for each database query.
from sqlalchemy import create_engine, event, text
from picotel import Tracer, SpanKind, ConsoleExporter

tracer = Tracer("my-service", exporters=[ConsoleExporter()])
engine = create_engine("postgresql://localhost/mydb")

@event.listens_for(engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    # Attach a new subspan to the connection; requires a root span on conn.info
    root = conn.info.get("span")
    if root is not None:
        child = tracer.start_span(
            "db.query",
            parent=root,
            attributes={"db.statement": statement[:200]},
            kind=SpanKind.CLIENT,
        )
        conn.info["query_span"] = child

@event.listens_for(engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    child = conn.info.pop("query_span", None)
    if child is not None:
        child.finish()

# Usage:
with tracer.span("handle-request") as root:
    with engine.connect() as conn:
        conn.info["span"] = root          # attach the root span to the connection
        result = conn.execute(text("SELECT * FROM users WHERE id = :id"), {"id": 1})
```

```python
# Django: add a root span to each request via middleware.
import sys
from django.utils.deprecation import MiddlewareMixin
from picotel import Tracer, SpanKind, TraceparentHeader, tracer_from_env

tracer = tracer_from_env()

class TracingMiddleware(MiddlewareMixin):
    def process_request(self, request):
        parent = TraceparentHeader.parse(request.headers.get("traceparent", ""))
        span = tracer.start_span(
            f"{request.method} {request.path}",
            parent=parent,
            attributes={"http.method": request.method, "http.path": request.path},
            kind=SpanKind.SERVER,
        )
        request.span = span

    def process_response(self, request, response):
        span = getattr(request, "span", None)
        if span is not None:
            span.attributes["http.status_code"] = response.status_code
            span.finish()
        return response

    def process_exception(self, request, exception):
        span = getattr(request, "span", None)
        if span is not None:
            span.finish(exc_info=sys.exc_info())
            request.span = None  # prevent process_response from finishing it again

# Add 'myapp.middleware.TracingMiddleware' to MIDDLEWARE in settings.py.
# Access the span in views via request.span for subspans and events.
```


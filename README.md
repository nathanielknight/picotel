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
# TODO: FastAPI request trace example; set a root span on the request
```
You can also instrument SQLAlchemy 

```python
# TODO: SQLAlchemy example; initiate a session with a root span and add subspans for each database query
```

```python
# TODO: Django request trace example; add a root span to the request object
```


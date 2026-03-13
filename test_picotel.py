"""Unit and generative tests for picotel."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from picotel import (
    ConsoleExporter,
    FinishedSpan,
    HTTPExporter,
    Span,
    TraceparentHeader,
    Tracer,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ListExporter:
    """Collects exported spans into a list for assertions."""

    def __init__(self):
        self.spans: list[FinishedSpan] = []

    def export(self, span: FinishedSpan) -> None:
        self.spans.append(span)

    def shutdown(self) -> None:
        pass


def make_tracer(**kwargs) -> tuple[Tracer, ListExporter]:
    exporter = ListExporter()
    tracer = Tracer(
        service_name=kwargs.get("service_name", "test-service"),
        exporters=[exporter],
        default_attributes=kwargs.get("default_attributes"),
    )
    return tracer, exporter


# ---------------------------------------------------------------------------
# 1. Basic span creation and export
# ---------------------------------------------------------------------------


def test_basic_span_creates_ids():
    tracer, exporter = make_tracer()
    with tracer.span("my-op") as span:
        assert len(span.trace_id) == 32
        assert all(c in "0123456789abcdef" for c in span.trace_id)
        assert len(span.span_id) == 16
        assert all(c in "0123456789abcdef" for c in span.span_id)


def test_basic_span_timing():
    tracer, exporter = make_tracer()
    before = time.time_ns()
    with tracer.span("timed") as span:
        assert span.start_time_ns >= before
        assert span.end_time_ns is None  # not yet finished
    after = time.time_ns()
    assert span.end_time_ns is not None
    assert span.start_time_ns <= span.end_time_ns <= after


def test_basic_span_exported():
    tracer, exporter = make_tracer()
    with tracer.span("export-me"):
        pass
    assert len(exporter.spans) == 1
    assert exporter.spans[0].name == "export-me"


def test_basic_span_status_ok():
    tracer, exporter = make_tracer()
    with tracer.span("ok-span"):
        pass
    assert exporter.spans[0].status == "OK"


def test_span_no_parent():
    tracer, exporter = make_tracer()
    with tracer.span("root") as span:
        pass
    assert span.parent_span_id is None


# ---------------------------------------------------------------------------
# 2. Parent-child relationship
# ---------------------------------------------------------------------------


def test_child_inherits_trace_id():
    tracer, exporter = make_tracer()
    with tracer.span("parent") as parent:
        with tracer.span("child", parent=parent) as child:
            pass
    assert child.trace_id == parent.trace_id


def test_child_has_correct_parent_span_id():
    tracer, exporter = make_tracer()
    with tracer.span("parent") as parent:
        with tracer.span("child", parent=parent) as child:
            pass
    assert child.parent_span_id == parent.span_id


def test_child_has_different_span_id():
    tracer, exporter = make_tracer()
    with tracer.span("parent") as parent:
        with tracer.span("child", parent=parent) as child:
            pass
    assert child.span_id != parent.span_id


# ---------------------------------------------------------------------------
# 3. TraceparentHeader parsing
# ---------------------------------------------------------------------------


def test_traceparent_parse_valid():
    header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    tp = TraceparentHeader.parse(header)
    assert tp is not None
    assert tp.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert tp.parent_id == "00f067aa0ba902b7"
    assert tp.trace_flags == 0x01


def test_traceparent_parse_malformed_returns_none():
    assert TraceparentHeader.parse("not-a-traceparent") is None
    assert TraceparentHeader.parse("") is None
    assert TraceparentHeader.parse("00-abc-def-01") is None  # wrong lengths


def test_traceparent_parse_all_zeros_trace_id_returns_none():
    header = "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
    assert TraceparentHeader.parse(header) is None


def test_traceparent_parse_all_zeros_parent_id_returns_none():
    header = "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"
    assert TraceparentHeader.parse(header) is None


def test_traceparent_parse_wrong_version_returns_none():
    header = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    assert TraceparentHeader.parse(header) is None


def test_traceparent_parse_non_hex_returns_none():
    header = "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01"
    assert TraceparentHeader.parse(header) is None


# ---------------------------------------------------------------------------
# 4. TraceparentHeader generation
# ---------------------------------------------------------------------------


def test_traceparent_encode():
    tp = TraceparentHeader(
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        parent_id="00f067aa0ba902b7",
        trace_flags=0x01,
    )
    assert tp.encode() == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_traceparent_for_span():
    tracer, exporter = make_tracer()
    with tracer.span("root") as span:
        header = tracer.traceparent_for(span)
    assert header == f"00-{span.trace_id}-{span.span_id}-01"
    # should be parseable
    tp = TraceparentHeader.parse(header)
    assert tp is not None


# ---------------------------------------------------------------------------
# 5. Traceparent as parent
# ---------------------------------------------------------------------------


def test_span_with_traceparent_parent():
    tracer, exporter = make_tracer()
    tp = TraceparentHeader(
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        parent_id="00f067aa0ba902b7",
        trace_flags=0x01,
    )
    with tracer.span("child", parent=tp) as span:
        pass
    assert span.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert span.parent_span_id == "00f067aa0ba902b7"


# ---------------------------------------------------------------------------
# 6. Error capture
# ---------------------------------------------------------------------------


def test_exception_sets_error_status():
    tracer, exporter = make_tracer()
    with pytest.raises(ValueError):
        with tracer.span("failing"):
            raise ValueError("boom")
    assert exporter.spans[0].status == "ERROR"


def test_exception_records_event():
    tracer, exporter = make_tracer()
    with pytest.raises(RuntimeError):
        with tracer.span("failing"):
            raise RuntimeError("something broke")
    events = exporter.spans[0].events
    assert len(events) == 1
    assert events[0]["name"] == "exception"
    attrs = events[0]["attributes"]
    assert "exception.type" in attrs
    assert "RuntimeError" in attrs["exception.type"]
    assert "exception.message" in attrs
    assert "something broke" in attrs["exception.message"]
    assert "exception.stacktrace" in attrs


def test_exception_is_reraised():
    tracer, exporter = make_tracer()
    with pytest.raises(KeyError):
        with tracer.span("re-raise"):
            raise KeyError("key")


def test_exception_span_has_end_time():
    tracer, exporter = make_tracer()
    with pytest.raises(ValueError):
        with tracer.span("failing"):
            raise ValueError("x")
    assert exporter.spans[0].end_time_ns is not None


# ---------------------------------------------------------------------------
# 7. OTLP JSON serialization
# ---------------------------------------------------------------------------


def test_otlp_serialization_structure():
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "svc"
    exporter.default_attributes = {}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="test",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={"key": "val", "count": 1, "rate": 1.5, "flag": True},
        events=[],
    )

    body = exporter._serialize_batch([span])

    assert "resourceSpans" in body
    rs = body["resourceSpans"][0]
    assert "resource" in rs
    assert "scopeSpans" in rs

    scope_spans = rs["scopeSpans"][0]
    assert scope_spans["scope"]["name"] == "picotel"

    s = scope_spans["spans"][0]
    assert s["traceId"] == "a" * 32
    assert s["spanId"] == "b" * 16
    assert s["parentSpanId"] == ""
    assert s["name"] == "test"
    assert s["startTimeUnixNano"] == "1000000000"
    assert s["endTimeUnixNano"] == "2000000000"
    assert s["status"]["code"] == 1  # OK


def test_otlp_typed_attributes():
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "svc"
    exporter.default_attributes = {}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="test",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={"s": "hello", "i": 42, "f": 3.14, "b": True},
        events=[],
    )

    body = exporter._serialize_batch([span])
    attrs = {
        a["key"]: a["value"]
        for a in body["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
    }

    assert attrs["s"] == {"stringValue": "hello"}
    assert attrs["i"] == {"intValue": "42"}
    assert attrs["f"] == {"doubleValue": 3.14}
    assert attrs["b"] == {"boolValue": True}


def test_otlp_with_parent_span_id():
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "svc"
    exporter.default_attributes = {}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id="c" * 16,
        name="child",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={},
        events=[],
    )
    body = exporter._serialize_batch([span])
    s = body["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert s["parentSpanId"] == "c" * 16


def test_otlp_status_codes():
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "svc"
    exporter.default_attributes = {}

    for status, expected_code in [("UNSET", 0), ("OK", 1), ("ERROR", 2)]:
        span = FinishedSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            parent_span_id=None,
            name="s",
            start_time_ns=1,
            end_time_ns=2,
            status=status,
            attributes={},
            events=[],
        )
        body = exporter._serialize_batch([span])
        code = body["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["status"]["code"]
        assert code == expected_code, (
            f"Expected {expected_code} for {status}, got {code}"
        )


def test_otlp_nanoseconds_are_strings():
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "svc"
    exporter.default_attributes = {}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="s",
        start_time_ns=123456789,
        end_time_ns=987654321,
        status="OK",
        attributes={},
        events=[],
    )
    body = exporter._serialize_batch([span])
    s = body["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert isinstance(s["startTimeUnixNano"], str)
    assert isinstance(s["endTimeUnixNano"], str)


# ---------------------------------------------------------------------------
# 8. HTTP exporter batching (mock HTTP endpoint)
# ---------------------------------------------------------------------------


def test_http_exporter_posts_spans(tmp_path):
    """Spans are sent to the HTTP endpoint in OTLP JSON format."""
    posted = []

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    mock_client = MagicMock()
    mock_client.post = MagicMock(
        side_effect=lambda *a, **kw: (posted.append(kw), mock_response)[1]
    )

    with patch("httpx.Client", return_value=mock_client):
        exporter = HTTPExporter(
            endpoint="http://fake/v1/traces",
            batch_size=2,
            flush_interval_seconds=60.0,  # won't auto-flush during test
        )
        tracer = Tracer("http-test", exporters=[exporter])
        with tracer.span("s1"):
            pass
        with tracer.span("s2"):
            pass
        exporter.shutdown()

    assert len(posted) >= 1
    body = json.loads(posted[0]["content"])
    assert "resourceSpans" in body


def test_http_exporter_shutdown_flushes():
    """shutdown() sends remaining spans even below batch_size."""
    posted = []

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.post = MagicMock(
        side_effect=lambda *a, **kw: (posted.append(kw), mock_response)[1]
    )

    with patch("httpx.Client", return_value=mock_client):
        exporter = HTTPExporter(
            endpoint="http://fake/v1/traces",
            batch_size=100,
            flush_interval_seconds=60.0,
        )
        tracer = Tracer("flush-test", exporters=[exporter])
        with tracer.span("one"):
            pass
        exporter.shutdown()

    assert len(posted) == 1


# ---------------------------------------------------------------------------
# 9. Console exporter output
# ---------------------------------------------------------------------------


def test_console_exporter_writes_json(capsys):
    exporter = ConsoleExporter()
    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="console-test",
        start_time_ns=1_000_000_000,
        end_time_ns=1_001_000_000,
        status="OK",
        attributes={"foo": "bar"},
        events=[],
    )
    exporter.export(span)
    captured = capsys.readouterr()
    data = json.loads(captured.err)
    assert data["name"] == "console-test"
    assert data["status"] == "OK"
    assert "duration_ms" in data
    assert data["duration_ms"] == pytest.approx(1.0)


def test_console_exporter_drops_none_fields(capsys):
    exporter = ConsoleExporter()
    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="no-parent",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={},
        events=[],
    )
    exporter.export(span)
    captured = capsys.readouterr()
    data = json.loads(captured.err)
    assert "parent_span_id" not in data
    assert "events" not in data


# ---------------------------------------------------------------------------
# 10. Span events
# ---------------------------------------------------------------------------


def test_add_event():
    tracer, exporter = make_tracer()
    with tracer.span("with-event") as span:
        tracer.add_event(span, "checkpoint", {"step": "1"})
    events = exporter.spans[0].events
    assert len(events) == 1
    assert events[0]["name"] == "checkpoint"
    assert events[0]["attributes"] == {"step": "1"}
    assert "timestamp_ns" in events[0]


def test_add_event_no_attributes():
    tracer, exporter = make_tracer()
    with tracer.span("simple-event") as span:
        tracer.add_event(span, "ping")
    events = exporter.spans[0].events
    assert events[0]["attributes"] == {}


# ---------------------------------------------------------------------------
# 11. Default attributes
# ---------------------------------------------------------------------------


def test_default_attributes_in_serialization():
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "my-svc"
    exporter.default_attributes = {"env": "prod", "version": "1.0"}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="s",
        start_time_ns=1,
        end_time_ns=2,
        status="OK",
        attributes={},
        events=[],
    )
    body = exporter._serialize_batch([span])
    resource_attrs = {
        a["key"]: a["value"] for a in body["resourceSpans"][0]["resource"]["attributes"]
    }
    assert resource_attrs["service.name"] == {"stringValue": "my-svc"}
    assert resource_attrs["env"] == {"stringValue": "prod"}


# ---------------------------------------------------------------------------
# 12. start_span / finish_span
# ---------------------------------------------------------------------------


def test_start_finish_span():
    tracer, exporter = make_tracer()
    span = tracer.start_span("manual")
    assert span.end_time_ns is None
    assert span.status == "UNSET"
    tracer.finish_span(span)
    assert span.end_time_ns is not None
    assert span.status == "OK"
    assert len(exporter.spans) == 1


def test_start_finish_span_with_parent():
    tracer, exporter = make_tracer()
    parent = tracer.start_span("parent")
    child = tracer.start_span("child", parent=parent)
    assert child.trace_id == parent.trace_id
    assert child.parent_span_id == parent.span_id
    tracer.finish_span(child)
    tracer.finish_span(parent)
    assert len(exporter.spans) == 2


# ---------------------------------------------------------------------------
# 13. Span.finished() conversion
# ---------------------------------------------------------------------------


def test_span_finished_creates_finished_span():
    span = Span(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="convert-me",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={"key": "val"},
        events=[],
    )
    finished = span.finished()
    assert isinstance(finished, FinishedSpan)
    assert finished.trace_id == span.trace_id
    assert finished.span_id == span.span_id
    assert finished.name == span.name
    assert finished.end_time_ns == 2_000_000_000
    assert finished.status == "OK"
    assert finished.attributes == {"key": "val"}
    assert finished.kind == span.kind


def test_span_finished_sets_end_time_if_none():
    before = time.time_ns()
    span = Span(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="unfinished",
        start_time_ns=1_000_000_000,
        end_time_ns=None,
        status="UNSET",
        attributes={},
        events=[],
    )
    finished = span.finished()
    after = time.time_ns()
    assert before <= finished.end_time_ns <= after
    assert span.end_time_ns == finished.end_time_ns


# ---------------------------------------------------------------------------
# 14. FinishedSpan immutability
# ---------------------------------------------------------------------------


def test_finished_span_is_immutable():
    finished = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="frozen",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={},
        events=[],
    )
    with pytest.raises(AttributeError, match="immutable FinishedSpan"):
        finished.status = "ERROR"
    with pytest.raises(AttributeError, match="immutable FinishedSpan"):
        finished.end_time_ns = 999


def test_finished_span_cannot_delete_attributes():
    finished = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="frozen",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes={},
        events=[],
    )
    with pytest.raises(AttributeError, match="immutable FinishedSpan"):
        del finished.name


# ---------------------------------------------------------------------------
# Hypothesis property-based tests
# ---------------------------------------------------------------------------


@given(
    trace_id=st.text(alphabet="0123456789abcdef", min_size=32, max_size=32).filter(
        lambda x: x != "0" * 32
    ),
    parent_id=st.text(alphabet="0123456789abcdef", min_size=16, max_size=16).filter(
        lambda x: x != "0" * 16
    ),
    trace_flags=st.integers(min_value=0, max_value=255),
)
def test_traceparent_roundtrip(trace_id, parent_id, trace_flags):
    """encode() followed by parse() returns the original values."""
    tp = TraceparentHeader(
        trace_id=trace_id,
        parent_id=parent_id,
        trace_flags=trace_flags,
    )
    parsed = TraceparentHeader.parse(tp.encode())
    assert parsed is not None
    assert parsed.trace_id == trace_id
    assert parsed.parent_id == parent_id
    assert parsed.trace_flags == trace_flags


@given(st.text())
def test_traceparent_parse_never_raises(s):
    """parse() should never raise, only return None for invalid input."""
    result = TraceparentHeader.parse(s)
    assert result is None or isinstance(result, TraceparentHeader)


@given(
    name=st.text(min_size=1, max_size=100),
    attrs=st.dictionaries(
        st.text(min_size=1, max_size=20),
        st.one_of(
            st.text(max_size=50),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
        ),
        max_size=5,
    ),
)
@settings(max_examples=30)
def test_span_creation_with_arbitrary_inputs(name, attrs):
    """Tracer.span() doesn't crash with arbitrary names and attributes."""
    tracer, exporter = make_tracer()
    with tracer.span(name, attributes=attrs):
        pass
    assert len(exporter.spans) == 1
    assert exporter.spans[0].name == name


# Shared strategy for valid span attribute values.
_attr_values = st.one_of(
    st.text(max_size=50),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
)
_attrs = st.dictionaries(st.text(min_size=1, max_size=20), _attr_values, max_size=5)


@given(st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=6))
@settings(max_examples=40)
def test_nested_spans_parent_relationships(names):
    """State machine: build a chain of nested spans, verify trace/parent IDs."""
    tracer, exporter = make_tracer()

    # Build a nested chain programmatically (start_span / finish_span so we
    # can control nesting depth without recursive context managers).
    spans: list[Span] = []
    for name in names:
        parent = spans[-1] if spans else None
        spans.append(tracer.start_span(name, parent=parent))
    for span in reversed(spans):
        tracer.finish_span(span)

    root_trace_id = spans[0].trace_id
    span_ids = [s.span_id for s in spans]

    for i, span in enumerate(spans):
        # All spans share the same trace_id.
        assert span.trace_id == root_trace_id
        # Root has no parent; all others point at the previous span.
        if i == 0:
            assert span.parent_span_id is None
        else:
            assert span.parent_span_id == spans[i - 1].span_id

    # All span_ids are unique.
    assert len(span_ids) == len(set(span_ids))


@given(st.text(min_size=1, max_size=100))
@settings(max_examples=50)
def test_span_timing_invariant(name):
    """start_time_ns <= end_time_ns for every finished span."""
    tracer, exporter = make_tracer()
    with tracer.span(name):
        pass
    s = exporter.spans[0]
    assert s.start_time_ns <= s.end_time_ns


@given(_attrs)
@settings(max_examples=50)
def test_otlp_serialization_is_valid_json_with_required_structure(attrs):
    """Arbitrary attributes produce valid OTLP JSON with the required keys."""
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "prop-test"
    exporter.default_attributes = {}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="s",
        start_time_ns=1_000_000_000,
        end_time_ns=2_000_000_000,
        status="OK",
        attributes=attrs,
        events=[],
    )
    body = exporter._serialize_batch([span])

    # Must be serialisable to JSON without error.
    raw = json.dumps(body)
    parsed = json.loads(raw)

    # Required OTLP structure.
    rs = parsed["resourceSpans"][0]
    assert "resource" in rs
    scope_span = rs["scopeSpans"][0]["spans"][0]
    assert scope_span["traceId"] == "a" * 32
    assert isinstance(scope_span["startTimeUnixNano"], str)
    assert isinstance(scope_span["endTimeUnixNano"], str)


@given(_attrs)
@settings(max_examples=50)
def test_otlp_attribute_types_are_preserved(attrs):
    """Each Python type maps to the correct OTLP typed value wrapper."""
    exporter = HTTPExporter.__new__(HTTPExporter)
    exporter.service_name = "prop-test"
    exporter.default_attributes = {}

    span = FinishedSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="s",
        start_time_ns=1,
        end_time_ns=2,
        status="OK",
        attributes=attrs,
        events=[],
    )
    body = exporter._serialize_batch([span])
    otlp_attrs = {
        a["key"]: a["value"]
        for a in body["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
    }

    for key, value in attrs.items():
        typed = otlp_attrs[key]
        if isinstance(value, bool):
            assert "boolValue" in typed
            assert typed["boolValue"] == value
        elif isinstance(value, int):
            assert "intValue" in typed
            assert typed["intValue"] == str(value)
        elif isinstance(value, float):
            assert "doubleValue" in typed
        else:
            assert "stringValue" in typed


@given(st.text(min_size=1, max_size=100), _attrs)
@settings(max_examples=40)
def test_traceparent_for_always_parseable(name, attrs):
    """traceparent_for() always returns a header that parses back correctly."""
    tracer, exporter = make_tracer()
    with tracer.span(name, attributes=attrs) as span:
        header = tracer.traceparent_for(span)

    tp = TraceparentHeader.parse(header)
    assert tp is not None
    assert tp.trace_id == span.trace_id
    assert tp.parent_id == span.span_id


@given(
    st.text(min_size=1, max_size=50),
    st.text(min_size=0, max_size=100),
)
@settings(max_examples=40)
def test_exception_spans_always_have_error_event(exc_type_name, exc_message):
    """Any exception produces status=ERROR and exactly one exception event."""
    tracer, exporter = make_tracer()
    error = ValueError(exc_message)
    with pytest.raises(ValueError):
        with tracer.span("failing"):
            raise error

    finished = exporter.spans[0]
    assert finished.status == "ERROR"
    assert len(finished.events) == 1
    event = finished.events[0]
    assert event["name"] == "exception"
    assert "exception.type" in event["attributes"]
    assert "exception.message" in event["attributes"]
    assert "exception.stacktrace" in event["attributes"]

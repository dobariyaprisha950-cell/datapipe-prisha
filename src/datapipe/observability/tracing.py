"""OpenTelemetry tracer used for pipeline-stage instrumentation."""

from opentelemetry import trace

tracer = trace.get_tracer("datapipe")

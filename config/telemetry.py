import os
import logging
from opentelemetry import trace

from opentelemetry import propagate
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

logger = logging.getLogger(__name__)

def setup_otel(service_name: str):
    try: 
        resource = Resource(attributes={
            "service.name": service_name
        })
        
        otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        
        if not otlp_endpoint:
            logger.warning("OTEL_EXPORTER_OTLP_ENDPOINT not set. Tracing will be disabled.")
            return
        
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        trace.set_tracer_provider(provider)
        
        propagate.set_global_textmap(TraceContextTextMapPropagator())
        
        logger.info(f"OpenTelemetry tracing set up for service: {service_name} and exporting to {otlp_endpoint}")
            
    except Exception as e: 
        logger.error(f"Error setting up OpenTelemetry: {e}")
    
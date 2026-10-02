package com.flowforge.common.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

/**
 * Carries W3C trace context (traceparent) across Kafka by hand.
 * Spring Kafka's automatic propagation only covers record listeners; ours are batch listeners
 * (that is where the throughput comes from), so each record's context is extracted and injected explicitly.
 */
public final class KafkaTraceContext {

    private final Tracer tracer;
    private final Propagator propagator;

    public KafkaTraceContext(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** Starts a CONSUMER span continuing the trace in the record's headers (or a new, sampled-or-not root). */
    public Span startConsumerSpan(String name, Headers headers) {
        return propagator.extract(headers, (Headers h, String key) -> {
                    Header header = h.lastHeader(key);
                    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
                })
                .name(name)
                .kind(Span.Kind.CONSUMER)
                .start();
    }

    /** A record carrying {@code parent}'s context, or the current span's if {@code parent} is null. */
    public ProducerRecord<String, String> record(String topic, String key, String value, Span parent) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Span span = parent != null ? parent : tracer.currentSpan();
        if (span != null) {
            propagator.inject(span.context(), record.headers(),
                    (Headers h, String k, String v) -> h.add(k, v.getBytes(StandardCharsets.UTF_8)));
        }
        return record;
    }
}

package com.flowforge.worker.runtime;

import com.flowforge.common.tracing.KafkaTraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingConfig {

    @Bean
    KafkaTraceContext kafkaTraceContext(Tracer tracer, Propagator propagator) {
        return new KafkaTraceContext(tracer, propagator);
    }
}

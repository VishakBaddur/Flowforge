package com.flowforge.api.config;

import com.flowforge.common.event.EventCodec;
import com.flowforge.common.tracing.KafkaTraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.LoggingCacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

@Configuration
@EnableCaching
public class ApiConfig implements CachingConfigurer {

    /** A Redis outage degrades @Cacheable to a cache miss instead of failing the request. */
    @Override
    public CacheErrorHandler errorHandler() {
        return new LoggingCacheErrorHandler();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventCodec eventCodec(JsonMapper json) {
        return new EventCodec(json);
    }

    @Bean
    KafkaTraceContext kafkaTraceContext(Tracer tracer, Propagator propagator) {
        return new KafkaTraceContext(tracer, propagator);
    }
}

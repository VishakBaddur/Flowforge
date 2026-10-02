package com.flowforge.api.config;

import com.flowforge.common.event.EventCodec;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

@Configuration
@EnableCaching
public class ApiConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventCodec eventCodec(JsonMapper json) {
        return new EventCodec(json);
    }
}

package com.flowforge.orchestrator.engine;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class EngineConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}

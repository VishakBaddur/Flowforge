package com.flowforge.worker.handler;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Demo and load-test task types. */
@Configuration
public class BuiltInHandlers {

    /** Returns immediately. Used for throughput benchmarks. */
    @Bean
    TaskHandler noop() {
        return handler("noop", c -> Map.of("ok", "true"));
    }

    /** Sleeps for input "ms" (default 100). Exercises timeouts. */
    @Bean
    TaskHandler sleep() {
        return handler("sleep", c -> {
            long ms = Long.parseLong(c.input().getOrDefault("ms", "100"));
            Thread.sleep(ms);
            return Map.of("sleptMs", String.valueOf(ms));
        });
    }

    /** Fails with probability input "failureRate" (default 0.5). Exercises retries and backoff. */
    @Bean
    TaskHandler flaky() {
        return handler("flaky", c -> {
            double rate = Double.parseDouble(c.input().getOrDefault("failureRate", "0.5"));
            if (ThreadLocalRandom.current().nextDouble() < rate) {
                throw new IllegalStateException("simulated failure (attempt " + c.attempt() + ")");
            }
            return Map.of("succeededOnAttempt", String.valueOf(c.attempt()));
        });
    }

    /** Always fails. Exercises dead-lettering and skip propagation. */
    @Bean
    TaskHandler fail() {
        return handler("fail", c -> {
            throw new IllegalStateException(c.input().getOrDefault("message", "always fails"));
        });
    }

    @FunctionalInterface
    private interface Body {
        Map<String, String> run(TaskCommand command) throws Exception;
    }

    private static TaskHandler handler(String type, Body body) {
        return new TaskHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public Map<String, String> execute(TaskCommand command) throws Exception {
                return body.run(command);
            }
        };
    }
}

package com.flowforge.common.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffPolicyTest {

    private final BackoffPolicy noJitter =
            new BackoffPolicy(Duration.ofMillis(500), 2.0, Duration.ofSeconds(30), false);

    @Test
    void growsExponentially() {
        assertThat(noJitter.delay(1, 0.0)).isEqualTo(Duration.ofMillis(500));
        assertThat(noJitter.delay(2, 0.0)).isEqualTo(Duration.ofMillis(1000));
        assertThat(noJitter.delay(3, 0.0)).isEqualTo(Duration.ofMillis(2000));
        assertThat(noJitter.delay(4, 0.0)).isEqualTo(Duration.ofMillis(4000));
    }

    @Test
    void isCappedAtMaxDelay() {
        assertThat(noJitter.baseDelay(20)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void hugeAttemptNumberDoesNotOverflow() {
        assertThat(noJitter.baseDelay(10_000)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void fullJitterScalesBaseDelay() {
        BackoffPolicy jittered = new BackoffPolicy(Duration.ofMillis(500), 2.0, Duration.ofSeconds(30), true);
        assertThat(jittered.delay(3, 0.5)).isEqualTo(Duration.ofMillis(1000));
        assertThat(jittered.delay(3, 0.0)).isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(5), 2.0, Duration.ofSeconds(1), true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(1), 0.5, Duration.ofSeconds(5), true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> noJitter.delay(0, 0.0)).isInstanceOf(IllegalArgumentException.class);
    }
}

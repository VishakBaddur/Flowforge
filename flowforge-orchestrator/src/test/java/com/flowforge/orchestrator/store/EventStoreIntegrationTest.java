package com.flowforge.orchestrator.store;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.model.BackoffPolicy;
import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.TaskStatus;
import com.flowforge.common.model.WorkflowDefinition;
import com.flowforge.common.model.WorkflowStatus;
import com.flowforge.common.state.Decision;
import com.flowforge.common.state.WorkflowDecider;
import com.flowforge.common.state.WorkflowState;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class EventStoreIntegrationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:17-alpine");
        }
    }

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    @Autowired EventStore store;
    @Autowired WorkflowRepository repository;
    private final WorkflowDecider decider = new WorkflowDecider(() -> 0.5);

    private static WorkflowDefinition etl() {
        return new WorkflowDefinition("etl", List.of(
                new TaskDefinition("extract", "http", Map.of("url", "https://example.com"), List.of(), 3,
                        Duration.ofSeconds(10), new BackoffPolicy(Duration.ofMillis(200), 3.0, Duration.ofSeconds(5), true)),
                new TaskDefinition("load", "sql", null, List.of("extract"), 1, null, null)));
    }

    private static String newId() {
        return "wf-" + UUID.randomUUID();
    }

    private void persist(WorkflowState state, Decision decision, List<WorkflowEvent> log) {
        store.append(state, decision.events());
        log.addAll(decision.events());
    }

    @Test
    void eventsRoundTripExactlyAndReplayToSameState() {
        String id = newId();
        WorkflowState state = new WorkflowState(id);
        List<WorkflowEvent> log = new ArrayList<>();

        persist(state, decider.start(state, etl(), T), log);
        persist(state, decider.taskStarted(state, "extract", 1, "w1", T.plusSeconds(10), T), log);
        persist(state, decider.taskFailed(state, "extract", 1, "boom", T), log);
        persist(state, decider.retryDue(state, "extract", T.plusSeconds(5)), log);
        persist(state, decider.taskSucceeded(state, "extract", 2, Map.of("rows", "42"), T.plusSeconds(6)), log);
        persist(state, decider.taskSucceeded(state, "load", 1, Map.of(), T.plusSeconds(7)), log);

        assertThat(store.load(id)).isEqualTo(log);

        WorkflowState replayed = repository.load(id).orElseThrow();
        assertThat(replayed.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(replayed.version()).isEqualTo(state.version());
        assertThat(replayed.tasks()).isEqualTo(state.tasks());
        assertThat(replayed.definition()).isEqualTo(etl());
    }

    @Test
    void concurrentWriterLosesWithConflict() {
        String id = newId();
        WorkflowState initial = new WorkflowState(id);
        store.append(initial, decider.start(initial, etl(), T).events());

        WorkflowState writerA = repository.load(id).orElseThrow();
        WorkflowState writerB = repository.load(id).orElseThrow();

        store.append(writerA, decider.taskStarted(writerA, "extract", 1, "w1", T.plusSeconds(10), T).events());
        Decision lateDecision = decider.cancel(writerB, "user request", T);

        assertThatThrownBy(() -> store.append(writerB, lateDecision.events()))
                .isInstanceOf(ConcurrencyConflictException.class);

        WorkflowState fresh = repository.load(id).orElseThrow();
        assertThat(fresh.version()).isEqualTo(3);
        assertThat(fresh.status()).isEqualTo(WorkflowStatus.RUNNING);
        assertThat(fresh.task("extract").status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    void runningIndexTracksWorkflowStatus() {
        String id = newId();
        WorkflowState state = new WorkflowState(id);
        store.append(state, decider.start(state, etl(), T).events());
        assertThat(store.findRunningWorkflowIds()).contains(id);

        store.append(state, decider.cancel(state, "test", T).events());
        assertThat(store.findRunningWorkflowIds()).doesNotContain(id);
    }

    @Test
    void unknownWorkflowLoadsEmpty() {
        assertThat(repository.load("does-not-exist")).isEmpty();
    }
}

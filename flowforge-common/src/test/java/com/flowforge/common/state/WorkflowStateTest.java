package com.flowforge.common.state;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.event.WorkflowEvent.*;
import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.TaskStatus;
import com.flowforge.common.model.WorkflowDefinition;
import com.flowforge.common.model.WorkflowStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowStateTest {

    private static final String WF = "wf-1";
    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    /** a -> (b, c) -> d */
    private static final WorkflowDefinition DIAMOND = new WorkflowDefinition("diamond", List.of(
            task("a"), task("b", "a"), task("c", "a"), task("d", "b", "c")));

    private static TaskDefinition task(String id, String... deps) {
        return new TaskDefinition(id, "noop", null, List.of(deps), 2, null, null);
    }

    private static List<WorkflowEvent> runTaskOk(String id) {
        return List.of(
                new TaskQueued(WF, T, id, 1),
                new TaskStarted(WF, T, id, 1, "worker-1", T.plusSeconds(30)),
                new TaskSucceeded(WF, T, id, 1, Map.of("result", id)));
    }

    private static List<WorkflowEvent> happyPath() {
        List<WorkflowEvent> events = new ArrayList<>();
        events.add(new WorkflowStarted(WF, T, DIAMOND));
        for (String id : List.of("a", "b", "c", "d")) events.addAll(runTaskOk(id));
        events.add(new WorkflowCompleted(WF, T));
        return events;
    }

    @Test
    void replayingHappyPathCompletesWorkflow() {
        WorkflowState state = WorkflowState.replay(WF, happyPath());
        assertThat(state.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(state.version()).isEqualTo(14);
        assertThat(state.succeededTaskIds()).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(state.task("d").output()).containsEntry("result", "d");
    }

    @Test
    void replayIsDeterministic() {
        WorkflowState first = WorkflowState.replay(WF, happyPath());
        WorkflowState second = WorkflowState.replay(WF, happyPath());
        assertThat(second.tasks()).isEqualTo(first.tasks());
        assertThat(second.version()).isEqualTo(first.version());
    }

    @Test
    void retryCycleIncrementsAttempt() {
        WorkflowState state = WorkflowState.replay(WF, List.of(
                new WorkflowStarted(WF, T, DIAMOND),
                new TaskQueued(WF, T, "a", 1),
                new TaskStarted(WF, T, "a", 1, "worker-1", T.plusSeconds(30)),
                new TaskFailed(WF, T, "a", 1, "boom", T.plusSeconds(1))));
        assertThat(state.task("a").status()).isEqualTo(TaskStatus.RETRY_WAIT);
        assertThat(state.task("a").retryAt()).isEqualTo(T.plusSeconds(1));

        state.apply(new TaskQueued(WF, T, "a", 2));
        assertThat(state.task("a").status()).isEqualTo(TaskStatus.QUEUED);
        assertThat(state.task("a").attempt()).isEqualTo(2);
        assertThat(state.task("a").lastError()).isEqualTo("boom");
    }

    @Test
    void rejectsStaleResultFromEarlierAttempt() {
        WorkflowState state = WorkflowState.replay(WF, List.of(
                new WorkflowStarted(WF, T, DIAMOND),
                new TaskQueued(WF, T, "a", 1),
                new TaskFailed(WF, T, "a", 1, "timeout", T),
                new TaskQueued(WF, T, "a", 2)));
        assertThatThrownBy(() -> state.apply(new TaskSucceeded(WF, T, "a", 1, Map.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale attempt 1");
    }

    @Test
    void rejectsEventsBeforeStart() {
        WorkflowState state = new WorkflowState(WF);
        assertThatThrownBy(() -> state.apply(new TaskQueued(WF, T, "a", 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("first event must be WorkflowStarted");
    }

    @Test
    void rejectsSuccessForTaskThatWasNeverQueued() {
        WorkflowState state = WorkflowState.replay(WF, List.<WorkflowEvent>of(new WorkflowStarted(WF, T, DIAMOND)));
        assertThatThrownBy(() -> state.apply(new TaskSucceeded(WF, T, "a", 0, Map.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is PENDING");
    }

    @Test
    void rejectsCompletionWhileTasksIncomplete() {
        WorkflowState state = WorkflowState.replay(WF, List.<WorkflowEvent>of(new WorkflowStarted(WF, T, DIAMOND)));
        assertThatThrownBy(() -> state.apply(new WorkflowCompleted(WF, T)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsEventsAfterTerminalStatus() {
        WorkflowState state = WorkflowState.replay(WF, happyPath());
        assertThatThrownBy(() -> state.apply(new WorkflowCancelled(WF, T, "too late")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("COMPLETED");
    }

    @Test
    void deadLetterThenSkipDownstreamThenFail() {
        WorkflowState state = WorkflowState.replay(WF, List.of(
                new WorkflowStarted(WF, T, DIAMOND),
                new TaskQueued(WF, T, "a", 1),
                new TaskDeadLettered(WF, T, "a", 1, "fatal"),
                new TaskSkipped(WF, T, "b", "upstream a dead-lettered"),
                new TaskSkipped(WF, T, "c", "upstream a dead-lettered"),
                new TaskSkipped(WF, T, "d", "upstream a dead-lettered"),
                new WorkflowFailed(WF, T, "task a dead-lettered")));
        assertThat(state.status()).isEqualTo(WorkflowStatus.FAILED);
        assertThat(state.anyDeadLettered()).isTrue();
        assertThat(state.task("d").status()).isEqualTo(TaskStatus.SKIPPED);
    }
}

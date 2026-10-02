package com.flowforge.common.state;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.event.WorkflowEvent.*;
import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.TaskStatus;
import com.flowforge.common.model.WorkflowDefinition;
import com.flowforge.common.model.WorkflowStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowDeciderTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");
    private final WorkflowDecider decider = new WorkflowDecider(() -> 0.5);   // deterministic jitter
    private WorkflowState state;

    private static TaskDefinition task(String id, int maxRetries, String... deps) {
        return new TaskDefinition(id, "noop", null, List.of(deps), maxRetries, null, null);
    }

    /** a -> (b, c) -> d, two retries each */
    private static WorkflowDefinition diamond() {
        return new WorkflowDefinition("diamond", List.of(
                task("a", 2), task("b", 2, "a"), task("c", 2, "a"), task("d", 2, "b", "c")));
    }

    @BeforeEach
    void setUp() {
        state = new WorkflowState("wf-1");
    }

    private static List<String> types(Decision d) {
        return d.events().stream().map(WorkflowEvent::type).toList();
    }

    private Decision succeed(String taskId) {
        int attempt = state.task(taskId).attempt();
        decider.taskStarted(state, taskId, attempt, "w1", T.plusSeconds(30), T);
        return decider.taskSucceeded(state, taskId, attempt, Map.of(), T);
    }

    @Test
    void startQueuesOnlyRoots() {
        Decision d = decider.start(state, diamond(), T);
        assertThat(types(d)).containsExactly("WorkflowStarted", "TaskQueued");
        assertThat(state.task("a").status()).isEqualTo(TaskStatus.QUEUED);
        assertThat(state.task("b").status()).isEqualTo(TaskStatus.PENDING);
    }

    @Test
    void successFansOutToReadyDependents() {
        decider.start(state, diamond(), T);
        Decision d = succeed("a");
        assertThat(types(d)).containsExactly("TaskSucceeded", "TaskQueued", "TaskQueued");
        assertThat(state.task("b").status()).isEqualTo(TaskStatus.QUEUED);
        assertThat(state.task("c").status()).isEqualTo(TaskStatus.QUEUED);
    }

    @Test
    void fanInWaitsForAllParents() {
        decider.start(state, diamond(), T);
        succeed("a");
        succeed("b");
        assertThat(state.task("d").status()).isEqualTo(TaskStatus.PENDING);
        succeed("c");
        assertThat(state.task("d").status()).isEqualTo(TaskStatus.QUEUED);
    }

    @Test
    void happyPathCompletesWorkflow() {
        decider.start(state, diamond(), T);
        succeed("a");
        succeed("b");
        succeed("c");
        Decision last = succeed("d");
        assertThat(types(last)).containsExactly("TaskSucceeded", "WorkflowCompleted");
        assertThat(state.status()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void failureSchedulesRetryWithBackoff() {
        decider.start(state, diamond(), T);
        Decision d = decider.taskFailed(state, "a", 1, "boom", T);
        // default backoff: 500ms base for attempt 1, full jitter with random=0.5 -> 250ms
        TaskFailed failed = (TaskFailed) d.events().getFirst();
        assertThat(failed.retryAt()).isEqualTo(T.plus(Duration.ofMillis(250)));
        assertThat(state.task("a").status()).isEqualTo(TaskStatus.RETRY_WAIT);
    }

    @Test
    void retryIsNotRequeuedBeforeItIsDue() {
        decider.start(state, diamond(), T);
        decider.taskFailed(state, "a", 1, "boom", T);
        assertThat(decider.retryDue(state, "a", T.plusMillis(100)).isIgnored()).isTrue();

        Decision d = decider.retryDue(state, "a", T.plusMillis(250));
        assertThat(((TaskQueued) d.events().getFirst()).attempt()).isEqualTo(2);
    }

    @Test
    void exhaustedRetriesDeadLetterSkipDownstreamAndFailWorkflow() {
        decider.start(state, diamond(), T);
        for (int attempt = 1; attempt <= 2; attempt++) {
            decider.taskFailed(state, "a", attempt, "boom", T);
            decider.retryDue(state, "a", T.plusSeconds(60));
        }
        Decision d = decider.taskFailed(state, "a", 3, "boom", T);
        assertThat(types(d)).containsExactly(
                "TaskDeadLettered", "TaskSkipped", "TaskSkipped", "TaskSkipped", "WorkflowFailed");
        assertThat(state.status()).isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    void independentBranchKeepsRunningAfterDeadLetter() {
        decider.start(state, new WorkflowDefinition("two-roots", List.of(task("x", 0), task("y", 0))), T);
        decider.taskFailed(state, "x", 1, "fatal", T);
        assertThat(state.status()).isEqualTo(WorkflowStatus.RUNNING);

        Decision d = succeed("y");
        assertThat(types(d)).containsExactly("TaskSucceeded", "WorkflowFailed");
    }

    @Test
    void duplicateResultIsIgnored() {
        decider.start(state, diamond(), T);
        succeed("a");
        long version = state.version();

        Decision dup = decider.taskSucceeded(state, "a", 1, Map.of(), T);
        assertThat(dup.isIgnored()).isTrue();
        assertThat(state.version()).isEqualTo(version);
    }

    @Test
    void staleResultFromOldAttemptIsIgnored() {
        decider.start(state, diamond(), T);
        decider.taskFailed(state, "a", 1, "timeout", T);
        decider.retryDue(state, "a", T.plusSeconds(1));

        Decision late = decider.taskSucceeded(state, "a", 1, Map.of(), T);
        assertThat(late.ignoredReason()).contains("stale attempt 1");
    }

    @Test
    void expiredLeaseTriggersRetry() {
        decider.start(state, diamond(), T);
        decider.taskStarted(state, "a", 1, "w1", T.plusSeconds(30), T);

        assertThat(decider.leaseExpired(state, "a", 1, T.plusSeconds(10)).isIgnored()).isTrue();

        Decision d = decider.leaseExpired(state, "a", 1, T.plusSeconds(31));
        assertThat(types(d)).containsExactly("TaskFailed");
        assertThat(state.task("a").lastError()).contains("lease expired");
    }

    @Test
    void inputsAfterTerminalStatusAreIgnored() {
        decider.start(state, diamond(), T);
        decider.cancel(state, "user request", T);
        assertThat(succeed("a").isIgnored()).isTrue();
        assertThat(state.status()).isEqualTo(WorkflowStatus.CANCELLED);
    }
}

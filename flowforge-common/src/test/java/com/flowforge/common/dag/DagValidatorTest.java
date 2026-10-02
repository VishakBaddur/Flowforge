package com.flowforge.common.dag;

import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DagValidatorTest {

    private static TaskDefinition task(String id, String... deps) {
        return new TaskDefinition(id, "noop", null, List.of(deps), 0, null, null);
    }

    private static WorkflowDefinition wf(TaskDefinition... tasks) {
        return new WorkflowDefinition("test", List.of(tasks));
    }

    @Test
    void linearChainIsOrdered() {
        Dag dag = DagValidator.validate(wf(task("c", "b"), task("b", "a"), task("a")));
        assertThat(dag.topologicalOrder()).containsExactly("a", "b", "c");
        assertThat(dag.roots()).containsExactly("a");
    }

    @Test
    void diamondRunsFanOutThenFanIn() {
        Dag dag = DagValidator.validate(wf(task("a"), task("b", "a"), task("c", "a"), task("d", "b", "c")));
        assertThat(dag.topologicalOrder()).startsWith("a").endsWith("d");
        assertThat(dag.dependentsOf("a")).containsExactlyInAnyOrder("b", "c");
        assertThat(dag.downstreamOf("a")).containsExactlyInAnyOrder("b", "c", "d");
        assertThat(dag.downstreamOf("d")).isEmpty();
    }

    @Test
    void readinessRequiresAllDependencies() {
        Dag dag = DagValidator.validate(wf(task("a"), task("b"), task("c", "a", "b")));
        assertThat(dag.isReady("c", Set.of("a"))).isFalse();
        assertThat(dag.isReady("c", Set.of("a", "b"))).isTrue();
        assertThat(dag.isReady("a", Set.of())).isTrue();
    }

    @Test
    void detectsCycleAndReportsPath() {
        assertThatThrownBy(() -> DagValidator.validate(wf(task("a", "c"), task("b", "a"), task("c", "b"))))
                .isInstanceOf(InvalidWorkflowException.class)
                .hasMessageContaining("cycle detected")
                .hasMessageContaining("a -> c -> b -> a");
    }

    @Test
    void detectsCycleBehindValidRoot() {
        assertThatThrownBy(() -> DagValidator.validate(wf(task("root"), task("x", "root", "y"), task("y", "x"))))
                .isInstanceOf(InvalidWorkflowException.class)
                .hasMessageContaining("cycle detected");
    }

    @Test
    void reportsAllStructuralErrorsAtOnce() {
        assertThatThrownBy(() -> DagValidator.validate(wf(task("a"), task("a"), task("b", "missing"), task("c", "c"))))
                .isInstanceOfSatisfying(InvalidWorkflowException.class, e -> assertThat(e.errors()).containsExactly(
                        "duplicate task id 'a'",
                        "task 'b' depends on unknown task 'missing'",
                        "task 'c' depends on itself"));
    }

    @Test
    void duplicateDependencyIsHarmless() {
        Dag dag = DagValidator.validate(wf(task("a"), task("b", "a", "a")));
        assertThat(dag.topologicalOrder()).containsExactly("a", "b");
    }

    @Test
    void handlesLargeChainWithoutStackOverflow() {
        List<TaskDefinition> tasks = new ArrayList<>();
        tasks.add(task("t0"));
        for (int i = 1; i < 10_000; i++) tasks.add(task("t" + i, "t" + (i - 1)));
        Dag dag = DagValidator.validate(new WorkflowDefinition("big", tasks));
        assertThat(dag.topologicalOrder()).hasSize(10_000).startsWith("t0").endsWith("t9999");
    }
}

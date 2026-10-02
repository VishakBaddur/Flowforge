package com.flowforge.common.dag;

import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.WorkflowDefinition;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A validated, acyclic workflow graph. Only DagValidator creates these. */
public record Dag(
        WorkflowDefinition definition,
        Map<String, TaskDefinition> tasks,
        List<String> topologicalOrder,
        List<String> roots,
        Map<String, List<String>> dependents) {

    public TaskDefinition task(String id) {
        TaskDefinition t = tasks.get(id);
        if (t == null) throw new IllegalArgumentException("unknown task '" + id + "'");
        return t;
    }

    public List<String> dependentsOf(String id) {
        return dependents.getOrDefault(id, List.of());
    }

    /** True when every dependency of {@code taskId} has succeeded. */
    public boolean isReady(String taskId, Set<String> succeeded) {
        return succeeded.containsAll(task(taskId).dependsOn());
    }

    /** Every task transitively downstream of {@code id}; these get SKIPPED if {@code id} dead-letters. */
    public Set<String> downstreamOf(String id) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>(dependentsOf(id));
        while (!queue.isEmpty()) {
            String next = queue.poll();
            if (seen.add(next)) queue.addAll(dependentsOf(next));
        }
        return seen;
    }
}

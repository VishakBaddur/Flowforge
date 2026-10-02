package com.flowforge.common.dag;

import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.WorkflowDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates a workflow and produces a topologically sorted Dag (Kahn's algorithm, O(V + E)). */
public final class DagValidator {

    private DagValidator() {
    }

    public static Dag validate(WorkflowDefinition def) {
        List<String> errors = new ArrayList<>();

        Map<String, TaskDefinition> byId = new LinkedHashMap<>();
        for (TaskDefinition t : def.tasks()) {
            if (byId.putIfAbsent(t.id(), t) != null) {
                errors.add("duplicate task id '" + t.id() + "'");
            }
        }
        for (TaskDefinition t : def.tasks()) {
            for (String dep : t.dependsOn()) {
                if (dep.equals(t.id())) {
                    errors.add("task '" + t.id() + "' depends on itself");
                } else if (!byId.containsKey(dep)) {
                    errors.add("task '" + t.id() + "' depends on unknown task '" + dep + "'");
                }
            }
        }
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> dependents = new LinkedHashMap<>();
        for (String id : byId.keySet()) {
            indegree.put(id, 0);
            dependents.put(id, new ArrayList<>());
        }
        for (TaskDefinition t : byId.values()) {
            for (String dep : new LinkedHashSet<>(t.dependsOn())) {   // ignore duplicate deps
                if (dep.equals(t.id()) || !byId.containsKey(dep)) continue;   // already reported above
                indegree.merge(t.id(), 1, Integer::sum);
                dependents.get(dep).add(t.id());
            }
        }

        Deque<String> queue = new ArrayDeque<>();
        for (String id : byId.keySet()) {
            if (indegree.get(id) == 0) queue.add(id);
        }
        List<String> roots = List.copyOf(queue);

        List<String> order = new ArrayList<>(byId.size());
        while (!queue.isEmpty()) {
            String id = queue.poll();
            order.add(id);
            for (String next : dependents.get(id)) {
                if (indegree.merge(next, -1, Integer::sum) == 0) queue.add(next);
            }
        }

        if (order.size() != byId.size()) {
            errors.add("cycle detected (depends on): " + String.join(" -> ", findCycle(byId, order)));
        }
        if (!errors.isEmpty()) throw new InvalidWorkflowException(errors);

        Map<String, List<String>> frozen = new LinkedHashMap<>();
        dependents.forEach((k, v) -> frozen.put(k, List.copyOf(v)));
        return new Dag(def, Collections.unmodifiableMap(byId), List.copyOf(order), roots,
                Collections.unmodifiableMap(frozen));
    }

    /**
     * Every unprocessed task still has an unprocessed dependency, so following those edges
     * must eventually revisit a node. The loop between the two visits is the cycle.
     */
    private static List<String> findCycle(Map<String, TaskDefinition> byId, List<String> processed) {
        Set<String> done = new HashSet<>(processed);
        String current = byId.keySet().stream().filter(id -> !done.contains(id)).findFirst().orElseThrow();
        List<String> path = new ArrayList<>();
        Map<String, Integer> indexInPath = new HashMap<>();
        while (!indexInPath.containsKey(current)) {
            indexInPath.put(current, path.size());
            path.add(current);
            String from = current;
            current = byId.get(from).dependsOn().stream()
                    .filter(d -> !d.equals(from) && byId.containsKey(d) && !done.contains(d))
                    .findFirst().orElseThrow();
        }
        List<String> cycle = new ArrayList<>(path.subList(indexInPath.get(current), path.size()));
        cycle.add(current);
        return cycle;
    }
}

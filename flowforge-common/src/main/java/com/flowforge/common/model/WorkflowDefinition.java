package com.flowforge.common.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A named DAG of tasks. Structural validation (cycles, missing deps) lives in DagValidator. */
public record WorkflowDefinition(String name, List<TaskDefinition> tasks) {

    public WorkflowDefinition {
        if (name == null || name.isBlank())
            throw new IllegalArgumentException("workflow name is required");
        if (tasks == null || tasks.isEmpty())
            throw new IllegalArgumentException("workflow must have at least one task");
        tasks = List.copyOf(tasks);
    }

    /** Tasks keyed by id, preserving declaration order. Assumes ids are unique (validated separately). */
    public Map<String, TaskDefinition> tasksById() {
        Map<String, TaskDefinition> byId = new LinkedHashMap<>();
        for (TaskDefinition t : tasks) byId.putIfAbsent(t.id(), t);
        return Collections.unmodifiableMap(byId);
    }
}

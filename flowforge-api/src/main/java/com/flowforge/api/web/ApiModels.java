package com.flowforge.api.web;

import com.flowforge.api.persistence.WorkflowRunEntity;
import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.state.TaskState;
import com.flowforge.common.state.WorkflowState;
import tools.jackson.databind.JsonNode;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Response shapes. WorkflowView is Serializable because it is stored in the Redis cache. */
public final class ApiModels {

    private ApiModels() {
    }

    public record SubmitResponse(String workflowId, String status) {}

    public record WorkflowView(String workflowId, String name, String owner, String status, long version,
                               Instant startedAt, Instant finishedAt, List<TaskView> tasks) implements Serializable {

        public boolean terminal() {
            return !"RUNNING".equals(status);
        }

        public static WorkflowView from(WorkflowState s) {
            List<TaskView> tasks = new ArrayList<>();
            for (TaskDefinition def : s.definition().tasks()) {
                TaskState t = s.task(def.id());
                tasks.add(new TaskView(def.id(), def.type(), new ArrayList<>(def.dependsOn()), t.status().name(),
                        t.attempt(), t.lastError(), new HashMap<>(t.output())));
            }
            return new WorkflowView(s.workflowId(), s.definition().name(), s.owner(), s.status().name(), s.version(),
                    s.startedAt(), s.finishedAt(), tasks);
        }
    }

    public record TaskView(String id, String type, List<String> dependsOn, String status, int attempt,
                           String lastError, Map<String, String> output) implements Serializable {}

    public record WorkflowSummary(String workflowId, String name, String owner, String status, long version,
                                  Instant createdAt, Instant updatedAt) {

        public static WorkflowSummary from(WorkflowRunEntity e) {
            return new WorkflowSummary(e.getWorkflowId(), e.getName(), e.getOwner(), e.getStatus(), e.getVersion(),
                    e.getCreatedAt(), e.getUpdatedAt());
        }
    }

    public record EventView(long sequence, String type, Instant createdAt, JsonNode payload) {}

    public record PageResponse<T>(List<T> items, int page, int size, long totalElements, int totalPages) {}
}

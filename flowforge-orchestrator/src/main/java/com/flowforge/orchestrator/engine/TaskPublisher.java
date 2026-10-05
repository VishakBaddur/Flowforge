package com.flowforge.orchestrator.engine;

import com.flowforge.common.messaging.DeadLetter;
import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.common.messaging.Topics;
import com.flowforge.common.tracing.KafkaTraceContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;

@Component
public class TaskPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final KafkaTraceContext trace;

    public TaskPublisher(KafkaTemplate<String, String> kafka, JsonMapper json, KafkaTraceContext trace) {
        this.trace = trace;
        this.kafka = kafka;
        this.json = json;
    }

    public CompletableFuture<SendResult<String, String>> sendTask(TaskCommand command) {
        // Keyed per TASK, not per workflow: workers need no ordering within a workflow, so one large DAG
        // (e.g. a 30-day backfill) spreads across every partition and every worker instead of one.
        // Orchestrator topics stay keyed by workflowId, which is what gives each workflow a single owner.
        return kafka.send(trace.record(Topics.TASKS, command.workflowId() + ":" + command.taskId(),
                json.writeValueAsString(command), null));
    }

    public CompletableFuture<SendResult<String, String>> sendDeadLetter(DeadLetter deadLetter) {
        return kafka.send(trace.record(Topics.TASKS_DLQ, deadLetter.workflowId(), json.writeValueAsString(deadLetter), null));
    }
}

package com.flowforge.orchestrator.engine;

import com.flowforge.common.messaging.DeadLetter;
import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.common.messaging.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;

@Component
public class TaskPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    public TaskPublisher(KafkaTemplate<String, String> kafka, JsonMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    public CompletableFuture<SendResult<String, String>> sendTask(TaskCommand command) {
        return kafka.send(Topics.TASKS, command.workflowId(), json.writeValueAsString(command));
    }

    public CompletableFuture<SendResult<String, String>> sendDeadLetter(DeadLetter deadLetter) {
        return kafka.send(Topics.TASKS_DLQ, deadLetter.workflowId(), json.writeValueAsString(deadLetter));
    }
}

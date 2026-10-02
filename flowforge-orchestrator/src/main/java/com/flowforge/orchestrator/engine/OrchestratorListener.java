package com.flowforge.orchestrator.engine;

import com.flowforge.common.messaging.TaskResult;
import com.flowforge.common.messaging.Topics;
import com.flowforge.common.messaging.WorkflowCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One listener for BOTH topics, so with the RangeAssignor the same consumer owns partition N of each:
 * every command and result for a given workflow is handled by one instance, in order.
 */
@Component
public class OrchestratorListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorListener.class);

    private final WorkflowEngine engine;
    private final JsonMapper json;

    public OrchestratorListener(WorkflowEngine engine, JsonMapper json) {
        this.engine = engine;
        this.json = json;
    }

    @KafkaListener(topics = {Topics.WORKFLOW_COMMANDS, Topics.TASK_RESULTS})
    public void onMessage(ConsumerRecord<String, String> record) {
        switch (record.topic()) {
            case Topics.WORKFLOW_COMMANDS -> engine.handle(json.readValue(record.value(), WorkflowCommand.class));
            case Topics.TASK_RESULTS -> engine.handle(json.readValue(record.value(), TaskResult.class));
            default -> log.warn("Unexpected topic {}", record.topic());
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        engine.onPartitionsAssigned(partitionNumbers(assignments.keySet()));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        engine.onPartitionsRevoked(partitionNumbers(partitions));
    }

    private static Set<Integer> partitionNumbers(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).collect(Collectors.toSet());
    }
}

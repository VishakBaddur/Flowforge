package com.flowforge.orchestrator.kafka;

import com.flowforge.common.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;

/** Topics are created explicitly at startup (auto-create is disabled on the broker). */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    KafkaAdmin.NewTopics flowforgeTopics(@Value("${flowforge.kafka.replicas:1}") int replicas) {
        return new KafkaAdmin.NewTopics(
                topic(Topics.WORKFLOW_COMMANDS, Topics.PARTITIONS, replicas),
                topic(Topics.TASKS, Topics.PARTITIONS, replicas),
                topic(Topics.TASK_RESULTS, Topics.PARTITIONS, replicas),
                TopicBuilder.name(Topics.TASKS_DLQ).partitions(3).replicas(replicas)
                        .config("retention.ms", String.valueOf(Duration.ofDays(30).toMillis()))
                        .build());
    }

    private static NewTopic topic(String name, int partitions, int replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}

package com.flowforge.orchestrator.engine;

import com.flowforge.common.messaging.Topics;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;

/** Same formula Kafka's default partitioner uses for keyed records: murmur2(key) mod partitions. */
public final class Partitions {

    private Partitions() {
    }

    public static int of(String workflowId) {
        return Utils.toPositive(Utils.murmur2(workflowId.getBytes(StandardCharsets.UTF_8))) % Topics.PARTITIONS;
    }
}

package com.flowforge.common.model;

public enum TaskStatus {
    PENDING,        // waiting for dependencies
    QUEUED,         // published to Kafka, waiting for a worker
    RUNNING,        // leased by a worker
    RETRY_WAIT,     // failed or timed out; waiting for backoff to elapse
    SUCCEEDED,
    DEAD_LETTERED,  // retries exhausted; sent to the DLQ
    SKIPPED;        // an upstream task dead-lettered, so this will never run

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD_LETTERED || this == SKIPPED;
    }
}

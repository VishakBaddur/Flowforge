package com.flowforge.common.model;

public enum WorkflowStatus {
    RUNNING,
    COMPLETED,  // every task SUCCEEDED
    FAILED,     // at least one task DEAD_LETTERED
    CANCELLED;

    public boolean isTerminal() {
        return this != RUNNING;
    }
}

package com.flowforge.common.messaging;

/** Every topic is keyed by workflowId, so one workflow's messages stay ordered on one partition. */
public final class Topics {

    public static final String WORKFLOW_COMMANDS = "flowforge.workflow-commands";
    public static final String TASKS = "flowforge.tasks";
    public static final String TASK_RESULTS = "flowforge.task-results";
    public static final String TASKS_DLQ = "flowforge.tasks.dlq";

    /** Upper bound on parallelism: max useful orchestrator instances and workers per group. */
    public static final int PARTITIONS = 12;

    private Topics() {
    }
}

package com.flowforge.worker.handler;

import com.flowforge.common.messaging.TaskCommand;

import java.util.Map;

/** Implement this and register it as a Spring bean to add a new task type. */
public interface TaskHandler {

    String type();

    /** Throw to fail the attempt. Must respond to interruption so timeouts can cancel it. */
    Map<String, String> execute(TaskCommand command) throws Exception;
}

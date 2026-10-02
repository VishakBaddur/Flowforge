package com.flowforge.api.service;

public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(String workflowId) {
        super("workflow '" + workflowId + "' not found");
    }
}

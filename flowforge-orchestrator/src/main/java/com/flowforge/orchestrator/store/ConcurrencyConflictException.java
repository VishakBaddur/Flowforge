package com.flowforge.orchestrator.store;

/** Another writer appended to this workflow after we loaded it. Reload and retry. */
public class ConcurrencyConflictException extends RuntimeException {

    public ConcurrencyConflictException(String workflowId, long expectedVersion, Throwable cause) {
        super("workflow " + workflowId + " changed after version " + expectedVersion, cause);
    }
}

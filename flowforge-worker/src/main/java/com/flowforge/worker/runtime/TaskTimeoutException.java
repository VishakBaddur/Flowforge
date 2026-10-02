package com.flowforge.worker.runtime;

public class TaskTimeoutException extends Exception {

    public TaskTimeoutException(long timeoutMillis) {
        super("timed out after " + timeoutMillis + " ms");
    }
}

package com.flowforge.worker.etl;

/** A partition failed a data-quality gate. Deterministic, so the validate task is configured with no retries. */
public class DataQualityException extends RuntimeException {

    public DataQualityException(String message) {
        super(message);
    }
}

package io.regret.sdk;

import java.util.List;

/**
 * User-facing adapter interface.
 */
public interface Adapter {

    /**
     * Execute a single operation against the target system.
     *
     * <p>Throw {@link AdapterException#transient_(String, Throwable)} for retryable errors
     * (connection lost, timeout) — the SDK will retry with backoff.
     * Throw {@link AdapterException#permanent(String, Throwable)} for non-retryable errors.
     * Any other exception is treated as transient.
     */
    OpResult executeOp(Operation op) throws AdapterException;

    /**
     * Read all records under the given key prefix.
     */
    List<Record> readState(String keyPrefix) throws Exception;

    /**
     * Cleanup data under the given key prefix.
     */
    default void cleanup(String keyPrefix) throws Exception {}
}

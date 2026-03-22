package io.regret.sdk;

import java.util.List;

/**
 * User-facing adapter interface.
 * Implement this to connect a target system to Regret for correctness testing.
 */
public interface Adapter {

    /**
     * Execute a batch of operations against the target system.
     * Fence items are synchronization points: all ops before a fence
     * must complete before ops after the fence begin.
     */
    BatchResponse executeBatch(Batch batch) throws Exception;

    /**
     * Read current state of the given keys.
     * Called by regret-pilot during checkpoint.
     * Return Record with null value for keys that do not exist.
     */
    List<Record> readState(List<String> keys) throws Exception;

    /**
     * Optional cleanup. Called when run ends or pilot deletes the adapter.
     */
    default void cleanup() throws Exception {}
}

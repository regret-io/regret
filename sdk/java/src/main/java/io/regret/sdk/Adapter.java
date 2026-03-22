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
     * Read all records under the given key prefix.
     * Called by regret-pilot during checkpoint verification.
     * Uses range scan to retrieve all keys matching the prefix.
     */
    List<Record> readState(String keyPrefix) throws Exception;

    /**
     * Optional cleanup. Called when run ends or pilot deletes the adapter.
     */
    default void cleanup() throws Exception {}
}

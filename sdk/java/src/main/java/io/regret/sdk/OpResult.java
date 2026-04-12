package io.regret.sdk;

import java.util.List;

public record OpResult(String opId, String status, String message, OpResultData data) {

    public sealed interface OpResultData {}
    public record PutData(String key, long versionId) implements OpResultData {}
    public record GetData(String key, String value, long versionId) implements OpResultData {}
    public record DeleteData() implements OpResultData {}
    public record DeleteRangeData() implements OpResultData {}
    public record ScanData(List<ScanRecord> records) implements OpResultData {}
    public record ListData(List<String> keys) implements OpResultData {}
    public record CasData(String key, long versionId) implements OpResultData {}

    public record ScanRecord(String key, String value, long versionId) {}

    public static OpResult put(String opId, String key, long versionId) {
        return new OpResult(opId, "ok", null, new PutData(key, versionId));
    }

    public static OpResult get(String opId, String key, String value, long versionId) {
        return new OpResult(opId, "ok", null, new GetData(key, value, versionId));
    }

    public static OpResult delete(String opId) {
        return new OpResult(opId, "ok", null, new DeleteData());
    }

    public static OpResult deleteRange(String opId) {
        return new OpResult(opId, "ok", null, new DeleteRangeData());
    }

    public static OpResult scan(String opId, List<ScanRecord> records) {
        return new OpResult(opId, "ok", null, new ScanData(records));
    }

    public static OpResult list(String opId, List<String> keys) {
        return new OpResult(opId, "ok", null, new ListData(keys));
    }

    public static OpResult cas(String opId, String key, long versionId) {
        return new OpResult(opId, "ok", null, new CasData(key, versionId));
    }

    public static OpResult notFound(String opId) {
        return new OpResult(opId, "not_found", null, null);
    }

    public static OpResult versionMismatch(String opId) {
        return new OpResult(opId, "version_mismatch", null, null);
    }

    public static OpResult error(String opId, String message) {
        return new OpResult(opId, "error", message, null);
    }
}

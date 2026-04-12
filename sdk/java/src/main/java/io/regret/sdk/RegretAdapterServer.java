package io.regret.sdk;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import regret.v1.AdapterServiceGrpc;
import regret.v1.Regret;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RegretAdapterServer {

    private static final Logger LOG = LoggerFactory.getLogger(RegretAdapterServer.class);

    public static void serve(Adapter adapter) throws Exception {
        serve(adapter, AdapterMetrics.createFromEnv(adapter.getClass().getSimpleName()));
    }

    public static void serve(Adapter adapter, AdapterMetrics metrics) throws Exception {
        int port = 9090;
        Server server = ServerBuilder.forPort(port)
                .addService(new AdapterServiceImpl(adapter, metrics))
                .build().start();
        LOG.info("Adapter gRPC server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down adapter server");
            server.shutdown();
            if (metrics != null) metrics.close();
        }));
        server.awaitTermination();
    }

    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 10_000;

    private static class AdapterServiceImpl extends AdapterServiceGrpc.AdapterServiceImplBase {
        private final Adapter adapter;
        private final AdapterMetrics metrics;

        AdapterServiceImpl(Adapter adapter, AdapterMetrics metrics) {
            this.adapter = adapter;
            this.metrics = metrics;
        }

        @Override
        public void executeBatch(Regret.BatchRequest request,
                StreamObserver<Regret.BatchResponse> responseObserver) {
            try {
                long batchStartNanos = System.nanoTime();
                LOG.info("executeBatch batchId={} ops={}", request.getBatchId(), request.getOpsCount());

                List<OpResult> results = new ArrayList<>();
                for (Regret.Operation protoOp : request.getOpsList()) {
                    final Operation op = fromProto(protoOp);
                    final String opTypeStr = op.opType().value();
                    long startNanos = System.nanoTime();
                    String status = "error";
                    try {
                        OpResult result = executeWithRetry(op, opTypeStr);
                        status = result != null && result.status() != null ? result.status() : "unknown";
                        results.add(result);
                    } catch (AdapterException e) {
                        if (metrics != null) metrics.recordPermanentError(opTypeStr);
                        results.add(OpResult.error(op.opId(), e.getMessage()));
                    } catch (Exception e) {
                        results.add(OpResult.error(op.opId(), e.getMessage()));
                    } finally {
                        double elapsed = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                        if (metrics != null) metrics.recordOp(opTypeStr, status, elapsed);
                    }
                }

                Regret.BatchResponse.Builder rb = Regret.BatchResponse.newBuilder()
                        .setBatchId(request.getBatchId());
                for (var r : results) {
                    rb.addResults(toProto(r));
                }

                double batchElapsedSec = (System.nanoTime() - batchStartNanos) / 1_000_000_000.0;
                long ms = (long) (batchElapsedSec * 1000.0);
                LOG.info("executeBatch completed batchId={} ops={} {}ms", request.getBatchId(), request.getOpsCount(), ms);
                if (metrics != null) metrics.recordBatch(request.getOpsCount(), batchElapsedSec);

                responseObserver.onNext(rb.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("executeBatch failed", e);
                if (metrics != null) metrics.recordGrpcError("executeBatch");
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }

        private OpResult executeWithRetry(Operation op, String opTypeStr) throws AdapterException {
            long backoffMs = INITIAL_BACKOFF_MS;
            int attempt = 0;
            while (true) {
                attempt++;
                try {
                    return adapter.executeOp(op);
                } catch (AdapterException e) {
                    if (!e.isTransient()) {
                        throw e;
                    }
                    if (metrics != null) metrics.recordRetry(opTypeStr);
                    LOG.warn("op {} transient error (attempt {}), retrying in {}ms: {}",
                            op.opId(), attempt, backoffMs, e.getMessage());
                } catch (Exception e) {
                    if (metrics != null) metrics.recordRetry(opTypeStr);
                    LOG.warn("op {} error (attempt {}), retrying in {}ms: {}",
                            op.opId(), attempt, backoffMs, e.getMessage());
                }
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }

        private static void sleep(long ms) {
            try { Thread.sleep(ms); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void readState(Regret.ReadStateRequest request,
                StreamObserver<Regret.ReadStateResponse> responseObserver) {
            long startNanos = System.nanoTime();
            try {
                List<Record> records = adapter.readState(request.getKeyPrefix());
                LOG.info("readState prefix={} returned {} records", request.getKeyPrefix(), records.size());
                Regret.ReadStateResponse.Builder b = Regret.ReadStateResponse.newBuilder();
                for (Record r : records) {
                    Regret.Record.Builder rb = Regret.Record.newBuilder().setKey(r.getKey());
                    if (r.getValue() != null) rb.setPayload(ByteString.copyFrom(r.getValue()));
                    if (r.getMetadata() != null && r.getMetadata().containsKey("version_id")) {
                        long vid = Long.parseLong(r.getMetadata().get("version_id"));
                        rb.setMeta(Regret.RecordMeta.newBuilder().setVersionId(vid));
                    }
                    b.addRecords(rb.build());
                }
                double elapsed = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                if (metrics != null) metrics.recordReadState(records.size(), elapsed);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("readState failed", e);
                if (metrics != null) metrics.recordGrpcError("readState");
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }

        @Override
        public void cleanup(Regret.CleanupRequest request,
                StreamObserver<Regret.CleanupResponse> responseObserver) {
            try {
                adapter.cleanup(request.getKeyPrefix());
                if (metrics != null) metrics.recordCleanup("ok");
                responseObserver.onNext(Regret.CleanupResponse.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("cleanup failed", e);
                if (metrics != null) {
                    metrics.recordCleanup("error");
                    metrics.recordGrpcError("cleanup");
                }
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }

        // --- Proto <-> SDK conversion ---

        private static Operation fromProto(Regret.Operation proto) {
            String opId = proto.getOpId();
            if (proto.hasPut()) {
                var p = proto.getPut();
                if (p.getSequence()) {
                    return new Operation(opId, OpType.SEQUENCE_PUT,
                            new io.regret.sdk.payload.SequencePutPayload(p.getPrefix(),
                                    p.getValue().toString(StandardCharsets.UTF_8), p.getDelta()).toBytes());
                }
                if (p.getEphemeral()) {
                    return new Operation(opId, OpType.EPHEMERAL_PUT,
                            new io.regret.sdk.payload.EphemeralPutPayload(p.getKey(),
                                    p.getValue().toString(StandardCharsets.UTF_8)).toBytes());
                }
                if (!p.getIndexName().isEmpty()) {
                    return new Operation(opId, OpType.INDEXED_PUT,
                            new io.regret.sdk.payload.IndexedPutPayload(p.getKey(),
                                    p.getValue().toString(StandardCharsets.UTF_8),
                                    p.getIndexName(), p.getIndexKey()).toBytes());
                }
                return new Operation(opId, OpType.PUT,
                        new io.regret.sdk.payload.PutPayload(p.getKey(),
                                p.getValue().toString(StandardCharsets.UTF_8)).toBytes());
            }
            if (proto.hasGet()) {
                var g = proto.getGet();
                OpType type = switch (g.getComparison()) {
                    case "floor" -> OpType.GET_FLOOR;
                    case "ceiling" -> OpType.GET_CEILING;
                    case "lower" -> OpType.GET_LOWER;
                    case "higher" -> OpType.GET_HIGHER;
                    default -> OpType.GET;
                };
                return new Operation(opId, type,
                        new io.regret.sdk.payload.GetPayload(g.getKey()).toBytes());
            }
            if (proto.hasDelete()) {
                return new Operation(opId, OpType.DELETE,
                        new io.regret.sdk.payload.DeletePayload(proto.getDelete().getKey()).toBytes());
            }
            if (proto.hasDeleteRange()) {
                var dr = proto.getDeleteRange();
                return new Operation(opId, OpType.DELETE_RANGE,
                        new io.regret.sdk.payload.DeleteRangePayload(dr.getStart(), dr.getEnd()).toBytes());
            }
            if (proto.hasScan()) {
                var s = proto.getScan();
                if (!s.getIndexName().isEmpty()) {
                    return new Operation(opId, OpType.INDEXED_RANGE_SCAN,
                            new io.regret.sdk.payload.IndexedRangeScanPayload(s.getIndexName(),
                                    s.getStart(), s.getEnd()).toBytes());
                }
                return new Operation(opId, OpType.RANGE_SCAN,
                        new io.regret.sdk.payload.RangeScanPayload(s.getStart(), s.getEnd()).toBytes());
            }
            if (proto.hasList()) {
                var l = proto.getList();
                if (!l.getIndexName().isEmpty()) {
                    return new Operation(opId, OpType.INDEXED_LIST,
                            new io.regret.sdk.payload.IndexedListPayload(l.getIndexName(),
                                    l.getStart(), l.getEnd()).toBytes());
                }
                return new Operation(opId, OpType.LIST,
                        new io.regret.sdk.payload.ListPayload(l.getStart(), l.getEnd()).toBytes());
            }
            if (proto.hasCas()) {
                var c = proto.getCas();
                return new Operation(opId, OpType.CAS,
                        new io.regret.sdk.payload.CasPayload(c.getKey(), c.getExpectedVersionId(),
                                c.getNewValue().toString(StandardCharsets.UTF_8)).toBytes());
            }
            throw new IllegalArgumentException("Unknown operation type in proto: " + proto);
        }

        private static Regret.OpResult toProto(OpResult result) {
            Regret.OpResult.Builder b = Regret.OpResult.newBuilder()
                    .setOpId(result.opId())
                    .setStatus(result.status());
            if (result.message() != null) b.setMessage(result.message());

            if (result.data() == null) return b.build();

            switch (result.data()) {
                case OpResult.PutData put -> b.setPut(Regret.PutResult.newBuilder()
                        .setRecord(Regret.Record.newBuilder()
                                .setKey(put.key() != null ? put.key() : "")
                                .setMeta(Regret.RecordMeta.newBuilder().setVersionId(put.versionId()))));
                case OpResult.GetData get -> {
                    var rec = Regret.Record.newBuilder()
                            .setKey(get.key() != null ? get.key() : "")
                            .setMeta(Regret.RecordMeta.newBuilder().setVersionId(get.versionId()));
                    if (get.value() != null) {
                        rec.setPayload(ByteString.copyFrom(get.value(), StandardCharsets.UTF_8));
                    }
                    b.setGet(Regret.GetResult.newBuilder().setRecord(rec));
                }
                case OpResult.DeleteData ignored -> b.setDelete(Regret.DeleteResult.newBuilder());
                case OpResult.DeleteRangeData ignored -> b.setDeleteRange(Regret.DeleteRangeResult.newBuilder());
                case OpResult.ScanData scan -> {
                    var sr = Regret.ScanResult.newBuilder();
                    for (var r : scan.records()) {
                        sr.addRecords(Regret.Record.newBuilder()
                                .setKey(r.key())
                                .setMeta(Regret.RecordMeta.newBuilder().setVersionId(r.versionId()))
                                .setPayload(ByteString.copyFrom(r.value(), StandardCharsets.UTF_8)));
                    }
                    b.setScan(sr);
                }
                case OpResult.ListData list -> b.setList(Regret.ListResult.newBuilder().addAllKeys(list.keys()));
                case OpResult.CasData cas -> b.setCas(Regret.CasResult.newBuilder()
                        .setRecord(Regret.Record.newBuilder()
                                .setKey(cas.key() != null ? cas.key() : "")
                                .setMeta(Regret.RecordMeta.newBuilder().setVersionId(cas.versionId()))));
            }
            return b.build();
        }
    }
}

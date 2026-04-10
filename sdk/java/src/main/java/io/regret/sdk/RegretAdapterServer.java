package io.regret.sdk;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import regret.v1.AdapterServiceGrpc;
import regret.v1.Regret;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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

                // Execute all ops concurrently, recording per-op latency + status.
                List<CompletableFuture<OpResult>> futures = new ArrayList<>();
                for (Regret.Operation protoOp : request.getOpsList()) {
                    final Operation op = new Operation(
                            protoOp.getOpId(),
                            OpType.fromString(protoOp.getOpType()),
                            protoOp.getPayload().toByteArray());
                    final String opTypeStr = protoOp.getOpType();
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        long startNanos = System.nanoTime();
                        OpResult result;
                        String status = "error";
                        try {
                            result = adapter.executeOp(op);
                            status = result != null && result.status() != null ? result.status() : "unknown";
                            return result;
                        } finally {
                            double elapsed = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                            if (metrics != null) metrics.recordOp(opTypeStr, status, elapsed);
                        }
                    }));
                }

                // Wait for all
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // Build response
                Regret.BatchResponse.Builder rb = Regret.BatchResponse.newBuilder()
                        .setBatchId(request.getBatchId());
                for (var f : futures) {
                    OpResult result = f.get();
                    Regret.OpResult.Builder b = Regret.OpResult.newBuilder()
                            .setOpId(result.opId())
                            .setStatus(result.status());
                    if (result.payload() != null) b.setPayload(ByteString.copyFrom(result.payload()));
                    if (result.message() != null) b.setMessage(result.message());
                    rb.addResults(b.build());
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
                    if (r.getValue() != null) rb.setValue(ByteString.copyFrom(r.getValue()));
                    if (r.getMetadata() != null) rb.putAllMetadata(r.getMetadata());
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
    }
}

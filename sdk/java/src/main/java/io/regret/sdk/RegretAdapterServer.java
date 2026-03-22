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
        int port = 9090;
        Server server = ServerBuilder.forPort(port)
                .addService(new AdapterServiceImpl(adapter))
                .build()
                .start();

        LOG.info("Adapter gRPC server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down adapter server");
            server.shutdown();
        }));
        server.awaitTermination();
    }

    private static class AdapterServiceImpl extends AdapterServiceGrpc.AdapterServiceImplBase {

        private final Adapter adapter;

        AdapterServiceImpl(Adapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public StreamObserver<Regret.ExecuteRequest> execute(
                StreamObserver<Regret.ExecuteResponse> responseObserver) {

            return new StreamObserver<>() {
                private final List<CompletableFuture<Void>> pending = new ArrayList<>();

                @Override
                public void onNext(Regret.ExecuteRequest request) {
                    if (request.hasOp()) {
                        Regret.Operation protoOp = request.getOp();
                        Operation op = new Operation(
                                protoOp.getOpId(),
                                OpType.fromString(protoOp.getOpType()),
                                protoOp.getPayload().toByteArray());

                        // Execute concurrently, send result when done
                        CompletableFuture<Void> future = CompletableFuture
                                .supplyAsync(() -> adapter.executeOp(op))
                                .thenAccept(result -> {
                                    Regret.OpResult.Builder b = Regret.OpResult.newBuilder()
                                            .setOpId(result.opId())
                                            .setStatus(result.status());
                                    if (result.payload() != null) b.setPayload(ByteString.copyFrom(result.payload()));
                                    if (result.message() != null) b.setMessage(result.message());
                                    synchronized (responseObserver) {
                                        responseObserver.onNext(Regret.ExecuteResponse.newBuilder()
                                                .setResult(b.build()).build());
                                    }
                                });
                        pending.add(future);

                    } else if (request.hasFence()) {
                        // Wait for all pending ops, then ack
                        long fenceId = request.getFence().getFenceId();
                        try {
                            CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
                        } catch (Exception e) {
                            LOG.error("Error at fence {}", fenceId, e);
                        }
                        pending.clear();
                        synchronized (responseObserver) {
                            responseObserver.onNext(Regret.ExecuteResponse.newBuilder()
                                    .setFenceAck(Regret.FenceAck.newBuilder().setFenceId(fenceId).build())
                                    .build());
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    LOG.error("Execute stream error", t);
                }

                @Override
                public void onCompleted() {
                    try {
                        CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
                    } catch (Exception e) {
                        LOG.error("Error draining pending ops", e);
                    }
                    pending.clear();
                    synchronized (responseObserver) {
                        responseObserver.onCompleted();
                    }
                }
            };
        }

        @Override
        public void readState(Regret.ReadStateRequest request,
                StreamObserver<Regret.ReadStateResponse> responseObserver) {
            try {
                List<Record> records = adapter.readState(request.getKeyPrefix());
                Regret.ReadStateResponse.Builder b = Regret.ReadStateResponse.newBuilder();
                for (Record r : records) {
                    Regret.Record.Builder rb = Regret.Record.newBuilder().setKey(r.getKey());
                    if (r.getValue() != null) rb.setValue(ByteString.copyFrom(r.getValue()));
                    if (r.getMetadata() != null) rb.putAllMetadata(r.getMetadata());
                    b.addRecords(rb.build());
                }
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("readState failed", e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }

        @Override
        public void cleanup(Regret.CleanupRequest request,
                StreamObserver<Regret.CleanupResponse> responseObserver) {
            try {
                adapter.cleanup(request.getKeyPrefix());
                responseObserver.onNext(Regret.CleanupResponse.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("cleanup failed", e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }
    }
}

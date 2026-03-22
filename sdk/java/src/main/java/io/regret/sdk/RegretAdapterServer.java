package io.regret.sdk;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import regret.v1.AdapterServiceGrpc;
import regret.v1.PilotServiceGrpc;
import regret.v1.Regret;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * gRPC server that bridges proto messages to the user's {@link Adapter} implementation.
 */
public class RegretAdapterServer {

    private static final Logger LOG = LoggerFactory.getLogger(RegretAdapterServer.class);

    /**
     * Start the adapter server. Reads configuration from environment variables,
     * registers with the pilot, and serves gRPC on port 9090.
     */
    public static void serve(Adapter adapter) throws Exception {
        String pilotAddr = System.getenv("REGRET_PILOT_ADDR");
        String hypothesisId = System.getenv("REGRET_HYPOTHESIS_ID");
        String adapterName = System.getenv("REGRET_ADAPTER_NAME");

        if (pilotAddr == null || hypothesisId == null || adapterName == null) {
            throw new IllegalStateException(
                    "Missing required env vars: REGRET_PILOT_ADDR, REGRET_HYPOTHESIS_ID, REGRET_ADAPTER_NAME");
        }

        int port = 9090;

        // Start gRPC server
        Server server = ServerBuilder.forPort(port)
                .addService(new AdapterServiceImpl(adapter))
                .build()
                .start();

        LOG.info("Adapter gRPC server started on port {}", port);

        // Register with pilot
        String grpcAddr = getHostname() + ":" + port;
        registerWithPilot(pilotAddr, hypothesisId, adapterName, grpcAddr);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down adapter server");
            try {
                adapter.cleanup();
            } catch (Exception e) {
                LOG.error("Cleanup failed", e);
            }
            server.shutdown();
        }));

        server.awaitTermination();
    }

    private static void registerWithPilot(
            String pilotAddr, String hypothesisId, String adapterName, String grpcAddr) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(pilotAddr)
                .usePlaintext()
                .build();

        try {
            PilotServiceGrpc.PilotServiceBlockingStub stub =
                    PilotServiceGrpc.newBlockingStub(channel);

            Regret.RegisterResponse response = stub.registerAdapter(
                    Regret.RegisterRequest.newBuilder()
                            .setHypothesisId(hypothesisId)
                            .setAdapterName(adapterName)
                            .setGrpcAddr(grpcAddr)
                            .build());

            if (response.getAccepted()) {
                LOG.info("Registered with pilot: hypothesisId={}, adapterName={}, grpcAddr={}",
                        hypothesisId, adapterName, grpcAddr);
            } else {
                LOG.error("Pilot rejected registration");
            }
        } finally {
            channel.shutdownNow();
        }
    }

    private static String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    /**
     * gRPC service implementation that delegates to the user's Adapter.
     */
    private static class AdapterServiceImpl extends AdapterServiceGrpc.AdapterServiceImplBase {

        private final Adapter adapter;

        AdapterServiceImpl(Adapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public void executeBatch(
                Regret.BatchRequest request,
                StreamObserver<Regret.BatchResponse> responseObserver) {

            try {
                // Convert proto to SDK types
                List<Item> items = new ArrayList<>();
                for (Regret.Item protoItem : request.getItemsList()) {
                    if (protoItem.hasFence()) {
                        items.add(new Item.Fence());
                    } else if (protoItem.hasOp()) {
                        Regret.Operation protoOp = protoItem.getOp();
                        items.add(new Item.Op(new Operation(
                                protoOp.getOpId(),
                                protoOp.getOpType(),
                                protoOp.getPayload().toByteArray())));
                    }
                }

                Batch batch = new Batch(request.getBatchId(), request.getTraceId(), items);

                // Set trace ID in MDC for logging
                org.slf4j.MDC.put("trace_id", request.getTraceId());

                // Call user adapter
                BatchResponse sdkResponse = adapter.executeBatch(batch);

                // Convert back to proto
                Regret.BatchResponse.Builder responseBuilder = Regret.BatchResponse.newBuilder()
                        .setBatchId(sdkResponse.batchId());

                for (OpResult result : sdkResponse.results()) {
                    Regret.OpResult.Builder opResultBuilder = Regret.OpResult.newBuilder()
                            .setOpId(result.opId())
                            .setStatus(result.status());

                    if (result.payload() != null) {
                        opResultBuilder.setPayload(ByteString.copyFrom(result.payload()));
                    }
                    if (result.message() != null) {
                        opResultBuilder.setMessage(result.message());
                    }

                    responseBuilder.addResults(opResultBuilder.build());
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();

            } catch (Exception e) {
                LOG.error("executeBatch failed", e);
                responseObserver.onError(
                        io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            } finally {
                org.slf4j.MDC.remove("trace_id");
            }
        }

        @Override
        public void readState(
                Regret.ReadStateRequest request,
                StreamObserver<Regret.ReadStateResponse> responseObserver) {

            try {
                List<Record> records = adapter.readState(request.getKeysList());

                Regret.ReadStateResponse.Builder responseBuilder =
                        Regret.ReadStateResponse.newBuilder();

                for (Record record : records) {
                    Regret.Record.Builder recBuilder = Regret.Record.newBuilder()
                            .setKey(record.getKey());

                    if (record.getValue() != null) {
                        recBuilder.setValue(ByteString.copyFrom(record.getValue()));
                    }

                    if (record.getMetadata() != null) {
                        recBuilder.putAllMetadata(record.getMetadata());
                    }

                    responseBuilder.addRecords(recBuilder.build());
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();

            } catch (Exception e) {
                LOG.error("readState failed", e);
                responseObserver.onError(
                        io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }

        @Override
        public void cleanup(
                Regret.CleanupRequest request,
                StreamObserver<Regret.CleanupResponse> responseObserver) {

            try {
                adapter.cleanup();
                responseObserver.onNext(Regret.CleanupResponse.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("cleanup failed", e);
                responseObserver.onError(
                        io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        }
    }
}

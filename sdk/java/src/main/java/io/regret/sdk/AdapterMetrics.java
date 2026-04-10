package io.regret.sdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * OpenTelemetry metrics for the adapter SDK.
 *
 * <p>Exposes a Prometheus text endpoint that the pilot scrapes periodically.
 * Default port is 9091 (override via env {@code REGRET_METRICS_PORT}).
 *
 * <p>Instruments cover the three gRPC RPCs served by
 * {@link RegretAdapterServer}:
 * <ul>
 *   <li>{@code executeBatch} — per-op counters and latency histograms plus
 *       batch-level duration and size histograms.</li>
 *   <li>{@code readState} — duration and record-count histograms.</li>
 *   <li>{@code cleanup} — a status counter.</li>
 * </ul>
 * Any RPC-level exception increments {@code regret_adapter_grpc_errors_total}.
 */
public final class AdapterMetrics implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AdapterMetrics.class);

    public static final int DEFAULT_PORT = 9091;

    static final AttributeKey<String> OP_TYPE = AttributeKey.stringKey("op_type");
    static final AttributeKey<String> STATUS = AttributeKey.stringKey("status");
    static final AttributeKey<String> METHOD = AttributeKey.stringKey("method");
    private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

    /**
     * Explicit bucket boundaries for latency histograms in seconds.
     *
     * <p>OTel Java's default boundaries are {@code [0, 5, 10, 25, … 10000]}
     * which are sized for milliseconds; applied to a seconds-unit histogram
     * they collapse everything under 5 s into one bucket and make quantiles
     * useless. These boundaries cover 100 µs → 10 s with Prometheus-style
     * resolution so p50/p95/p99 actually mean something.
     */
    private static final List<Double> LATENCY_BOUNDS_SECONDS = Arrays.asList(
            0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
            0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0);

    private final SdkMeterProvider meterProvider;
    private final PrometheusHttpServer prometheusServer;

    private final LongCounter opTotal;
    private final DoubleHistogram opDurationSeconds;
    private final DoubleHistogram batchDurationSeconds;
    private final LongHistogram batchSize;
    private final DoubleHistogram readStateDurationSeconds;
    private final LongHistogram readStateRecords;
    private final LongCounter cleanupTotal;
    private final LongCounter grpcErrorsTotal;

    private AdapterMetrics(SdkMeterProvider meterProvider, PrometheusHttpServer prometheusServer) {
        this.meterProvider = meterProvider;
        this.prometheusServer = prometheusServer;

        Meter meter = meterProvider.get("io.regret.sdk");

        this.opTotal = meter.counterBuilder("regret_adapter_op_total")
                .setDescription("Total adapter operations executed")
                .build();
        this.opDurationSeconds = meter.histogramBuilder("regret_adapter_op_duration_seconds")
                .setDescription("Per-operation execution latency")
                .setUnit("s")
                .setExplicitBucketBoundariesAdvice(LATENCY_BOUNDS_SECONDS)
                .build();
        this.batchDurationSeconds = meter.histogramBuilder("regret_adapter_batch_duration_seconds")
                .setDescription("End-to-end executeBatch duration")
                .setUnit("s")
                .setExplicitBucketBoundariesAdvice(LATENCY_BOUNDS_SECONDS)
                .build();
        this.batchSize = meter.histogramBuilder("regret_adapter_batch_size")
                .setDescription("Number of ops per executeBatch request")
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(Arrays.asList(
                        1L, 2L, 5L, 10L, 25L, 50L, 100L, 250L, 500L, 1000L))
                .build();
        this.readStateDurationSeconds = meter.histogramBuilder("regret_adapter_read_state_duration_seconds")
                .setDescription("readState RPC duration")
                .setUnit("s")
                .setExplicitBucketBoundariesAdvice(LATENCY_BOUNDS_SECONDS)
                .build();
        this.readStateRecords = meter.histogramBuilder("regret_adapter_read_state_records")
                .setDescription("Number of records returned by readState")
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(Arrays.asList(
                        1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L))
                .build();
        this.cleanupTotal = meter.counterBuilder("regret_adapter_cleanup_total")
                .setDescription("Total cleanup RPC invocations")
                .build();
        this.grpcErrorsTotal = meter.counterBuilder("regret_adapter_grpc_errors_total")
                .setDescription("RPCs that failed with an exception")
                .build();
    }

    /**
     * Build metrics with a Prometheus HTTP exporter on the given port.
     * Pass {@code 0} or a negative value to use {@link #DEFAULT_PORT}.
     */
    public static AdapterMetrics create(String serviceName, int port) {
        int resolvedPort = port > 0 ? port : DEFAULT_PORT;

        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.of(
                        SERVICE_NAME,
                        serviceName != null ? serviceName : "regret-adapter")));

        PrometheusHttpServer exporter = PrometheusHttpServer.builder()
                .setPort(resolvedPort)
                .build();

        SdkMeterProvider provider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(exporter)
                .build();

        LOG.info("Prometheus metrics exporter listening on :{}/metrics", resolvedPort);
        return new AdapterMetrics(provider, exporter);
    }

    /** Build with default port taken from {@code REGRET_METRICS_PORT} or {@link #DEFAULT_PORT}. */
    public static AdapterMetrics createFromEnv(String serviceName) {
        int port = DEFAULT_PORT;
        String env = System.getenv("REGRET_METRICS_PORT");
        if (env != null && !env.isBlank()) {
            try {
                port = Integer.parseInt(env.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid REGRET_METRICS_PORT='{}', falling back to {}", env, DEFAULT_PORT);
            }
        }
        return create(serviceName, port);
    }

    // ── Recording helpers ────────────────────────────────────────────────

    public void recordOp(String opType, String status, double durationSeconds) {
        Attributes attrs = Attributes.of(OP_TYPE, nullSafe(opType), STATUS, nullSafe(status));
        opTotal.add(1, attrs);
        opDurationSeconds.record(durationSeconds, Attributes.of(OP_TYPE, nullSafe(opType)));
    }

    public void recordBatch(int opCount, double durationSeconds) {
        batchDurationSeconds.record(durationSeconds);
        batchSize.record(opCount);
    }

    public void recordReadState(int recordCount, double durationSeconds) {
        readStateDurationSeconds.record(durationSeconds);
        readStateRecords.record(recordCount);
    }

    public void recordCleanup(String status) {
        cleanupTotal.add(1, Attributes.of(STATUS, nullSafe(status)));
    }

    public void recordGrpcError(String method) {
        grpcErrorsTotal.add(1, Attributes.of(METHOD, nullSafe(method)));
    }

    private static String nullSafe(String s) {
        return s == null ? "unknown" : s;
    }

    @Override
    public void close() {
        if (prometheusServer != null) {
            prometheusServer.close();
        }
        if (meterProvider != null) {
            meterProvider.close();
        }
    }
}

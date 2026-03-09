package ai.pipestream.module.pipelineprobe;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.module.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Test processor for integration testing and as a reference implementation.
 * Includes full observability with metrics and tracing.
 */
@Singleton
@GrpcService
public class TestProcessorServiceImpl implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(TestProcessorServiceImpl.class);

    @ConfigProperty(name = "test.processor.name", defaultValue = "test-processor")
    String processorName;

    @ConfigProperty(name = "test.processor.delay.ms", defaultValue = "0")
    Long processingDelayMs;

    @ConfigProperty(name = "test.processor.failure.rate", defaultValue = "0.0")
    Double randomFailureRate;

    @Inject
    MeterRegistry registry;

    public void setProcessingDelayMs(long delayMs) {
        this.processingDelayMs = delayMs;
        LOG.infof("Processing delay set to %d ms", delayMs);
    }

    public void setRandomFailureRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException("Failure rate must be between 0.0 and 1.0");
        }
        this.randomFailureRate = rate;
        LOG.infof("Random failure rate set to %.2f", rate);
    }

    private Counter processedDocuments;
    private Counter failedDocuments;
    private Timer processingTimer;

    @jakarta.annotation.PostConstruct
    void init() {
        if (randomFailureRate < 0.0 || randomFailureRate > 1.0) {
            throw new IllegalArgumentException("test.processor.failure.rate must be between 0.0 and 1.0, got: " + randomFailureRate);
        }

        if (randomFailureRate > 0.0) {
            LOG.infof("Test processor configured with random failure rate: %.2f", randomFailureRate);
        }

        this.processedDocuments = Counter.builder("test.processor.documents.processed")
                .description("Number of documents processed")
                .register(registry);

        this.failedDocuments = Counter.builder("test.processor.documents.failed")
                .description("Number of documents that failed processing")
                .register(registry);

        this.processingTimer = Timer.builder("test.processor.processing.time")
                .description("Time taken to process documents")
                .register(registry);
    }

    @Override
    public Uni<ProcessDataResponse> processData(ProcessDataRequest request) {
        return processingTimer.record(() -> processDataInternal(request));
    }

    Uni<ProcessDataResponse> processDataInternal(ProcessDataRequest request) {
        LOG.infof("TestProcessor received request for document: %s",
                request.hasDocument() ? request.getDocument().getDocId() : "no-document");

        // Simulate random failures if configured
        if (randomFailureRate > 0.0) {
            double random = Math.random();
            if (random < randomFailureRate) {
                LOG.infof("Simulating random failure (random=%.2f, threshold=%.2f)", random, randomFailureRate);

                ProcessDataResponse errorResponse = ProcessDataResponse.newBuilder()
                        .setSuccess(false)
                        .addProcessorLogs("TestProcessor: Simulated random failure")
                        .setErrorDetails(Struct.newBuilder()
                                .putFields("error_type", Value.newBuilder().setStringValue("SimulatedFailure").build())
                                .putFields("error_message", Value.newBuilder().setStringValue("Simulated random failure").build())
                                .build())
                        .build();

                return Uni.createFrom().item(errorResponse);
            }
        }

        // Add artificial delay if configured
        Uni<Void> delay = processingDelayMs > 0
                ? Uni.createFrom().<Void>nullItem().onItem().delayIt().by(java.time.Duration.ofMillis(processingDelayMs))
                : Uni.createFrom().voidItem();

        return delay.onItem().transformToUni(v -> {
            try {
                ProcessDataResponse.Builder responseBuilder = ProcessDataResponse.newBuilder()
                        .setSuccess(true)
                        .addProcessorLogs("TestProcessor: Starting document processing");

                if (request.hasDocument()) {
                    PipeDoc doc = request.getDocument();

                    if (request.hasMetadata()) {
                        LOG.debugf("Processing document from pipeline: %s, step: %s, hop: %d",
                                request.getMetadata().getPipelineName(),
                                request.getMetadata().getPipeStepName(),
                                request.getMetadata().getCurrentHopNumber());
                    }

                    Struct.Builder customDataBuilder = doc.getSearchMetadata().hasCustomFields()
                            ? doc.getSearchMetadata().getCustomFields().toBuilder()
                            : Struct.newBuilder();

                    customDataBuilder
                            .putFields("processed_by", Value.newBuilder().setStringValue(processorName).build())
                            .putFields("processing_timestamp", Value.newBuilder().setStringValue(String.valueOf(System.currentTimeMillis())).build())
                            .putFields("test_module_version", Value.newBuilder().setStringValue("1.0.0").build());

                    if (request.hasConfig() && request.getConfig().getConfigParamsCount() > 0) {
                        request.getConfig().getConfigParamsMap().forEach((key, value) ->
                                customDataBuilder.putFields("config_" + key, Value.newBuilder().setStringValue(value).build())
                        );
                    }

                    String mode = "test";
                    boolean requireSchema = false;
                    boolean simulateError = false;

                    if (request.hasConfig() && request.getConfig().hasJsonConfig()) {
                        Struct config = request.getConfig().getJsonConfig();
                        if (config.containsFields("mode")) {
                            mode = config.getFieldsMap().get("mode").getStringValue();
                        }
                        if (config.containsFields("requireSchema")) {
                            requireSchema = config.getFieldsMap().get("requireSchema").getBoolValue();
                        }
                        if (config.containsFields("simulateError")) {
                            simulateError = config.getFieldsMap().get("simulateError").getBoolValue();
                        }
                    }

                    if (simulateError) {
                        throw new RuntimeException("Simulated error for testing");
                    }

                    if (mode.equals("validate") || requireSchema) {
                        if (!doc.getSearchMetadata().hasTitle() || doc.getSearchMetadata().getTitle().isEmpty()) {
                            throw new IllegalArgumentException("Schema validation failed: title is required");
                        }
                        if (!doc.getSearchMetadata().hasBody() || doc.getSearchMetadata().getBody().isEmpty()) {
                            throw new IllegalArgumentException("Schema validation failed: body is required");
                        }
                        responseBuilder.addProcessorLogs("TestProcessor: Schema validation passed");
                    }

                    PipeDoc modifiedDoc = doc.toBuilder()
                            .setSearchMetadata(doc.getSearchMetadata().toBuilder()
                                    .setCustomFields(customDataBuilder.build())
                                    .build())
                            .build();

                    responseBuilder.setOutputDoc(modifiedDoc);
                    responseBuilder.addProcessorLogs("TestProcessor: Added metadata to document");
                    responseBuilder.addProcessorLogs("TestProcessor: Document processed successfully");

                    processedDocuments.increment();
                } else {
                    responseBuilder.addProcessorLogs("TestProcessor: No document provided");
                }

                ProcessDataResponse response = responseBuilder.build();
                LOG.infof("TestProcessor returning success: %s", response.getSuccess());

                return Uni.createFrom().item(response);

            } catch (Exception e) {
                LOG.errorf(e, "Error in TestProcessor");
                failedDocuments.increment();

                ProcessDataResponse errorResponse = ProcessDataResponse.newBuilder()
                        .setSuccess(false)
                        .addProcessorLogs("TestProcessor: Error - " + e.getMessage())
                        .setErrorDetails(Struct.newBuilder()
                                .putFields("error_type", Value.newBuilder().setStringValue(e.getClass().getSimpleName()).build())
                                .putFields("error_message", Value.newBuilder().setStringValue(e.getMessage()).build())
                                .build())
                        .build();

                return Uni.createFrom().item(errorResponse);
            }
        });
    }

    @Override
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        LOG.debugf("TestProcessor registration requested");

        return Uni.createFrom().item(GetServiceRegistrationResponse.newBuilder()
                .setModuleName("test-processor")
                .setVersion("1.0.0")
                .build());
    }
}

package ai.pipestream.module.pipelineprobe;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.module.v1.*;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 * Base test class for TestProcessor service testing.
 * This abstract class can be extended by both unit tests (@QuarkusTest)
 * and integration tests (@QuarkusIntegrationTest).
 */
public abstract class TestProcessorTestBase {

    private static final Logger LOG = Logger.getLogger(TestProcessorTestBase.class);

    protected abstract PipeStepProcessorService getTestProcessor();

    @Test
    void testProcessData() {
        // Create test document
        PipeDoc document = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Test Document")
                        .setBody("This is test content for the TestProcessor")
                        .setCustomFields(Struct.newBuilder()
                                .putFields("source", Value.newBuilder().setStringValue("test").build())
                                .build())
                        .build())
                .build();

        // Create request with metadata
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(document)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .putConfigParams("mode", "test")
                        .putConfigParams("addMetadata", "true")
                        .build())
                .build();

        // Process and verify
        LOG.debugf("Sending test request with document: %s", document.getDocId());

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Processing should be successful", response.getSuccess(), is(true));
        assertThat("Response should have output document", response.hasOutputDoc(), is(true));
        assertThat("Output document ID should match input", response.getOutputDoc().getDocId(), is(document.getDocId()));

        // Verify custom_data was enhanced
        Struct customData = response.getOutputDoc().getSearchMetadata().getCustomFields();
        assertThat("Custom data should contain processed_by", customData.getFieldsMap(), hasKey("processed_by"));
        assertThat("Custom data should contain processing_timestamp", customData.getFieldsMap(), hasKey("processing_timestamp"));
        assertThat("Custom data should contain test_module_version", customData.getFieldsMap(), hasKey("test_module_version"));
        assertThat("Custom data should contain config_mode", customData.getFieldsMap(), hasKey("config_mode"));
        assertThat("Custom data should contain config_addMetadata", customData.getFieldsMap(), hasKey("config_addMetadata"));

        // Verify logs
        assertThat("Should have processor logs", response.getProcessorLogsList(), is(not(empty())));
        assertThat("Should contain success log message", response.getProcessorLogsList(), hasItem(containsString("TestProcessor: Document processed successfully")));

        LOG.debugf("Test completed successfully, response: %s", response.getSuccess());
    }

    @Test
    void testProcessDataWithoutDocument() {
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .build();

        LOG.debugf("Sending test request without document");

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Processing without document should succeed", response.getSuccess(), is(true));
        assertThat("Should not have output document when no input", response.hasOutputDoc(), is(false));
        assertThat("Should contain no document log message", response.getProcessorLogsList(), hasItem(containsString("TestProcessor: No document provided")));

        LOG.debugf("Test without document completed successfully");
    }

    @Test
    void testGetServiceRegistration() {
        LOG.debugf("Testing service registration");

        UniAssertSubscriber<GetServiceRegistrationResponse> subscriber = getTestProcessor()
                .getServiceRegistration(GetServiceRegistrationRequest.newBuilder().build())
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        GetServiceRegistrationResponse registration = subscriber.awaitItem().getItem();

        assertThat("Registration response should not be null", registration, is(notNullValue()));
        assertThat("Module name should not be empty", registration.getModuleName(), is(not(emptyString())));

        LOG.debugf("Service registration test completed, module name: %s", registration.getModuleName());
    }

    @Test
    void testProcessDataWithDelay() {
        // Create test document
        PipeDoc document = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Delay Test Document")
                        .setBody("Testing processing delay")
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(document)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor-delay")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .build();

        LOG.debugf("Sending test request with delay");

        long startTime = System.currentTimeMillis();

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        long endTime = System.currentTimeMillis();

        assertThat("Delay test response should not be null", response, is(notNullValue()));
        assertThat("Delay test processing should succeed", response.getSuccess(), is(true));
        assertThat("Delay test should have output document", response.hasOutputDoc(), is(true));

        LOG.debugf("Test with delay completed in %d ms", endTime - startTime);
    }

    @Test
    void testSchemaValidationMode() {
        // Test with valid document
        PipeDoc validDocument = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Valid Document")
                        .setBody("This document has all required fields")
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(validDocument)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.newBuilder()
                                .putFields("mode", Value.newBuilder().setStringValue("validate").build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing schema validation with valid document");

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        assertThat("Schema validation response should not be null", response, is(notNullValue()));
        assertThat("Schema validation should succeed", response.getSuccess(), is(true));
        assertThat("Should contain schema validation passed log", response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")));

        LOG.debugf("Schema validation test with valid document passed");
    }

    @Test
    void testSchemaValidationModeWithMissingTitle() {
        // Test with document missing title
        PipeDoc invalidDocument = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("This document is missing title")
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(invalidDocument)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.newBuilder()
                                .putFields("mode", Value.newBuilder().setStringValue("validate").build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing schema validation with missing title");

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        assertThat("Invalid document response should not be null", response, is(notNullValue()));
        assertThat("Invalid document processing should fail", response.getSuccess(), is(false));
        assertThat("Should have error details", response.hasErrorDetails(), is(true));
        assertThat("Error message should contain schema validation failure", response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(), containsString("Schema validation failed: title is required"));

        LOG.debugf("Schema validation test with missing title correctly failed");
    }

    @Test
    void testSchemaValidationWithRequireSchemaFlag() {
        // Test requireSchema flag overrides mode
        PipeDoc validDocument = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Valid Document")
                        .setBody("Testing requireSchema flag")
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(validDocument)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.newBuilder()
                                .putFields("mode", Value.newBuilder().setStringValue("test").build())
                                .putFields("requireSchema", Value.newBuilder().setBoolValue(true).build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing requireSchema flag");

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        assertThat("RequireSchema flag response should not be null", response, is(notNullValue()));
        assertThat("RequireSchema flag processing should succeed", response.getSuccess(), is(true));
        assertThat("Should contain schema validation passed log from requireSchema flag", response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")));

        LOG.debugf("RequireSchema flag test passed");
    }

    @Test
    void testSimulateError() {
        PipeDoc document = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Error Test Document")
                        .setBody("Testing error simulation")
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(document)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.newBuilder()
                                .putFields("simulateError", Value.newBuilder().setBoolValue(true).build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing error simulation");

        UniAssertSubscriber<ProcessDataResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        assertThat("Error simulation response should not be null", response, is(notNullValue()));
        assertThat("Error simulation should fail", response.getSuccess(), is(false));
        assertThat("Error simulation should have error details", response.hasErrorDetails(), is(true));
        assertThat("Error message should contain simulation text", response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(), containsString("Simulated error for testing"));

        LOG.debugf("Error simulation test passed");
    }
}

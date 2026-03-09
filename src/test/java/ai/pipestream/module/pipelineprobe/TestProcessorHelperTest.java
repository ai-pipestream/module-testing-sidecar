package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;
import org.jboss.logging.Logger;

import static ai.pipestream.module.pipelineprobe.TestProcessorHelper.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 * Tests for the TestProcessorHelper utility class.
 * Demonstrates how to use the helper to test various scenarios.
 */
@QuarkusTest
class TestProcessorHelperTest {

    private static final Logger LOG = Logger.getLogger(TestProcessorHelperTest.class);

    @GrpcClient
    PipeStepProcessorService pipeStepProcessorService;

    @Test
    void testHelperWithSimpleRequest() {
        // Given
        LOG.debug("Testing TestProcessorHelper with simple request");
        ProcessDataRequest request = createSimpleRequest();

        // When
        UniAssertSubscriber<ProcessDataResponse> subscriber = pipeStepProcessorService
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        // Then
        assertThat("Response should be successful", response.getSuccess(), is(true));
        assertThat("Response should have output document", response.hasOutputDoc(), is(true));

        LOG.debugf("Simple request test completed - success: %s, has output: %s",
                  response.getSuccess(), response.hasOutputDoc());
    }

    @Test
    void testHelperWithSchemaValidation() {
        // Given
        LOG.debug("Testing schema validation with valid and invalid documents");

        // Test with valid document
        PipeDoc validDoc = createValidDocument();
        ProcessDataRequest validRequest = createSchemaValidationRequest(validDoc);

        // When - Process valid document
        UniAssertSubscriber<ProcessDataResponse> validSubscriber = pipeStepProcessorService
                .processData(validRequest)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse validResponse = validSubscriber.awaitItem().getItem();

        // Then - valid document
        assertThat("Valid document should be processed successfully", validResponse.getSuccess(), is(true));
        assertThat("Should contain schema validation passed log", validResponse.getProcessorLogsList(), hasItem(containsString("Schema validation passed")));

        // Given - Test with invalid document (missing title)
        PipeDoc invalidDoc = createDocumentWithoutTitle();
        ProcessDataRequest invalidRequest = createSchemaValidationRequest(invalidDoc);

        // When - Process invalid document
        UniAssertSubscriber<ProcessDataResponse> invalidSubscriber = pipeStepProcessorService
                .processData(invalidRequest)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse invalidResponse = invalidSubscriber.awaitItem().getItem();

        // Then - invalid document
        assertThat("Invalid document should fail processing", invalidResponse.getSuccess(), is(false));
        assertThat("Should have error details", invalidResponse.hasErrorDetails(), is(true));
        assertThat("Error message should contain schema validation failure",
                  invalidResponse.getErrorDetails().getFieldsMap().get("error_message").getStringValue(),
                  containsString("Schema validation failed: title is required"));

        LOG.debugf("Schema validation test completed - valid doc success: %s, invalid doc success: %s",
                  validResponse.getSuccess(), invalidResponse.getSuccess());
    }

    @Test
    void testHelperWithCustomConfiguration() {
        // Given
        LOG.debug("Testing custom configuration with document builder and request builder");

        PipeDoc document = documentBuilder()
                .withTitle("Custom Test Document")
                .withBody("Testing custom configuration")
                .withCustomField("version", "1.0.0")
                .withCustomField("processed", false)
                .withCustomField("score", 0.95)
                .build();

        ProcessDataRequest request = requestBuilder()
                .withDocument(document)
                .withPipelineName("custom-pipeline")
                .withStepName("custom-test-step")
                .withStreamId("custom-stream-123")
                .withHopNumber(5)
                .withMode(TestProcessorHelper.ProcessingMode.TRANSFORM)
                .withSchemaValidation(false)
                .withAddMetadata(true)
                .build();

        // When
        UniAssertSubscriber<ProcessDataResponse> subscriber = pipeStepProcessorService
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        // Then
        assertThat("Custom configuration processing should be successful", response.getSuccess(), is(true));
        assertThat("Should have output document", response.hasOutputDoc(), is(true));
        assertThat("Should contain processed_by metadata",
                  response.getOutputDoc().getSearchMetadata().getCustomFields().getFieldsMap(), hasKey("processed_by"));
        assertThat("Should contain processing_timestamp metadata",
                  response.getOutputDoc().getSearchMetadata().getCustomFields().getFieldsMap(), hasKey("processing_timestamp"));
        assertThat("Should contain test_module_version metadata",
                  response.getOutputDoc().getSearchMetadata().getCustomFields().getFieldsMap(), hasKey("test_module_version"));

        LOG.debugf("Custom configuration test completed - document title: %s, success: %s",
                  document.getSearchMetadata().getTitle(), response.getSuccess());
    }

    @Test
    void testHelperWithErrorSimulation() {
        // Given
        LOG.debug("Testing error simulation with error request");
        ProcessDataRequest errorRequest = createErrorRequest();

        // When
        UniAssertSubscriber<ProcessDataResponse> subscriber = pipeStepProcessorService
                .processData(errorRequest)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessDataResponse response = subscriber.awaitItem().getItem();

        // Then
        assertThat("Error simulation should fail", response.getSuccess(), is(false));
        assertThat("Should have error details", response.hasErrorDetails(), is(true));
        assertThat("Error type should be RuntimeException",
                  response.getErrorDetails().getFieldsMap().get("error_type").getStringValue(), is("RuntimeException"));
        assertThat("Error message should contain simulation text",
                  response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(),
                  containsString("Simulated error for testing"));

        LOG.debugf("Error simulation test completed - success: %s, error type: %s",
                  response.getSuccess(),
                  response.getErrorDetails().getFieldsMap().get("error_type").getStringValue());
    }

    @Test
    void testComplexScenarioWithMultipleSteps() {
        // Given
        LOG.debug("Testing complex multi-step pipeline processing scenario");

        // Simulate a multi-step pipeline processing
        PipeDoc initialDoc = documentBuilder()
                .withTitle("Multi-Step Document")
                .withBody("This document will go through multiple processing steps")
                .withCustomField("step_count", 0.0)
                .build();

        // Step 1: Initial processing
        LOG.debug("Executing Step 1: Initial processing");
        ProcessDataRequest step1Request = requestBuilder()
                .withDocument(initialDoc)
                .withStepName("step-1")
                .withHopNumber(1)
                .withMode(ProcessingMode.TEST)
                .build();

        // When - Process Step 1
        ProcessDataResponse step1Response = pipeStepProcessorService
                .processData(step1Request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem().getItem();

        // Then
        assertThat("Step 1 should be successful", step1Response.getSuccess(), is(true));

        // Step 2: Validation
        LOG.debug("Executing Step 2: Validation processing");
        ProcessDataRequest step2Request = requestBuilder()
                .withDocument(step1Response.getOutputDoc())
                .withStepName("step-2-validation")
                .withHopNumber(2)
                .withMode(ProcessingMode.VALIDATE)
                .build();

        // When - Process Step 2
        ProcessDataResponse step2Response = pipeStepProcessorService
                .processData(step2Request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem().getItem();

        // Then
        assertThat("Step 2 validation should be successful", step2Response.getSuccess(), is(true));
        assertThat("Step 2 should contain validation passed log", step2Response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")));

        // Step 3: Transform
        LOG.debug("Executing Step 3: Transform processing");
        ProcessDataRequest step3Request = requestBuilder()
                .withDocument(step2Response.getOutputDoc())
                .withStepName("step-3-transform")
                .withHopNumber(3)
                .withMode(TestProcessorHelper.ProcessingMode.TRANSFORM)
                .withAddMetadata(true)
                .build();

        // When - Process Step 3
        ProcessDataResponse step3Response = pipeStepProcessorService
                .processData(step3Request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem().getItem();

        // Then
        assertThat("Step 3 transform should be successful", step3Response.getSuccess(), is(true));
        assertThat("Step 3 should add processed_by metadata", step3Response.getOutputDoc().getSearchMetadata().getCustomFields().getFieldsMap(), hasKey("processed_by"));

        LOG.debugf("Complex multi-step test completed - Step 1 success: %s, Step 2 success: %s, Step 3 success: %s, final document title: %s",
                  step1Response.getSuccess(), step2Response.getSuccess(), step3Response.getSuccess(),
                  step3Response.getOutputDoc().getSearchMetadata().getTitle());
    }
}

package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Unit test for TestProcessor using Quarkus Test framework.
 */
@QuarkusTest
class TestProcessorUnitTest extends TestProcessorTestBase {

    @GrpcClient
    PipeStepProcessorService pipeStepProcessorService;

    @Override
    protected PipeStepProcessorService getTestProcessor() {
        return pipeStepProcessorService;
    }
}

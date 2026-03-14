package ai.pipestream.module.pipelineprobe;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

import java.util.Map;

/**
 * Global exception mapper that captures unhandled errors, records them in
 * {@link LastErrorTracker}, and returns a structured JSON 500 response.
 * WebApplicationException (404s, etc.) are re-thrown to preserve normal JAX-RS behavior.
 */
public class GlobalExceptionMapper {

    private static final Logger LOG = Logger.getLogger(GlobalExceptionMapper.class);

    @ServerExceptionMapper
    public Response mapUnexpectedErrors(Throwable throwable) {
        if (throwable instanceof WebApplicationException wae) {
            return wae.getResponse();
        }

        if (throwable instanceof IllegalArgumentException) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(Map.of("error", throwable.getMessage()))
                .type(MediaType.APPLICATION_JSON)
                .build();
        }

        String location = "unknown";
        if (throwable.getStackTrace().length > 0) {
            StackTraceElement top = throwable.getStackTrace()[0];
            location = top.getClassName() + "." + top.getMethodName();
        }

        LastErrorTracker.record(location, "unknown", throwable);
        LOG.errorf(throwable, "Unhandled error at %s: %s", location, throwable.getMessage());

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(Map.of(
                "error", "Internal server error",
                "message", throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getSimpleName(),
                "exceptionClass", throwable.getClass().getName(),
                "debugHint", "GET /test-sidecar/v1/debug/last-error for full stack trace"
            ))
            .type(MediaType.APPLICATION_JSON)
            .build();
    }
}

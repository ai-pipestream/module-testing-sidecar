package ai.pipestream.module.pipelineprobe;

import java.time.Instant;
import java.util.Map;

/**
 * In-memory tracker for the most recent 500-level error.
 * Exposed via GET /test-sidecar/v1/debug/last-error for quick diagnosis.
 */
public final class LastErrorTracker {

    private static volatile LastError lastError;

    private LastErrorTracker() {
    }

    public static void record(String endpoint, String method, Throwable throwable) {
        lastError = new LastError(
            Instant.now(),
            endpoint,
            method,
            throwable.getClass().getName(),
            throwable.getMessage(),
            stackTraceToString(throwable)
        );
    }

    public static LastError get() {
        return lastError;
    }

    public static void clear() {
        lastError = null;
    }

    private static String stackTraceToString(Throwable t) {
        if (t == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement e : t.getStackTrace()) {
            sb.append("  at ").append(e.toString()).append("\n");
        }
        Throwable cause = t.getCause();
        if (cause != null && cause != t) {
            sb.append("Caused by: ").append(cause.getClass().getName())
              .append(": ").append(cause.getMessage()).append("\n");
            sb.append(stackTraceToString(cause));
        }
        return sb.toString();
    }

    public record LastError(
        Instant timestamp,
        String endpoint,
        String method,
        String exceptionClass,
        String message,
        String stackTrace
    ) {
        public Map<String, Object> toMap() {
            return Map.of(
                "timestamp", timestamp.toString(),
                "endpoint", endpoint != null ? endpoint : "",
                "method", method != null ? method : "",
                "exceptionClass", exceptionClass != null ? exceptionClass : "",
                "message", message != null ? message : "",
                "stackTrace", stackTrace != null ? stackTrace : ""
            );
        }
    }
}

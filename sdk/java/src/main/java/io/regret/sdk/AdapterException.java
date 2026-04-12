package io.regret.sdk;

/**
 * Exception thrown by adapter implementations to signal error severity.
 *
 * <ul>
 *   <li>{@link #transient_(String, Throwable)} — retryable errors (connection lost, timeout, resource temporarily unavailable).
 *       The SDK will retry with exponential backoff.</li>
 *   <li>{@link #permanent(String, Throwable)} — non-retryable errors (invalid input, unsupported operation).
 *       The SDK will immediately return an error result to the pilot.</li>
 * </ul>
 */
public class AdapterException extends Exception {

    public enum Severity { TRANSIENT, PERMANENT }

    private final Severity severity;

    private AdapterException(Severity severity, String message, Throwable cause) {
        super(message, cause);
        this.severity = severity;
    }

    public Severity severity() {
        return severity;
    }

    public boolean isTransient() {
        return severity == Severity.TRANSIENT;
    }

    public static AdapterException transient_(String message, Throwable cause) {
        return new AdapterException(Severity.TRANSIENT, message, cause);
    }

    public static AdapterException transient_(String message) {
        return new AdapterException(Severity.TRANSIENT, message, null);
    }

    public static AdapterException permanent(String message, Throwable cause) {
        return new AdapterException(Severity.PERMANENT, message, cause);
    }

    public static AdapterException permanent(String message) {
        return new AdapterException(Severity.PERMANENT, message, null);
    }
}

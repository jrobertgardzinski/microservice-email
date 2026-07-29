package com.jrobertgardzinski.mail.boundary;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Boundary guard: only trusted callers presenting the shared secret in the {@code X-Api-Key} header
 * may send mail. Anything else is refused with 401. The service is internal — this keeps it from
 * being an open relay.
 *
 * <p>{@code @Priority(AUTHENTICATION)} because {@link RateLimitFilter} documents itself as running
 * "AFTER the API key", and until 2026-07-29 nothing made that true: this filter had no
 * {@code @Priority} at all, so it defaulted to {@code USER} — the same 5000 the limiter declares —
 * and the specification leaves the order of equal priorities undefined. It happened to work; a
 * Quarkus upgrade re-tying the break the other way would have silently reversed it, and the
 * consequence is not cosmetic. With the limiter first, 120 keyless requests a minute keep the
 * window full, so trusted callers presenting a correct key get 429 instead of 202 — the mail
 * pipeline throttled by exactly the traffic the key exists to turn away, and the log cannot tell
 * that apart from legitimate load.
 */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class ApiKeyFilter implements ContainerRequestFilter {

    @ConfigProperty(name = "mail.api-key")
    String apiKey;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        String presented = requestContext.getHeaderString("X-Api-Key");
        if (presented == null || !constantTimeEquals(apiKey, presented)) {
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
        }
    }

    /** Constant-time comparison, so response timing does not leak how much of the key matched. */
    private static boolean constantTimeEquals(String expected, String presented) {
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), presented.getBytes(StandardCharsets.UTF_8));
    }
}

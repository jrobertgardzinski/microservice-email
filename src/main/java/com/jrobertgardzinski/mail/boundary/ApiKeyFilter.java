package com.jrobertgardzinski.mail.boundary;

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
 */
@Provider
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

package com.jrobertgardzinski.mail.boundary;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
        if (!apiKey.equals(requestContext.getHeaderString("X-Api-Key"))) {
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
        }
    }
}

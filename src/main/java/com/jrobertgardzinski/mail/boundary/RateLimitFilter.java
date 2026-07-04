package com.jrobertgardzinski.mail.boundary;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Boundary guard number two, AFTER the API key: a fixed per-minute window over the whole REST
 * boundary, so one runaway caller cannot flood the SMTP server through us (the Kafka boundary has
 * its own pacing — the topic is the buffer). Over the limit is {@code 429} with a Retry-After.
 * Zero disables the guard.
 */
@Provider
@Priority(Priorities.USER)
public class RateLimitFilter implements ContainerRequestFilter {

    @ConfigProperty(name = "mail.rate-limit.per-minute", defaultValue = "120")
    int perMinute;

    private final AtomicLong windowStart = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger sentThisWindow = new AtomicInteger();

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (perMinute <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        long start = windowStart.get();
        if (now - start >= 60_000 && windowStart.compareAndSet(start, now)) {
            sentThisWindow.set(0);
        }
        if (sentThisWindow.incrementAndGet() > perMinute) {
            long retryAfterSec = Math.max(1, 60 - (now - windowStart.get()) / 1000);
            requestContext.abortWith(Response.status(429)
                    .header("Retry-After", retryAfterSec)
                    .entity(Map.of("status", "RATE_LIMITED",
                            "detail", perMinute + " mails per minute is the ceiling"))
                    .build());
        }
    }
}

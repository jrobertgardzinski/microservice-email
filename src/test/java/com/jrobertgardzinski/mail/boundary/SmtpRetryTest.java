package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The resilience policy alone (no Quarkus, no broker): an SMTP hiccup is retried with backoff,
 * a server that stays down exhausts the attempts and fails — the consumer above then logs and
 * moves on instead of wedging the partition.
 */
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class SmtpRetryTest {

    private static final Duration TINY = Duration.ofMillis(5);

    @Test
    @DisplayName("a hiccup is survived: fail twice, deliver on the third try")
    void a_hiccup_is_survived() {
        AtomicInteger attempts = new AtomicInteger();
        Uni<Void> flaky = Uni.createFrom().deferred(() ->
                attempts.incrementAndGet() < 3
                        ? Uni.createFrom().failure(new IllegalStateException("SMTP 421, try later"))
                        : Uni.createFrom().voidItem());

        MailRequestsConsumer.withRetry(flaky, TINY).await().atMost(Duration.ofSeconds(5));

        assertEquals(3, attempts.get(), "two hiccups cost two retries, then the mail goes out");
    }

    @Test
    @DisplayName("a server that stays down exhausts the attempts and fails")
    void a_dead_server_exhausts_the_attempts() {
        AtomicInteger attempts = new AtomicInteger();
        Uni<Void> dead = Uni.createFrom().deferred(() -> {
            attempts.incrementAndGet();
            return Uni.createFrom().failure(new IllegalStateException("connection refused"));
        });

        assertThrows(IllegalStateException.class,
                () -> MailRequestsConsumer.withRetry(dead, TINY).await().atMost(Duration.ofSeconds(5)));
        assertEquals(MailRequestsConsumer.SMTP_RETRIES + 1, attempts.get(),
                "the first try plus every allowed retry");
    }
}

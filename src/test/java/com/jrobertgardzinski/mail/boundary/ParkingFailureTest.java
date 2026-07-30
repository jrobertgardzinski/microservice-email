package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.LinkMail;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The branch no Quarkus test in this module can reach: PARKING ITSELF IS REFUSED.
 *
 * <p>Every channel runs on {@code smallrye-in-memory} in the test profile, and an in-memory sink
 * accepts everything — so the recovery inside {@code park()} had no coverage at all, and it was the
 * branch that lost mail. On a real broker the write to {@code mail-requests-dlq} can be rejected:
 * the parked record is the original event plus its failure, so it is LARGER than the event that
 * already arrived, and quota or ACL changes refuse writes outright.
 *
 * <p>So the consumer is assembled by hand here (no Quarkus, like {@code SmtpRetryTest}) with an
 * emitter that nacks whatever it is handed. What matters is the Kafka acknowledgement: a mail that
 * was neither delivered nor parked must NOT be acknowledged, because the offset is the only thing
 * still holding it — security's outbox row is already marked published and no retry exists anywhere
 * else in the system.
 */
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class ParkingFailureTest {

    /** A real event of the worst kind to lose: a one-shot password-reset link. */
    private static final String RESET_REQUEST = """
            {"id":"reset-1","type":"PASSWORD_RESET","to":"victim@example.com",\
            "link":"https://app/reset?token=secret"}""";

    private final AtomicBoolean acked = new AtomicBoolean();
    private final AtomicReference<Throwable> nacked = new AtomicReference<>();

    private final Message<String> request = Message.of(RESET_REQUEST,
            () -> {
                acked.set(true);
                return CompletableFuture.completedFuture(null);
            },
            failure -> {
                nacked.set(failure);
                return CompletableFuture.completedFuture(null);
            });

    @Test
    @DisplayName("a mail that could be neither sent nor parked is left unacknowledged, not lost")
    void an_unparkable_mail_is_not_acknowledged() {
        MailRequestsConsumer consumer = consumerParkingOn(new RefusingDeadLetters());

        // the Uni must still COMPLETE: a failed Uni here cancels the channel, which would stop every
        // other mail as surely as acknowledging this one would lose it
        consumer.consume(request).await().atMost(Duration.ofSeconds(30));

        assertFalse(acked.get(), "the mail went nowhere — acknowledging it commits the offset past "
                + "the only remaining copy of the password-reset request");
        assertNull(nacked.get(), "and it must not be nacked either: the default `fail` strategy "
                + "would stop the consumer client, and `ignore` commits the record (see "
                + "KafkaIgnoreFailure) — unsettled is what keeps it on the topic");
    }

    @Test
    @DisplayName("a mail that could not be sent but WAS parked is still acknowledged")
    void a_parked_mail_is_still_acknowledged() {
        MailRequestsConsumer consumer = consumerParkingOn(new AcceptingDeadLetters());

        consumer.consume(request).await().atMost(Duration.ofSeconds(30));

        assertTrue(acked.get(), "the record is on the dead-letter topic, so it is settled — not "
                + "acknowledging a parked mail would replay it for ever");
    }

    /** The consumer with its SMTP server down for good, parking onto the given emitter. */
    private static MailRequestsConsumer consumerParkingOn(Emitter<String> deadLetters) {
        MailRequestsConsumer consumer = new MailRequestsConsumer();
        consumer.mapper = new ObjectMapper();
        consumer.deadLetters = deadLetters;
        consumer.dispatcher = new MailDispatcher() {
            @Override
            public Uni<Void> sendPasswordResetLink(LinkMail mail) {
                return Uni.createFrom().failure(new IllegalStateException("connection refused"));
            }
        };
        return consumer;
    }

    /** A dead-letter topic that refuses the record — the outage the in-memory connector cannot do. */
    private static final class RefusingDeadLetters extends StubEmitter {
        @Override
        public <M extends Message<? extends String>> void send(M record) {
            record.nack(new IOException("record too large for mail-requests-dlq"));
        }
    }

    /** The normal parking path, for the control case. */
    private static final class AcceptingDeadLetters extends StubEmitter {
        @Override
        public <M extends Message<? extends String>> void send(M record) {
            record.ack();
        }
    }

    /** Only {@code send(Message)} is used by the consumer; the rest of the contract is inert. */
    private abstract static class StubEmitter implements Emitter<String> {

        @Override
        public CompletionStage<Void> send(String payload) {
            throw new UnsupportedOperationException("the consumer sends keyed Messages, not payloads");
        }

        @Override
        public void complete() {
        }

        @Override
        public void error(Exception e) {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean hasRequests() {
            return true;
        }
    }
}

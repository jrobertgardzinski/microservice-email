package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.LinkMail;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The asynchronous boundary (BCE): mail requests arriving on the {@code mail-requests} Kafka topic
 * — published by microservice-security's transactional outbox. Delivery is at-least-once, so the
 * consumer deduplicates by event id (a bounded in-memory set: a walking-skeleton trade-off, a
 * shared store takes over when this service scales out). Unknown types are logged and dropped, so
 * one bad event never wedges the partition. An SMTP hiccup is retried with backoff; only a send
 * that keeps failing is given up on (logged loudly) — and an event is remembered as processed
 * ONLY after its mail actually went out, so a redelivery after a crash still delivers. A send
 * that keeps failing is PARKED on the dead-letter topic with the failure attached — nothing is
 * silently lost, and an operator (or a future re-drive job) finds the whole story in one place.
 */
@ApplicationScoped
public class MailRequestsConsumer {

    private static final Logger LOG = Logger.getLogger(MailRequestsConsumer.class);
    private static final int REMEMBERED_EVENTS = 10_000;
    static final int SMTP_RETRIES = 3;
    static final Duration SMTP_BACKOFF = Duration.ofSeconds(1);

    @Inject
    MailDispatcher dispatcher;

    @Inject
    ObjectMapper mapper;

    @Inject
    @Channel("mail-requests-dlq")
    Emitter<String> deadLetters;

    private final Set<String> processedIds = Collections.newSetFromMap(
            Collections.synchronizedMap(new LinkedHashMap<>() {
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REMEMBERED_EVENTS;
                }
            }));

    @Incoming("mail-requests")
    public Uni<Void> consume(String payload) {
        JsonNode event;
        try {
            event = mapper.readTree(payload);
        } catch (Exception malformed) {
            LOG.warnf("dropping malformed mail request: %s", payload);
            return Uni.createFrom().voidItem();
        }
        String id = event.path("id").asText();
        if (!id.isEmpty() && processedIds.contains(id)) {
            LOG.infof("skipping duplicate mail request %s", id);
            return Uni.createFrom().voidItem();
        }
        String type = event.path("type").asText();
        String to = event.path("to").asText();
        Uni<Void> delivery = switch (type) {
            case "VERIFICATION" -> dispatcher.sendVerificationLink(new LinkMail(to, event.path("link").asText()));
            case "PASSWORD_RESET" -> dispatcher.sendPasswordResetLink(new LinkMail(to, event.path("link").asText()));
            case "ACCOUNT_DELETED" -> dispatcher.sendAccountDeleted(to);
            case "ACCOUNT_DELETION_FAILED" -> dispatcher.sendAccountDeletionFailed(to);
            default -> {
                LOG.warnf("dropping mail request %s of unknown type '%s'", id, type);
                yield null;
            }
        };
        if (delivery == null) {
            return Uni.createFrom().voidItem();
        }
        return withRetry(delivery, SMTP_BACKOFF)
                .onItem().invoke(() -> {
                    if (!id.isEmpty()) {
                        processedIds.add(id);
                    }
                })
                .onFailure().recoverWithUni(smtpDown -> {
                    LOG.errorf(smtpDown, "parking mail request %s (%s to %s) on the dead-letter "
                            + "topic after %d attempts", id, type, to, SMTP_RETRIES + 1);
                    return park(payload, smtpDown);
                });
    }

    /** The original event plus what killed it, parked for an operator or a re-drive job. */
    private Uni<Void> park(String payload, Throwable smtpDown) {
        try {
            var parked = mapper.createObjectNode();
            parked.set("event", mapper.readTree(payload));
            parked.put("failure", String.valueOf(smtpDown.getMessage()))
                    .put("attempts", SMTP_RETRIES + 1);
            return Uni.createFrom().completionStage(deadLetters.send(mapper.writeValueAsString(parked)))
                    .onFailure().recoverWithUni(dlqDown -> {
                        LOG.errorf(dlqDown, "the dead-letter topic is down too; the event is lost: %s", payload);
                        return Uni.createFrom().voidItem();
                    });
        } catch (Exception impossible) {
            LOG.errorf(impossible, "could not park %s", payload);
            return Uni.createFrom().voidItem();
        }
    }

    /** The resilience policy, alone: retry an SMTP hiccup with exponential backoff. */
    static Uni<Void> withRetry(Uni<Void> send, Duration initialBackoff) {
        return send.onFailure().retry()
                .withBackOff(initialBackoff, initialBackoff.multipliedBy(8))
                .atMost(SMTP_RETRIES);
    }
}

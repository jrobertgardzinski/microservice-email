package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.LinkMail;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.nio.charset.StandardCharsets;
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

    /**
     * A dead-letter record, KEYED BY EVENT ID.
     *
     * <p>The key is what makes this topic a ledger rather than a tape. Records went out unkeyed
     * until now, and the whole design around them — an in-memory ledger rebuilt by replaying the
     * entire topic, retractions instead of deletes — rested on the assumption that the topic keeps
     * everything for ever. Nothing had ever configured it, so the broker's default applied: seven
     * days. A mail parked over a holiday was silently swept by the broker, and after the next
     * restart it was gone from the only window an operator has onto the dead-letter queue.
     *
     * <p>With a key and {@code cleanup.policy=compact} (see docker-compose.identity.yml), the topic
     * keeps the LATEST record per event for ever: an unsettled mail keeps its parked record, and a
     * settled one collapses to its retraction. Replay at startup therefore stays proportional to
     * the number of DISTINCT parked mails rather than to the history of the service.
     */
    private java.util.concurrent.CompletionStage<Void> sendKeyed(String eventId, String body) {
        // Emitter.send(Message) returns void, so the delivery signal is built from the message's own
        // ack/nack. It has to exist: the callers below recover from a failed dead-letter write, and
        // without a completion they would treat "never sent" as "sent".
        java.util.concurrent.CompletableFuture<Void> settled = new java.util.concurrent.CompletableFuture<>();
        deadLetters.send(org.eclipse.microprofile.reactive.messaging.Message.of(body)
                .addMetadata(io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata
                        .<String>builder().withKey(eventId).build())
                .withAck(() -> {
                    settled.complete(null);
                    return java.util.concurrent.CompletableFuture.completedFuture(null);
                })
                .withNack(refused -> {
                    settled.completeExceptionally(refused);
                    return java.util.concurrent.CompletableFuture.completedFuture(null);
                }));
        return settled;
    }

    private final Set<String> processedIds = Collections.newSetFromMap(
            Collections.synchronizedMap(new LinkedHashMap<>() {
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REMEMBERED_EVENTS;
                }
            }));

    @Incoming("mail-requests")
    public Uni<Void> consume(Message<String> message) {
        // continue the trace: security's outbox stamped the originating request's cid as a Kafka
        // header. It rides the synchronous decision logs below; the async SMTP send runs on other
        // threads (Mutiny), where MDC would need context propagation — a deliberate walking-skeleton
        // trade-off, like the in-memory dedup set.
        String cid = correlationId(message);
        if (cid != null) {
            MDC.put("cid", cid);
        }
        try {
            // the ack rides on SETTLED, not on delivered: a parked mail is handled too — its
            // record is on the dead-letter topic, and not acking would replay it for ever
            return process(message.getPayload())
                    .chain(settled -> Uni.createFrom().completionStage(message.ack()));
        } finally {
            MDC.remove("cid");
        }
    }

    private static String correlationId(Message<String> message) {
        return message.getMetadata(IncomingKafkaRecordMetadata.class)
                .map(md -> md.getHeaders().lastHeader("X-Correlation-Id"))
                .map(header -> header == null ? null : new String(header.value(), StandardCharsets.UTF_8))
                .orElse(null);
    }

    /**
     * The delivery itself, without the Kafka envelope — reused by the DLQ re-drive endpoint.
     *
     * @return whether the mail actually WENT OUT. It used to return {@code Uni<Void>}, and that was
     *         a trap rather than a simplification: every failure path here recovers (an SMTP outage
     *         parks the event, and parking recovers its own failure too), so the Uni always
     *         completed successfully and a caller had no way at all to tell a delivery from a
     *         parking. The re-drive endpoint chained its retraction onto that Uni and therefore
     *         retracted a mail that had just been parked again — see {@code DlqResource}.
     */
    Uni<Boolean> process(String payload) {
        return process(payload, null);
    }

    /**
     * The same delivery, told which identity to re-park under if it fails again.
     *
     * <p>A re-drive hands back an event that is already ON the dead-letter topic under a known key,
     * and a failed re-drive parks it a second time. If that second parking mints a FRESH synthetic
     * id — which it does for an id-less event, since the id is the only thing to derive one from —
     * the topic ends up with two records for one undelivered mail, neither of which compaction will
     * ever collapse into the other, and the operator's list grows one duplicate per attempt. Keeping
     * the same key makes the re-park an overwrite, which is what it is.
     *
     * @param parkedIdIfItFails the id the record is already filed under, or {@code null} for a mail
     *                          arriving from the {@code mail-requests} topic, which has none yet
     */
    Uni<Boolean> process(String payload, String parkedIdIfItFails) {
        JsonNode event;
        try {
            event = mapper.readTree(payload);
        } catch (Exception malformed) {
            // the payload itself never reaches the log: these events carry password-reset links and
            // one-time MFA codes, and the logs ship to Loki. The record stays on the topic, which is
            // the durable evidence — the log only has to say that one arrived and could not be read.
            LOG.warnf("dropping malformed mail request (%d bytes)", payload.length());
            return Uni.createFrom().item(false);
        }
        String id = event.path("id").asText();
        if (!id.isEmpty() && processedIds.contains(id)) {
            LOG.infof("skipping duplicate mail request %s", id);
            // already delivered once, so from the caller's point of view this IS settled
            return Uni.createFrom().item(true);
        }
        String type = event.path("type").asText();
        String to = event.path("to").asText();
        LOG.infof("mail request received (%s to %s)", type, masked(to));   // cid rides the log format
        Uni<Void> delivery = switch (type) {
            case "VERIFICATION" -> dispatcher.sendVerificationLink(new LinkMail(to, event.path("link").asText()));
            case "PASSWORD_RESET" -> dispatcher.sendPasswordResetLink(new LinkMail(to, event.path("link").asText()));
            case "ACCOUNT_DELETED" -> dispatcher.sendAccountDeleted(to);
            case "ACCOUNT_DELETION_FAILED" -> dispatcher.sendAccountDeletionFailed(to);
            case "ALREADY_REGISTERED" -> dispatcher.sendAlreadyRegistered(to);
            case "AUTH_CODE" -> dispatcher.sendAuthCode(to, event.path("code").asText());
            default -> {
                LOG.warnf("dropping mail request %s of unknown type '%s'", id, type);
                yield null;
            }
        };
        if (delivery == null) {
            return Uni.createFrom().item(false);   // unknown type: dropped, not delivered
        }
        return withRetry(delivery, SMTP_BACKOFF)
                .onItem().invoke(() -> {
                    if (!id.isEmpty()) {
                        processedIds.add(id);
                    }
                })
                .replaceWith(true)
                .onFailure().recoverWithUni(smtpDown -> {
                    LOG.errorf(smtpDown, "parking mail request %s (%s to %s) on the dead-letter "
                            + "topic after %d attempts", id, type, masked(to), SMTP_RETRIES + 1);
                    return park(payload, smtpDown, parkedIdIfItFails).replaceWith(false);
                });
    }

    /**
     * An address reduced to what an operator needs to recognise a report and no more. Every mail
     * request logged its recipient in full, and the logs go to Loki — which made the log an
     * unadvertised copy of the user table, harvestable by anyone who can read it. Two characters
     * and the domain still let a support conversation confirm "yes, that was your address".
     */
    static String masked(String address) {
        int at = address.indexOf('@');
        if (at <= 0) {
            return "***";                      // no local part to keep: say nothing rather than guess
        }
        return address.substring(0, Math.min(2, at)) + "***" + address.substring(at);
    }

    /**
     * Retracts a parked event once an operator has re-driven it successfully.
     *
     * <p>The ledger in {@link ParkedMails} is rebuilt by replaying the whole dead-letter topic at
     * every start, so without this marker a settled mail would climb back into the operator's list
     * after every restart — for good. Kafka has no delete, so the retraction is itself a record.
     */
    Uni<Void> markRedriven(String id) {
        try {
            String marker = mapper.writeValueAsString(mapper.createObjectNode().put("redriven", id));
            return Uni.createFrom().completionStage(sendKeyed(id, marker))
                    .onFailure().recoverWithUni(dlqDown -> {
                        // the mail HAS gone out; the worst case is that a replay shows it again
                        LOG.warnf(dlqDown, "could not retract re-driven %s from the dead-letter "
                                + "topic; it will reappear in the ledger after a restart", id);
                        return Uni.createFrom().voidItem();
                    });
        } catch (Exception impossible) {
            LOG.warnf(impossible, "could not retract re-driven %s", id);
            return Uni.createFrom().voidItem();
        }
    }

    /** The original event plus what killed it, parked for an operator or a re-drive job. */
    private Uni<Void> park(String payload, Throwable smtpDown, String knownParkedId) {
        // computed ONCE, here: it is the record's identity on a compacted topic, in the ledger and
        // in its eventual retraction, and those three have to be the same string. A re-drive already
        // knows it — reusing it makes the re-park overwrite the record rather than sit beside it.
        String parkedId = knownParkedId != null ? knownParkedId : parkedIdFor(payload);
        try {
            var parked = mapper.createObjectNode();
            parked.set("event", mapper.readTree(payload));
            parked.put("failure", String.valueOf(smtpDown.getMessage()))
                    .put("attempts", SMTP_RETRIES + 1)
                    // written INTO the record, so the ledger keys itself the same way the topic is
                    // keyed without having to reach for the Kafka metadata
                    .put("parkedId", parkedId);
            return Uni.createFrom().completionStage(
                            sendKeyed(parkedId, mapper.writeValueAsString(parked)))
                    .onFailure().recoverWithUni(dlqDown -> {
                        // no payload: at THIS point it parsed, so it is a real event carrying a real
                        // reset link or MFA code, and this is the one branch where it would reach the
                        // log intact. The id is enough to correlate with the topic.
                        LOG.errorf(dlqDown, "the dead-letter topic is down too; mail request %s "
                                + "(%d bytes) is lost", parkedId, payload.length());
                        return Uni.createFrom().voidItem();
                    });
        } catch (Exception impossible) {
            LOG.errorf(impossible, "could not park mail request %s", parkedId);
            return Uni.createFrom().voidItem();
        }
    }

    /**
     * The identity a parked record is keyed by: the event's own id when it has one, and otherwise a
     * synthetic one that is <b>stable and unique</b>.
     *
     * <p>Both properties are load-bearing, and neither was there. Every id-less event was keyed
     * {@code "<no id>"} — one Kafka key for all of them, on a topic whose {@code cleanup.policy} is
     * {@code compact}. Park two id-less mails and the broker keeps only the second: the sole durable
     * record of the first undelivered mail destroyed, with no log line anywhere, which is precisely
     * the silent destruction of evidence the keying was introduced to end. Meanwhile the ledger gave
     * the same records a DIFFERENT and non-monotonic key ({@code "unidentified-" + ledger.size()},
     * which {@code take()} and eviction walk backwards), so entries overwrote one another in memory
     * and every retraction was published under a key no parked record ever had — settled mails came
     * back from the dead at the next restart, and a second re-drive sent the reset or MFA mail
     * twice.
     *
     * <p>A UUID minted at parking time and carried in the record fixes all of it at once: unique, so
     * compaction keeps every distinct parked mail; stable, so the ledger, the topic and the
     * retraction agree for ever, including across restarts.
     */
    private String parkedIdFor(String payload) {
        try {
            String id = mapper.readTree(payload).path("id").asText();
            if (!id.isEmpty()) {
                return id;
            }
        } catch (Exception unreadable) {
            // fall through: an event we cannot read still deserves a record of its own
        }
        return UNIDENTIFIED_PREFIX + java.util.UUID.randomUUID();
    }

    /** How a synthetic parked id announces itself to an operator reading {@code /mails/dlq}. */
    static final String UNIDENTIFIED_PREFIX = "unidentified-";

    /**
     * How long ONE attempt may take before it counts as failed.
     *
     * <p>An SMTP send that never completes — a half-open connection, a server that accepts and then
     * says nothing — used to hang the channel for ever, because the retry policy only reacts to a
     * FAILURE and a hung Uni never produces one. The commit strategy's age watchdog was the only
     * thing that eventually noticed, and it was switched off on 2026-07-28 (for good reason: it
     * killed the consumer outright). Removing that watchdog without giving the send a deadline of
     * its own would have traded a loud failure for a silent one.
     */
    static final Duration SMTP_ATTEMPT_TIMEOUT = Duration.ofSeconds(30);

    /** The resilience policy, alone: bound each attempt, then retry an SMTP hiccup with backoff. */
    static Uni<Void> withRetry(Uni<Void> send, Duration initialBackoff) {
        return withRetry(send, initialBackoff, SMTP_ATTEMPT_TIMEOUT);
    }

    /** The same, with the per-attempt deadline spelled out — a test must not wait thirty seconds. */
    static Uni<Void> withRetry(Uni<Void> send, Duration initialBackoff, Duration attemptTimeout) {
        return send.ifNoItem().after(attemptTimeout).fail()
                .onFailure().retry()
                .withBackOff(initialBackoff, initialBackoff.multipliedBy(8))
                .atMost(SMTP_RETRIES);
    }
}

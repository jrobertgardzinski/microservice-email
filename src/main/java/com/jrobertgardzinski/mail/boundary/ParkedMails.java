package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The operator's view of the dead-letter topic: parked events are read back into a bounded
 * in-memory ledger so {@code GET /mails/dlq} can show them and a re-drive can resubmit one
 * through the normal delivery path. Walking-skeleton trade-off like the dedup set: the topic
 * itself remains the durable record; this ledger is the window onto it.
 */
@ApplicationScoped
public class ParkedMails {

    private static final Logger LOG = Logger.getLogger(ParkedMails.class);
    private static final int LEDGER_LIMIT = 1_000;

    @Inject
    ObjectMapper mapper;

    private final Map<String, JsonNode> ledger = Collections.synchronizedMap(
            new LinkedHashMap<>() {
                protected boolean removeEldestEntry(Map.Entry<String, JsonNode> eldest) {
                    return size() > LEDGER_LIMIT;
                }
            });

    @Incoming("mail-requests-dlq-in")
    public Uni<Void> onParked(String payload) {
        try {
            JsonNode parked = mapper.readTree(payload);
            // a retraction, not a parked mail: an operator already re-drove this one successfully
            // (MailRequestsConsumer#markRedriven). The channel replays the whole topic at every
            // start, so without honouring these the ledger would hand back settled mails for ever.
            JsonNode redriven = parked.get("redriven");
            if (redriven != null) {
                ledger.remove(redriven.asText());
                return Uni.createFrom().voidItem();
            }
            ledger.put(ledgerKey(parked), parked);
        } catch (Exception malformed) {
            // never the payload: a parked record wraps the original event, reset links and MFA codes
            // included, and these logs ship to Loki
            LOG.warnf("unreadable dead letter (%d bytes)", payload.length());
        }
        return Uni.createFrom().voidItem();
    }

    /**
     * The key a parked record is filed under — <b>the same string the record is keyed by on the
     * topic</b>, so that the ledger, the compacted topic and the retraction that eventually settles
     * it all name one thing.
     *
     * <p>{@code parkedId} is written by {@code MailRequestsConsumer#park} and is the event's own id
     * when it had one. This ledger used to derive its own key instead — {@code event.id}, or
     * {@code "unidentified-" + ledger.size()} when there was none. That counter is not monotonic
     * ({@code take()} and eviction shrink the map), so two id-less mails could collide in memory,
     * and it matched nothing on the topic, so a retraction published under it removed nothing:
     * settled mails climbed back into the operator's list after every restart, and re-driving one a
     * second time sent the reset link or MFA code again.
     *
     * <p>The fallbacks are for records parked by an older build. {@code event.id} still identifies
     * most of them; the id-less ones already share ONE key on a compacted topic, so at most one of
     * them survives there and a single ledger entry is not a loss of anything the broker still has.
     */
    private static String ledgerKey(JsonNode parked) {
        String parkedId = parked.path("parkedId").asText();
        if (!parkedId.isEmpty()) {
            return parkedId;
        }
        String eventId = parked.path("event").path("id").asText();
        return eventId.isEmpty() ? LEGACY_UNIDENTIFIED : eventId;
    }

    /** The one entry every pre-2026-07-29 id-less record collapses into — as it has on the topic. */
    static final String LEGACY_UNIDENTIFIED = "unidentified-legacy";

    /** What is parked right now: the event plus why it died. */
    public List<JsonNode> all() {
        synchronized (ledger) {
            return List.copyOf(ledger.values());
        }
    }

    /** Take one parked event off the ledger for a re-drive (it returns if it fails again). */
    public Optional<JsonNode> take(String id) {
        return Optional.ofNullable(ledger.remove(id));
    }

    /**
     * Put back a record that was taken but could not be re-driven at all.
     *
     * <p>The normal failure path needs nothing of the sort: a re-drive that fails is parked again by
     * the consumer, the record comes back down the dead-letter channel, and the ledger rebuilds
     * itself. A record whose shape the re-drive cannot even read never reaches that path, so without
     * this it would simply vanish from the operator's only window — see {@code DlqResource}.
     */
    public void restore(String id, JsonNode record) {
        ledger.put(id, record);
    }
}

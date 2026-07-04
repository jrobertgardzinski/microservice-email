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
            String id = parked.path("event").path("id").asText();
            ledger.put(id.isEmpty() ? "unidentified-" + ledger.size() : id, parked);
        } catch (Exception malformed) {
            LOG.warnf("unreadable dead letter: %s", payload);
        }
        return Uni.createFrom().voidItem();
    }

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
}

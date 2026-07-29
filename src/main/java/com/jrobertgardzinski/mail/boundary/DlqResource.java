package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

/**
 * The re-drive: an operator lists what died on the dead-letter topic and pushes one event back
 * through the NORMAL delivery path (same consumer, same retries — and the same parking if the
 * world is still broken). Guarded by the API key like every other mail endpoint.
 */
@Path("/mails/dlq")
@Produces(MediaType.APPLICATION_JSON)
public class DlqResource {

    @Inject
    ParkedMails parked;

    @Inject
    MailRequestsConsumer consumer;

    @GET
    public List<JsonNode> list() {
        return parked.all();
    }

    /**
     * A record whose shape this endpoint cannot re-drive: no {@code event} to hand the consumer.
     *
     * <p>{@link ParkedMails} lets such records in deliberately — it reads the id down a null-safe
     * path precisely because the dead-letter topic is durable and anything may be published on it by
     * hand — so they are listable, and therefore re-drivable by an operator clicking through the
     * list. The record was already TAKEN off the ledger by then, and {@code record.get("event")}
     * threw an NPE: 500 to the operator, and the record gone from the only window onto the queue
     * until the next restart. An endpoint built so that dead mail is visible made one invisible.
     */
    private Uni<Response> unreadable(String id, JsonNode record) {
        return unreadable(id, record, "the parked record carries no 'event' to re-drive");
    }

    /**
     * The same refusal, for every shape the consumer would DROP rather than deliver.
     *
     * <p>The first version of this guard tested {@code record.get("event") == null} and nothing
     * else, which covers exactly one of the three ways a record can be undeliverable. A literal
     * {@code "event": null} parses to a NullNode — not null — and an event whose {@code type} this
     * consumer does not know parses fine; both went through to {@code process()}, which drops them
     * WITHOUT parking, so the record was gone from the ledger, absent from the topic, and the
     * operator was told "PARKED_AGAIN — the ledger will show it again". It does not. A second POST
     * then answers 404 NOT_PARKED, which is the endpoint contradicting itself about a mail that
     * still exists nowhere. The javadoc above promises this endpoint cannot make dead mail
     * invisible; this is what makes the promise true for the other two shapes.
     */
    private Uni<Response> unreadable(String id, JsonNode record, String reason) {
        parked.restore(id, record);   // it was taken; nothing else will put it back
        return Uni.createFrom().item(Response.status(422)
                .entity(Map.of("status", "NOT_REDRIVABLE", "id", id,
                        "reason", reason,
                        "record", record))
                .build());
    }

    @POST
    @Path("/{id}/redrive")
    public Uni<Response> redrive(@PathParam("id") String id) {
        return parked.take(id)
                .map(record -> notRedrivable(record)
                        .map(reason -> unreadable(id, record, reason))
                        .orElseGet(() -> redrive(id, record)))
                .orElse(Uni.createFrom().item(
                        Response.status(Response.Status.NOT_FOUND)
                                .entity(Map.of("status", "NOT_PARKED", "id", id)).build()));
    }

    /**
     * Why this record can never be delivered, or empty when it is worth trying.
     *
     * <p>{@link ParkedMails} admits records of any shape on purpose — the dead-letter topic is
     * durable and anything may be published on it by hand — so they are listable, and therefore
     * clickable. Everything the consumer would silently drop has to be refused HERE, while the
     * record can still be put back.
     */
    private java.util.Optional<String> notRedrivable(JsonNode record) {
        JsonNode event = record.path("event");
        if (event.isMissingNode() || event.isNull() || !event.isObject()) {
            return java.util.Optional.of("the parked record carries no 'event' object to re-drive");
        }
        String type = event.path("type").asText();
        if (!MailRequestsConsumer.canBeDelivered(type)) {
            return java.util.Optional.of("this build has no delivery for mail type '" + type
                    + "' — re-driving it would drop the record without parking it again");
        }
        return java.util.Optional.empty();
    }

    private Uni<Response> redrive(String id, JsonNode record) {
        // The retraction is published ONLY if the mail actually went out.
        //
        // This used to chain unconditionally, with a comment reasoning that "a re-drive that fails
        // is parked again by the consumer itself" — true, and exactly half the story. The retraction
        // then followed the re-parked record onto the same topic and removed it again, so an
        // operator re-driving during an outage silently destroyed the only record of an undelivered
        // password-reset or MFA mail, and got 202 REDRIVEN for it.
        // the id it is ALREADY filed under travels with it: a re-drive that fails is parked again,
        // and a second parking that minted a fresh synthetic id would leave two records for one
        // undelivered mail on a compacted topic — one duplicate in the operator's list per attempt
        return consumer.process(record.get("event").toString(), id)
                .chain(delivered -> delivered
                        ? consumer.markRedriven(id).replaceWith(
                                Response.accepted()
                                        .entity(Map.of("status", "REDRIVEN", "id", id)).build())
                        // still broken: the event is back on the dead-letter topic, the ledger will
                        // show it again, and the operator is told the truth
                        : Uni.createFrom().item(Response.status(503)
                                .entity(Map.of("status", "PARKED_AGAIN", "id", id)).build()));
    }
}

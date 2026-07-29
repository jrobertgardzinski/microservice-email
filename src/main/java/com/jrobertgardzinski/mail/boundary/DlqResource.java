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
        parked.restore(id, record);   // it was taken; nothing else will put it back
        return Uni.createFrom().item(Response.status(422)
                .entity(Map.of("status", "NOT_REDRIVABLE", "id", id,
                        "reason", "the parked record carries no 'event' to re-drive",
                        "record", record))
                .build());
    }

    @POST
    @Path("/{id}/redrive")
    public Uni<Response> redrive(@PathParam("id") String id) {
        return parked.take(id)
                .map(record -> record.get("event") == null ? unreadable(id, record) : redrive(id, record))
                .orElse(Uni.createFrom().item(
                        Response.status(Response.Status.NOT_FOUND)
                                .entity(Map.of("status", "NOT_PARKED", "id", id)).build()));
    }

    private Uni<Response> redrive(String id, JsonNode record) {
        // The retraction is published ONLY if the mail actually went out.
        //
        // This used to chain unconditionally, with a comment reasoning that "a re-drive that fails
        // is parked again by the consumer itself" — true, and exactly half the story. The retraction
        // then followed the re-parked record onto the same topic and removed it again, so an
        // operator re-driving during an outage silently destroyed the only record of an undelivered
        // password-reset or MFA mail, and got 202 REDRIVEN for it.
        return consumer.process(record.get("event").toString())
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

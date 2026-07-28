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

    @POST
    @Path("/{id}/redrive")
    public Uni<Response> redrive(@PathParam("id") String id) {
        return parked.take(id)
                // The retraction is published ONLY if the mail actually went out.
                //
                // This used to chain unconditionally, with a comment reasoning that "a re-drive that
                // fails is parked again by the consumer itself" — true, and exactly half the story.
                // The retraction then followed the re-parked record onto the same topic and removed
                // it again, so an operator re-driving during an outage silently destroyed the only
                // record of an undelivered password-reset or MFA mail, and got 202 REDRIVEN for it.
                .map(record -> consumer.process(record.get("event").toString())
                        .chain(delivered -> delivered
                                ? consumer.markRedriven(id).replaceWith(
                                        Response.accepted()
                                                .entity(Map.of("status", "REDRIVEN", "id", id)).build())
                                // still broken: the event is back on the dead-letter topic, the
                                // ledger will show it again, and the operator is told the truth
                                : Uni.createFrom().item(Response.status(503)
                                        .entity(Map.of("status", "PARKED_AGAIN", "id", id)).build())))
                .orElse(Uni.createFrom().item(
                        Response.status(Response.Status.NOT_FOUND)
                                .entity(Map.of("status", "NOT_PARKED", "id", id)).build()));
    }
}

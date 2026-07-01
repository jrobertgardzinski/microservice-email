package com.jrobertgardzinski.mail.boundary;

import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.Mail;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Boundary (BCE): the REST entry point other services send mail commands to. Thin — it accepts the
 * command and hands it to the {@link MailDispatcher} control, returning 202 Accepted.
 */
@Path("/mails")
public class MailResource {

    @Inject
    MailDispatcher dispatcher;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response send(Mail mail) {
        dispatcher.dispatch(mail);
        return Response.accepted().build();
    }
}

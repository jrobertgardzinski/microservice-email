package com.jrobertgardzinski.mail.boundary;

import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.LinkMail;
import com.jrobertgardzinski.mail.entity.Mail;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Boundary (BCE): the REST entry point other services send mail commands to. Thin — it accepts a
 * command and hands it to the {@link MailDispatcher} control, returning 202 Accepted. Access is
 * guarded by {@link ApiKeyFilter}.
 */
@Path("/mails")
@Consumes(MediaType.APPLICATION_JSON)
public class MailResource {

    @Inject
    MailDispatcher dispatcher;

    @POST
    public Response send(Mail mail) {
        dispatcher.dispatch(mail);
        return Response.accepted().build();
    }

    @POST
    @Path("/verification")
    public Response sendVerification(LinkMail mail) {
        dispatcher.sendVerificationLink(mail);
        return Response.accepted().build();
    }

    @POST
    @Path("/password-reset")
    public Response sendPasswordReset(LinkMail mail) {
        dispatcher.sendPasswordResetLink(mail);
        return Response.accepted().build();
    }
}

package com.jrobertgardzinski.mail.boundary;

import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.LinkMail;
import com.jrobertgardzinski.mail.entity.Mail;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Boundary (BCE): the REST entry point other services send mail commands to. Thin — it validates
 * the command ({@code @Valid}, malformed commands are refused with 400) and hands it to the
 * {@link MailDispatcher} control; 202 Accepted once the send completes. Access is guarded by
 * {@link ApiKeyFilter}.
 */
@Path("/mails")
@Consumes(MediaType.APPLICATION_JSON)
public class MailResource {

    @Inject
    MailDispatcher dispatcher;

    @POST
    public Uni<Response> send(@Valid Mail mail) {
        return dispatcher.dispatch(mail).replaceWith(Response.accepted().build());
    }

    @POST
    @Path("/verification")
    public Uni<Response> sendVerification(@Valid LinkMail mail) {
        return dispatcher.sendVerificationLink(mail).replaceWith(Response.accepted().build());
    }

    @POST
    @Path("/password-reset")
    public Uni<Response> sendPasswordReset(@Valid LinkMail mail) {
        return dispatcher.sendPasswordResetLink(mail).replaceWith(Response.accepted().build());
    }
}

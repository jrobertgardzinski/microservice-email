package com.jrobertgardzinski.mail.control;

import com.jrobertgardzinski.mail.entity.LinkMail;
import com.jrobertgardzinski.mail.entity.Mail;
import io.quarkus.mailer.reactive.ReactiveMailer;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Control (BCE): turns entities into actual sends via the {@link ReactiveMailer}. Reactive on
 * purpose: sends are awaited by the HTTP boundary but must NOT block the Kafka boundary — a
 * blocking send inside a message handler deadlocks Vert.x's ordered worker queue (the await parks
 * the very thread its completion needs). Plain messages go out as-is; the verification and
 * password-reset messages render a Qute template with the action link.
 */
@ApplicationScoped
public class MailDispatcher {

    @Inject
    ReactiveMailer mailer;

    @Inject
    @Location("verification")
    Template verification;

    @Inject
    @Location("password-reset")
    Template passwordReset;

    public Uni<Void> dispatch(Mail mail) {
        return mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), mail.subject(), mail.text()));
    }

    public Uni<Void> sendVerificationLink(LinkMail mail) {
        String body = verification.data("link", mail.link()).render();
        return mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), "Verify your email", body));
    }

    public Uni<Void> sendPasswordResetLink(LinkMail mail) {
        String body = passwordReset.data("link", mail.link()).render();
        return mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), "Reset your password", body));
    }
}

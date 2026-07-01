package com.jrobertgardzinski.mail.control;

import com.jrobertgardzinski.mail.entity.LinkMail;
import com.jrobertgardzinski.mail.entity.Mail;
import io.quarkus.mailer.Mailer;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Control (BCE): turns entities into actual sends via Quarkus' {@link Mailer}. Plain messages go out
 * as-is; the verification and password-reset messages render a Qute template with the action link.
 * This is where future logic lives (retries, rate limiting), keeping the boundary thin.
 */
@ApplicationScoped
public class MailDispatcher {

    @Inject
    Mailer mailer;

    @Inject
    @Location("verification")
    Template verification;

    @Inject
    @Location("password-reset")
    Template passwordReset;

    public void dispatch(Mail mail) {
        mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), mail.subject(), mail.text()));
    }

    public void sendVerificationLink(LinkMail mail) {
        String body = verification.data("link", mail.link()).render();
        mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), "Verify your email", body));
    }

    public void sendPasswordResetLink(LinkMail mail) {
        String body = passwordReset.data("link", mail.link()).render();
        mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), "Reset your password", body));
    }
}

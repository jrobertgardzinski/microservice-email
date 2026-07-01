package com.jrobertgardzinski.mail.control;

import com.jrobertgardzinski.mail.entity.Mail;
import io.quarkus.mailer.Mailer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Control (BCE): turns a {@link Mail} entity into an actual send via Quarkus' {@link Mailer}. This
 * is where any future logic lives (templating, retries, rate limiting), keeping the boundary thin.
 */
@ApplicationScoped
public class MailDispatcher {

    @Inject
    Mailer mailer;

    public void dispatch(Mail mail) {
        mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), mail.subject(), mail.text()));
    }
}

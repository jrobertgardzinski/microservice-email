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
 * the very thread its completion needs). Plain messages go out as-is; the templated messages go
 * out as MULTIPART — the same Qute template in a .txt and an .html variant (the plain part is the
 * accessible fallback every mail client can render), so the locations name their suffix
 * explicitly.
 */
@ApplicationScoped
public class MailDispatcher {

    @Inject
    ReactiveMailer mailer;

    @Inject
    @Location("verification.txt")
    Template verification;

    @Inject
    @Location("verification.html")
    Template verificationHtml;

    @Inject
    @Location("password-reset.txt")
    Template passwordReset;

    @Inject
    @Location("password-reset.html")
    Template passwordResetHtml;

    @Inject
    @Location("account-deleted.txt")
    Template accountDeleted;

    @Inject
    @Location("account-deleted.html")
    Template accountDeletedHtml;

    @Inject
    @Location("account-deletion-failed.txt")
    Template accountDeletionFailed;

    @Inject
    @Location("account-deletion-failed.html")
    Template accountDeletionFailedHtml;

    public Uni<Void> dispatch(Mail mail) {
        return mailer.send(io.quarkus.mailer.Mail.withText(mail.to(), mail.subject(), mail.text()));
    }

    public Uni<Void> sendVerificationLink(LinkMail mail) {
        return mailer.send(multipart(mail.to(), "Verify your email",
                verification.data("link", mail.link()).render(),
                verificationHtml.data("link", mail.link()).render()));
    }

    public Uni<Void> sendPasswordResetLink(LinkMail mail) {
        return mailer.send(multipart(mail.to(), "Reset your password",
                passwordReset.data("link", mail.link()).render(),
                passwordResetHtml.data("link", mail.link()).render()));
    }

    public Uni<Void> sendAccountDeleted(String to) {
        return mailer.send(multipart(to, "Your account is deleted",
                accountDeleted.render(), accountDeletedHtml.render()));
    }

    public Uni<Void> sendAccountDeletionFailed(String to) {
        return mailer.send(multipart(to, "We could not delete your account",
                accountDeletionFailed.render(), accountDeletionFailedHtml.render()));
    }

    private static io.quarkus.mailer.Mail multipart(String to, String subject, String text, String html) {
        return io.quarkus.mailer.Mail.withText(to, subject, text).setHtml(html);
    }
}

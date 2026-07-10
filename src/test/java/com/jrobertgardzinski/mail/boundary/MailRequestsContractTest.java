package com.jrobertgardzinski.mail.boundary;

import au.com.dius.pact.consumer.MessagePactBuilder;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.model.messaging.Message;
import au.com.dius.pact.core.model.messaging.MessagePact;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.mail.control.MailDispatcher;
import com.jrobertgardzinski.mail.entity.LinkMail;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The consumer's half of the mail-requests contract: each pact interaction states the exact shape
 * of a {@code mail-requests} event this service can act on, and proves it by driving the real
 * consumer with the pact's payload. On a green run the pact lands in {@code pacts/} (committed);
 * microservice-security's provider tests then verify that its REAL producers emit exactly these
 * shapes — the two repos can no longer drift apart silently. The contract lists only the fields
 * this consumer reads; the producer is free to add more (tolerant reader).
 */
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-security", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class MailRequestsContractTest {

    private final MailDispatcher dispatcher = Mockito.mock(MailDispatcher.class);
    private final MailRequestsConsumer consumer = new MailRequestsConsumer();

    @BeforeEach
    void wire() {
        consumer.dispatcher = dispatcher;
        consumer.mapper = new ObjectMapper();
    }

    // --- the templated link mails -------------------------------------------------------------

    @Pact(consumer = "microservice-email")
    MessagePact verificationMail(MessagePactBuilder builder) {
        return builder.expectsToReceive("a verification mail request")
                .withContent(new PactDslJsonBody()
                        .uuid("id")
                        .stringValue("type", "VERIFICATION")
                        .stringType("to", "user@example.com")
                        .stringType("link", "http://localhost:8083/?verify=abc"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "verificationMail")
    void deliversTheVerificationLink(List<Message> messages) {
        when(dispatcher.sendVerificationLink(any())).thenReturn(Uni.createFrom().voidItem());
        process(messages);
        verify(dispatcher).sendVerificationLink(
                new LinkMail("user@example.com", "http://localhost:8083/?verify=abc"));
    }

    @Pact(consumer = "microservice-email")
    MessagePact passwordResetMail(MessagePactBuilder builder) {
        return builder.expectsToReceive("a password reset mail request")
                .withContent(new PactDslJsonBody()
                        .uuid("id")
                        .stringValue("type", "PASSWORD_RESET")
                        .stringType("to", "user@example.com")
                        .stringType("link", "http://localhost:8080/reset-password?token=x"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "passwordResetMail")
    void deliversThePasswordResetLink(List<Message> messages) {
        when(dispatcher.sendPasswordResetLink(any())).thenReturn(Uni.createFrom().voidItem());
        process(messages);
        verify(dispatcher).sendPasswordResetLink(
                new LinkMail("user@example.com", "http://localhost:8080/reset-password?token=x"));
    }

    // --- the plain notices ---------------------------------------------------------------------

    @Pact(consumer = "microservice-email")
    MessagePact alreadyRegisteredMail(MessagePactBuilder builder) {
        return builder.expectsToReceive("an already-registered notice mail request")
                .withContent(new PactDslJsonBody()
                        .uuid("id")
                        .stringValue("type", "ALREADY_REGISTERED")
                        .stringType("to", "owner@example.com"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "alreadyRegisteredMail")
    void deliversTheAlreadyRegisteredNotice(List<Message> messages) {
        when(dispatcher.sendAlreadyRegistered(anyString())).thenReturn(Uni.createFrom().voidItem());
        process(messages);
        verify(dispatcher).sendAlreadyRegistered("owner@example.com");
    }

    @Pact(consumer = "microservice-email")
    MessagePact accountDeletedMail(MessagePactBuilder builder) {
        return builder.expectsToReceive("an account deleted mail request")
                .withContent(new PactDslJsonBody()
                        .uuid("id")
                        .stringValue("type", "ACCOUNT_DELETED")
                        .stringType("to", "leaver@example.com"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "accountDeletedMail")
    void deliversTheGoodbyeMail(List<Message> messages) {
        when(dispatcher.sendAccountDeleted(anyString())).thenReturn(Uni.createFrom().voidItem());
        process(messages);
        verify(dispatcher).sendAccountDeleted("leaver@example.com");
    }

    @Pact(consumer = "microservice-email")
    MessagePact accountDeletionFailedMail(MessagePactBuilder builder) {
        return builder.expectsToReceive("an account deletion failed mail request")
                .withContent(new PactDslJsonBody()
                        .uuid("id")
                        .stringValue("type", "ACCOUNT_DELETION_FAILED")
                        .stringType("to", "leaver@example.com"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "accountDeletionFailedMail")
    void deliversTheApologyMail(List<Message> messages) {
        when(dispatcher.sendAccountDeletionFailed(anyString())).thenReturn(Uni.createFrom().voidItem());
        process(messages);
        verify(dispatcher).sendAccountDeletionFailed("leaver@example.com");
    }

    // --- the MFA code --------------------------------------------------------------------------

    @Pact(consumer = "microservice-email")
    MessagePact authCodeMail(MessagePactBuilder builder) {
        return builder.expectsToReceive("an auth code mail request")
                .withContent(new PactDslJsonBody()
                        .uuid("id")
                        .stringValue("type", "AUTH_CODE")
                        .stringType("to", "signin@example.com")
                        .stringType("code", "482913"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "authCodeMail")
    void deliversTheAuthCode(List<Message> messages) {
        when(dispatcher.sendAuthCode(anyString(), anyString())).thenReturn(Uni.createFrom().voidItem());
        process(messages);
        verify(dispatcher).sendAuthCode("signin@example.com", "482913");
    }

    private void process(List<Message> messages) {
        consumer.process(messages.get(0).contentsAsString()).await().atMost(Duration.ofSeconds(5));
    }
}

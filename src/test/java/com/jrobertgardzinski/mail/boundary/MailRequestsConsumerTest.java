package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.mailer.Mail;
import io.quarkus.mailer.MockMailbox;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The asynchronous boundary, driven through the in-memory channel (tests need no broker): a mail
 * request event becomes a delivered mail; a redelivered event (same id) is deduplicated; garbage
 * is dropped without wedging the channel.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class MailRequestsConsumerTest {

    @Inject
    MockMailbox mailbox;

    @Inject
    @Any
    InMemoryConnector connector;

    @BeforeEach
    void clearMailbox() {
        mailbox.clear();
    }

    @Test
    void a_verification_request_event_becomes_a_mail() {
        connector.source("mail-requests").send(
                "{\"id\":\"e1\",\"type\":\"VERIFICATION\",\"to\":\"user@example.com\",\"link\":\"https://app/verify?token=abc\"}");

        await().until(() -> mailbox.getMailsSentTo("user@example.com").size() == 1);
        List<Mail> sent = mailbox.getMailsSentTo("user@example.com");
        assertEquals("Verify your email", sent.get(0).getSubject());
        assertTrue(sent.get(0).getText().contains("https://app/verify?token=abc"));
        assertTrue(sent.get(0).getHtml().contains("https://app/verify?token=abc"),
                "the mail is multipart: an HTML body rides along with the plain-text fallback");
    }

    @Test
    void an_already_registered_notice_event_becomes_a_mail() {
        connector.source("mail-requests").send(
                "{\"id\":\"e3\",\"type\":\"ALREADY_REGISTERED\",\"to\":\"owner@example.com\"}");

        await().until(() -> mailbox.getMailsSentTo("owner@example.com").size() == 1);
        Mail sent = mailbox.getMailsSentTo("owner@example.com").get(0);
        assertEquals("You already have an account", sent.getSubject());
        assertTrue(sent.getText().contains("account of its own"));
        assertTrue(sent.getHtml().contains("account of its own"),
                "the mail is multipart: an HTML body rides along with the plain-text fallback");
    }

    @Test
    void an_auth_code_event_becomes_a_mail_with_the_code() {
        connector.source("mail-requests").send(
                "{\"id\":\"e4\",\"type\":\"AUTH_CODE\",\"to\":\"signin@example.com\",\"code\":\"482913\"}");

        await().until(() -> mailbox.getMailsSentTo("signin@example.com").size() == 1);
        Mail sent = mailbox.getMailsSentTo("signin@example.com").get(0);
        assertEquals("Your sign-in code", sent.getSubject());
        assertTrue(sent.getText().contains("482913"));
        assertTrue(sent.getHtml().contains("482913"),
                "the mail is multipart: an HTML body rides along with the plain-text fallback");
    }

    @Test
    void a_redelivered_event_is_deduplicated_and_garbage_is_dropped() {
        connector.source("mail-requests").send("not json at all");
        connector.source("mail-requests").send(
                "{\"id\":\"e2\",\"type\":\"PASSWORD_RESET\",\"to\":\"dup@example.com\",\"link\":\"https://app/reset?token=x\"}");
        connector.source("mail-requests").send(
                "{\"id\":\"e2\",\"type\":\"PASSWORD_RESET\",\"to\":\"dup@example.com\",\"link\":\"https://app/reset?token=x\"}");

        await().until(() -> mailbox.getMailsSentTo("dup@example.com").size() == 1);
        assertEquals("Reset your password", mailbox.getMailsSentTo("dup@example.com").get(0).getSubject());
    }
}

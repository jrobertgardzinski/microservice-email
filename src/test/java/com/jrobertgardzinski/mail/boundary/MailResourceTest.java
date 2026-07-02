package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.mailer.MockMailbox;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Black-box tests of the boundary: mails posted with the API key are sent (captured by
 * {@link MockMailbox}, so no real SMTP); without the key the request is refused; malformed commands
 * are refused with 400; the templated endpoints render the action link into the body.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail commands")
class MailResourceTest {

    private static final String KEY = "test-key";

    @Inject
    MockMailbox mailbox;

    @BeforeEach
    void clearMailbox() {
        mailbox.clear();
    }

    @Test
    void posting_a_mail_with_the_api_key_sends_it() {
        given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"subject\":\"Hi\",\"text\":\"Hello\"}")
                .when().post("/mails")
                .then().statusCode(202);

        List<io.quarkus.mailer.Mail> sent = mailbox.getMailsSentTo("user@example.com");
        assertEquals(1, sent.size());
        assertEquals("Hi", sent.get(0).getSubject());
    }

    @Test
    void posting_a_malformed_mail_is_refused() {
        given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"not-an-address\",\"subject\":\"Hi\",\"text\":\"Hello\"}")
                .when().post("/mails")
                .then().statusCode(400);

        given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"subject\":\"\",\"text\":\"Hello\"}")
                .when().post("/mails")
                .then().statusCode(400);

        assertEquals(0, mailbox.getTotalMessagesSent());
    }

    @Test
    void posting_without_the_api_key_is_refused() {
        given().contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"subject\":\"Hi\",\"text\":\"Hello\"}")
                .when().post("/mails")
                .then().statusCode(401);

        assertEquals(0, mailbox.getTotalMessagesSent());
    }

    @Test
    void verification_endpoint_renders_the_link() {
        given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"link\":\"https://app/verify?token=abc\"}")
                .when().post("/mails/verification")
                .then().statusCode(202);

        List<io.quarkus.mailer.Mail> sent = mailbox.getMailsSentTo("user@example.com");
        assertEquals(1, sent.size());
        assertEquals("Verify your email", sent.get(0).getSubject());
        org.hamcrest.MatcherAssert.assertThat(sent.get(0).getText(),
                containsString("https://app/verify?token=abc"));
    }

    @Test
    void password_reset_endpoint_renders_the_link() {
        given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"link\":\"https://app/reset?token=xyz\"}")
                .when().post("/mails/password-reset")
                .then().statusCode(202);

        List<io.quarkus.mailer.Mail> sent = mailbox.getMailsSentTo("user@example.com");
        assertEquals(1, sent.size());
        assertEquals("Reset your password", sent.get(0).getSubject());
        org.hamcrest.MatcherAssert.assertThat(sent.get(0).getText(),
                containsString("https://app/reset?token=xyz"));
    }
}

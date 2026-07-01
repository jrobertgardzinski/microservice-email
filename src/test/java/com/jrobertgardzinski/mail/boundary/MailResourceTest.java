package com.jrobertgardzinski.mail.boundary;

import io.quarkus.mailer.MockMailbox;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Black-box test of the boundary: POST a mail command and assert it was sent (captured by
 * {@link MockMailbox}, so no real SMTP is touched).
 */
@QuarkusTest
class MailResourceTest {

    @Inject
    MockMailbox mailbox;

    @BeforeEach
    void clearMailbox() {
        mailbox.clear();
    }

    @Test
    void posting_a_mail_sends_it() {
        given()
                .contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"subject\":\"Verify your email\",\"text\":\"Click the link\"}")
                .when()
                .post("/mails")
                .then()
                .statusCode(202);

        List<io.quarkus.mailer.Mail> sent = mailbox.getMailsSentTo("user@example.com");
        assertEquals(1, sent.size());
        assertEquals("Verify your email", sent.get(0).getSubject());
        assertEquals("Click the link", sent.get(0).getText());
    }
}

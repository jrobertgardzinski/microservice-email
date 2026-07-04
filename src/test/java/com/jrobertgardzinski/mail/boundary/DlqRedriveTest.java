package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.mailer.MockMailbox;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The operator's loop closed: a parked event shows on the ledger, one POST pushes it back
 * through the normal delivery path, and when the world works again the mail actually leaves.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class DlqRedriveTest {

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
    @DisplayName("a parked event is listed, re-driven, delivered, and off the ledger")
    void parked_event_is_redriven() {
        connector.source("mail-requests-dlq-in").send(
                "{\"event\":{\"id\":\"parked-7\",\"type\":\"VERIFICATION\",\"to\":\"late@example.com\","
                        + "\"link\":\"https://app/verify?token=z\"},"
                        + "\"failure\":\"connection refused\",\"attempts\":4}");

        await().atMost(Duration.ofSeconds(5)).until(() ->
                RestAssured.given().header("X-Api-Key", "test-key").get("/mails/dlq")
                        .jsonPath().getList("$").size() == 1);
        var listed = RestAssured.given().header("X-Api-Key", "test-key").get("/mails/dlq");
        assertEquals("connection refused", listed.jsonPath().getString("[0].failure"));

        RestAssured.given().header("X-Api-Key", "test-key")
                .post("/mails/dlq/parked-7/redrive")
                .then().statusCode(202);

        await().atMost(Duration.ofSeconds(5))
                .until(() -> mailbox.getMailsSentTo("late@example.com").size() == 1);
        assertEquals(0, RestAssured.given().header("X-Api-Key", "test-key")
                .get("/mails/dlq").jsonPath().getList("$").size(), "off the ledger once re-driven");

        RestAssured.given().header("X-Api-Key", "test-key")
                .post("/mails/dlq/parked-7/redrive")
                .then().statusCode(404);
    }
}

package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.mail.control.MailDispatcher;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;

/**
 * A re-drive attempted while the world is STILL broken.
 *
 * <p>This is the case the happy-path re-drive test could never reach, and the one that mattered.
 * Every failure path inside {@code process} recovers — an SMTP outage parks the event, and parking
 * recovers its own failure too — so the delivery Uni always completed successfully. The endpoint
 * chained its retraction onto that Uni, which meant a re-drive during an outage published a
 * retraction for a mail that had just been parked AGAIN. Both records land on the same topic from
 * the same producer, in that order, so the ledger did put-then-remove and the only trace of an
 * undelivered password-reset or MFA mail disappeared — while the operator was told 202 REDRIVEN.
 *
 * <p>An operator re-driving during an outage is not an edge case. It is the single most likely
 * moment for somebody to press that button.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class DlqRedriveDuringOutageTest {

    @InjectMock
    MailDispatcher dispatcher;

    @Inject
    @Any
    InMemoryConnector connector;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("a re-drive that fails again keeps the mail on the ledger and says so")
    void a_failed_redrive_neither_retracts_nor_claims_success() throws Exception {
        Mockito.when(dispatcher.sendPasswordResetLink(any()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("smtp still down")));
        connector.source("mail-requests-dlq-in").send(
                "{\"event\":{\"id\":\"still-doomed\",\"type\":\"PASSWORD_RESET\","
                        + "\"to\":\"victim@example.com\",\"link\":\"https://app/reset?token=z\"},"
                        + "\"failure\":\"connection refused\",\"attempts\":4}");
        await().atMost(Duration.ofSeconds(5)).until(() -> ledgerSize() == 1);
        var deadLetters = connector.sink("mail-requests-dlq");
        int recordsBefore = deadLetters.received().size();

        RestAssured.given().header("X-Api-Key", "test-key")
                .post("/mails/dlq/still-doomed/redrive")
                .then().statusCode(503)
                .body("status", org.hamcrest.Matchers.is("PARKED_AGAIN"));

        // the event went back onto the dead-letter topic, and NOTHING retracted it
        await().atMost(Duration.ofSeconds(20))
                .until(() -> deadLetters.received().size() > recordsBefore);
        for (int record = recordsBefore; record < deadLetters.received().size(); record++) {
            JsonNode published = mapper.readTree(
                    String.valueOf(deadLetters.received().get(record).getPayload()));
            assertFalse(published.has("redriven"),
                    "a mail that did not go out must never be retracted: " + published);
        }
        assertEquals("still-doomed", mapper.readTree(String.valueOf(
                deadLetters.received().get(deadLetters.received().size() - 1).getPayload()))
                .get("event").get("id").asText(), "it is parked again, with its story");
    }

    private int ledgerSize() {
        return RestAssured.given().header("X-Api-Key", "test-key")
                .get("/mails/dlq").jsonPath().getList("$").size();
    }
}

package com.jrobertgardzinski.mail.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import com.jrobertgardzinski.mail.control.MailDispatcher;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
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
import static org.mockito.ArgumentMatchers.any;

/**
 * The dead letter: when the SMTP server stays down past every retry, the event is PARKED on the
 * {@code mail-requests-dlq} channel with the failure attached — not dropped, not wedging the
 * partition.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class DeadLetterTest {

    @InjectMock
    MailDispatcher dispatcher;

    @Inject
    @Any
    InMemoryConnector connector;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("a send that keeps failing is parked with its story, not lost")
    void exhausted_sends_are_parked() throws Exception {
        Mockito.when(dispatcher.sendVerificationLink(any()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("connection refused")));
        var deadLetters = connector.sink("mail-requests-dlq");

        connector.source("mail-requests").send(
                "{\"id\":\"doomed-1\",\"type\":\"VERIFICATION\",\"to\":\"user@example.com\","
                        + "\"link\":\"https://app/verify?token=x\"}");

        await().atMost(Duration.ofSeconds(20)).until(() -> deadLetters.received().size() == 1);
        JsonNode parked = mapper.readTree(String.valueOf(deadLetters.received().get(0).getPayload()));
        assertEquals("doomed-1", parked.get("event").get("id").asText(),
                "the original event rides inside the parked record");
        assertEquals(MailRequestsConsumer.SMTP_RETRIES + 1, parked.get("attempts").asInt());
        assertEquals("connection refused", parked.get("failure").asText());
    }
}

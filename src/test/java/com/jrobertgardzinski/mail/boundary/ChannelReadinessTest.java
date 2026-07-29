package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.restassured.RestAssured;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasItem;

/**
 * That the mail pipeline's own channel is enrolled in readiness — the assertion
 * {@link HealthEndpointTest} claimed to make and could not.
 *
 * <p>This whole service is one Kafka channel. If {@code mail-requests} drops out of the readiness
 * report — renamed, or un-enrolled by a default that flips in a SmallRye upgrade, which is the same
 * class of silent change as the CORS default that flipped in Micronaut 5 — then a dead mail pipeline
 * answers 200 UP, and verification links, password resets and MFA codes stop arriving with every
 * probe green. The umbrella check's NAME is present either way, so asserting on it proves nothing
 * about the channel.
 *
 * <p>It needs the Kafka connector to prove it: only that connector contributes per-channel data to
 * the health report, and the {@code %test} profile swaps every channel to the in-memory one. So this
 * class puts the real connector back and points it at a bootstrap address with nothing behind it.
 * The channel is then reported DOWN, readiness answers 503, and that is exactly the shape being
 * asserted — this test is about the channel being NAMED, not about it being well. A broker is
 * deliberately not started: "the pipeline is dead" is the situation readiness exists to report.
 */
@QuarkusTest
@TestProfile(ChannelReadinessTest.KafkaConnectorWithoutABroker.class)
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class ChannelReadinessTest {

    public static class KafkaConnectorWithoutABroker implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "mp.messaging.incoming.mail-requests.connector", "smallrye-kafka",
                    "mp.messaging.outgoing.mail-requests-dlq.connector", "smallrye-kafka",
                    "mp.messaging.incoming.mail-requests-dlq-in.connector", "smallrye-kafka",
                    // nothing listens here, on purpose
                    "kafka.bootstrap.servers", "localhost:1");
        }
    }

    @Test
    @DisplayName("the readiness report names the mail-requests channel, whatever it says about it")
    void readiness_names_the_mail_requests_channel() {
        RestAssured.given().get("/q/health/ready")
                .then()
                .body("checks.name", hasItem("SmallRye Reactive Messaging - readiness check"))
                .body("checks.find { it.name == 'SmallRye Reactive Messaging - readiness check' }.data",
                        hasKey("mail-requests"));
    }
}

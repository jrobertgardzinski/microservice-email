package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The per-minute ceiling on the REST boundary: within it, sends are accepted; above it, {@code
 * 429} with a Retry-After — the SMTP server behind us is a shared resource, not a firehose.
 */
@QuarkusTest
@TestProfile(RateLimitTest.ThreePerMinute.class)
@Epic("Boundary")
@Feature("Send mail")
class RateLimitTest {

    public static class ThreePerMinute implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("mail.rate-limit.per-minute", "3");
        }
    }

    @Test
    @DisplayName("the ceiling holds: three go out, the fourth waits its turn")
    void the_ceiling_holds() {
        for (int i = 0; i < 3; i++) {
            RestAssured.given()
                    .header("X-Api-Key", "test-key")
                    .contentType("application/json")
                    .body("{\"to\":\"user@example.com\",\"subject\":\"s\",\"text\":\"t\"}")
                    .post("/mails")
                    .then().statusCode(202);
        }
        var refused = RestAssured.given()
                .header("X-Api-Key", "test-key")
                .contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"subject\":\"s\",\"text\":\"t\"}")
                .post("/mails");
        assertEquals(429, refused.statusCode());
        assertEquals("RATE_LIMITED", refused.jsonPath().getString("status"));
        assertNotNull(refused.header("Retry-After"), "a polite refusal says when to come back");
    }
}

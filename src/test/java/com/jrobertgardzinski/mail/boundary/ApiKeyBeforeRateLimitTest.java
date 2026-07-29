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

/**
 * Which of the two boundary guards decides first — asserted through behaviour, because the answer
 * was a coin toss the specification does not settle.
 *
 * <p>{@link RateLimitFilter} describes itself as "boundary guard number two, AFTER the API key". It
 * declares {@code @Priority(Priorities.USER)} = 5000; {@link ApiKeyFilter} declared no priority at
 * all, which is the same 5000, and JAX-RS leaves the order of equal priorities undefined. The
 * documented sequence was therefore whatever the container happened to do, and a Quarkus upgrade
 * re-tying it is not a hypothetical — this estate has already been bitten by a framework default
 * flipping under it.
 *
 * <p>The cost of the reversal is the point. Put the limiter first and the per-minute window becomes
 * a resource ANY anonymous caller can exhaust: keyless requests spend it, callers holding a correct
 * key get 429 where they should get 202, and the mail pipeline is throttled by exactly the traffic
 * the key exists to turn away — with a log that cannot tell that apart from legitimate load.
 *
 * <p>Its own profile, and therefore its own application instance: the rate-limit window is process
 * state, so a test that fills it deliberately must not share one with the test that measures it.
 */
@QuarkusTest
@TestProfile(ApiKeyBeforeRateLimitTest.OnePerMinute.class)
@Epic("Boundary")
@Feature("Send mail")
class ApiKeyBeforeRateLimitTest {

    public static class OnePerMinute implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            // one, so that a single counted keyless request would already be enough to break the
            // keyed send below — no reliance on volume
            return Map.of("mail.rate-limit.per-minute", "1");
        }
    }

    private static io.restassured.specification.RequestSpecification send() {
        return RestAssured.given()
                .contentType("application/json")
                .body("{\"to\":\"user@example.com\",\"subject\":\"s\",\"text\":\"t\"}");
    }

    @Test
    @DisplayName("keyless callers are refused before they can spend the window")
    void the_key_is_checked_before_the_window_is_spent() {
        for (int keyless = 0; keyless < 5; keyless++) {
            send().post("/mails").then().statusCode(401);
        }

        send().header("X-Api-Key", "test-key").post("/mails")
                .then().statusCode(202);
    }
}

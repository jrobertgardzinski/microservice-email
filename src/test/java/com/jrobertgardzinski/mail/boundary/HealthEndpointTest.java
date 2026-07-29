package com.jrobertgardzinski.mail.boundary;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

/**
 * The probes have something to ask.
 *
 * <p>This service's entire job runs on a Kafka channel, and a channel can shut itself down while
 * the HTTP port stays open. The container healthcheck and the k8s probes were bare TCP connects for
 * exactly as long as {@code /q/health} answered 404 — so a mail pipeline that had stopped sending
 * verification links, password resets and MFA codes reported itself green the whole time. Nothing
 * in the build noticed the endpoint was missing, which is why this test exists at all.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class HealthEndpointTest {

    @Test
    @DisplayName("readiness answers, and the reactive-messaging check is what it answers with")
    void readiness_reports_the_messaging_subsystem() {
        RestAssured.given().get("/q/health/ready")
                .then().statusCode(200)
                .body("status", is("UP"))
                // the point is not that SOMETHING answers — a bare 200 would be no better than the
                // TCP probe it replaces.
                //
                // This is the UMBRELLA's name, and that is all it is. The @DisplayName used to
                // claim this line names the mail-requests CHANNEL, and a comment underneath
                // insisted "the channels have to be what is being reported on" — while the string
                // "mail-requests" appeared nowhere in the file. Under the test profile the channels
                // run on the in-memory connector, which contributes no per-channel data at all, so
                // the assertion this class needed could not be written here. It is written in
                // ChannelReadinessTest, which stands the Kafka connector up instead.
                .body("checks.name", hasItem("SmallRye Reactive Messaging - readiness check"));
    }

    @Test
    @DisplayName("liveness answers too — it is what the k8s livenessProbe asks")
    void liveness_answers() {
        RestAssured.given().get("/q/health/live")
                .then().statusCode(200)
                .body("status", is("UP"));
    }
}

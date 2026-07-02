package com.jrobertgardzinski.mail.boundary.cucumber;

import io.cucumber.java.Before;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.quarkus.mailer.Mail;
import io.quarkus.mailer.MockMailbox;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import jakarta.inject.Inject;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Glue for {@code send-mail.feature}: drives the boundary over real HTTP (RestAssured against the
 * Quarkus test server); deliveries are observed through {@link MockMailbox}, so no real SMTP.
 */
public class MailContractSteps {

    private static final String KEY = "test-key";

    @Inject
    MockMailbox mailbox;

    private Response response;
    private String lastRecipient;

    @Before
    public void clearMailbox() {
        mailbox.clear();
    }

    @When("a trusted service posts a mail to {string} with subject {string}")
    public void a_trusted_service_posts_a_mail(String to, String subject) {
        lastRecipient = to;
        response = RestAssured.given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"" + to + "\",\"subject\":\"" + subject + "\",\"text\":\"Hello\"}")
                .post("/mails");
    }

    @When("an unknown caller posts a mail to {string} with subject {string}")
    public void an_unknown_caller_posts_a_mail(String to, String subject) {
        response = RestAssured.given().contentType("application/json")
                .body("{\"to\":\"" + to + "\",\"subject\":\"" + subject + "\",\"text\":\"Hello\"}")
                .post("/mails");
    }

    @When("a trusted service posts a mail with a blank recipient")
    public void a_trusted_service_posts_a_blank_recipient() {
        response = RestAssured.given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"\",\"subject\":\"Hi\",\"text\":\"Hello\"}")
                .post("/mails");
    }

    @When("a trusted service requests a verification mail to {string} with link {string}")
    public void a_trusted_service_requests_a_verification_mail(String to, String link) {
        lastRecipient = to;
        response = RestAssured.given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"" + to + "\",\"link\":\"" + link + "\"}")
                .post("/mails/verification");
    }

    @When("a trusted service requests a password-reset mail to {string} with link {string}")
    public void a_trusted_service_requests_a_password_reset_mail(String to, String link) {
        lastRecipient = to;
        response = RestAssured.given().header("X-Api-Key", KEY).contentType("application/json")
                .body("{\"to\":\"" + to + "\",\"link\":\"" + link + "\"}")
                .post("/mails/password-reset");
    }

    @Then("the request is accepted")
    public void the_request_is_accepted() {
        assertEquals(202, response.statusCode());
    }

    @Then("the request is refused as unauthorized")
    public void the_request_is_refused_as_unauthorized() {
        assertEquals(401, response.statusCode());
    }

    @Then("the request is refused as invalid")
    public void the_request_is_refused_as_invalid() {
        assertEquals(400, response.statusCode());
    }

    @Then("a mail with subject {string} is delivered to {string}")
    public void a_mail_is_delivered(String subject, String to) {
        List<Mail> sent = mailbox.getMailsSentTo(to);
        assertEquals(1, sent.size());
        assertEquals(subject, sent.get(0).getSubject());
    }

    @Then("the delivered mail body contains {string}")
    public void the_delivered_mail_body_contains(String needle) {
        List<Mail> sent = mailbox.getMailsSentTo(lastRecipient);
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).getText().contains(needle),
                "expected body to contain " + needle + " but was: " + sent.get(0).getText());
    }

    @Then("no mail is delivered")
    public void no_mail_is_delivered() {
        assertEquals(0, mailbox.getTotalMessagesSent());
    }
}

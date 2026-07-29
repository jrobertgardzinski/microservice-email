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
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * What a parked record is CALLED, on the topic and in the ledger — which is the whole of whether the
 * dead-letter queue keeps evidence or destroys it.
 *
 * <p>{@code mail-requests-dlq} is compacted (docker-compose.identity.yml, and the k8s topic job),
 * so the key is not a label: it is what the broker keeps one record per. Every event without an id
 * used to be keyed with the literal {@code "<no id>"} — one key for all of them — so parking a
 * second id-less mail told the broker to delete the first. No log line, no ledger entry, nothing to
 * find afterwards: exactly the silent loss the keying was added to prevent. Meanwhile the ledger
 * invented its own key, {@code "unidentified-" + ledger.size()}, which is neither stable across a
 * {@code take()} nor equal to anything on the topic, so retractions were published under a key no
 * record had.
 */
@QuarkusTest
@Epic("Boundary")
@Feature("Mail requests over Kafka")
class DlqLedgerKeysTest {

    @InjectMock
    MailDispatcher dispatcher;

    @Inject
    @Any
    InMemoryConnector connector;

    private final ObjectMapper mapper = new ObjectMapper();

    private static String anonymousVerification(String recipient) {
        // no "id": pre-ADR-0004 events and anything hand-published on the topic look like this, and
        // ParkedMails admits them deliberately
        return "{\"type\":\"VERIFICATION\",\"to\":\"" + recipient + "\","
                + "\"link\":\"https://app/verify?token=x\"}";
    }

    @Test
    @DisplayName("two id-less mails get two keys, so compaction keeps both")
    void id_less_mails_do_not_share_one_key() {
        Mockito.when(dispatcher.sendVerificationLink(any()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("connection refused")));
        var deadLetters = connector.<String>sink("mail-requests-dlq");
        int before = deadLetters.received().size();

        connector.source("mail-requests").send(anonymousVerification("first@example.com"));
        connector.source("mail-requests").send(anonymousVerification("second@example.com"));

        await().atMost(Duration.ofSeconds(30))
                .until(() -> deadLetters.received().size() == before + 2);
        String firstKey = keyOf(deadLetters.received().get(before));
        String secondKey = keyOf(deadLetters.received().get(before + 1));
        assertNotEquals(firstKey, secondKey,
                "one key for every id-less mail means the broker keeps only the last of them: the"
                        + " record of the first undelivered mail is destroyed by compaction, silently");
        assertTrue(firstKey.startsWith(MailRequestsConsumer.UNIDENTIFIED_PREFIX), firstKey);
        assertTrue(secondKey.startsWith(MailRequestsConsumer.UNIDENTIFIED_PREFIX), secondKey);
    }

    @Test
    @DisplayName("the key the record is published under is the id the ledger and a redrive use")
    void the_topic_key_and_the_ledger_key_are_the_same_string() throws Exception {
        Mockito.when(dispatcher.sendVerificationLink(any()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("connection refused")));
        var deadLetters = connector.<String>sink("mail-requests-dlq");
        int before = deadLetters.received().size();

        connector.source("mail-requests").send(anonymousVerification("nameless@example.com"));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> deadLetters.received().size() == before + 1);

        Message<String> record = deadLetters.received().get(before);
        String key = keyOf(record);
        // the restart: the topic replays into the ledger the operator reads
        connector.source("mail-requests-dlq-in").send(record.getPayload());
        await().atMost(Duration.ofSeconds(5)).until(() -> listed(key) != null);

        assertEquals("nameless@example.com", listed(key).path("event").path("to").asText());
        // and it can be acted on under that id — the retraction the re-drive publishes will
        // therefore land on the record it settles, instead of on a key nothing was ever written to.
        // 503 and not 202 because the dispatcher above is still refusing: the point is that the id
        // was FOUND (404 would mean the ledger files records under a name the topic never used).
        RestAssured.given().header("X-Api-Key", "test-key")
                .post("/mails/dlq/" + key + "/redrive")
                .then().statusCode(503);
    }

    @Test
    @DisplayName("a re-drive that fails again re-parks under the SAME id, it does not breed records")
    void a_failed_redrive_does_not_multiply_the_record() {
        // The regression this catches was introduced by the fix above it. Once an id-less mail gets
        // a synthetic id minted at parking time, a re-drive that fails would mint ANOTHER one — and
        // on a compacted topic two different keys are two records that nothing ever collapses. One
        // undelivered mail would become one more entry in the operator's list per attempt, which is
        // the same "the ledger stops telling the truth" the keying exists to prevent, from the other
        // direction.
        Mockito.when(dispatcher.sendVerificationLink(any()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("connection refused")));
        var deadLetters = connector.<String>sink("mail-requests-dlq");

        String id = MailRequestsConsumer.UNIDENTIFIED_PREFIX + "already-parked-once";
        connector.source("mail-requests-dlq-in").send(
                "{\"parkedId\":\"" + id + "\",\"event\":" + anonymousVerification("again@example.com")
                        + ",\"failure\":\"connection refused\",\"attempts\":4}");
        await().atMost(Duration.ofSeconds(5)).until(() -> listed(id) != null);
        int before = deadLetters.received().size();

        RestAssured.given().header("X-Api-Key", "test-key")
                .post("/mails/dlq/" + id + "/redrive")
                .then().statusCode(503)
                .body("status", org.hamcrest.Matchers.equalTo("PARKED_AGAIN"));

        await().atMost(Duration.ofSeconds(30))
                .until(() -> deadLetters.received().size() > before);
        assertEquals(id, keyOf(deadLetters.received().get(before)),
                "the re-parked record must overwrite the one the operator re-drove, not sit beside"
                        + " it under a freshly minted id");

        connector.source("mail-requests-dlq-in").send("{\"redriven\":\"" + id + "\"}");
        await().atMost(Duration.ofSeconds(5)).until(() -> listed(id) == null);
    }

    @Test
    @DisplayName("re-driving a record with no event answers 422 and keeps it on the ledger")
    void a_shapeless_record_is_not_swallowed_by_the_redrive() {
        // ParkedMails reads the id down a null-safe path precisely because anything may be published
        // on a durable topic by hand, so such records ARE listed — and were therefore re-drivable.
        // take() removed it, record.get("event") NPE'd, the operator got a 500, and the only window
        // onto that dead mail was closed until the next restart.
        String id = MailRequestsConsumer.UNIDENTIFIED_PREFIX + "shapeless-1";
        connector.source("mail-requests-dlq-in").send(
                "{\"parkedId\":\"" + id + "\",\"failure\":\"published by hand\",\"attempts\":1}");
        await().atMost(Duration.ofSeconds(5)).until(() -> listed(id) != null);

        RestAssured.given().header("X-Api-Key", "test-key")
                .post("/mails/dlq/" + id + "/redrive")
                .then().statusCode(422)
                .body("status", org.hamcrest.Matchers.equalTo("NOT_REDRIVABLE"));

        assertTrue(listed(id) != null,
                "the record was taken off the ledger to be re-driven and nothing put it back:"
                        + " an endpoint built to make dead mail visible made this one invisible");

        // The ledger is application-scoped and every test in this suite shares it, so this one
        // settles what it parked — through the production path, a retraction record, which is the
        // only way anything ever leaves the ledger for good.
        connector.source("mail-requests-dlq-in").send("{\"redriven\":\"" + id + "\"}");
        await().atMost(Duration.ofSeconds(5)).until(() -> listed(id) == null);
    }

    @Test
    @DisplayName("dwa stare bezidowe rekordy to dwa wpisy — kompakcja nie tknęła aktywnego segmentu")
    void two_legacy_id_less_records_are_two_entries() {
        // The first version of ledgerKey folded every id-less legacy record into one constant, on
        // the reasoning that "they already share ONE key on a compacted topic, so at most one of
        // them survives there". The log cleaner never touches the ACTIVE segment, and on a topic
        // that sees a record a month that segment may never roll — so both records are still on the
        // broker, and folding them in memory means the second replay overwrites the first. The older
        // undelivered mail (a reset link, an MFA code) becomes invisible and un-redrivable while the
        // broker still has it: the exact loss this ledger exists to prevent, from the inside.
        String older = "{\"event\":{\"type\":\"VERIFICATION\",\"to\":\"older@example.com\","
                + "\"link\":\"https://app/verify?token=a\"},\"failure\":\"connection refused\",\"attempts\":4}";
        String newer = "{\"event\":{\"type\":\"VERIFICATION\",\"to\":\"newer@example.com\","
                + "\"link\":\"https://app/verify?token=b\"},\"failure\":\"connection refused\",\"attempts\":4}";
        connector.source("mail-requests-dlq-in").send(older);
        connector.source("mail-requests-dlq-in").send(newer);

        await().atMost(Duration.ofSeconds(5)).until(() -> listedTo("older@example.com") != null
                && listedTo("newer@example.com") != null);

        assertNotNull(listedTo("older@example.com"),
                "the older id-less record is gone from the operator's only window while the broker"
                        + " still has it — one ledger key for all of them is a silent overwrite");
        assertNotNull(listedTo("newer@example.com"));
    }

    /** The parked record whose event was addressed to this recipient, or null. */
    private JsonNode listedTo(String recipient) {
        try {
            JsonNode all = mapper.readTree(RestAssured.given().header("X-Api-Key", "test-key")
                    .get("/mails/dlq").asString());
            for (JsonNode record : all) {
                if (recipient.equals(record.path("event").path("to").asText())) {
                    return record;
                }
            }
            return null;
        } catch (Exception unreadable) {
            throw new AssertionError(unreadable);
        }
    }

    @Test
    @DisplayName("re-drive rekordu z event:null i z nieznanym typem — 422 i wpis zostaje na ledgerze")
    void every_undeliverable_shape_is_refused_rather_than_swallowed() {
        // The guard added in 48e92fa tested `record.get("event") == null` and nothing else, which
        // covers one of three shapes. A literal "event": null parses to a NullNode — not null — and
        // an unknown type parses fine; both reached process(), which DROPS them without parking, so
        // the record was gone from the ledger, absent from the topic, and the operator was told
        // "PARKED_AGAIN — the ledger will show it again". It does not.
        record Shape(String id, String body) { }
        java.util.List<Shape> shapes = java.util.List.of(
                new Shape(MailRequestsConsumer.UNIDENTIFIED_PREFIX + "null-event",
                        "{\"parkedId\":\"" + MailRequestsConsumer.UNIDENTIFIED_PREFIX
                                + "null-event\",\"event\":null,\"failure\":\"by hand\",\"attempts\":1}"),
                new Shape(MailRequestsConsumer.UNIDENTIFIED_PREFIX + "unknown-type",
                        "{\"parkedId\":\"" + MailRequestsConsumer.UNIDENTIFIED_PREFIX
                                + "unknown-type\",\"event\":{\"type\":\"NEWSLETTER\","
                                + "\"to\":\"someone@example.com\"},\"failure\":\"by hand\",\"attempts\":1}"));

        for (Shape shape : shapes) {
            connector.source("mail-requests-dlq-in").send(shape.body());
            await().atMost(Duration.ofSeconds(5)).until(() -> listed(shape.id()) != null);

            RestAssured.given().header("X-Api-Key", "test-key")
                    .post("/mails/dlq/" + shape.id() + "/redrive")
                    .then().statusCode(422)
                    .body("status", org.hamcrest.Matchers.equalTo("NOT_REDRIVABLE"));

            assertNotNull(listed(shape.id()),
                    shape.id() + " was taken off the ledger and never came back — the operator got"
                            + " a 503 promising the ledger would show it again, and it will not");

            connector.source("mail-requests-dlq-in").send("{\"redriven\":\"" + shape.id() + "\"}");
            await().atMost(Duration.ofSeconds(5)).until(() -> listed(shape.id()) == null);
        }
    }

    private static String keyOf(Message<? extends String> record) {
        return record.getMetadata(OutgoingKafkaRecordMetadata.class)
                .map(metadata -> String.valueOf(metadata.getKey()))
                .orElseThrow(() -> new AssertionError(
                        "a parked record without a key is a record compaction cannot keep"));
    }

    /** The parked record the operator would see under this id, or null. */
    private JsonNode listed(String id) {
        try {
            JsonNode all = mapper.readTree(RestAssured.given().header("X-Api-Key", "test-key")
                    .get("/mails/dlq").asString());
            for (JsonNode record : all) {
                if (id.equals(record.path("parkedId").asText())
                        || id.equals(record.path("event").path("id").asText())) {
                    return record;
                }
            }
            return null;
        } catch (Exception unreadable) {
            throw new AssertionError(unreadable);
        }
    }
}

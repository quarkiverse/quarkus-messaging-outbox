package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

/** Deduplication ids on AMQP: _AMQ_DUPL_ID, the message id and the outbox-id application property. */
@QuarkusTest
public class AmqpDeduplicationTest {

    @Test
    void artemisDropsTheRedelivery() {
        given().queryParam("msg", "dup-1").when().post("/amqp-dedup/send").then().statusCode(204);

        // The store loses the first delivered() call, so the relay re-sends. Artemis drops the second copy
        // because _AMQ_DUPL_ID is the same. The record is eventually settled.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(copiesOf("dup-1")).hasSize(1);
            assertThat(pending("amqp-dedup")).isZero();
        });
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(copiesOf("dup-1")).hasSize(1));

        Map<String, String> copy = copiesOf("dup-1").get(0);
        String outboxId = copy.get("outboxId");
        UUID.fromString(outboxId);
        assertThat(copy.get("messageId")).isEqualTo(outboxId);
        assertThat(copy.get("duplicateId")).isEqualTo(outboxId);
    }

    @Test
    void applicationDuplicateIdIsKept() {
        given().queryParam("msg", "app-dupl-1").queryParam("id", "business-42")
                .when().post("/amqp-dedup/send-with-duplicate-id").then().statusCode(204);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(copiesOf("app-dupl-1")).hasSize(1));
        assertThat(copiesOf("app-dupl-1").get(0).get("duplicateId")).isEqualTo("business-42");
    }

    @Test
    void applicationMessageIdIsKept() {
        given().queryParam("msg", "app-1").queryParam("id", "app-message-1")
                .when().post("/amqp-dedup/send-with-message-id").then().statusCode(204);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(copiesOf("app-1")).hasSize(1));

        Map<String, String> copy = copiesOf("app-1").get(0);
        assertThat(copy.get("messageId")).isEqualTo("app-message-1");
        UUID.fromString(copy.get("outboxId"));
    }

    private static long pending(String channel) {
        return Long.parseLong(given().queryParam("channel", channel).when().get("/amqp-dedup/pending")
                .then().statusCode(200).extract().body().asString());
    }

    private static List<Map<String, String>> copiesOf(String payload) {
        List<Map<String, String>> all = given().when().get("/amqp-dedup/received")
                .then().statusCode(200).extract().jsonPath().getList(".");
        return all.stream().filter(entry -> payload.equals(entry.get("payload"))).toList();
    }
}

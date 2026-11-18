package io.quarkiverse.messaging.outbox.it.rabbitmq;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

/** The ids a RabbitMQ consumer deduplicates on: the message-id property and the outbox-id header. */
@QuarkusTest
public class RabbitMQDeduplicationTest {

    @Test
    void redeliveredMessageCarriesTheSameIds() {
        given().queryParam("msg", "dup-1").when().post("/rabbitmq-dedup/send").then().statusCode(204);

        // The store loses the first delivered() call, so the relay re-sends and the consumer sees two copies.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(copiesOf("dup-1")).hasSize(2));

        List<Map<String, String>> copies = copiesOf("dup-1");
        String outboxId = copies.get(0).get("outboxId");
        UUID.fromString(outboxId);
        assertThat(copies).allSatisfy(copy -> {
            assertThat(copy.get("outboxId")).isEqualTo(outboxId);
            assertThat(copy.get("messageId")).isEqualTo(outboxId);
        });
    }

    @Test
    void applicationMessageIdIsKept() {
        given().queryParam("msg", "app-1").queryParam("id", "app-message-1")
                .when().post("/rabbitmq-dedup/send-with-message-id").then().statusCode(204);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(copiesOf("app-1")).hasSize(1));

        Map<String, String> copy = copiesOf("app-1").get(0);
        assertThat(copy.get("messageId")).isEqualTo("app-message-1");
        // The outbox id is still there, as a header.
        UUID.fromString(copy.get("outboxId"));
    }

    private static List<Map<String, String>> copiesOf(String payload) {
        List<Map<String, String>> all = given().when().get("/rabbitmq-dedup/received")
                .then().statusCode(200).extract().jsonPath().getList(".");
        return all.stream().filter(entry -> payload.equals(entry.get("payload"))).toList();
    }
}

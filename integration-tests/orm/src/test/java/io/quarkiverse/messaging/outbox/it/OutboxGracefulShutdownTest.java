package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(AmqpToxiproxyResource.class)
public class OutboxGracefulShutdownTest {

    @BeforeEach
    void clearState() {
        given().when().post("/outbox/received/clear").then().statusCode(204);
    }

    @Test
    void f13_noClaimedRecordsOrPendingAfterBatchDelivery() {
        // Wait for records from other tests (e.g. the dedup store that fails delivered() once) to settle.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long pending = Long.parseLong(given().when().get("/outbox/outbox-table/pending")
                    .then().statusCode(200).extract().body().asString());
            assertThat(pending).isZero();
        });

        given().queryParam("count", 20)
                .when().post("/outbox/send-batch")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(20);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            long claimed = Long.parseLong(given().when().get("/outbox/outbox-table/claimed")
                    .then().statusCode(200).extract().body().asString());
            assertThat(claimed).isEqualTo(0);
        });

        long pending = Long.parseLong(given().when().get("/outbox/outbox-table/pending")
                .then().statusCode(200).extract().body().asString());
        assertThat(pending).isEqualTo(0);
    }
}

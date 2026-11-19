package io.quarkiverse.messaging.outbox.it.rabbitmq;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

/**
 * The ids RabbitMQ consumers deduplicate on. {@code rabbitmq-dedup} loses the first ack of each record, so every
 * message arrives twice; {@code rabbitmq-app-id} lets the application set its own message id.
 */
@Path("/rabbitmq-dedup")
@ApplicationScoped
public class RabbitMQDeduplicationResource {

    @Inject
    @Channel("rabbitmq-dedup")
    OutboxEmitter<String> dedupEmitter;

    @Inject
    @Channel("rabbitmq-app-id")
    OutboxEmitter<String> appIdEmitter;

    private final List<Map<String, String>> received = new CopyOnWriteArrayList<>();

    @Incoming("rabbitmq-dedup-in")
    public void consumeDedup(String payload, IncomingRabbitMQMetadata metadata) {
        record(payload, metadata);
    }

    @Incoming("rabbitmq-app-id-in")
    public void consumeAppId(String payload, IncomingRabbitMQMetadata metadata) {
        record(payload, metadata);
    }

    private void record(String payload, IncomingRabbitMQMetadata metadata) {
        Object outboxId = metadata.getHeaders().get("outbox-id");
        received.add(Map.of("payload", payload,
                "messageId", Objects.requireNonNullElse(metadata.getMessageId(), "null"),
                // Header values may arrive as RabbitMQ's LongString, whose toString() is the text.
                "outboxId", String.valueOf(outboxId)));
    }

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("msg") String msg) {
        dedupEmitter.sendAndAwait(msg);
    }

    @POST
    @Path("/send-with-message-id")
    @Transactional
    public void sendWithMessageId(@QueryParam("msg") String msg, @QueryParam("id") String id) {
        appIdEmitter.sendMessageAndAwait(
                Message.of(msg, Metadata.of(OutgoingRabbitMQMetadata.builder().withMessageId(id).build())));
    }

    /** What consumers received, one entry per copy: payload, message id and the outbox-id header. */
    @GET
    @Path("/received")
    public List<Map<String, String>> received() {
        return List.copyOf(received);
    }
}

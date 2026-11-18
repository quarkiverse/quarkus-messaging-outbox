package io.quarkiverse.messaging.outbox.it;

import java.util.List;
import java.util.Map;
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
import org.hibernate.SessionFactory;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.vertx.core.json.JsonObject;

/**
 * Deduplication ids on AMQP. {@code amqp-dedup} uses a store that loses the first {@code delivered()} call, so
 * the relay re-sends; Artemis drops the second copy via {@code _AMQ_DUPL_ID}. {@code amqp-app-id} lets the
 * application set its own message id or duplicate id.
 */
@Path("/amqp-dedup")
@ApplicationScoped
public class AmqpDeduplicationResource {

    @Inject
    @Channel("amqp-dedup")
    OutboxEmitter<String> dedupEmitter;

    @Inject
    @Channel("amqp-app-id")
    OutboxEmitter<String> appIdEmitter;

    @Inject
    SessionFactory sessionFactory;

    private final List<Map<String, String>> received = new CopyOnWriteArrayList<>();

    @Incoming("amqp-dedup-in")
    public void consumeDedup(String payload, IncomingAmqpMetadata metadata) {
        record(payload, metadata);
    }

    @Incoming("amqp-app-id-in")
    public void consumeAppId(String payload, IncomingAmqpMetadata metadata) {
        record(payload, metadata);
    }

    private void record(String payload, IncomingAmqpMetadata metadata) {
        JsonObject properties = metadata.getProperties() != null ? metadata.getProperties() : new JsonObject();
        received.add(Map.of("payload", payload,
                "messageId", String.valueOf(metadata.getId()),
                "outboxId", String.valueOf(properties.getString("outbox-id")),
                "duplicateId", String.valueOf(properties.getString("_AMQ_DUPL_ID"))));
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
                Message.of(msg, Metadata.of(OutgoingAmqpMetadata.builder().withMessageId(id).build())));
    }

    @POST
    @Path("/send-with-duplicate-id")
    @Transactional
    public void sendWithDuplicateId(@QueryParam("msg") String msg, @QueryParam("id") String id) {
        appIdEmitter.sendMessageAndAwait(Message.of(msg, Metadata.of(OutgoingAmqpMetadata.builder()
                .withApplicationProperties(new JsonObject().put("_AMQ_DUPL_ID", id)).build())));
    }

    /** The channel's records still in the outbox table. */
    @GET
    @Path("/pending")
    public long pending(@QueryParam("channel") String channel) {
        try (var session = sessionFactory.openStatelessSession()) {
            return session.createNativeQuery("select count(*) from outbox_record where channel = :channel", Long.class)
                    .setParameter("channel", channel)
                    .getSingleResult();
        }
    }

    /**
     * What consumers received, one entry per copy: payload, message id, the outbox-id property and the
     * _AMQ_DUPL_ID property.
     */
    @GET
    @Path("/received")
    public List<Map<String, String>> received() {
        return List.copyOf(received);
    }
}

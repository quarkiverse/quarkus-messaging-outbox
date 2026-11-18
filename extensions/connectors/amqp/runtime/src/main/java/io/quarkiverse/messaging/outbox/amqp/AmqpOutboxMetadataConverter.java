package io.quarkiverse.messaging.outbox.amqp;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.vertx.core.json.JsonObject;

@Identifier(AmqpConnector.CONNECTOR_NAME)
@ApplicationScoped
public class AmqpOutboxMetadataConverter implements OutboxMetadataConverter {

    private static final String PREFIX = "amqp.";

    /** The application property ActiveMQ Artemis keys its duplicate detection on. */
    static final String ARTEMIS_DUPLICATE_ID = "_AMQ_DUPL_ID";

    @Override
    public Optional<String> extractKey(Message<?> message) {
        return message.getMetadata(OutgoingAmqpMetadata.class)
                .map(meta -> meta.getGroupId() != null ? meta.getGroupId() : null);
    }

    @Override
    public Map<String, Object> extract(Message<?> message) {
        var opt = message.getMetadata(OutgoingAmqpMetadata.class);
        if (opt.isEmpty()) {
            return Map.of();
        }
        OutgoingAmqpMetadata meta = opt.get();
        Map<String, Object> map = new LinkedHashMap<>();

        if (meta.getContentType() != null) {
            map.put(PREFIX + "content-type", meta.getContentType());
        }
        if (meta.getSubject() != null) {
            map.put(PREFIX + "subject", meta.getSubject());
        }
        if (meta.getCorrelationId() != null) {
            map.put(PREFIX + "correlation-id", meta.getCorrelationId());
        }
        if (meta.getReplyTo() != null) {
            map.put(PREFIX + "reply-to", meta.getReplyTo());
        }
        if (meta.getTtl() > 0) {
            map.put(PREFIX + "ttl", meta.getTtl());
        }
        if (meta.getPriority() > 0) {
            map.put(PREFIX + "priority", (int) meta.getPriority());
        }
        if (meta.isDurable()) {
            map.put(PREFIX + "durable", true);
        }
        if (meta.getMessageId() != null) {
            map.put(PREFIX + "message-id", meta.getMessageId());
        }
        JsonObject props = meta.getProperties();
        if (props != null && !props.isEmpty()) {
            map.put(PREFIX + "properties", props.getMap());
        }
        return map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Metadata restore(OutboxRecord record) {
        var builder = OutgoingAmqpMetadata.builder();
        Map<String, Object> stored = record.getHeaders() != null ? record.getHeaders() : Map.of();
        String key = record.getMsgKey();

        if (key != null) {
            builder.withGroupId(key);
        }

        Object contentType = stored.get(PREFIX + "content-type");
        if (contentType instanceof String ct) {
            builder.withContentType(ct);
        }

        Object subject = stored.get(PREFIX + "subject");
        if (subject instanceof String s) {
            builder.withSubject(s);
        }

        Object correlationId = stored.get(PREFIX + "correlation-id");
        if (correlationId instanceof String cid) {
            builder.withCorrelationId(cid);
        }

        Object replyTo = stored.get(PREFIX + "reply-to");
        if (replyTo instanceof String rt) {
            builder.withReplyTo(rt);
        }

        Object ttl = stored.get(PREFIX + "ttl");
        if (ttl instanceof Number t) {
            builder.withTtl(t.longValue());
        }

        Object priority = stored.get(PREFIX + "priority");
        if (priority instanceof Number p) {
            builder.withPriority(p.shortValue());
        }

        Object durable = stored.get(PREFIX + "durable");
        if (Boolean.TRUE.equals(durable)) {
            builder.withDurable(true);
        }

        Object storedMessageId = stored.get(PREFIX + "message-id");
        if (storedMessageId instanceof String mid) {
            builder.withMessageId(mid);
        } else if (record.getId() != null) {
            builder.withMessageId(record.getId().toString());
        }

        Map<String, Object> appProps = new LinkedHashMap<>();
        Object props = stored.get(PREFIX + "properties");
        if (props instanceof Map<?, ?> propMap) {
            appProps.putAll((Map<String, Object>) propMap);
        }
        if (record.getId() != null) {
            appProps.put("outbox-id", record.getId().toString());
            appProps.putIfAbsent(ARTEMIS_DUPLICATE_ID, record.getId().toString());
        }
        if (!appProps.isEmpty()) {
            builder.withApplicationProperties(new JsonObject(appProps));
        }

        return Metadata.of(builder.build());
    }
}

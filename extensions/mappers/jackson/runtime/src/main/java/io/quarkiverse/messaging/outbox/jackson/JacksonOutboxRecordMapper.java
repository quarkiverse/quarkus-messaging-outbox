package io.quarkiverse.messaging.outbox.jackson;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Reserve;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Reserve
@Priority(0)
@ApplicationScoped
public class JacksonOutboxRecordMapper implements OutboxRecordMapper {

    @Inject
    Instance<ObjectMapper> objectMapperInstance;

    private ObjectMapper objectMapper;

    @PostConstruct
    void init() {
        objectMapper = objectMapperInstance.isResolvable()
                ? objectMapperInstance.get()
                : new ObjectMapper();
    }

    @Override
    public OutboxRecord toRecord(String channel, Message<?> message) {
        OutboxRecord record = new OutboxRecord();
        record.setChannel(channel);
        record.setStatus(OutboxRecord.STATUS_PENDING);

        Object payload = message.getPayload();
        record.setPayloadType(payload.getClass().getName());

        if (payload instanceof byte[] bytes) {
            record.setPayload(bytes);
        } else if (payload instanceof String str) {
            record.setPayload(str.getBytes(StandardCharsets.UTF_8));
        } else {
            try {
                record.setPayload(objectMapper.writeValueAsBytes(payload));
            } catch (JacksonException e) {
                throw new RuntimeException("Failed to serialize payload of type " + payload.getClass().getName(), e);
            }
        }

        message.getMetadata(OutboxMetadata.class).ifPresent(meta -> meta.getKey().ifPresent(record::setMsgKey));

        return record;
    }

    @Override
    public Message<?> toMessage(OutboxRecord record, Type payloadType) {
        OutboxMetadata meta = OutboxMetadata.withKey(record.getMsgKey());
        Metadata metadata = Metadata.of(meta);

        Object payload = deserializePayload(record.getPayload(), payloadType);
        return Message.of(payload, metadata);
    }

    private Object deserializePayload(byte[] bytes, Type payloadType) {
        if (payloadType == null) {
            return bytes;
        }
        if (payloadType == byte[].class) {
            return bytes;
        }
        if (payloadType == String.class) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        try {
            return objectMapper.readValue(bytes, objectMapper.constructType(payloadType));
        } catch (Exception e) {
            // The relay parks the record. Sending raw bytes instead would silently change the wire format.
            throw new IllegalStateException("Failed to deserialize outbox payload as " + payloadType, e);
        }
    }
}

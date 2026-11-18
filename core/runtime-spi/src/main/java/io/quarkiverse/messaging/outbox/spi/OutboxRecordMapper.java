package io.quarkiverse.messaging.outbox.spi;

import java.lang.reflect.Type;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Serializes messages to outbox records and deserializes them back. The default implementation uses Jackson.
 * Applications can provide their own bean to replace it, for example for Avro payloads.
 */
public interface OutboxRecordMapper {

    /**
     * Serializes the message payload and builds an {@link OutboxRecord} for the given channel.
     * <p>
     * The mapper does not need to set the record's ID: the emitter assigns it after this call, from
     * {@link io.quarkiverse.messaging.outbox.OutboxMetadata#withId} when the application set one, or a new random
     * one otherwise.
     */
    OutboxRecord toRecord(String channel, Message<?> message);

    /** Deserializes an {@link OutboxRecord} back into a {@link Message}, using {@code payloadType} for the payload. */
    Message<?> toMessage(OutboxRecord record, Type payloadType);
}

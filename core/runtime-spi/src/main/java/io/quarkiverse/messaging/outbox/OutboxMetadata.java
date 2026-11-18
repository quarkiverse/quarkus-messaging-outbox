package io.quarkiverse.messaging.outbox;

import java.util.Optional;
import java.util.UUID;

/**
 * Metadata attached to every message the relay sends. Applications use it on the send side to set
 * the ordering key or the outbox ID, and on the receive side (via outgoing interceptors) to read the outbox ID and
 * attempt number.
 * <p>
 * Consumers should use the broker-level {@code outbox-id} header for deduplication instead of this
 * class, since {@code OutboxMetadata} is on the outgoing message only and does not cross the broker.
 */
public class OutboxMetadata {

    private final UUID id;
    private final String key;
    private final int attempts;

    private OutboxMetadata(UUID id, String key, int attempts) {
        this.id = id;
        this.key = key;
        this.attempts = attempts;
    }

    /** Creates metadata with an ordering key, for use on the send side. */
    public static OutboxMetadata withKey(String key) {
        return new OutboxMetadata(null, key, 0);
    }

    /**
     * Creates metadata that sets the outbox ID, for use on the send side. Without it, the emitter generates a random
     * one. The ID must be unique: the outbox table rejects a second record with the same ID, which fails the
     * transaction that sends it.
     */
    public static OutboxMetadata withId(UUID id) {
        return new OutboxMetadata(id, null, 0);
    }

    /** Creates metadata that sets both the outbox ID and the ordering key, for use on the send side. */
    public static OutboxMetadata withId(UUID id, String key) {
        return new OutboxMetadata(id, key, 0);
    }

    /** Creates metadata with all fields, used by the relay when building the outgoing message. */
    public static OutboxMetadata of(UUID id, String key, int attempts) {
        return new OutboxMetadata(id, key, attempts);
    }

    /** The outbox record's UUID, stable across retries and redeliveries. */
    public Optional<UUID> getId() {
        return Optional.ofNullable(id);
    }

    /** The ordering key, if one was set. */
    public Optional<String> getKey() {
        return Optional.ofNullable(key);
    }

    /** The delivery attempt number, starting at 0 for the first send. */
    public int getAttempt() {
        return attempts;
    }
}

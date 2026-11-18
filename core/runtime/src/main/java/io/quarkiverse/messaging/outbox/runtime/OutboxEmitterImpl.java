package io.quarkiverse.messaging.outbox.runtime;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.jboss.logging.Logger;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;

public class OutboxEmitterImpl<T> implements OutboxEmitter<T>, MessagePublisherProvider<Object> {

    private static final Logger log = Logger.getLogger(OutboxEmitterImpl.class);

    private final String channel;
    private final OutboxStore store;
    private final OutboxRecordMapper mapper;
    private final OutboxRelay relay;
    private final List<OutboxMetadataConverter> metadataConverters;
    private final KeyValueExtractor keyExtractor;
    private final boolean fallbackMode;
    private final Duration fallbackTimeout;

    // Fallback direct path
    private Multi<Message<?>> directPublisher;
    private volatile MultiEmitter<? super Message<?>> internalEmitter;

    public OutboxEmitterImpl(String channel, OutboxStore store, OutboxRecordMapper mapper, OutboxRelay relay,
            List<OutboxMetadataConverter> metadataConverters, KeyValueExtractor keyExtractor, OutboxConfig config) {
        this.channel = channel;
        this.store = store;
        this.mapper = mapper;
        this.relay = relay;
        this.metadataConverters = metadataConverters;
        this.keyExtractor = keyExtractor;
        this.fallbackMode = config.isFallback();
        this.fallbackTimeout = config.fallbackTimeout();
        if (fallbackMode) {
            this.directPublisher = Multi.createFrom().emitter(emitter -> this.internalEmitter = emitter);
        }
    }

    @Override
    public Uni<Void> send(T payload) {
        return sendMessage(Message.of(payload));
    }

    @Override
    public Uni<Void> send(String key, T payload) {
        Message<T> message = Message.of(payload, Metadata.of(OutboxMetadata.withKey(key)));
        return sendMessage(message);
    }

    @Override
    public <M extends Message<? extends T>> Uni<Void> sendMessage(M message) {
        if (fallbackMode) {
            return sendFallback(message);
        }
        return sendViaStore(message);
    }

    private <M extends Message<? extends T>> Uni<Void> sendViaStore(M message) {
        Message<? extends T> keyed = enrichWithKey(message);
        Message<? extends T> enriched = unwrapPayload(keyed);
        OutboxRecord record = mapper.toRecord(channel, enriched);
        assignId(record, keyed);
        // Converters see the message as sent, before a wrapper payload such as Record<K, V> is unwrapped.
        extractMetadata(record, keyed);
        log.debugf("Outbox persisting message on channel '%s'", channel);
        Runnable afterCommit = relay != null ? relay::signal : () -> {
        };
        return store.persist(record, afterCommit).chain(() -> Uni.createFrom().completionStage(enriched.ack()));
    }

    @SuppressWarnings("unchecked")
    private <M extends Message<? extends T>> Uni<Void> sendFallback(M message) {
        Message<? extends T> keyed = enrichWithKey(message);
        Message<? extends T> enriched = unwrapPayload(keyed);
        OutboxRecord record = mapper.toRecord(channel, enriched);
        assignId(record, keyed);
        extractMetadata(record, keyed);
        Type payloadType = resolveType(record.getPayloadType());
        Message<?> wireMessage = restoreMetadata(mapper.toMessage(record, payloadType), record);

        if (internalEmitter == null) {
            log.debugf("Outbox fallback: no subscriber on channel '%s', storing to outbox", channel);
            return fallbackToStore(record)
                    .chain(() -> Uni.createFrom().completionStage(enriched.ack()));
        }

        // A nack and the timeout can both ask for the fallback write. It runs once, and both wait for its result.
        Uni<Void> storeOnce = Uni.createFrom().<Void> deferred(() -> fallbackToStore(record)).memoize().indefinitely();
        return Uni.createFrom().<Void> emitter(uniEmitter -> {
            Message<Object> wrapped = ((Message<Object>) wireMessage)
                    .withAck(() -> {
                        uniEmitter.complete(null);
                        return enriched.ack();
                    })
                    .withNack(t -> {
                        log.debugf("Outbox fallback: direct send nacked on channel '%s', storing to outbox", channel);
                        storeOnce.subscribe().with(
                                v -> uniEmitter.complete(null),
                                err -> {
                                    if (err != t) {
                                        err.addSuppressed(t);
                                    }
                                    uniEmitter.fail(err);
                                });
                        return enriched.ack();
                    });
            internalEmitter.emit(wrapped);
        }).ifNoItem().after(fallbackTimeout).recoverWithUni(() -> {
            log.debugf("Outbox fallback: direct send timed out on channel '%s', storing to outbox", channel);
            return storeOnce.chain(() -> Uni.createFrom().completionStage(enriched.ack()));
        });
    }

    private Uni<Void> fallbackToStore(OutboxRecord record) {
        Runnable afterCommit = relay != null ? relay::signal : () -> {
        };
        return store.persist(record, afterCommit);
    }

    @SuppressWarnings("unchecked")
    private <M extends Message<? extends T>> Message<? extends T> enrichWithKey(M message) {
        if (message.getMetadata(OutboxMetadata.class).flatMap(OutboxMetadata::getKey).isPresent()) {
            return message;
        }
        String key = extractKeyFromConverters(message);
        if (key == null && keyExtractor != null
                && keyExtractor.canExtract(message, String.class, Object.class)) {
            Object extracted = keyExtractor.extractKey(message, String.class);
            if (extracted instanceof String s) {
                key = s;
            } else if (extracted != null) {
                // Outbox keys are strings. Converting silently could produce keys consumers do not expect.
                throw new IllegalArgumentException("Key extractor " + keyExtractor.getClass().getName()
                        + " returned a " + extracted.getClass().getName() + " for channel '" + channel
                        + "', but outbox keys must be strings");
            }
        }
        if (key != null) {
            // Adding OutboxMetadata replaces the one on the message: keep the ID the application may have set.
            UUID id = message.getMetadata(OutboxMetadata.class).flatMap(OutboxMetadata::getId).orElse(null);
            return (Message<? extends T>) ((Message<Object>) message)
                    .addMetadata(OutboxMetadata.withId(id, key));
        }
        return message;
    }

    /**
     * The one place the outbox ID is assigned, before anything reads it: the direct send in fallback mode, the
     * converters and the store. The application's {@link OutboxMetadata#withId} wins over an ID the mapper set.
     */
    private static void assignId(OutboxRecord record, Message<?> message) {
        Optional<UUID> requested = message.getMetadata(OutboxMetadata.class).flatMap(OutboxMetadata::getId);
        if (requested.isPresent()) {
            record.setId(requested.get());
        } else if (record.getId() == null) {
            record.setId(UUID.randomUUID());
        }
    }

    private void extractMetadata(OutboxRecord record, Message<?> message) {
        for (OutboxMetadataConverter converter : metadataConverters) {
            record.getHeaders().putAll(converter.extract(message));
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private Message<?> restoreMetadata(Message<?> msg, OutboxRecord record) {
        for (OutboxMetadataConverter converter : metadataConverters) {
            Metadata restored = converter.restore(record);
            for (Object m : restored) {
                msg = ((Message) msg).addMetadata(m);
            }
        }
        return msg;
    }

    @SuppressWarnings("unchecked")
    private <M extends Message<? extends T>> Message<? extends T> unwrapPayload(M message) {
        for (OutboxMetadataConverter converter : metadataConverters) {
            var value = converter.extractValue(message);
            if (value.isPresent()) {
                return (Message<? extends T>) Message.of(value.get(), message.getMetadata());
            }
        }
        return message;
    }

    private String extractKeyFromConverters(Message<?> message) {
        for (OutboxMetadataConverter converter : metadataConverters) {
            var key = converter.extractKey(message);
            if (key.isPresent()) {
                return key.get();
            }
        }
        return null;
    }

    @Override
    public void sendAndAwait(T payload) {
        send(payload).await().indefinitely();
    }

    @Override
    public void sendAndAwait(String key, T payload) {
        send(key, payload).await().indefinitely();
    }

    @Override
    public <M extends Message<? extends T>> void sendMessageAndAwait(M message) {
        sendMessage(message).await().indefinitely();
    }

    @Override
    public Publisher<Message<? extends Object>> getPublisher() {
        if (fallbackMode && relay != null) {
            return Multi.createBy().merging().streams(directPublisher, relay.getPublisher());
        }
        if (fallbackMode) {
            return directPublisher;
        }
        if (relay != null) {
            return relay.getPublisher();
        }
        return Multi.createFrom().nothing();
    }

    public String getChannel() {
        return channel;
    }

    private static Type resolveType(String typeName) {
        if (typeName == null) {
            return null;
        }
        try {
            return Class.forName(typeName, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}

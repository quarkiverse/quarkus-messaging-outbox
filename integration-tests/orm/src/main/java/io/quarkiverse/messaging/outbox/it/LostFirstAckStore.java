package io.quarkiverse.messaging.outbox.it;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStats;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkus.arc.Unremovable;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;

/**
 * Wraps the real store but makes the first {@code delivered()} call per record fail: the broker has the message,
 * but the outbox still holds the record. The relay re-claims and re-sends it, producing the duplicate that
 * at-least-once delivery allows.
 */
public class LostFirstAckStore implements OutboxStore {

    private final OutboxStore delegate;
    private final Set<UUID> failedOnce = ConcurrentHashMap.newKeySet();

    LostFirstAckStore(OutboxStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public Uni<Void> delivered(OutboxRecord record, boolean keep) {
        if (failedOnce.add(record.getId())) {
            return Uni.createFrom().failure(new RuntimeException("delivered lost on purpose for " + record.getId()));
        }
        return delegate.delivered(record, keep);
    }

    @Override
    public Uni<Void> persist(OutboxRecord record, Runnable afterCommit) {
        return delegate.persist(record, afterCommit);
    }

    @Override
    public Uni<List<OutboxRecord>> claim(ClaimRequest request) {
        return delegate.claim(request);
    }

    @Override
    public Uni<Void> retry(OutboxRecord record, Instant nextAttempt, Throwable failure) {
        return delegate.retry(record, nextAttempt, failure);
    }

    @Override
    public Uni<Void> park(OutboxRecord record, Throwable failure) {
        return delegate.park(record, failure);
    }

    @Override
    public Uni<Void> release(List<OutboxRecord> records) {
        return delegate.release(records);
    }

    @Override
    public Uni<Void> renewClaims(List<UUID> recordIds, Duration lease) {
        return delegate.renewClaims(recordIds, lease);
    }

    @Override
    public Uni<Boolean> holdLease(Duration lease, boolean acquire) {
        return delegate.holdLease(lease, acquire);
    }

    @Override
    public Uni<OutboxStats> stats() {
        return delegate.stats();
    }

    @Override
    public Uni<Integer> purge(Instant sentBefore) {
        return delegate.purge(sentBefore);
    }

    @ApplicationScoped
    public static class Producer {

        @Inject
        @Identifier("hibernate-orm")
        OutboxStore.Factory realFactory;

        @Produces
        @Identifier("lost-first-ack")
        @ApplicationScoped
        @Unremovable
        OutboxStore.Factory lostFirstAckFactory() {
            return context -> new LostFirstAckStore(realFactory.create(context));
        }
    }
}

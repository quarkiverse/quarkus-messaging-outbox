package io.quarkiverse.messaging.outbox.hibernate.orm;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.persistence.LockModeType;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Timeouts;
import org.hibernate.Transaction;
import org.jboss.logging.Logger;

import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.LeaseLostException;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStats;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxLeaseEntity;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxRecordEntity;
import io.quarkus.runtime.BlockingOperationControl;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

public class HibernateOrmOutboxStore implements OutboxStore {

    private static final Logger log = Logger.getLogger(HibernateOrmOutboxStore.class);

    private final SessionFactory sessionFactory;
    private final TransactionSynchronizationRegistry txRegistry;
    private final OutboxStoreContext context;
    private final Clock clock;
    private final boolean requireTransaction;
    private final boolean removeAfterInsert;

    public HibernateOrmOutboxStore(SessionFactory sessionFactory,
            TransactionSynchronizationRegistry txRegistry,
            OutboxStoreContext context,
            Clock clock,
            boolean requireTransaction) {
        this(sessionFactory, txRegistry, context, clock, requireTransaction, false);
    }

    /**
     * @param removeAfterInsert delete the record in the transaction that writes it, for an external change data
     *        capture relay ({@code outbox.remove-after-insert})
     */
    public HibernateOrmOutboxStore(SessionFactory sessionFactory,
            TransactionSynchronizationRegistry txRegistry,
            OutboxStoreContext context,
            Clock clock,
            boolean requireTransaction,
            boolean removeAfterInsert) {
        this.sessionFactory = sessionFactory;
        this.txRegistry = txRegistry;
        this.context = context;
        this.clock = clock;
        this.requireTransaction = requireTransaction;
        this.removeAfterInsert = removeAfterInsert;
    }

    @Override
    public Uni<Void> persist(OutboxRecord record, Runnable afterCommit) {
        return Uni.createFrom().<Void> deferred(() -> {
            int txStatus = txRegistry.getTransactionStatus();
            if (txStatus == Status.STATUS_ACTIVE) {
                // Must run on the caller's thread: that is where the transaction lives.
                persistInCallerTransaction(record, afterCommit);
                return Uni.createFrom().voidItem();
            }
            if (requireTransaction) {
                return Uni.createFrom().failure(new IllegalStateException(
                        "No active transaction. OutboxEmitter.send() requires a transaction on channel '"
                                + context.getChannel() + "'"));
            }
            Uni<Void> own = Uni.createFrom().<Void> item(() -> {
                persistInOwnTransaction(record, afterCommit);
                return null;
            });
            // The fallback write can be triggered from a connector's event loop, where JDBC must not run.
            return BlockingOperationControl.isBlockingAllowed()
                    ? own
                    : own.runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
        });
    }

    private void persistInCallerTransaction(OutboxRecord record, Runnable afterCommit) {
        OutboxRecordEntity entity = toEntity(record);
        Session session = sessionFactory.getCurrentSession();
        session.persist(entity);

        record.setSeq(entity.getSeq());
        if (removeAfterInsert) {
            // The identity key makes persist() run the INSERT now; the DELETE follows at flush. Both reach the
            // transaction log. Were the insert only queued, Hibernate could cancel the pair and write nothing.
            session.remove(entity);
        }

        txRegistry.registerInterposedSynchronization(new Synchronization() {
            @Override
            public void beforeCompletion() {
            }

            @Override
            public void afterCompletion(int status) {
                if (status == Status.STATUS_COMMITTED) {
                    afterCommit.run();
                }
            }
        });
    }

    private void persistInOwnTransaction(OutboxRecord record, Runnable afterCommit) {
        OutboxRecordEntity entity = toEntity(record);
        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            Transaction tx = session.beginTransaction();
            try {
                session.insert(entity);
                if (removeAfterInsert) {
                    session.delete(entity);
                }
                tx.commit();
                afterCommit.run();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    @Override
    public Uni<List<OutboxRecord>> claim(ClaimRequest request) {
        return Uni.createFrom().item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    // Locks the lease row, if the channel has one, until the claim commits: a relay that lost the
                    // lease cannot claim, and a relay taking it over waits for this claim before clearing claims.
                    checkLeaseOwnership(session, LockModeType.PESSIMISTIC_READ);
                    Instant now = clock.instant();
                    StringBuilder hql = new StringBuilder(
                            "from OutboxRecordEntity e where e.channel = :channel"
                                    + " and e.status = :status"
                                    + " and (e.claimedUntil is null or e.claimedUntil < :now)");
                    if (request.dueOnly()) {
                        hql.append(" and (e.nextAttemptAt is null or e.nextAttemptAt <= :now)");
                    }
                    if (!request.excludedKeys().isEmpty()) {
                        hql.append(" and (e.msgKey is null or e.msgKey not in :excludedKeys)");
                    }
                    hql.append(" order by e.seq");

                    var query = session.createQuery(hql.toString(), OutboxRecordEntity.class)
                            .setParameter("channel", context.getChannel())
                            .setParameter("status", OutboxRecord.STATUS_PENDING)
                            .setParameter("now", now)
                            .setMaxResults(request.max())
                            // select ... for update skip locked: concurrent claimers lock disjoint rows and never wait.
                            .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                            .setLockTimeout(Timeouts.SKIP_LOCKED);
                    if (!request.excludedKeys().isEmpty()) {
                        query.setParameter("excludedKeys", request.excludedKeys());
                    }

                    List<OutboxRecordEntity> entities = query.getResultList();

                    Instant claimedUntil = now.plus(request.lease());
                    for (OutboxRecordEntity entity : entities) {
                        entity.setClaimedBy(context.getRelayInstanceId());
                        entity.setClaimedUntil(claimedUntil);
                        session.update(entity);
                    }

                    tx.commit();
                    return entities.stream().map(this::fromEntity).toList();
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> delivered(OutboxRecord record, boolean keep) {
        return Uni.createFrom().<Void> item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    checkLeaseOwnership(session);
                    OutboxRecordEntity entity = findRecord(session, record.getId());
                    if (entity != null) {
                        if (keep) {
                            entity.setStatus(OutboxRecord.STATUS_SENT);
                            entity.setClaimedBy(null);
                            entity.setClaimedUntil(null);
                            session.update(entity);
                        } else {
                            session.delete(entity);
                        }
                    }
                    tx.commit();
                    return null;
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> retry(OutboxRecord record, Instant nextAttempt, Throwable failure) {
        return Uni.createFrom().<Void> item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    checkLeaseOwnership(session);
                    OutboxRecordEntity entity = findRecord(session, record.getId());
                    entity.setAttempts(record.getAttempts());
                    entity.setNextAttemptAt(nextAttempt);
                    entity.setLastError(
                            failure != null ? truncate(failure.getMessage(), OutboxRecordEntity.LAST_ERROR_LENGTH) : null);
                    session.update(entity);
                    tx.commit();
                    return null;
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> park(OutboxRecord record, Throwable failure) {
        return Uni.createFrom().<Void> item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    checkLeaseOwnership(session);
                    OutboxRecordEntity entity = findRecord(session, record.getId());
                    entity.setStatus(OutboxRecord.STATUS_FAILED);
                    entity.setAttempts(record.getAttempts());
                    entity.setLastError(
                            failure != null ? truncate(failure.getMessage(), OutboxRecordEntity.LAST_ERROR_LENGTH) : null);
                    entity.setClaimedBy(null);
                    entity.setClaimedUntil(null);
                    session.update(entity);
                    tx.commit();
                    return null;
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> release(List<OutboxRecord> records) {
        return Uni.createFrom().<Void> item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    checkLeaseOwnership(session);
                    for (OutboxRecord record : records) {
                        OutboxRecordEntity entity = findRecord(session, record.getId());
                        entity.setClaimedBy(null);
                        entity.setClaimedUntil(null);
                        session.update(entity);
                    }
                    tx.commit();
                    return null;
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> renewClaims(List<UUID> recordIds, Duration lease) {
        if (recordIds.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().<Void> item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    Instant claimedUntil = clock.instant().plus(lease);
                    session.createMutationQuery(
                            "update OutboxRecordEntity set claimedUntil = :until"
                                    + " where outboxId in :ids and claimedBy = :owner")
                            .setParameter("until", claimedUntil)
                            .setParameter("ids", recordIds)
                            .setParameter("owner", context.getRelayInstanceId())
                            .execute();
                    tx.commit();
                    return null;
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Boolean> holdLease(Duration lease, boolean acquire) {
        return Uni.createFrom().item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    String channelName = context.getChannel();
                    String ownerId = context.getRelayInstanceId();
                    Instant now = clock.instant();

                    if (lease.isZero()) {
                        session.createMutationQuery(
                                "delete from OutboxLeaseEntity where channel = :channel and owner = :owner")
                                .setParameter("channel", channelName)
                                .setParameter("owner", ownerId)
                                .execute();
                        tx.commit();
                        return true;
                    }

                    Instant expiresAt = now.plus(lease);
                    int updated = session.createMutationQuery(
                            "update OutboxLeaseEntity set owner = :owner, expiresAt = :until"
                                    + " where channel = :channel and (owner = :owner or expiresAt < :now)")
                            .setParameter("owner", ownerId)
                            .setParameter("until", expiresAt)
                            .setParameter("channel", channelName)
                            .setParameter("now", now)
                            .execute();
                    if (updated > 0) {
                        if (acquire) {
                            clearClaims(session);
                        }
                        tx.commit();
                        return true;
                    }

                    // No row matched: another owner holds a valid lease, or no lease row exists yet.
                    // Check first, so that instances on standby do not fail an insert at every renewal.
                    long existing = session.createQuery(
                            "select count(*) from OutboxLeaseEntity where channel = :channel", Long.class)
                            .setParameter("channel", channelName)
                            .getSingleResult();
                    if (existing > 0) {
                        tx.commit();
                        return false;
                    }
                    // Only a real race reaches the catch: another instance inserted the row since the check.
                    try {
                        OutboxLeaseEntity entity = new OutboxLeaseEntity();
                        entity.setChannel(channelName);
                        entity.setOwner(ownerId);
                        entity.setExpiresAt(expiresAt);
                        session.insert(entity);
                        if (acquire) {
                            clearClaims(session);
                        }
                        tx.commit();
                        return true;
                    } catch (Exception insertErr) {
                        tx.rollback();
                        return false;
                    }
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /** Clears the claims on the channel's pending records, for a relay that has just acquired the lease. */
    private void clearClaims(StatelessSession session) {
        session.createMutationQuery(
                "update OutboxRecordEntity set claimedBy = null, claimedUntil = null"
                        + " where channel = :channel and status = :status and claimedBy is not null")
                .setParameter("channel", context.getChannel())
                .setParameter("status", OutboxRecord.STATUS_PENDING)
                .execute();
    }

    private void checkLeaseOwnership(StatelessSession session) {
        checkLeaseOwnership(session, LockModeType.NONE);
    }

    private void checkLeaseOwnership(StatelessSession session, LockModeType lockMode) {
        OutboxLeaseEntity lease = session
                .createQuery("from OutboxLeaseEntity where channel = :channel", OutboxLeaseEntity.class)
                .setParameter("channel", context.getChannel())
                .setLockMode(lockMode)
                .uniqueResultOptional()
                .orElse(null);
        if (lease != null && !context.getRelayInstanceId().equals(lease.getOwner())) {
            log.warnf("Lease lost on channel '%s': held by '%s', not by '%s'",
                    context.getChannel(), lease.getOwner(), context.getRelayInstanceId());
            throw new LeaseLostException(
                    "Lease for channel '" + context.getChannel() + "' is held by '" + lease.getOwner()
                            + "', not by '" + context.getRelayInstanceId() + "'");
        }
    }

    @Override
    public Uni<OutboxStats> stats() {
        return Uni.createFrom().item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Long pending = session.createQuery(
                        "select count(e) from OutboxRecordEntity e where e.channel = :channel and e.status = :status",
                        Long.class)
                        .setParameter("channel", context.getChannel())
                        .setParameter("status", OutboxRecord.STATUS_PENDING)
                        .getSingleResult();
                Long failed = session.createQuery(
                        "select count(e) from OutboxRecordEntity e where e.channel = :channel and e.status = :status",
                        Long.class)
                        .setParameter("channel", context.getChannel())
                        .setParameter("status", OutboxRecord.STATUS_FAILED)
                        .getSingleResult();
                Instant oldest = session.createQuery(
                        "select min(e.createdAt) from OutboxRecordEntity e where e.channel = :channel and e.status = :status",
                        Instant.class)
                        .setParameter("channel", context.getChannel())
                        .setParameter("status", OutboxRecord.STATUS_PENDING)
                        .getSingleResultOrNull();
                Duration oldestAge = oldest == null ? Duration.ZERO : Duration.between(oldest, clock.instant());
                return new OutboxStats(pending, failed, oldestAge.isNegative() ? Duration.ZERO : oldestAge);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Integer> purge(Instant sentBefore) {
        return Uni.createFrom().item(() -> {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    int deleted = session.createMutationQuery(
                            "delete from OutboxRecordEntity e where e.channel = :channel"
                                    + " and e.status = :status and e.createdAt < :before")
                            .setParameter("channel", context.getChannel())
                            .setParameter("status", OutboxRecord.STATUS_SENT)
                            .setParameter("before", sentBefore)
                            .execute();
                    tx.commit();
                    return deleted;
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                }
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /** The record with this outbox id, or {@code null}. */
    private static OutboxRecordEntity findRecord(StatelessSession session, UUID outboxId) {
        return session.createQuery("from OutboxRecordEntity where outboxId = :id", OutboxRecordEntity.class)
                .setParameter("id", outboxId)
                .uniqueResult();
    }

    private OutboxRecordEntity toEntity(OutboxRecord record) {
        OutboxRecordEntity entity = new OutboxRecordEntity();
        entity.setOutboxId(record.getId() != null ? record.getId() : UUID.randomUUID());
        entity.setChannel(record.getChannel());
        entity.setMsgKey(record.getMsgKey());
        entity.setPayload(record.getPayload());
        entity.setPayloadType(record.getPayloadType());
        entity.setHeaders(record.getHeaders());
        entity.setStatus(record.getStatus());
        entity.setAttempts(record.getAttempts());
        entity.setNextAttemptAt(record.getNextAttemptAt() != null ? record.getNextAttemptAt() : clock.instant());
        entity.setCreatedAt(clock.instant());
        return entity;
    }

    private OutboxRecord fromEntity(OutboxRecordEntity entity) {
        OutboxRecord record = new OutboxRecord();
        record.setId(entity.getOutboxId());
        record.setSeq(entity.getSeq());
        record.setChannel(entity.getChannel());
        record.setMsgKey(entity.getMsgKey());
        record.setPayload(entity.getPayload());
        record.setPayloadType(entity.getPayloadType());
        record.setHeaders(entity.getHeaders());
        record.setStatus(entity.getStatus());
        record.setAttempts(entity.getAttempts());
        record.setNextAttemptAt(entity.getNextAttemptAt());
        record.setClaimedBy(entity.getClaimedBy());
        record.setClaimedUntil(entity.getClaimedUntil());
        record.setCreatedAt(entity.getCreatedAt());
        record.setLastError(entity.getLastError());
        return record;
    }

    private static String truncate(String s, int maxLen) {
        return s != null && s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}

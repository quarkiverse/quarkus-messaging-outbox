package io.quarkiverse.messaging.outbox.hibernate.reactive;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.hibernate.LockMode;
import org.hibernate.reactive.mutiny.Mutiny;

import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.LeaseLostException;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStats;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxLeaseEntity;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxRecordEntity;
import io.quarkus.reactive.transaction.runtime.TransactionalInterceptorBase;
import io.quarkus.vertx.core.runtime.context.VertxContextSafetyToggle;
import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class HibernateReactiveOutboxStore implements OutboxStore {

    private final Mutiny.SessionFactory sf;
    private final Supplier<Mutiny.Session> transactionalSession;
    private final boolean requireTransaction;
    private final boolean removeAfterInsert;
    private final OutboxStoreContext context;
    private final Clock clock;
    private final Vertx vertx;
    private final Executor delayExecutor;

    public HibernateReactiveOutboxStore(Mutiny.SessionFactory sf, OutboxStoreContext context,
            Clock clock, Duration signalDelay, Vertx vertx) {
        this(sf, null, context, clock, signalDelay, vertx, true);
    }

    /**
     * @param transactionalSession the CDI {@code Mutiny.Session}, bound to the current reactive {@code @Transactional}
     *        transaction; {@code null} when it is not available
     * @param requireTransaction fail {@code persist} when the caller has no active transaction, instead of writing
     *        the record in a transaction of its own
     */
    public HibernateReactiveOutboxStore(Mutiny.SessionFactory sf, Supplier<Mutiny.Session> transactionalSession,
            OutboxStoreContext context, Clock clock, Duration signalDelay, Vertx vertx, boolean requireTransaction) {
        this(sf, transactionalSession, context, clock, signalDelay, vertx, requireTransaction, false);
    }

    /**
     * @param removeAfterInsert delete the record in the transaction that writes it, for an external change data
     *        capture relay ({@code outbox.remove-after-insert})
     */
    public HibernateReactiveOutboxStore(Mutiny.SessionFactory sf, Supplier<Mutiny.Session> transactionalSession,
            OutboxStoreContext context, Clock clock, Duration signalDelay, Vertx vertx, boolean requireTransaction,
            boolean removeAfterInsert) {
        this.sf = sf;
        this.transactionalSession = transactionalSession;
        this.requireTransaction = requireTransaction;
        this.removeAfterInsert = removeAfterInsert;
        this.context = context;
        this.clock = clock;
        long delayMs = signalDelay.toMillis();
        // Vert.x rejects timers shorter than 1 ms.
        this.delayExecutor = cmd -> {
            if (delayMs < 1) {
                cmd.run();
            } else {
                vertx.setTimer(delayMs, id -> cmd.run());
            }
        };
        this.vertx = vertx;
    }

    /**
     * Runs a store operation on a Vert.x context of its own.
     * Hibernate Reactive keeps the open session in the current context, so two operations sharing a context
     * would share a session and a transaction. Acks arrive concurrently and from arbitrary threads.
     */
    private <T> Uni<T> onCtx(Supplier<Uni<T>> work) {
        return Uni.createFrom().<T> deferred(() -> {
            Context ctx = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
            VertxContextSafetyToggle.setContextSafe(ctx, true);
            return Uni.createFrom().<T> deferred(work::get)
                    .runSubscriptionOn(cmd -> ctx.runOnContext(v -> cmd.run()));
        });
    }

    /**
     * Writes the record in the caller's transaction.
     * <ul>
     * <li>Inside a reactive {@code @Transactional} method, the transaction belongs to the interceptor and the record
     * is written with the CDI {@code Mutiny.Session}. {@code sf.withTransaction} cannot be used there: it would
     * close the interceptor's connection while its transaction is live (HR000090).</li>
     * <li>Otherwise {@code sf.withSession} returns the session the caller opened on this context
     * ({@code sf.withTransaction}, {@code @WithTransaction}). If it has an active transaction, the record joins
     * it. If not, {@code persist} fails, or, with {@code require-transaction=false}, writes the record in a
     * transaction of its own.</li>
     * </ul>
     * Under {@code @Transactional} the commit happens after this method returns and the interceptor offers no
     * after-commit hook to the store, so the relay is signalled before the commit. If the relay claims before
     * the commit is visible, the record is picked up by the next poll.
     */
    @Override
    public Uni<Void> persist(OutboxRecord record, Runnable afterCommit) {
        OutboxRecordEntity entity = toEntity(record);
        Uni<Void> write;
        if (inReactiveTransactional()) {
            write = insert(transactionalSession.get(), entity);
        } else {
            write = sf.withSession(session -> {
                if (session.currentTransaction() != null) {
                    return insert(session, entity);
                }
                if (requireTransaction) {
                    return Uni.createFrom().failure(new IllegalStateException(
                            "No active transaction. OutboxEmitter.send() requires a transaction on channel '"
                                    + context.getChannel() + "'"));
                }
                return session.withTransaction(tx -> insert(session, entity));
            });
        }
        return write
                .invoke(() -> record.setSeq(entity.getSeq()))
                .invoke(() -> delayExecutor.execute(afterCommit));
    }

    /**
     * Whether the reactive {@code @Transactional} interceptor marked the current context. quarkus-hibernate-reactive
     * depends on quarkus-reactive-transactions, so the constant is always on the class path. It is in a runtime
     * package of a preview extension; {@code ReactiveTransactionalTest} fails if it changes meaning.
     */
    /**
     * Persists the record and, with {@code outbox.remove-after-insert}, removes it again in the same transaction.
     * The identity key makes persist() run the INSERT at once; the DELETE follows at flush. Both reach the
     * transaction log. Were the insert only queued, Hibernate could cancel the pair and write nothing.
     */
    private Uni<Void> insert(Mutiny.Session session, OutboxRecordEntity entity) {
        Uni<Void> persisted = session.persist(entity);
        return removeAfterInsert ? persisted.chain(() -> session.remove(entity)) : persisted;
    }

    private boolean inReactiveTransactional() {
        return transactionalSession != null
                && VertxContext.isOnDuplicatedContext()
                && Boolean.TRUE.equals(ContextLocals.get(TransactionalInterceptorBase.TRANSACTIONAL_METHOD_KEY, Boolean.FALSE));
    }

    @Override
    public Uni<List<OutboxRecord>> claim(ClaimRequest request) {
        // Locks the lease row, if the channel has one, until the claim commits: a relay that lost the lease cannot
        // claim, and a relay taking it over waits for this claim before clearing claims.
        return onCtx(() -> sf.withStatelessTransaction(session -> checkLease(session, LockMode.PESSIMISTIC_READ).chain(() -> {
            Instant now = clock.instant();
            StringBuilder hql = createHql(request);
            var query = session.createQuery(hql.toString(), OutboxRecordEntity.class)
                    .setParameter("channel", context.getChannel())
                    .setParameter("status", OutboxRecord.STATUS_PENDING)
                    .setParameter("now", now)
                    .setMaxResults(request.max())
                    .setLockMode(LockMode.UPGRADE_SKIPLOCKED);
            if (!request.excludedKeys().isEmpty()) {
                query.setParameter("excludedKeys", request.excludedKeys());
            }
            return query.getResultList().chain(entities -> {
                Instant until = now.plus(request.lease());
                Uni<Void> chain = Uni.createFrom().voidItem();
                for (OutboxRecordEntity e : entities) {
                    e.setClaimedBy(context.getRelayInstanceId());
                    e.setClaimedUntil(until);
                    chain = chain.chain(() -> session.update(e));
                }
                return chain.replaceWith(entities.stream().map(this::fromEntity).toList());
            });
        })));
    }

    private static StringBuilder createHql(ClaimRequest request) {
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
        return hql;
    }

    @Override
    public Uni<Void> delivered(OutboxRecord record, boolean keep) {
        return onCtx(() -> sf.withStatelessTransaction(session -> checkLease(session)
                .chain(() -> session.createQuery("from OutboxRecordEntity where outboxId = :id", OutboxRecordEntity.class)
                        .setParameter("id", record.getId()).getSingleResultOrNull())
                .chain(e -> {
                    if (e == null) {
                        return Uni.createFrom().voidItem();
                    }
                    if (keep) {
                        e.setStatus(OutboxRecord.STATUS_SENT);
                        e.setClaimedBy(null);
                        e.setClaimedUntil(null);
                        return session.update(e);
                    }
                    return session.delete(e);
                })));
    }

    @Override
    public Uni<Void> retry(OutboxRecord record, Instant nextAttempt, Throwable failure) {
        return onCtx(() -> sf.withStatelessTransaction(session -> checkLease(session)
                .chain(() -> session.createQuery("from OutboxRecordEntity where outboxId = :id", OutboxRecordEntity.class)
                        .setParameter("id", record.getId()).getSingleResultOrNull())
                .chain(e -> {
                    if (e != null) {
                        e.setAttempts(record.getAttempts());
                        e.setNextAttemptAt(nextAttempt);
                        e.setLastError(
                                failure != null ? truncate(failure.getMessage(), OutboxRecordEntity.LAST_ERROR_LENGTH) : null);
                        return session.update(e);
                    }
                    return Uni.createFrom().voidItem();
                })));
    }

    @Override
    public Uni<Void> park(OutboxRecord record, Throwable failure) {
        return onCtx(() -> sf.withStatelessTransaction(session -> checkLease(session)
                .chain(() -> session.createQuery("from OutboxRecordEntity where outboxId = :id", OutboxRecordEntity.class)
                        .setParameter("id", record.getId()).getSingleResultOrNull())
                .chain(e -> {
                    if (e != null) {
                        e.setStatus(OutboxRecord.STATUS_FAILED);
                        e.setAttempts(record.getAttempts());
                        e.setLastError(
                                failure != null ? truncate(failure.getMessage(), OutboxRecordEntity.LAST_ERROR_LENGTH) : null);
                        e.setClaimedBy(null);
                        e.setClaimedUntil(null);
                        return session.update(e);
                    }
                    return Uni.createFrom().voidItem();
                })));
    }

    @Override
    public Uni<Void> release(List<OutboxRecord> records) {
        return onCtx(() -> sf.withStatelessTransaction(session -> checkLease(session)
                .chain(() -> {
                    Uni<Void> chain = Uni.createFrom().voidItem();
                    for (OutboxRecord r : records) {
                        chain = chain.chain(() -> session
                                .createQuery("from OutboxRecordEntity where outboxId = :id", OutboxRecordEntity.class)
                                .setParameter("id", r.getId()).getSingleResultOrNull()
                                .chain(e -> {
                                    if (e != null) {
                                        e.setClaimedBy(null);
                                        e.setClaimedUntil(null);
                                        return session.update(e);
                                    }
                                    return Uni.createFrom().voidItem();
                                }));
                    }
                    return chain;
                })));
    }

    @Override
    public Uni<Void> renewClaims(List<UUID> recordIds, Duration lease) {
        if (recordIds.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return onCtx(() -> sf.withStatelessTransaction(session -> {
            Instant claimedUntil = clock.instant().plus(lease);
            return session.createMutationQuery(
                    "update OutboxRecordEntity set claimedUntil = :until"
                            + " where outboxId in :ids and claimedBy = :owner")
                    .setParameter("until", claimedUntil)
                    .setParameter("ids", recordIds)
                    .setParameter("owner", context.getRelayInstanceId())
                    .executeUpdate().replaceWithVoid();
        }));
    }

    @Override
    public Uni<Boolean> holdLease(Duration lease, boolean acquire) {
        String ch = context.getChannel();
        String owner = context.getRelayInstanceId();
        if (lease.isZero()) {
            return onCtx(() -> sf.withStatelessTransaction(session -> session.createMutationQuery(
                    "delete from OutboxLeaseEntity where channel = :channel and owner = :owner")
                    .setParameter("channel", ch)
                    .setParameter("owner", owner)
                    .executeUpdate().replaceWith(true)));
        }
        Instant now = clock.instant();
        Instant expiresAt = now.plus(lease);
        // Renew or take over an expired lease. The result is null when no lease row exists yet.
        return onCtx(() -> sf.withStatelessTransaction(session -> session.createMutationQuery(
                "update OutboxLeaseEntity set owner = :owner, expiresAt = :until"
                        + " where channel = :channel and (owner = :owner or expiresAt < :now)")
                .setParameter("owner", owner)
                .setParameter("until", expiresAt)
                .setParameter("channel", ch)
                .setParameter("now", now)
                .executeUpdate()
                .chain(updated -> {
                    if (updated > 0) {
                        return acquire ? clearClaims(session).replaceWith(Boolean.TRUE) : Uni.createFrom().item(Boolean.TRUE);
                    }
                    // Another owner holds a valid lease, or there is no row. Check before inserting, so that
                    // instances on standby do not fail an insert at every renewal.
                    return session.createQuery("select count(*) from OutboxLeaseEntity where channel = :channel",
                            Long.class)
                            .setParameter("channel", ch)
                            .getSingleResult()
                            .map(count -> count > 0 ? Boolean.FALSE : null);
                })))
                .chain(held -> held != null ? Uni.createFrom().item(held) : insertLease(ch, owner, expiresAt, acquire));
    }

    /**
     * Creates the lease row in a transaction of its own: a failed insert aborts the transaction it runs in.
     * It fails only if another instance inserted the row since the check, which means that instance won.
     */
    private Uni<Boolean> insertLease(String channel, String owner, Instant expiresAt, boolean acquire) {
        OutboxLeaseEntity e = new OutboxLeaseEntity();
        e.setChannel(channel);
        e.setOwner(owner);
        e.setExpiresAt(expiresAt);
        return onCtx(() -> sf.withStatelessTransaction(session -> session.insert(e)
                .chain(() -> acquire ? clearClaims(session) : Uni.createFrom().voidItem())))
                .replaceWith(Boolean.TRUE)
                .onFailure().recoverWithItem(Boolean.FALSE);
    }

    @Override
    public Uni<OutboxStats> stats() {
        return onCtx(() -> sf.withStatelessSession(session -> Uni.combine().all().unis(
                session.createQuery(
                        "select count(e) from OutboxRecordEntity e where e.channel = :channel and e.status = :status",
                        Long.class)
                        .setParameter("channel", context.getChannel()).setParameter("status", OutboxRecord.STATUS_PENDING)
                        .getSingleResult(),
                session.createQuery(
                        "select count(e) from OutboxRecordEntity e where e.channel = :channel and e.status = :status",
                        Long.class)
                        .setParameter("channel", context.getChannel()).setParameter("status", OutboxRecord.STATUS_FAILED)
                        .getSingleResult())
                .asTuple()
                .chain(counts -> session.createQuery(
                        "select min(e.createdAt) from OutboxRecordEntity e where e.channel = :channel and e.status = :status",
                        Instant.class)
                        .setParameter("channel", context.getChannel()).setParameter("status", OutboxRecord.STATUS_PENDING)
                        .getSingleResultOrNull()
                        .map(oldest -> new OutboxStats(counts.getItem1(), counts.getItem2(), ageOf(oldest))))));
    }

    private Duration ageOf(Instant oldest) {
        if (oldest == null) {
            return Duration.ZERO;
        }
        Duration age = Duration.between(oldest, clock.instant());
        return age.isNegative() ? Duration.ZERO : age;
    }

    @Override
    public Uni<Integer> purge(Instant sentBefore) {
        return onCtx(() -> sf.withStatelessTransaction(session -> session.createMutationQuery(
                "delete from OutboxRecordEntity e where e.channel = :channel and e.status = :status and e.createdAt < :before")
                .setParameter("channel", context.getChannel()).setParameter("status", OutboxRecord.STATUS_SENT)
                .setParameter("before", sentBefore).executeUpdate()));
    }

    /** Clears the claims on the channel's pending records, for a relay that has just acquired the lease. */
    private Uni<Void> clearClaims(Mutiny.StatelessSession session) {
        return session.createMutationQuery(
                "update OutboxRecordEntity set claimedBy = null, claimedUntil = null"
                        + " where channel = :channel and status = :status and claimedBy is not null")
                .setParameter("channel", context.getChannel())
                .setParameter("status", OutboxRecord.STATUS_PENDING)
                .executeUpdate()
                .replaceWithVoid();
    }

    private Uni<Void> checkLease(Mutiny.StatelessSession session) {
        return checkLease(session, LockMode.NONE);
    }

    private Uni<Void> checkLease(Mutiny.StatelessSession session, LockMode lockMode) {
        return session.createQuery("from OutboxLeaseEntity where channel = :channel", OutboxLeaseEntity.class)
                .setParameter("channel", context.getChannel())
                .setLockMode(lockMode)
                .getSingleResultOrNull()
                .chain(lease -> {
                    if (lease != null && !context.getRelayInstanceId().equals(lease.getOwner()))
                        return Uni.createFrom().failure(new LeaseLostException("Lease held by '" + lease.getOwner() + "'"));
                    return Uni.createFrom().voidItem();
                });
    }

    private OutboxRecordEntity toEntity(OutboxRecord r) {
        OutboxRecordEntity e = new OutboxRecordEntity();
        e.setOutboxId(r.getId() != null ? r.getId() : UUID.randomUUID());
        e.setChannel(r.getChannel());
        e.setMsgKey(r.getMsgKey());
        e.setPayload(r.getPayload());
        e.setPayloadType(r.getPayloadType());
        e.setHeaders(r.getHeaders());
        e.setStatus(r.getStatus());
        e.setAttempts(r.getAttempts());
        e.setNextAttemptAt(r.getNextAttemptAt() != null ? r.getNextAttemptAt() : clock.instant());
        e.setCreatedAt(clock.instant());
        return e;
    }

    private OutboxRecord fromEntity(OutboxRecordEntity e) {
        OutboxRecord r = new OutboxRecord();
        r.setId(e.getOutboxId());
        r.setSeq(e.getSeq());
        r.setChannel(e.getChannel());
        r.setMsgKey(e.getMsgKey());
        r.setPayload(e.getPayload());
        r.setPayloadType(e.getPayloadType());
        r.setHeaders(e.getHeaders());
        r.setStatus(e.getStatus());
        r.setAttempts(e.getAttempts());
        r.setNextAttemptAt(e.getNextAttemptAt());
        r.setClaimedBy(e.getClaimedBy());
        r.setClaimedUntil(e.getClaimedUntil());
        r.setCreatedAt(e.getCreatedAt());
        r.setLastError(e.getLastError());
        return r;
    }

    private static String truncate(String s, int maxLen) {
        return s != null && s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}

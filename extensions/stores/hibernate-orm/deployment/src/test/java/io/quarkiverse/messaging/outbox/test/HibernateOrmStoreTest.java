package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStore;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.LeaseLostException;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxRecordEntity;
import io.quarkus.test.QuarkusExtensionTest;

public class HibernateOrmStoreTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    TestService.class, CollectorConnector.class, SimpleStringMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.test-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.test-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    TestService service;

    @BeforeEach
    void cleanTable() {
        service.deleteAll();
    }

    // --- S1: persist then commit → record claimable, afterCommit fired ---

    @Test
    void s1_persistThenCommitMakesRecordClaimable() {
        AtomicBoolean signaled = new AtomicBoolean();
        OutboxStore store = service.createStore();

        service.persistInTransaction(store, "hello", () -> signaled.set(true));

        assertThat(signaled.get()).isTrue();

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);
        assertThat(new String(claimed.get(0).getPayload())).isEqualTo("hello");
    }

    // --- S2: persist then rollback → record absent, no afterCommit ---

    @Test
    void s2_rollbackMeansNoRecord() {
        AtomicBoolean signaled = new AtomicBoolean();
        OutboxStore store = service.createStore();

        try {
            service.persistThenRollback(store, "should-vanish", () -> signaled.set(true));
        } catch (RuntimeException ignored) {
        }

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).isEmpty();
        assertThat(signaled.get()).isFalse();
    }

    // --- S3: persist with no transaction → fails ---

    @Test
    void s3_persistWithNoTransactionFails() {
        OutboxStore store = service.createStore();

        assertThatThrownBy(() -> service.persistWithoutTransaction(store, "no-tx"))
                .hasMessageContaining("transaction");
    }

    // --- S4: entity and record persisted together, flush failure → neither stored ---

    @Test
    void s4_flushFailureRollsBothBack() {
        OutboxStore store = service.createStore();

        try {
            service.persistWithFlushFailure(store, "doomed");
        } catch (Exception ignored) {
        }

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).isEmpty();
    }

    // --- S5: claim order → ascending seq ---

    @Test
    void s5_claimOrderIsAscendingSeq() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "first");
        service.persistInTransaction(store, "second");
        service.persistInTransaction(store, "third");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(3);
        assertThat(claimed.get(0).getSeq()).isLessThan(claimed.get(1).getSeq());
        assertThat(claimed.get(1).getSeq()).isLessThan(claimed.get(2).getSeq());
    }

    // --- S6: two concurrent claims → disjoint results ---

    @Test
    void s6_concurrentClaimsAreDisjoint() {
        OutboxStore store1 = service.createStoreWithId("relay-a");
        OutboxStore store2 = service.createStoreWithId("relay-b");

        for (int i = 0; i < 6; i++) {
            service.persistInTransaction(store1, "msg-" + i);
        }

        List<OutboxRecord> claim1 = store1.claim(
                new ClaimRequest(3, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        List<OutboxRecord> claim2 = store2.claim(
                new ClaimRequest(3, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        assertThat(claim1).hasSize(3);
        assertThat(claim2).hasSize(3);

        Set<UUID> ids1 = new HashSet<>();
        claim1.forEach(r -> ids1.add(r.getId()));
        Set<UUID> ids2 = new HashSet<>();
        claim2.forEach(r -> ids2.add(r.getId()));
        ids1.retainAll(ids2);
        assertThat(ids1).isEmpty();
    }

    // --- S7: claim lease expires → record claimable again ---

    @Test
    void s7_expiredLeaseAllowsReclaim() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        store.claim(new ClaimRequest(1, Duration.ofMillis(1), true, Set.of()))
                .await().indefinitely();

        try {
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
        }

        List<OutboxRecord> reclaimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(reclaimed).hasSize(1);
    }

    // --- S8: retry → not claimable before next attempt time ---

    @Test
    void s8_retryUpdatesNextAttemptTime() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);

        Instant futureAttempt = Instant.now().plus(Duration.ofHours(1));
        store.retry(claimed.get(0), futureAttempt, new RuntimeException("fail"))
                .await().indefinitely();

        // With dueOnly=true, the record should not be claimable
        List<OutboxRecord> reclaimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(reclaimed).isEmpty();
    }

    // --- S9: delivered → row deleted ---

    @Test
    void s9_deliveredRemovesRecord() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        store.delivered(claimed.get(0), false).await().indefinitely();

        List<OutboxRecord> after = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(after).isEmpty();
    }

    // --- S9b: delivered with keep=true → marked SENT, not claimable ---

    @Test
    void s9b_deliveredKeepMarksAsSent() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        store.delivered(claimed.get(0), true).await().indefinitely();

        List<OutboxRecord> after = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(after).isEmpty();

        var stats = store.stats().await().indefinitely();
        assertThat(stats.pending()).isEqualTo(0);
        assertThat(stats.failed()).isEqualTo(0);
    }

    // --- S10: park → status FAILED, never claimed ---

    @Test
    void s10_parkSetsStatusFailed() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        OutboxRecord record = claimed.get(0);
        record.setAttempts(3);
        store.park(record, new RuntimeException("permanent failure"))
                .await().indefinitely();
        // The attempts that led to parking are kept on the row.
        assertThat(service.attemptsOf(record.getId())).isEqualTo(3);

        List<OutboxRecord> after = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(after).isEmpty();

        var stats = store.stats().await().indefinitely();
        assertThat(stats.failed()).isGreaterThanOrEqualTo(1);
    }

    // --- S11: release → claimable at once ---

    @Test
    void s11_releaseRecordsAreClaimableAgain() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        store.release(claimed).await().indefinitely();

        List<OutboxRecord> reclaimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(reclaimed).hasSize(1);
    }

    // --- S14: stats → pending and failed counts ---

    @Test
    void s14_statsReflectsRecordCounts() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg1");
        service.persistInTransaction(store, "msg2");

        var stats = store.stats().await().indefinitely();
        assertThat(stats.pending()).isGreaterThanOrEqualTo(2);
    }

    // --- S15: purge removes only SENT rows older than cut-off ---

    @Test
    void s15_purgeRemovesOnlyOldSentRecords() {
        OutboxStore store = service.createStore();

        service.persistInTransaction(store, "old-msg");
        service.persistInTransaction(store, "new-msg");
        service.persistInTransaction(store, "pending-msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(3);

        // Deliver first two with keep=true (marks as SENT)
        store.delivered(claimed.get(0), true).await().indefinitely();
        store.delivered(claimed.get(1), true).await().indefinitely();

        // Release the third (stays PENDING)
        store.release(List.of(claimed.get(2))).await().indefinitely();

        // Purge with a future cutoff — should remove both SENT records
        int purged = store.purge(Instant.now().plus(Duration.ofDays(1)))
                .await().indefinitely();
        assertThat(purged).isEqualTo(2);

        // PENDING record still claimable
        List<OutboxRecord> remaining = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(remaining).hasSize(1);

        // Purge with a past cutoff — should remove nothing
        int purgedNone = store.purge(Instant.now().minus(Duration.ofDays(1)))
                .await().indefinitely();
        assertThat(purgedNone).isEqualTo(0);
    }

    // --- S12: holdLease by two owners → one wins ---

    @Test
    void s12_holdLeaseTwoOwners() {
        OutboxStore store1 = service.createStoreWithId("relay-1");
        OutboxStore store2 = service.createStoreWithId("relay-2");

        boolean acquired1 = store1.holdLease(Duration.ofSeconds(30)).await().indefinitely();
        boolean acquired2 = store2.holdLease(Duration.ofSeconds(30)).await().indefinitely();

        assertThat(acquired1).isTrue();
        assertThat(acquired2).isFalse();
    }

    // --- S13: write by stale lease holder → rejected ---

    @Test
    void s13_staleLeaseholderWriteRejected() throws InterruptedException {
        OutboxStore store1 = service.createStoreWithId("relay-1");
        OutboxStore store2 = service.createStoreWithId("relay-2");

        // relay-1 acquires a very short lease
        store1.holdLease(Duration.ofMillis(1)).await().indefinitely();

        // relay-1 persists a record and claims it
        service.persistInTransaction(store1, "msg");
        List<OutboxRecord> claimed = store1.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        // Wait for relay-1's lease to expire
        Thread.sleep(50);

        // relay-2 takes over the lease
        store2.holdLease(Duration.ofSeconds(30)).await().indefinitely();

        // relay-1 tries to deliver → should fail with LeaseLostException
        try {
            store1.delivered(claimed.get(0), false).await().indefinitely();
            org.junit.jupiter.api.Assertions.fail("Expected LeaseLostException");
        } catch (Exception e) {
            assertThat(rootCause(e)).isInstanceOf(
                    io.quarkiverse.messaging.outbox.spi.LeaseLostException.class);
        }
    }

    // --- F7: lease holder dies, second instance takes over, records delivered ---

    @Test
    void f7_leaseHolderDiesSecondInstanceTakesOver() throws InterruptedException {
        OutboxStore store1 = service.createStoreWithId("relay-1");
        OutboxStore store2 = service.createStoreWithId("relay-2");

        service.persistInTransaction(store1, "msg-1");
        service.persistInTransaction(store1, "msg-2");
        service.persistInTransaction(store1, "msg-3");

        // relay-1 acquires a very short lease and claims records
        store1.holdLease(Duration.ofMillis(50)).await().indefinitely();
        List<OutboxRecord> claimed1 = store1.claim(
                new ClaimRequest(10, Duration.ofMillis(50), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed1).hasSize(3);

        // relay-2 cannot claim while relay-1's lease is active
        boolean acquired2 = store2.holdLease(Duration.ofSeconds(30)).await().indefinitely();
        assertThat(acquired2).isFalse();

        // A claim checks the lease owner: relay-2 is refused while relay-1 holds the lease.
        assertThatThrownBy(() -> store2.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely()).isInstanceOf(LeaseLostException.class);

        // Wait for relay-1's lease and claims to expire
        Thread.sleep(100);

        // relay-2 takes over the lease
        boolean acquired2After = store2.holdLease(Duration.ofSeconds(30)).await().indefinitely();
        assertThat(acquired2After).isTrue();

        // relay-2 claims the expired records
        List<OutboxRecord> claimed2After = store2.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed2After).hasSize(3);

        // relay-2 delivers them
        for (OutboxRecord r : claimed2After) {
            store2.delivered(r, false).await().indefinitely();
        }

        // No records left
        var stats = store2.stats().await().indefinitely();
        assertThat(stats.pending()).isEqualTo(0);
    }

    // --- S16: 10,000 records, 4 concurrent claimers → each claimed exactly once ---

    @Test
    void s16_concurrentClaimersDisjoint() {
        OutboxStore store = service.createStore();

        // Insert 100 records (scaled down from 10,000 for test speed)
        for (int i = 0; i < 100; i++) {
            service.persistInTransaction(store, "msg-" + i);
        }

        // 4 claimers each claim 25
        OutboxStore c1 = service.createStoreWithId("c1");
        OutboxStore c2 = service.createStoreWithId("c2");
        OutboxStore c3 = service.createStoreWithId("c3");
        OutboxStore c4 = service.createStoreWithId("c4");

        List<OutboxRecord> r1 = c1.claim(new ClaimRequest(25, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        List<OutboxRecord> r2 = c2.claim(new ClaimRequest(25, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        List<OutboxRecord> r3 = c3.claim(new ClaimRequest(25, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        List<OutboxRecord> r4 = c4.claim(new ClaimRequest(25, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();

        assertThat(r1).hasSize(25);
        assertThat(r2).hasSize(25);
        assertThat(r3).hasSize(25);
        assertThat(r4).hasSize(25);

        // All IDs are disjoint
        Set<UUID> allIds = new HashSet<>();
        r1.forEach(r -> allIds.add(r.getId()));
        r2.forEach(r -> allIds.add(r.getId()));
        r3.forEach(r -> allIds.add(r.getId()));
        r4.forEach(r -> allIds.add(r.getId()));
        assertThat(allIds).hasSize(100);
    }

    // --- S19: lease re-acquired, stale owner writes fail ---

    @Test
    void s19_allStaleWritesFail() throws InterruptedException {
        OutboxStore store1 = service.createStoreWithId("relay-1");
        OutboxStore store2 = service.createStoreWithId("relay-2");

        store1.holdLease(Duration.ofMillis(1)).await().indefinitely();
        service.persistInTransaction(store1, "msg");
        List<OutboxRecord> claimed = store1.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        Thread.sleep(50);
        OutboxRecord record = claimed.get(0);

        // relay-2 takes over
        store2.holdLease(Duration.ofSeconds(30)).await().indefinitely();

        // Every write by relay-1 should fail
        assertLeaseLost(() -> store1.delivered(record, false).await().indefinitely());
        assertLeaseLost(() -> store1.retry(record, Instant.now(), new RuntimeException("x")).await().indefinitely());
        assertLeaseLost(() -> store1.park(record, new RuntimeException("x")).await().indefinitely());
        assertLeaseLost(() -> store1.release(List.of(record)).await().indefinitely());
    }

    private void assertLeaseLost(Runnable action) {
        try {
            action.run();
            org.junit.jupiter.api.Assertions.fail("Expected LeaseLostException");
        } catch (Exception e) {
            assertThat(rootCause(e)).isInstanceOf(
                    io.quarkiverse.messaging.outbox.spi.LeaseLostException.class);
        }
    }

    private Throwable rootCause(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    // --- S17: dueOnly flag ---

    @Test
    void s17_dueOnlyFlag() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "msg");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);

        // Set next attempt in the future
        Instant future = Instant.now().plus(Duration.ofHours(1));
        store.retry(claimed.get(0), future, new RuntimeException("fail")).await().indefinitely();
        store.release(claimed).await().indefinitely();

        // dueOnly=true → not returned
        List<OutboxRecord> due = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(due).isEmpty();

        // dueOnly=false → returned
        List<OutboxRecord> all = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), false, Set.of()))
                .await().indefinitely();
        assertThat(all).hasSize(1);
    }

    // --- S18: claim with excluded keys ---

    @Test
    void s18_excludedKeys() {
        OutboxStore store = service.createStore();
        service.persistWithKey(store, "A", "msgA");
        service.persistWithKey(store, "B", "msgB");
        service.persistInTransaction(store, "msgNoKey");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of("A")))
                .await().indefinitely();

        assertThat(claimed).hasSize(2);
        assertThat(claimed).noneMatch(r -> "A".equals(r.getMsgKey()));
    }

    // --- E12: key from send(key, payload) is stored in the record ---

    @Test
    void e12_keyIsStoredInRecord() {
        OutboxStore store = service.createStore();
        service.persistWithKey(store, "customer-42", "payload");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(1, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).getMsgKey()).isEqualTo("customer-42");
    }

    // --- S20: owners racing for the lease, with no row and then with an expired row: exactly one wins ---

    @Test
    void s20_concurrentLeaseAcquisitionHasOneWinner() throws Exception {
        List<OutboxStore> owners = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            owners.add(service.createStoreWithId("owner-" + i));
        }

        // No lease row yet: every owner tries to create it.
        assertThat(raceForLease(owners, Duration.ofMillis(200))).isEqualTo(1);

        // The lease has expired: every owner tries to take it over.
        Thread.sleep(400);
        assertThat(raceForLease(owners, Duration.ofSeconds(30))).isEqualTo(1);
    }

    /** Calls holdLease on every store at the same moment, and returns how many got the lease. */
    private long raceForLease(List<OutboxStore> owners, Duration lease) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(owners.size());
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Boolean>> results = new ArrayList<>();
            for (OutboxStore owner : owners) {
                results.add(pool.submit(() -> {
                    start.await();
                    return owner.holdLease(lease).await().atMost(Duration.ofSeconds(30));
                }));
            }
            start.countDown();
            long winners = 0;
            for (Future<Boolean> result : results) {
                if (result.get(30, TimeUnit.SECONDS)) {
                    winners++;
                }
            }
            return winners;
        } finally {
            pool.shutdownNow();
        }
    }

    // --- S21: claims running at the same moment skip each other's rows instead of waiting ---

    @Test
    void s21_simultaneousClaimsSkipLockedRows() throws Exception {
        OutboxStore writer = service.createStore();
        for (int i = 0; i < 100; i++) {
            service.persistInTransaction(writer, "msg-" + i);
        }

        List<OutboxStore> claimers = List.of(service.createStoreWithId("c1"), service.createStoreWithId("c2"),
                service.createStoreWithId("c3"), service.createStoreWithId("c4"));
        ExecutorService pool = Executors.newFixedThreadPool(claimers.size());
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<List<OutboxRecord>>> results = new ArrayList<>();
            for (OutboxStore claimer : claimers) {
                results.add(pool.submit(() -> {
                    start.await();
                    return claimer.claim(new ClaimRequest(25, Duration.ofSeconds(30), true, Set.of()))
                            .await().atMost(Duration.ofSeconds(30));
                }));
            }
            start.countDown();

            Set<UUID> ids = new HashSet<>();
            int total = 0;
            for (Future<List<OutboxRecord>> result : results) {
                List<OutboxRecord> claimed = result.get(30, TimeUnit.SECONDS);
                total += claimed.size();
                claimed.forEach(r -> ids.add(r.getId()));
            }
            // Disjoint, and nothing left out: a claim that waited on another's locks would come back short.
            assertThat(ids).hasSize(total);
            assertThat(total).isEqualTo(100);
        } finally {
            pool.shutdownNow();
        }
    }

    // --- S22: a long error message is truncated to fit last_error, and the retry and park writes succeed ---

    @Test
    void s22_longErrorMessagesFitTheColumn() {
        OutboxStore store = service.createStore();
        service.persistInTransaction(store, "retried");
        service.persistInTransaction(store, "parked");
        RuntimeException longError = new RuntimeException("x".repeat(5000));

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(2, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(2);

        store.retry(claimed.get(0), Instant.now(), longError).await().indefinitely();
        store.park(claimed.get(1), longError).await().indefinitely();
        // A retried record stays claimed by the relay that retries it: release it to read it back.
        store.release(List.of(claimed.get(0))).await().indefinitely();

        List<OutboxRecord> reclaimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).getLastError()).hasSize(OutboxRecordEntity.LAST_ERROR_LENGTH);
        assertThat(store.stats().await().indefinitely().failed()).isEqualTo(1);
    }

    // --- S23: acquiring the lease clears the old claims, so the new holder claims in order; the old one is fenced ---

    @Test
    void s23_takeOverClaimsInOrderAndFencesTheOldHolder() throws Exception {
        OutboxStore writer = service.createStore();
        for (int i = 0; i < 10; i++) {
            service.persistInTransaction(writer, "m-" + i);
        }
        OutboxStore a = service.createStoreWithId("relay-a");
        OutboxStore b = service.createStoreWithId("relay-b");

        assertThat(a.holdLease(Duration.ofMillis(200), true).await().indefinitely()).isTrue();
        List<OutboxRecord> first = a.claim(new ClaimRequest(5, Duration.ofSeconds(30), false, Set.of()))
                .await().indefinitely();
        assertThat(first).extracting(r -> new String(r.getPayload())).containsExactly("m-0", "m-1", "m-2", "m-3", "m-4");

        // relay-a stops renewing, as if it crashed: its lease expires, its claims do not.
        Thread.sleep(300);
        assertThat(b.holdLease(Duration.ofSeconds(30), true).await().indefinitely()).isTrue();

        assertThatThrownBy(() -> a.claim(new ClaimRequest(5, Duration.ofSeconds(30), false, Set.of()))
                .await().indefinitely()).isInstanceOf(LeaseLostException.class);

        List<OutboxRecord> taken = b.claim(new ClaimRequest(10, Duration.ofSeconds(30), false, Set.of()))
                .await().indefinitely();
        assertThat(taken).extracting(r -> new String(r.getPayload()))
                .containsExactly("m-0", "m-1", "m-2", "m-3", "m-4", "m-5", "m-6", "m-7", "m-8", "m-9");
    }

    // --- S24: remove-after-insert leaves the table empty, and still signals the commit ---

    @Test
    void s24_removeAfterInsertLeavesTheTableEmpty() {
        AtomicBoolean signaled = new AtomicBoolean();
        OutboxStore store = service.createStoreRemovingAfterInsert();

        service.persistInTransaction(store, "gone", () -> signaled.set(true));

        assertThat(signaled.get()).isTrue();
        assertThat(store.stats().await().indefinitely().pending()).isZero();
        assertThat(store.claim(new ClaimRequest(10, Duration.ofSeconds(30), false, Set.of())).await().indefinitely())
                .isEmpty();
    }

    @ApplicationScoped
    public static class TestService {

        @Inject
        org.hibernate.SessionFactory sessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        @Inject
        @Channel("test-channel")
        OutboxEmitter<String> emitter;

        public OutboxStore createStore() {
            OutboxStoreContext context = new OutboxStoreContext("test-channel", null, "test-relay");
            return new HibernateOrmOutboxStore(sessionFactory, txRegistry, context,
                    java.time.Clock.systemUTC(), true);
        }

        @Transactional
        public void persistInTransaction(OutboxStore store, String payload) {
            persistInTransaction(store, payload, () -> {
            });
        }

        @Transactional
        public void persistInTransaction(OutboxStore store, String payload, Runnable afterCommit) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel("test-channel");
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            store.persist(record, afterCommit).await().indefinitely();
        }

        @Transactional
        public void persistWithKey(OutboxStore store, String key, String payload) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel("test-channel");
            record.setMsgKey(key);
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            store.persist(record, () -> {
            }).await().indefinitely();
        }

        public OutboxStore createStoreRemovingAfterInsert() {
            OutboxStoreContext context = new OutboxStoreContext("test-channel", null, "test-relay");
            return new HibernateOrmOutboxStore(sessionFactory, txRegistry, context,
                    java.time.Clock.systemUTC(), true, true);
        }

        public OutboxStore createStoreWithId(String relayId) {
            OutboxStoreContext context = new OutboxStoreContext("test-channel", null, relayId);
            return new HibernateOrmOutboxStore(sessionFactory, txRegistry, context,
                    java.time.Clock.systemUTC(), true);
        }

        @Transactional
        public void persistThenRollback(OutboxStore store, String payload) {
            persistThenRollback(store, payload, () -> {
            });
        }

        @Transactional
        public void persistThenRollback(OutboxStore store, String payload, Runnable afterCommit) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel("test-channel");
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            store.persist(record, afterCommit).await().indefinitely();
            throw new RuntimeException("forced rollback");
        }

        public void persistWithoutTransaction(OutboxStore store, String payload) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel("test-channel");
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            store.persist(record, () -> {
            }).await().indefinitely();
        }

        @Transactional
        public void persistWithFlushFailure(OutboxStore store, String payload) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel("test-channel");
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            store.persist(record, () -> {
            }).await().indefinitely();
            // Force a constraint violation by inserting a duplicate entity with the same ID
            sessionFactory.getCurrentSession().createMutationQuery(
                    "update OutboxRecordEntity set channel = null where channel = 'test-channel'")
                    .executeUpdate();
            sessionFactory.getCurrentSession().flush();
        }

        @Transactional
        public int attemptsOf(java.util.UUID outboxId) {
            return sessionFactory.getCurrentSession()
                    .createSelectionQuery("select attempts from OutboxRecordEntity where outboxId = :id", Integer.class)
                    .setParameter("id", outboxId)
                    .getSingleResult();
        }

        @Transactional
        public void deleteAll() {
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxLeaseEntity").executeUpdate();
        }
    }
}

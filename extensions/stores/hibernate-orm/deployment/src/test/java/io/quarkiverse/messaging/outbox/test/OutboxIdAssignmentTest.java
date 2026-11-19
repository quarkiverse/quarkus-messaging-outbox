package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStore;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;

/**
 * The emitter assigns the outbox ID: the one the application set with {@link OutboxMetadata#withId}, or a new one,
 * also when the mapper sets none.
 */
public class OutboxIdAssignmentTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    Sender.class, CollectorConnector.class, NoIdMapper.class, PrefixKeyExtractor.class))
            .overrideConfigKey("mp.messaging.outgoing.id-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.id-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("mp.messaging.outgoing.keyed-id-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.keyed-id-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("mp.messaging.outgoing.keyed-id-channel.outbox.key-extractor", "PrefixKeyExtractor")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    Sender sender;

    @BeforeEach
    void cleanTable() {
        sender.deleteAll();
    }

    @Test
    void mapperThatSetsNoIdStillGivesTheRecordOne() {
        sender.send(Message.of("no-id"));

        List<OutboxRecord> records = sender.claim("id-channel");
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getId()).isNotNull();
    }

    @Test
    void applicationIdBecomesTheOutboxId() {
        UUID id = UUID.randomUUID();
        sender.send(Message.of("with-id", Metadata.of(OutboxMetadata.withId(id))));

        List<OutboxRecord> records = sender.claim("id-channel");
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getId()).isEqualTo(id);
    }

    @Test
    void applicationIdAndKeyAreBothKept() {
        UUID id = UUID.randomUUID();
        sender.send(Message.of("with-id-and-key", Metadata.of(OutboxMetadata.withId(id, "k-1"))));

        List<OutboxRecord> records = sender.claim("id-channel");
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getId()).isEqualTo(id);
        assertThat(records.get(0).getMsgKey()).isEqualTo("k-1");
    }

    @Test
    void applicationIdSurvivesAnExtractedKey() {
        // The key extractor adds OutboxMetadata with the key: the ID the application set must stay.
        UUID id = UUID.randomUUID();
        sender.sendKeyed(Message.of("order-7", Metadata.of(OutboxMetadata.withId(id))));

        List<OutboxRecord> records = sender.claim("keyed-id-channel");
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getId()).isEqualTo(id);
        assertThat(records.get(0).getMsgKey()).isEqualTo("key:order-7");
    }

    /** A mapper that leaves the ID to the emitter. */
    @ApplicationScoped
    public static class NoIdMapper implements OutboxRecordMapper {

        @Override
        public OutboxRecord toRecord(String channel, Message<?> message) {
            OutboxRecord record = new OutboxRecord();
            record.setChannel(channel);
            record.setPayload(String.valueOf(message.getPayload()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            message.getMetadata(OutboxMetadata.class)
                    .ifPresent(meta -> meta.getKey().ifPresent(record::setMsgKey));
            return record;
        }

        @Override
        public Message<?> toMessage(OutboxRecord record, Type payloadType) {
            return Message.of(new String(record.getPayload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @ApplicationScoped
    public static class PrefixKeyExtractor implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> first, Type keyType, Type valueType) {
            return true;
        }

        @Override
        public Object extractKey(Message<?> message, Type keyType) {
            return "key:" + message.getPayload();
        }

        @Override
        public Object extractValue(Message<?> message, Type valueType) {
            return message.getPayload();
        }
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("id-channel")
        OutboxEmitter<String> emitter;

        @Inject
        @Channel("keyed-id-channel")
        OutboxEmitter<String> keyedEmitter;

        @Inject
        org.hibernate.SessionFactory sessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        @Transactional
        public void send(Message<String> message) {
            emitter.sendMessageAndAwait(message);
        }

        @Transactional
        public void sendKeyed(Message<String> message) {
            keyedEmitter.sendMessageAndAwait(message);
        }

        public List<OutboxRecord> claim(String channel) {
            OutboxStoreContext ctx = new OutboxStoreContext(channel, null, "check");
            OutboxStore store = new HibernateOrmOutboxStore(sessionFactory, txRegistry, ctx,
                    java.time.Clock.systemUTC(), false);
            return store.claim(new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                    .await().indefinitely();
        }

        @Transactional
        public void deleteAll() {
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
        }
    }
}

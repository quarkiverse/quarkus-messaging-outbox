package io.quarkiverse.messaging.outbox.hibernate.orm;

import java.time.Clock;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;

import org.hibernate.SessionFactory;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkus.hibernate.orm.PersistenceUnit;
import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
@Default
@Identifier("hibernate-orm")
public class HibernateOrmOutboxStoreFactory implements OutboxStore.Factory {

    @Inject
    @Any
    Instance<SessionFactory> sessionFactories;

    @Inject
    Instance<TransactionSynchronizationRegistry> txRegistryInstance;

    @Override
    public OutboxStore create(OutboxStoreContext context) {
        String puName = Optional.ofNullable(context.getChannelConfig())
                .flatMap(cfg -> cfg.getOptionalValue("outbox.persistence-unit", String.class))
                .orElse(null);

        SessionFactory sf;
        if (puName != null && !PersistenceUnit.DEFAULT.equals(puName)) {
            Instance<SessionFactory> selected = sessionFactories
                    .select(new PersistenceUnit.PersistenceUnitLiteral(puName));
            if (selected.isUnsatisfied()) {
                throw new IllegalStateException(
                        "Persistence unit '" + puName + "' not found for outbox channel '" + context.getChannel() + "'");
            }
            sf = selected.get();
        } else {
            sf = sessionFactories.get();
        }

        boolean isFallback = Optional.ofNullable(context.getChannelConfig())
                .flatMap(cfg -> cfg.getOptionalValue("outbox.mode", String.class))
                .map("fallback"::equals).orElse(false);
        boolean requireTx = Optional.ofNullable(context.getChannelConfig())
                .flatMap(cfg -> cfg.getOptionalValue("outbox.require-transaction", Boolean.class))
                .orElse(!isFallback);

        if (txRegistryInstance.isUnsatisfied()) {
            throw new IllegalStateException(
                    "TransactionSynchronizationRegistry not available. The Hibernate ORM outbox store requires JTA (quarkus-narayana-jta).");
        }
        return new HibernateOrmOutboxStore(sf, txRegistryInstance.get(), context, Clock.systemUTC(), requireTx,
                OutboxConfig.removeAfterInsert(context.getChannelConfig()));
    }
}

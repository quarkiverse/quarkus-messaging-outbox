package io.quarkiverse.messaging.outbox.hibernate.reactive;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.hibernate.reactive.mutiny.Mutiny;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.smallrye.common.annotation.Identifier;
import io.vertx.core.Vertx;

@ApplicationScoped
@Default
@Identifier("hibernate-reactive")
public class HibernateReactiveOutboxStoreFactory implements OutboxStore.Factory {

    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    Instance<Mutiny.Session> session;

    @Inject
    Vertx vertx;

    @Override
    public OutboxStore create(OutboxStoreContext context) {
        Duration signalDelay = Optional.ofNullable(context.getChannelConfig())
                .flatMap(cfg -> cfg.getOptionalValue("outbox.signal-delay", Duration.class))
                .orElse(Duration.ofMillis(100));
        // Same rule as the Hibernate ORM store: a transaction is required, except in fallback mode, where the
        // record is written after a failed broker send and usually outside any transaction.
        boolean isFallback = Optional.ofNullable(context.getChannelConfig())
                .flatMap(cfg -> cfg.getOptionalValue("outbox.mode", String.class))
                .map("fallback"::equals).orElse(false);
        boolean requireTx = Optional.ofNullable(context.getChannelConfig())
                .flatMap(cfg -> cfg.getOptionalValue("outbox.require-transaction", Boolean.class))
                .orElse(!isFallback);
        // The CDI session resolves to the session of the current reactive @Transactional transaction.
        return new HibernateReactiveOutboxStore(sessionFactory, session.isResolvable() ? session::get : null,
                context, Clock.systemUTC(), signalDelay, vertx, requireTx,
                OutboxConfig.removeAfterInsert(context.getChannelConfig()));
    }
}

package io.quarkiverse.messaging.outbox.runtime;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.enterprise.inject.spi.Prioritized;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.vertx.core.Vertx;

@EmitterFactoryFor(OutboxEmitter.class)
@ApplicationScoped
public class OutboxEmitterFactory implements EmitterFactory<OutboxEmitterImpl<Object>> {

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    @Any
    Instance<OutboxStore.Factory> storeFactories;

    @Inject
    Instance<OutboxRecordMapper> mapperInstance;

    @Inject
    @Any
    Instance<KeyValueExtractor> keyExtractors;

    @Inject
    @Any
    Instance<OutboxMetadataConverter> metadataConverters;

    @Inject
    Vertx vertx;

    @Inject
    @Any
    Instance<OutboxRecordSource.Factory> sourceFactories;

    private static final String POLLING_SOURCE = "polling";

    private final Map<String, Config> channelConfigs = new ConcurrentHashMap<>();
    private final List<OutboxRelay> relays = new CopyOnWriteArrayList<>();
    private final Map<String, OutboxStore> stores = new ConcurrentHashMap<>();

    @Override
    public OutboxEmitterImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return create(configuration, defaultBufferSize, null);
    }

    @Override
    public MessagePublisherProvider<?> createEmitter(EmitterConfiguration configuration, long defaultBufferSize,
            Config channelConfig) {
        return create(configuration, defaultBufferSize, channelConfig);
    }

    private OutboxEmitterImpl<Object> create(EmitterConfiguration configuration, long defaultBufferSize,
            Config channelConfig) {
        String channelName = configuration.name();
        if (channelConfig != null) {
            channelConfigs.put(channelName, channelConfig);
        }

        if (storeFactories.isUnsatisfied()) {
            throw new IllegalStateException(
                    "No OutboxStore implementation found for channel '" + channelName
                            + "'. Add quarkus-messaging-outbox-hibernate-orm or quarkus-messaging-outbox-hibernate-reactive.");
        }
        if (mapperInstance.isUnsatisfied()) {
            throw new IllegalStateException(
                    "No OutboxRecordMapper found for channel '" + channelName
                            + "'. Add quarkus-messaging-outbox-jackson or provide a custom mapper.");
        }

        OutboxConfig outboxConfig = OutboxConfig.from(channelConfig);
        OutboxRecordMapper mapper = mapperInstance.get();
        OutboxStore.Factory storeFactory = resolveStoreFactory(outboxConfig, channelName);
        KeyValueExtractor extractor = resolveKeyExtractor(outboxConfig);
        String relayId = UUID.randomUUID().toString();

        OutboxStoreContext context = new OutboxStoreContext(channelName, channelConfig, relayId);
        OutboxStore store = storeFactory.create(context);
        stores.put(channelName, store);
        List<OutboxMetadataConverter> converters = resolveMetadataConverters(channelConfig);
        OutboxRelay relay = null;
        if (outboxConfig.relayEnabled()) {
            OutboxRecordSource.Factory sourceFactory = resolveSourceFactory(channelConfig, channelName);
            relay = new OutboxRelay(store, mapper, converters, outboxConfig, Clock.systemUTC(), channelName, vertx,
                    sourceFactory, channelConfig);
            relays.add(relay);
        }
        return new OutboxEmitterImpl<>(channelName, store, mapper, relay, converters, extractor, outboxConfig);
    }

    /**
     * The record source named by {@code outbox.relay.source}, or {@code null} for the built-in polling source.
     * Other sources are beans implementing {@link OutboxRecordSource.Factory}, qualified with {@code @Identifier}.
     */
    private OutboxRecordSource.Factory resolveSourceFactory(Config channelConfig, String channelName) {
        String sourceId = channelConfig == null ? POLLING_SOURCE
                : channelConfig.getOptionalValue("outbox.relay.source", String.class).orElse(POLLING_SOURCE);
        if (POLLING_SOURCE.equals(sourceId)) {
            return null;
        }
        Instance<OutboxRecordSource.Factory> selected = sourceFactories.select(Identifier.Literal.of(sourceId));
        if (selected.isUnsatisfied()) {
            throw new IllegalStateException("Unknown outbox record source '" + sourceId + "' for channel '"
                    + channelName + "'. Use '" + POLLING_SOURCE + "', or add the extension that provides it.");
        }
        return selected.get();
    }

    private OutboxStore.Factory resolveStoreFactory(OutboxConfig config, String channelName) {
        return config.store()
                .map(storeId -> {
                    Instance<OutboxStore.Factory> selected = storeFactories.select(Identifier.Literal.of(storeId));
                    if (selected.isUnsatisfied()) {
                        throw new IllegalStateException(
                                "Unknown outbox store '" + storeId + "' for channel '" + channelName
                                        + "'. Available stores: "
                                        + storeFactories.stream()
                                                .map(f -> f.getClass().getSimpleName())
                                                .reduce((a, b) -> a + ", " + b).orElse("none"));
                    }
                    return selected.get();
                })
                .orElseGet(() -> storeFactories.select(Default.Literal.INSTANCE).get());
    }

    private KeyValueExtractor resolveKeyExtractor(OutboxConfig config) {
        if (keyExtractors.isUnsatisfied()) {
            return null;
        }
        return config.keyExtractor()
                .map(className -> {
                    for (KeyValueExtractor ext : keyExtractors) {
                        // Accepts the fully qualified class name or the simple name.
                        String name = beanClassName(ext);
                        if (name.equals(className) || name.endsWith("." + className) || name.endsWith("$" + className)) {
                            return ext;
                        }
                    }
                    throw new IllegalStateException("No KeyValueExtractor bean of class '" + className
                            + "' found for outbox.key-extractor");
                })
                .orElseGet(() -> {
                    KeyValueExtractor best = null;
                    for (KeyValueExtractor ext : keyExtractors) {
                        if (best == null || ext.getPriority() < best.getPriority()) {
                            best = ext;
                        }
                    }
                    return best;
                });
    }

    private List<OutboxMetadataConverter> resolveMetadataConverters(Config channelConfig) {
        if (metadataConverters.isUnsatisfied()) {
            return List.of();
        }
        List<OutboxMetadataConverter> result = new ArrayList<>();
        // Unqualified converters (cross-cutting, e.g. tracing)
        for (OutboxMetadataConverter c : metadataConverters) {
            if (!hasIdentifier(c)) {
                result.add(c);
            }
        }
        // Qualified converter matching the channel's connector
        if (channelConfig != null) {
            String connectorName = channelConfig.getOptionalValue("connector", String.class).orElse(null);
            if (connectorName != null) {
                Instance<OutboxMetadataConverter> selected = metadataConverters.select(Identifier.Literal.of(connectorName));
                if (selected.isResolvable()) {
                    result.add(selected.get());
                }
            }
        }
        result.sort(Comparator.comparingInt(Prioritized::getPriority));
        return result;
    }

    /** The bean class name, looking through a client proxy. */
    private static String beanClassName(Object bean) {
        Object target = bean instanceof io.quarkus.arc.ClientProxy proxy ? proxy.arc_contextualInstance() : bean;
        String name = target.getClass().getName();
        // Intercepted beans are subclasses named <BeanClass>_Subclass.
        return name.endsWith("_Subclass") ? name.substring(0, name.length() - "_Subclass".length()) : name;
    }

    private static boolean hasIdentifier(Object bean) {
        return bean.getClass().isAnnotationPresent(Identifier.class)
                || (bean instanceof io.quarkus.arc.ClientProxy proxy
                        && proxy.arc_contextualInstance().getClass().isAnnotationPresent(Identifier.class));
    }

    /** The store of each outbox channel, by channel name. */
    public Map<String, OutboxStore> getStores() {
        return Map.copyOf(stores);
    }

    public Config getChannelConfig(String channel) {
        return channelConfigs.get(channel);
    }

    /** The channels whose record source failed and waits to be started again, with the failure. */
    public Map<String, Throwable> getSourceFailures() {
        Map<String, Throwable> failures = new TreeMap<>();
        for (OutboxRelay relay : relays) {
            relay.getSourceFailure().ifPresent(f -> failures.put(relay.getChannel(), f));
        }
        return failures;
    }

    @PreDestroy
    public void shutdown() {
        CompletableFuture<?>[] futures = relays.stream()
                .map(OutboxRelay::shutdown)
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // best effort
        }
        relays.clear();
    }

    @Produces
    @Channel("")
    <T> OutboxEmitter<T> produce(InjectionPoint injectionPoint) {
        String channel = ChannelProducer.getChannelName(injectionPoint);
        return channelRegistry.getEmitter(channel, OutboxEmitter.class);
    }
}

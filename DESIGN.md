# Key design decisions

This file records the decisions behind Quarkus Messaging Outbox and the reasons for them. 
The user documentation in `docs` describes how to use the extension.

## Scope

- **Guarantee: atomic writes, at-least-once delivery.** No message for a rolled-back transaction, no lost message for a committed one. Duplicates are possible after a crash between the broker ack and delete, and they carry the same outbox id. Exactly-once is future work (see below).
- **Quarkus 4 and SmallRye Reactive Messaging 5 only.** The extension relies on the three-argument `EmitterFactory.createEmitter(configuration, bufferSize, channelConfig)`, new in 5.0, to read `outbox.*` keys from the channel configuration. It needs no change in Quarkus or SmallRye.
- **Quarkiverse, not Quarkus core.** Moving later mostly changes coordinates and packages.

## API

- **`OutboxEmitter<T>` on a normal outgoing channel.** It has the same send methods as `MutinyEmitter`, and the channel keeps its connector configuration. The outbox settings sit under `mp.messaging.outgoing.<channel>.outbox.`.
- **Completion means stored, not delivered.** The `Uni` completes once the record is written in the caller's transaction, and the message's ack is called then.
- **A transaction is required by default.** Without one, `send` fails, unless `require-transaction=false`.
- **Two modes.**
  - `transactional` (the default) always stores.
  - `fallback` sends directly and stores only on nack or timeout. It survives broker outages but can lose a message on a crash, and its ordering is always `none`.
- **The key is a `String`.** It drives `ordering=key` and is passed to the broker as its ordering key. A typed Kafka key is preserved by the Kafka converter, not by the store.
- **The payload type comes from the injection point.** It is captured at build time, which also registers the type for reflection in native mode.

## Modules

The SPI, the stores, the mapper, the connectors and the record sources are separate modules: `core/runtime-spi`, `extensions/stores/*`, `extensions/mappers/jackson`, `extensions/connectors/*`, `extensions/sources/debezium`. Each one activates through conditional dependencies, so an application pulls in only what it uses, and a reactive application doesn't get JTA or Agroal.

## Data model

- **One shared table for all channels, `outbox_record`.** Per-channel tables would need an entity class per channel.
- **`seq` is the primary key and an identity column. `outbox_id` is a unique UUID.**
  - JPA can only generate the identifier.
  - Identity values are assigned at insert, whereas Hibernate's sequence generators hand out blocks per instance, which breaks `seq` ordering across instances.
  - The UUID is the id consumers see.
- **`headers` is JSON in a text column, read as a `Map<String, Object>`.** It holds the connector metadata needed to rebuild the message.
- **A lease table, `outbox_lease`, with one row per ordered channel.** It records the holder and its expiry.
- **Both table names are build-time settings.** They are applied by renaming the mapped tables when Hibernate builds its metadata, with no generated classes or XML.
- **The DDL is shipped, not migrated.** Scripts for PostgreSQL and MySQL live in the docs, and `OutboxSchemaValidationTest` checks them with Flyway and Hibernate `validate`.

## Metadata and serialization

- **The mapper handles only the payload.** `OutboxRecordMapper` turns the payload into bytes and back; Jackson is the default and can be replaced.
- **`OutboxMetadataConverter` instead of `OutgoingInterceptor` bridges.**
  - Each connector's converter, selected by `@Identifier`, extracts the metadata at write time and restores it at relay time.
  - Interceptor bridges don't see relay-emitted messages. The converter round-trips the metadata through the record instead.
  - Converters exist for Kafka, AMQP and RabbitMQ.
- **Tracing is a cross-cutting converter.** It stores `traceparent` and restores it, so the send span links to the business request.
- **Channel configuration is not stored.** The topic, the serializers and the connector defaults apply when the relay sends. Only explicit per-message metadata is stored.
- **The outbox id reaches consumers as a broker header**, `outbox-id` on Kafka. It is stable across retries, so consumers can deduplicate on it.
- **The emitter assigns the outbox id**, right after the mapper builds the record and before anything reads it: the direct send in fallback mode, the converters, the store. The application can choose it with `OutboxMetadata.withId`. Mappers used to generate it, which the SPI never said, so a custom mapper that did not left fallback sends without an id and let the store generate a different one.
- **Artemis duplicate detection.** The AMQP converter always sets `_AMQ_DUPL_ID` to the outbox id, unless the application set it. The property is harmless on non-Artemis brokers and gives Artemis users dedup for free.

## Write side

- **The record is written through the caller's session, in the caller's transaction.**
- **After commit, the relay is signalled.**
  - The Hibernate ORM store registers a JTA synchronization.
  - Hibernate Reactive has no commit hook, so its store signals after `signal-delay`, and the poll timer covers the rest.
- **Under reactive `@Transactional`, the reactive store writes with the injected `Mutiny.Session`.** The interceptor owns the transaction, and `sf.withSession` would close its connection (HR000090).
  - The store detects the interceptor through its context marker, `TransactionalInterceptorBase.TRANSACTIONAL_METHOD_KEY`.
  - quarkus-reactive-transactions has no public API for "is a transaction active" or for an after-commit callback.
- **`remove-after-insert` supports external CDC.** The record is deleted in the transaction that inserts it, so the table stays empty and the insert stays in the transaction log. This requires `relay.enabled=false`.

## Relay

- **One relay per channel, as the channel's publisher.** The connector subscribes to it like any emitter.
- **One path to the broker.** The commit signal only triggers the normal claim query. Immediate dispatch and recovery polling can't send the same record twice, and both follow the ordering rules.
- **Records come from an `OutboxRecordSource` SPI.**
  - Polling is the default source; `outbox.relay.source` selects another by `@Identifier`.
  - The relay gives a source its timers and moves the Unis it returns back onto its own context.
  - `Listener.failed` reports a failure: the relay marks the channel DOWN in health, closes the source and restarts it with backoff.
- **The dispatcher owns ordering, retries and parking.** The source contract states when released records may be handed out again. Moving the per-key retry rule into the dispatcher was rejected, because one stuck key could then starve the others.
- **Nacks retry with exponential backoff and jitter.** After `max-attempts` (by default never), the record is parked as `FAILED`.
- **Warnings without noise.** The first nack and every 10th are logged at WARN, naming what waits behind the record, and so is a park. Other nacks are logged at DEBUG.

## Ordering and several instances

- **Three modes, all in version 1.**
  - `strict` has one record in flight per channel.
  - `key` has one record in flight per key, with keys in parallel up to `max-in-flight`.
  - `none` runs records in parallel, and all instances compete with skip-locked claims.
- **In ordered modes, one relay holds the channel lease.** Its in-memory state is then the only dispatch state. A failing record is retried in place, and nothing behind it overtakes it.
- **Fencing by owner, not by epoch.** Every claim locks the lease row (`PESSIMISTIC_READ`) and checks the owner in the same transaction. A relay that lost the lease can't claim, and a takeover waits for a claim in progress.
- **A takeover clears the previous holder's claims.** `holdLease(lease, acquire)` does this in the acquiring transaction. Otherwise the records behind the claimed ones would be sent first.
- **Claims are leases too.** A live relay renews its claims. Claims of a dead instance expire after `lease-duration`.

## Debezium record source (experimental)

- **Built on the Debezium embedded engine, as an `OutboxRecordSource`.** It sends through the same dispatcher and connector as polling, so ordering, retries and channel configuration behave the same.
- **It reads in commit order and doesn't poll.**
- **It holds the channel lease, so a channel's slot has one reader.**
- **The engine moves past a batch only when all its records are settled.**
- **Offsets are kept in memory.** At start, the source snapshots the pending rows in `seq` order and then streams. The table remains the source of truth, so no offset store is needed.
- **Defaults come from `quarkus.datasource.db-kind`, for PostgreSQL or MySQL.** The application adds its database's connector, and startup fails with a clear message if it's missing.
- **JVM only.**

## External CDC

- **`relay.enabled=false` leaves the table to another process,** for example Debezium on Kafka Connect.
- **No Reactive Messaging connector runs on this path.** Channel configuration and the per-message metadata in `headers` don't apply. With the outbox event router, `route.by.field=channel` gives topics named after channels.

## Known limits

- **`seq` is assigned at insert, not at commit.** With the polling source, two overlapping transactions can be delivered out of commit order. The Debezium source has the same limit, but only for records already pending when it starts.
- **No ack timeout.** A message that is never acked or nacked stays in flight, and its claim is renewed.
- **Tested on PostgreSQL and MySQL only.**
- **No metrics or Dev UI card yet.**

## Future work

- **Exactly-once into Kafka.**
  - The relay sends a batch and a progress record in one Kafka transaction, with one `transactional.id` per channel. The producer epoch then fences a stale holder, like the lease does.
  - For polling, the progress record lists the delivered but not yet deleted ids, because a `seq` watermark could skip a record that commits late.
  - For the Debezium source, it holds the engine offset, which is in commit order.
  - Consumers read with `read_committed`.
- **Commit-order sequencing.** These options are opt-in, because each one costs throughput or ties the extension to PostgreSQL:
  - a commit-time advisory lock on PostgreSQL (a deferred trigger);
  - a per-channel counter row, on any database;
  - an `xmin` visibility horizon on PostgreSQL.
- **A consumer inbox.** Consumers would store processed outbox ids in the same transaction as their own changes. It works with every broker.
- **Upstream follow-ups.**
  - quarkus-reactive-transactions: a public "transaction active" check and an after-commit hook that tells a commit from a rollback.
  - SmallRye: the payload type on `EmitterConfiguration`, and `@Keyed` on fields.

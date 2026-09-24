# Hibernate Reactive: concurrent deletes of one entity type never complete

This reproducer is built on [hibernate-test-case-templates/reactive](https://github.com/hibernate/hibernate-test-case-templates/tree/main/reactive).
It has no framework on top of Hibernate Reactive: just `Mutiny.SessionFactory`, PostgreSQL through Testcontainers, and JUnit.

## What happens

The test starts 50 transactions at once.
Each one runs in its own session (`openSession()`) and deletes a different, already existing row of the same entity type.

Most of these transactions never complete:
- the flush never finishes, so COMMIT is never sent;
- nothing fails and nothing is logged;
- the connections stay `idle in transaction` in PostgreSQL until the pool is exhausted.

A control test does the same thing with an update instead of a delete, and every transaction completes.

| Hibernate Reactive | 50 concurrent updates | 50 concurrent deletes |
|---|---|---|
| 3.1.10.Final | 50/50 | **31/50** |
| 3.4.3.Final | 50/50 | **31/50** |
| 4.5.7.Final | 50/50 | **31/50** |

## Likely cause

`ReactiveDeleteCoordinatorStandard` has one instance per entity persister, and that instance is shared by every session.
It keeps the pending delete in an instance field, `private CompletionStage<Void> stage`:

- `doStaticDelete` and `doDynamicDelete` write the field;
- `complete` reads it when the DELETE returns.

When deletes overlap, the first DELETE to return completes whatever stage the field holds at that moment.
That stage usually belongs to another session, and every other stage is never completed.

`ReactiveDeleteCoordinatorSoft` has the same field.

Updates are not affected, because the persister takes a coordinator per call for them (`ReactiveUpdateCoordinator.makeScopedCoordinator()`).
Deletes use the shared `getDeleteCoordinator()`.

## Workaround

Delete by query, for example `session.createMutationQuery("delete from MyEntity where id = :id")`, instead of `session.remove(entity)`.
A delete by query does not go through the coordinator.

## Run it

Docker is required, for Testcontainers.

```bash
cd hibernate-reactive-4 && mvn test -Dtest=ConcurrentDeleteTestCase                                          # 4.5.x (template default)
cd hibernate-reactive-4 && mvn test -Dtest=ConcurrentDeleteTestCase -Dversion.org.hibernate.reactive=4.5.7.Final
cd hibernate-reactive-3 && mvn test -Dtest=ConcurrentDeleteTestCase -Dversion.org.hibernate.reactive=3.1.10.Final
```

`concurrentUpdatesInSeparateSessionsAllComplete` passes.
`concurrentDeletesInSeparateSessionsAllComplete` fails with `COMPLETED 31/50`.

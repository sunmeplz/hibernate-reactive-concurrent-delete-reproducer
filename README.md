# Hibernate Reactive: concurrent deletes of one entity type never complete

This reproducer is built on [hibernate-test-case-templates/reactive](https://github.com/hibernate/hibernate-test-case-templates/tree/main/reactive).
It has no framework on top of Hibernate Reactive: just `Mutiny.SessionFactory`, PostgreSQL through Testcontainers, and JUnit.

## What happens

The test starts 50 transactions at once.
They all run on one event loop context of the Vert.x instance Hibernate Reactive is configured with.
Each transaction opens its own session with `openSession()`, deletes a different, already existing row of the same entity type, and closes the session.

Most of these transactions never complete.
A separate connection, outside Hibernate Reactive's pool, shows what the database sees:

| Scenario (50 transactions) | Completed | Connections `idle in transaction` | Rows left |
|---|---|---|---|
| deletes, one after another | 50/50 | 0 | 0 |
| **deletes, concurrent** | **31/50** | **19**, last statement `delete from MyEntity where id=$1` | **19** |
| the same, 30 s later | 31/50 | 19 | 19 |
| updates, concurrent | 50/50 | 0 | 50 (updated) |

The DELETE statements reach the database, but COMMIT is never sent: the flush never finishes.
Nothing fails and nothing is logged, and the stuck connections hold the pool until it is exhausted.

The results are the same on 3.1.10.Final, 3.4.3.Final and 4.5.7.Final (with 4.5.7, 32/50 complete and 18 stay stuck).

## Likely cause

`ReactiveDeleteCoordinatorStandard` has one instance per entity persister, and that instance is shared by every session.
It keeps the pending delete in an instance field, `private CompletionStage<Void> stage`:

- `doStaticDelete` and `doDynamicDelete` write the field;
- `complete` reads it when the DELETE returns.

When deletes overlap, the first DELETE to return completes whatever stage the field holds at that moment.
That stage usually belongs to another session, and every other stage is never completed.

The authors' own Javadoc for the update path names the rule this breaks: `ReactiveScopedUpdateCoordinator` is "Scoped to a single operation, so that we can keep instance scoped state".

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

`sequentialDeletesInSeparateSessions` and `concurrentUpdatesInSeparateSessions` pass.
`concurrentDeletesInSeparateSessions` fails, and prints the table rows above.

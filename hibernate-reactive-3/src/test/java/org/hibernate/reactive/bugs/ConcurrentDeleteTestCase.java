package org.hibernate.reactive.bugs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.bugs.model.MyEntity;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.util.Database;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.smallrye.mutiny.Uni;
import org.testcontainers.containers.JdbcDatabaseContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent deletes of the same entity type, each in its own session and its own transaction,
 * never complete: the flush hangs, COMMIT is never sent, nothing fails or logs.
 * <p>
 * {@code ReactiveDeleteCoordinatorStandard} is one instance per entity persister, shared by every
 * session, and keeps the pending delete in an instance field ({@code stage}): {@code doStaticDelete}
 * and {@code doDynamicDelete} write it, {@code complete} reads it. With overlapping deletes, the first
 * DELETE to return completes whichever stage the field holds at that moment, and the others are never
 * completed. {@code ReactiveDeleteCoordinatorSoft} has the same field. Updates are not affected: the
 * persister takes a per-call coordinator for them ({@code makeScopedCoordinator()}).
 */
@TestMethodOrder( MethodOrderer.OrderAnnotation.class )
public class ConcurrentDeleteTestCase {

	private static final int N = 50;
	private static final long TIMEOUT_SECONDS = 30;

	private static JdbcDatabaseContainer<?> container;
	private static Mutiny.SessionFactory sf;
	private static SessionFactory ormSf;

	@BeforeAll
	public static void setup() {
		container = Database.POSTGRESQL.startContainer();
		StandardServiceRegistryBuilder srb = new ReactiveServiceRegistryBuilder()
				.applySetting( "hibernate.connection.url", container.getJdbcUrl() )
				.applySetting( "hibernate.connection.username", container.getUsername() )
				.applySetting( "hibernate.connection.password", container.getPassword() )
				.applySetting( "hibernate.connection.pool_size", "20" )
				.applySetting( "hibernate.hbm2ddl.auto", "create" );
		Metadata metadata = new MetadataSources( srb.build() )
				.addAnnotatedClass( MyEntity.class )
				.buildMetadata();
		ormSf = metadata.buildSessionFactory();
		sf = ormSf.unwrap( Mutiny.SessionFactory.class );
	}

	@BeforeEach
	public void insertRows() {
		sf.withTransaction( session -> session.createMutationQuery( "delete from MyEntity" ).executeUpdate()
						.chain( () -> {
							List<MyEntity> rows = new ArrayList<>();
							for ( long id = 1; id <= N; id++ ) {
								MyEntity e = new MyEntity();
								e.setId( id );
								e.setName( "row-" + id );
								rows.add( e );
							}
							return session.persistAll( rows.toArray() );
						} ) )
				.await().atMost( java.time.Duration.ofSeconds( 30 ) );
	}

	/**
	 * Fails: most of the N transactions never complete, and the connections they hold stay
	 * "idle in transaction" (runs last, because it leaves the pool exhausted).
	 */
	@Test
	@Order( 2 )
	public void concurrentDeletesInSeparateSessionsAllComplete() throws Exception {
		int completed = runConcurrently( id -> session -> session.find( MyEntity.class, id ).chain( session::remove ) );
		assertThat( completed ).as( "transactions completed out of %d within %ds", N, TIMEOUT_SECONDS ).isEqualTo( N );
	}

	/** Passes: the same shape with an update instead of a delete. */
	@Test
	@Order( 1 )
	public void concurrentUpdatesInSeparateSessionsAllComplete() throws Exception {
		int completed = runConcurrently( id -> session -> session.find( MyEntity.class, id )
				.invoke( e -> e.setName( "updated" ) ).replaceWithVoid() );
		assertThat( completed ).as( "transactions completed out of %d within %ds", N, TIMEOUT_SECONDS ).isEqualTo( N );
	}

	/**
	 * Runs one transaction per row, all at once, each in a session of its own
	 * ({@code openSession()}, not the session bound to a context), and counts how many complete.
	 */
	private static int runConcurrently(Function<Long, Function<Mutiny.Session, Uni<Void>>> work) throws Exception {
		List<CompletableFuture<Void>> results = new ArrayList<>();
		for ( long id = 1; id <= N; id++ ) {
			Function<Mutiny.Session, Uni<Void>> body = work.apply( id );
			CompletableFuture<Void> done = new CompletableFuture<>();
			results.add( done );
			sf.openSession()
					.chain( session -> session.withTransaction( tx -> body.apply( session ) )
							.eventually( session::close ) )
					.subscribe().with( ok -> done.complete( null ), done::completeExceptionally );
		}
		try {
			CompletableFuture.allOf( results.toArray( CompletableFuture[]::new ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS );
		}
		catch (Exception ignored) {
			// counted below
		}
		long failed = results.stream().filter( CompletableFuture::isCompletedExceptionally ).count();
		assertThat( failed ).as( "transactions failed" ).isZero();
		System.out.println( "COMPLETED " + results.stream().filter( f -> f.isDone() && !f.isCompletedExceptionally() ).count() + "/" + N );
		return (int) results.stream().filter( f -> f.isDone() && !f.isCompletedExceptionally() ).count();
	}

	@AfterAll
	public static void tearDown() {
		if ( ormSf != null ) {
			ormSf.close();
		}
		if ( container != null ) {
			container.stop();
		}
	}
}

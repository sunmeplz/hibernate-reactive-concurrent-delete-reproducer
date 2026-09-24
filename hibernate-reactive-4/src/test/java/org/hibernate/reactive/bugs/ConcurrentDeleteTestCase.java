package org.hibernate.reactive.bugs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.bugs.model.MyEntity;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.util.Database;
import org.hibernate.reactive.vertx.VertxInstance;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import org.testcontainers.containers.JdbcDatabaseContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent deletes of the same entity type, each in its own session and its own transaction,
 * never complete: the flush never finishes, COMMIT is never sent, nothing fails or logs, and the
 * connections stay "idle in transaction" until the pool is exhausted.
 * <p>
 * Every session is opened on an event loop context of the Vert.x instance Hibernate Reactive is
 * configured with, and no session is shared: each transaction opens its own with
 * {@code openSession()} and closes it.
 * <p>
 * The same deletes run one after another all complete, and the same concurrency with an update
 * instead of a delete all completes too.
 */
public class ConcurrentDeleteTestCase {

	private static final int N = 50;
	private static final int POOL_SIZE = 20;
	private static final long TIMEOUT_SECONDS = 30;

	private static JdbcDatabaseContainer<?> container;
	private static Vertx vertx;
	private static SessionFactory ormSf;
	private static Mutiny.SessionFactory sf;

	@BeforeAll
	public static void setup() {
		container = Database.POSTGRESQL.startContainer();
		vertx = Vertx.vertx();
		StandardServiceRegistryBuilder srb = new ReactiveServiceRegistryBuilder()
				.applySetting( "hibernate.connection.url", container.getJdbcUrl() )
				.applySetting( "hibernate.connection.username", container.getUsername() )
				.applySetting( "hibernate.connection.password", container.getPassword() )
				.applySetting( "hibernate.connection.pool_size", String.valueOf( POOL_SIZE ) )
				.applySetting( "hibernate.hbm2ddl.auto", "create" )
				.addService( VertxInstance.class, (VertxInstance) () -> vertx );
		Metadata metadata = new MetadataSources( srb.build() )
				.addAnnotatedClass( MyEntity.class )
				.buildMetadata();
		ormSf = metadata.buildSessionFactory();
		sf = ormSf.unwrap( Mutiny.SessionFactory.class );
	}

	/** N rows, committed before each test. */
	@BeforeEach
	public void insertRows() throws Exception {
		onEventLoop( () -> sf.withTransaction( session -> session
						.createMutationQuery( "delete from MyEntity" ).executeUpdate()
						.chain( () -> {
							List<MyEntity> rows = new ArrayList<>();
							for ( long id = 1; id <= N; id++ ) {
								MyEntity e = new MyEntity();
								e.setId( id );
								e.setName( "row-" + id );
								rows.add( e );
							}
							return session.persistAll( rows.toArray() );
						} ) ) )
				.get( 30, TimeUnit.SECONDS );
	}

	/** A hung test leaves its connections open; end them so the next test gets a pool. */
	@AfterEach
	public void terminateHungTransactions() throws Exception {
		sql( "select pg_terminate_backend(pid) from pg_stat_activity where state = 'idle in transaction'" );
	}

	/** Fails: most transactions never complete. */
	@Test
	public void concurrentDeletesInSeparateSessions() throws Exception {
		List<CompletableFuture<Void>> results = runConcurrently(
				id -> session -> session.find( MyEntity.class, id ).chain( session::remove ) );
		report( "concurrent deletes", results );

		// Not slow, stuck: another 30 seconds change nothing.
		Thread.sleep( 30_000 );
		report( "concurrent deletes, 30 s later", results );

		assertThat( completed( results ) ).as( "transactions completed" ).isEqualTo( N );
	}

	/** Passes: the same deletes, one after another. */
	@Test
	public void sequentialDeletesInSeparateSessions() throws Exception {
		List<CompletableFuture<Void>> results = new ArrayList<>();
		for ( long id = 1; id <= N; id++ ) {
			long rowId = id;
			CompletableFuture<Void> done = onEventLoop( () -> sf.openSession().chain( session -> session
					.withTransaction( tx -> session.find( MyEntity.class, rowId ).chain( session::remove ) )
					.eventually( session::close ) ) );
			done.get( TIMEOUT_SECONDS, TimeUnit.SECONDS );
			results.add( done );
		}
		report( "sequential deletes", results );
		assertThat( completed( results ) ).isEqualTo( N );
	}

	/** Passes: the same concurrency, with an update instead of a delete. */
	@Test
	public void concurrentUpdatesInSeparateSessions() throws Exception {
		List<CompletableFuture<Void>> results = runConcurrently( id -> session -> session
				.find( MyEntity.class, id ).invoke( e -> e.setName( "updated" ) ).replaceWithVoid() );
		report( "concurrent updates", results );
		assertThat( completed( results ) ).as( "transactions completed" ).isEqualTo( N );
	}

	/** Starts N transactions at once on one event loop context, each in a session of its own. */
	private static List<CompletableFuture<Void>> runConcurrently(
			Function<Long, Function<Mutiny.Session, Uni<Void>>> work) {
		List<CompletableFuture<Void>> results = new ArrayList<>();
		for ( int i = 0; i < N; i++ ) {
			results.add( new CompletableFuture<>() );
		}
		Context context = vertx.getOrCreateContext();
		context.runOnContext( v -> {
			for ( int i = 0; i < N; i++ ) {
				Function<Mutiny.Session, Uni<Void>> body = work.apply( (long) i + 1 );
				CompletableFuture<Void> done = results.get( i );
				sf.openSession()
						.chain( session -> session.withTransaction( tx -> body.apply( session ) )
								.eventually( session::close ) )
						.subscribe().with( ok -> done.complete( null ), done::completeExceptionally );
			}
		} );
		try {
			CompletableFuture.allOf( results.toArray( CompletableFuture[]::new ) )
					.get( TIMEOUT_SECONDS, TimeUnit.SECONDS );
		}
		catch (Exception ignored) {
			// reported below
		}
		assertThat( results.stream().filter( CompletableFuture::isCompletedExceptionally ).count() )
				.as( "transactions failed" ).isZero();
		return results;
	}

	private static long completed(List<CompletableFuture<Void>> results) {
		return results.stream().filter( f -> f.isDone() && !f.isCompletedExceptionally() ).count();
	}

	/** What the database sees, through a connection of its own, outside Hibernate Reactive's pool. */
	private static void report(String label, List<CompletableFuture<Void>> results) throws Exception {
		List<Row> idle = sql( "select query from pg_stat_activity where state = 'idle in transaction'" );
		long rows = sql( "select count(*) from myentity" ).get( 0 ).getLong( 0 );
		System.out.printf(
				"[%s] completed %d/%d | connections idle in transaction: %d%s | rows left in the table: %d%n",
				label, completed( results ), N, idle.size(),
				idle.isEmpty() ? "" : " (last statement: " + idle.get( 0 ).getString( 0 ).trim() + ")",
				rows
		);
	}

	private static List<Row> sql(String query) throws Exception {
		PgConnectOptions options = new PgConnectOptions()
				.setHost( container.getHost() )
				.setPort( container.getMappedPort( 5432 ) )
				.setDatabase( container.getDatabaseName() )
				.setUser( container.getUsername() )
				.setPassword( container.getPassword() );
		return PgConnection.connect( vertx, options )
				.compose( conn -> conn.query( query ).execute()
						.map( rowSet -> {
							List<Row> rows = new ArrayList<>();
							rowSet.forEach( rows::add );
							return rows;
						} )
						.eventually( () -> conn.close() ) )
				.toCompletionStage().toCompletableFuture()
				.get( 10, TimeUnit.SECONDS );
	}

	private static <T> CompletableFuture<T> onEventLoop(Supplier<Uni<T>> work) {
		CompletableFuture<T> result = new CompletableFuture<>();
		vertx.getOrCreateContext().runOnContext( v -> work.get()
				.subscribe().with( result::complete, result::completeExceptionally ) );
		return result;
	}

	@AfterAll
	public static void tearDown() {
		if ( ormSf != null ) {
			ormSf.close();
		}
		if ( vertx != null ) {
			vertx.close();
		}
		if ( container != null ) {
			container.stop();
		}
	}
}

/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.core;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.*;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Integration tests for {@link CqlOperations}.
 *
 * @author David Webb
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class CqlOperationsIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private static final String BOOK_INSERT = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

	private static Logger log = LoggerFactory.getLogger(CqlOperationsIntegrationTests.class);

	private CqlOperations cqlTemplate;

	/*
	 * Objects used for test data
	 */
	final String ISBN_NINES = "999999999";
	final String TITLE_NINES = "Book of Nines";
	final Object[] o1 = new Object[] { "1234", "Moby Dick", "Herman Manville", new Integer(456) };
	final Object[] o2 = new Object[] { "2345", "War and Peace", "Russian Dude", new Integer(456) };
	final Object[] o3 = new Object[] { "3456", "Jane Ayre", "Charlotte", new Integer(456) };

	@Before
	public void setupTemplate() {

		execute("cassandraOperationsTest-cql-dataload.cql", this.keyspace);
		this.cqlTemplate = new CqlTemplate(session);
	}

	@Test
	public void ringTest() {

		List<RingMember> ring = cqlTemplate.describeRing();

		/*
		 * There must be 1 node in the cluster if the embedded server is
		 * running.
		 */
		assertNotNull(ring);
	}

	@Test
	public void hostMapperTest() {

		List<MyHost> ring = (List<MyHost>) cqlTemplate.describeRing(new HostMapper<MyHost>() {

			@Override
			public Collection<MyHost> mapHosts(Set<Host> host) throws DriverException {

				List<MyHost> list = new LinkedList<MyHost>();

				for (Host h : host) {
					MyHost mh = new MyHost();
					mh.someName = h.getAddress().getCanonicalHostName();
					list.add(mh);
				}

				return list;
			}

		});

		assertNotNull(ring);
		assertTrue(ring.size() > 0);

		for (MyHost h : ring) {
			log.info("hostMapperTest Host -> " + h.someName);
		}

	}

	@Test
	@SuppressWarnings("unchecked")
	public void ingestionTestListOfList() {

		WriteOptions options = new WriteOptions();
		options.setTtl(360);

		String cql = BOOK_INSERT;

		List<List<?>> values = new LinkedList<List<?>>();

		values.add(new LinkedList<Object>(CollectionUtils.arrayToList(o1)));
		values.add(new LinkedList<Object>(CollectionUtils.arrayToList(o2)));
		values.add(new LinkedList<Object>(CollectionUtils.arrayToList(o3)));

		cqlTemplate.ingest(cql, values, options);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBookWithRetry((String) o1[0]);
		Book b2 = getBookWithRetry((String) o2[0]);
		Book b3 = getBookWithRetry((String) o3[0]);

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));
	}

	/**
	 * Insert some Books needed to next test steps.
	 */
	private void insertTestObjectArray() {

		String cql = BOOK_INSERT;

		Object[][] values = new Object[3][];
		values[0] = o1;
		values[1] = o2;
		values[2] = o3;

		PreparedStatement pstmt = this.session.prepare(cql);
		BoundStatement binder = null;
		for (Object[] o : values) {
			binder = pstmt.bind(o);
			cqlTemplate.execute(binder);
		}

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));
	}

	@Test
	public void ingestTestObjectArray() {

		String cql = BOOK_INSERT;

		Object[][] values = new Object[3][];
		values[0] = o1;
		values[1] = o2;
		values[2] = o3;

		cqlTemplate.ingest(cql, values);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBookWithRetry((String) o1[0]);
		Book b2 = getBookWithRetry((String) o2[0]);
		Book b3 = getBookWithRetry((String) o3[0]);

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));

	}

	/**
	 * This is an implementation of RowIterator for the purposes of testing passing your own Impl to CqlTemplate
	 *
	 * @author David Webb
	 */
	final class MyRowIterator implements RowIterator {

		private Object[][] values;

		public MyRowIterator(Object[][] values) {
			this.values = values;
		}

		int index = 0;

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.RowIterator#next()
		 */
		@Override
		public Object[] next() {
			return values[index++];
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.RowIterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return index < values.length;
		}

	}

	@Test
	public void ingestionTestRowIterator() {

		String cql = BOOK_INSERT;

		final Object[][] v = new Object[3][];
		v[0] = o1;
		v[1] = o2;
		v[2] = o3;
		RowIterator ri = new MyRowIterator(v);

		cqlTemplate.ingest(cql, ri);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBookWithRetry((String) o1[0]);
		Book b2 = getBookWithRetry((String) o2[0]);
		Book b3 = getBookWithRetry((String) o3[0]);

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));

	}

	@Test
	public void executeTestSessionCallback() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				String cql = BOOK_INSERT;

				PreparedStatement ps = s.prepare(cql);
				BoundStatement bs = ps.bind(isbn, title, author, pages);

				s.execute(bs);

				return null;

			}
		});

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void executeTestCqlString() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cqlTemplate.execute("insert into book (isbn, title, author, pages) values ('" + isbn + "', '" + title + "', '"
				+ author + "', " + pages + ")");

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void executeAsynchronouslyTestCqlString() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cqlTemplate.executeAsynchronously("insert into book (isbn, title, author, pages) values ('" + isbn + "', '" + title
				+ "', '" + author + "', " + pages + ")");

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void queryTestCqlStringResultSetExtractor() {

		final String isbn = "999999999";

		Book b1 = cqlTemplate.query("select * from book where isbn='" + isbn + "'", new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
				Row r = rs.one();
				assertNotNull(r);

				Book b = rowToBook(r);

				return b;
			}
		});

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

	@Test
	public void queryAsynchronouslyTestCqlStringResultSetExtractor() {

		final String isbn = "999999999";

		Book b1 = cqlTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'",

				new ResultSetExtractor<Book>() {

					@Override
					public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
						Row r = rs.one();
						assertNotNull(r);

						Book b = rowToBook(r);

						return b;
					}
				}, 60l, TimeUnit.SECONDS);

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

	@Test
	public void queryAsynchronouslyTestCqlStringResultSetExtractorWithOptions() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DEFAULT);

		final String isbn = "999999999";

		Book b1 = cqlTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'",

				new ResultSetExtractor<Book>() {

					@Override
					public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
						Row r = rs.one();
						assertNotNull(r);

						Book b = rowToBook(r);

						return b;
					}
				}, 60l, TimeUnit.SECONDS, options);

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

	@Test
	public void queryAsynchronouslyWithListener() throws InterruptedException {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DEFAULT);

		final String isbn = "999999999";

		BookListener listener = new BookListener();
		cqlTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'", listener);
		listener.await();

		Book book2 = getBook(isbn);
		assertBook(listener.getBook(), book2);
	}

	@Test
	public void queryAsynchronouslyWithListenerAndExecutor() throws InterruptedException {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DEFAULT);

		final String isbn = "999999999";

		BookListener listener = new BookListener();

		cqlTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'", listener, new Executor() {

			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
		listener.await();

		Book book2 = getBook(isbn);
		assertBook(listener.getBook(), book2);
	}

	@Test
	public void queryAsynchronouslyWithListenerAndExecutorAndOptions() throws InterruptedException {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DEFAULT);

		final String isbn = "999999999";

		BookListener listener = new BookListener();

		cqlTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'", listener, options, new Executor() {

			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
		listener.await();

		Book book2 = getBook(isbn);
		assertBook(listener.getBook(), book2);
	}

	@Test
	public void queryTestCqlStringRowCallbackHandler() {

		final String isbn = "999999999";

		final Book b1 = getBook(isbn);

		cqlTemplate.query("select * from book where isbn='" + isbn + "'", new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

				assertNotNull(row);
				Book b = rowToBook(row);
				assertBook(b1, b);

			}
		});

	}

	@Test
	public void processTestResultSetRowCallbackHandlerWithAsyncOptions() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DEFAULT);

		final String isbn = "999999999";

		final Book b1 = getBook(isbn);

		ResultSetFuture rsf = cqlTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'", options);
		ResultSet rs = rsf.getUninterruptibly();

		assertNotNull(rs);

		cqlTemplate.process(rs, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

				assertNotNull(row);
				Book b = rowToBook(row);
				assertBook(b1, b);
			}

		});
	}

	@Test
	public void queryTestCqlStringRowMapper() {

		// Insert our 3 test books.
		insertTestObjectArray();

		List<Book> books = cqlTemplate.query("select * from book where isbn in ('1234','2345','3456')",
				new RowMapper<Book>() {

					@Override
					public Book mapRow(Row row, int rowNum) throws DriverException {
						Book b = rowToBook(row);
						return b;
					}
				});

		assertEquals(books.size(), 3);
		assertBook(books.get(0), getBook(books.get(0).getIsbn()));
		assertBook(books.get(1), getBook(books.get(1).getIsbn()));
		assertBook(books.get(2), getBook(books.get(2).getIsbn()));
	}

	@Test
	public void processTestResultSetRowMapper() {

		// Insert our 3 test books.
		insertTestObjectArray();

		ResultSetFuture rsf = cqlTemplate.queryAsynchronously("select * from book where isbn in ('1234','2345','3456')");
		ResultSet rs = rsf.getUninterruptibly();

		assertNotNull(rs);

		List<Book> books = cqlTemplate.process(rs, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				Book b = rowToBook(row);
				return b;
			}
		});

		assertEquals(books.size(), 3);
		assertBook(books.get(0), getBook(books.get(0).getIsbn()));
		assertBook(books.get(1), getBook(books.get(1).getIsbn()));
		assertBook(books.get(2), getBook(books.get(2).getIsbn()));

	}

	@Test
	public void queryForObjectTestCqlStringRowMapper() {

		Book book = cqlTemplate.queryForObject("select * from book where isbn in ('" + ISBN_NINES + "')",
				new RowMapper<Book>() {
					@Override
					public Book mapRow(Row row, int rowNum) throws DriverException {
						Book b = rowToBook(row);
						return b;
					}
				});

		assertNotNull(book);
		assertBook(book, getBook(ISBN_NINES));
	}

	/**
	 * Test that CQL for QueryForObject must only return 1 row or an IllegalArgumentException is thrown.
	 */
	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void queryForObjectTestCqlStringRowMapperNotOneRowReturned() {

		// Insert our 3 test books.
		insertTestObjectArray();

		@SuppressWarnings("unused")
		Book book = cqlTemplate.queryForObject("SELECT * FROM book WHERE isbn IN('1234','2345','3456')",
				new RowMapper<Book>() {
					@Override
					public Book mapRow(Row row, int rowNum) throws DriverException {
						return rowToBook(row);
					}
				});
	}

	@Test
	public void processOneTestResultSetRowMapper() {

		// Insert our 3 test books.
		insertTestObjectArray();

		ResultSetFuture rsf = cqlTemplate.queryAsynchronously("select * from book where isbn in ('" + ISBN_NINES + "')");

		ResultSet rs = rsf.getUninterruptibly();
		assertNotNull(rs);

		Book book = cqlTemplate.processOne(rs, new RowMapper<Book>() {
			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				Book b = rowToBook(row);
				return b;
			}
		});

		assertNotNull(book);
		assertBook(book, getBook(ISBN_NINES));
	}

	@Test
	public void quertForObjectTestCqlStringRequiredType() {

		String title = cqlTemplate.queryForObject("select title from book where isbn in ('" + ISBN_NINES + "')",
				String.class);

		assertEquals(title, TITLE_NINES);

	}

	@Test(expected = ClassCastException.class)
	public void queryForObjectTestCqlStringRequiredTypeInvalid() {

		@SuppressWarnings("unused")
		Float title = cqlTemplate.queryForObject("select title from book where isbn in ('" + ISBN_NINES + "')",
				Float.class);

	}

	@Test
	public void processOneTestResultSetType() {

		ResultSetFuture rsf = cqlTemplate
				.queryAsynchronously("select title from book where isbn in ('" + ISBN_NINES + "')");

		ResultSet rs = rsf.getUninterruptibly();
		assertNotNull(rs);

		String title = cqlTemplate.processOne(rs, String.class);

		assertNotNull(title);
		assertEquals(title, TITLE_NINES);
	}

	@Test
	public void queryForMapTestCqlString() {

		Map<String, Object> rsMap = cqlTemplate.queryForMap("select * from book where isbn in ('" + ISBN_NINES + "')");

		Book b1 = objectToBook(rsMap.get("isbn"), rsMap.get("title"), rsMap.get("author"), rsMap.get("pages"));
		Book b2 = getBook(ISBN_NINES);

		assertBook(b1, b2);
	}

	@Test
	public void processMapTestResultSet() {

		ResultSetFuture rsf = cqlTemplate.queryAsynchronously("select * from book where isbn in ('" + ISBN_NINES + "')");

		ResultSet rs = rsf.getUninterruptibly();
		assertNotNull(rs);

		Map<String, Object> rsMap = cqlTemplate.processMap(rs);

		Book b1 = objectToBook(rsMap.get("isbn"), rsMap.get("title"), rsMap.get("author"), rsMap.get("pages"));
		Book b2 = getBook(ISBN_NINES);

		assertBook(b1, b2);
	}

	@Test
	public void queryForListTestCqlStringType() {

		// Insert our 3 test books.
		insertTestObjectArray();

		List<String> titles = cqlTemplate.queryForList("select title from book where isbn in ('1234','2345','3456')",
				String.class);

		assertNotNull(titles);
		assertEquals(titles.size(), 3);
	}

	@Test
	public void processListTestResultSetType() {

		// Insert our 3 test books.
		insertTestObjectArray();

		ResultSetFuture rsf = cqlTemplate.queryAsynchronously("select * from book where isbn in ('1234','2345','3456')");
		ResultSet rs = rsf.getUninterruptibly();

		assertNotNull(rs);

		List<String> titles = cqlTemplate.processList(rs, String.class);
		assertNotNull(titles);
		assertEquals(titles.size(), 3);
	}

	@Test
	public void queryForListOfMapCqlString() {

		// Insert our 3 test books.
		insertTestObjectArray();

		List<Map<String, Object>> results = cqlTemplate
				.queryForListOfMap("select * from book where isbn in ('1234','2345','3456')");

		assertEquals(results.size(), 3);

	}

	@Test
	public void processListOfMapTestResultSet() {

		// Insert our 3 test books.
		insertTestObjectArray();

		ResultSetFuture rsf = cqlTemplate.queryAsynchronously("select * from book where isbn in ('1234','2345','3456')");

		ResultSet rs = rsf.getUninterruptibly();

		assertNotNull(rs);

		List<Map<String, Object>> results = cqlTemplate.processListOfMap(rs);

		assertEquals(results.size(), 3);

	}

	@Test
	public void executeTestCqlStringPreparedStatementCallback() {

		String cql = BOOK_INSERT;

		BoundStatement statement = cqlTemplate.execute(cql, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doInPreparedStatement(PreparedStatement ps) throws DriverException, DataAccessException {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void executeTestPreparedStatementCreatorPreparedStatementCallback() {

		final String cql = BOOK_INSERT;

		BoundStatement statement = cqlTemplate.execute(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doInPreparedStatement(PreparedStatement ps) throws DriverException, DataAccessException {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderResultSetExtractor() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		Book b1 = cqlTemplate.query(cql, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
				Row r = rs.one();
				assertNotNull(r);

				Book b = rowToBook(r);

				return b;
			}
		});

		Book b2 = getBook(isbn);

		assertBook(b1, b2);
	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		cqlTemplate.query(cql, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

				Book b = rowToBook(row);

				Book b2 = getBook(isbn);

				assertBook(b, b2);

			}
		});

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		List<Book> books = cqlTemplate.query(cql, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				return rowToBook(row);
			}
		});

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorResultSetExtractor() {

		insertTestObjectArray();

		final String cql = "select * from book";

		List<Book> books = cqlTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new ResultSetExtractor<List<Book>>() {

			@Override
			public List<Book> extractData(ResultSet rs) throws DriverException, DataAccessException {

				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		});

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorRowCallbackHandler() {

		insertTestObjectArray();

		final String cql = "select * from book";

		cqlTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

				rowToBook(row);
			}
		});

	}

	@Test
	public void queryTestPreparedStatementCreatorRowMapper() {

		insertTestObjectArray();

		final String cql = "select * from book";

		List<Book> books = cqlTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				return rowToBook(row);
			}
		});

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderResultSetExtractor() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		List<Book> books = cqlTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<List<Book>>() {

			@Override
			public List<Book> extractData(ResultSet rs) throws DriverException, DataAccessException {
				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		});

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		cqlTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {
				Book b = rowToBook(row);
				Book b2 = getBook(isbn);
				assertBook(b, b2);
			}
		});

	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		List<Book> books = cqlTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				return rowToBook(row);
			}
		});

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void insertAndTruncateQueryObjectTest() {

		String tableName = "truncate_test";

		CreateTableSpecification createTableSpec = new CreateTableSpecification();
		createTableSpec.name(tableName).partitionKeyColumn("id", DataType.text()).column("foo", DataType.text());
		cqlTemplate.execute(createTableSpec);

		Insert insert = QueryBuilder.insertInto(tableName).value("id", uuid()).value("foo", "bar");
		cqlTemplate.execute(insert);

		Truncate truncate = QueryBuilder.truncate(tableName);
		cqlTemplate.execute(truncate);

	}

	/**
	 * Assert that a Book matches the arguments expected
	 *
	 * @param b
	 * @param orderedElements
	 */
	private void assertBook(Book b, Object... orderedElements) {

		assertEquals(b.getIsbn(), orderedElements[0]);
		assertEquals(b.getTitle(), orderedElements[1]);
		assertEquals(b.getAuthor(), orderedElements[2]);
		assertEquals(b.getPages(), orderedElements[3]);

	}

	private Book rowToBook(Row row) {
		Book b = new Book();
		b.setIsbn(row.getString("isbn"));
		b.setTitle(row.getString("title"));
		b.setAuthor(row.getString("author"));
		b.setPages(row.getInt("pages"));
		return b;
	}

	/**
	 * Convert Object[] to a Book
	 *
	 * @param bookElements
	 * @return
	 */
	private Book objectToBook(Object... bookElements) {
		Book b = new Book();
		b.setIsbn((String) bookElements[0]);
		b.setTitle((String) bookElements[1]);
		b.setAuthor((String) bookElements[2]);
		b.setPages((Integer) bookElements[3]);
		return b;
	}

	/**
	 * Assert that 2 Book objects are the same
	 *
	 * @param b1
	 * @param b2
	 */
	public static void assertBook(Book b1, Book b2) {

		assertEquals(b1.getIsbn(), b2.getIsbn());
		assertEquals(b1.getTitle(), b2.getTitle());
		assertEquals(b1.getAuthor(), b2.getAuthor());
		assertEquals(b1.getPages(), b2.getPages());

	}

	/**
	 * Get a Book from Cassandra for assertions.
	 *
	 * @param isbn
	 * @return
	 */
	private Book getBook(final String isbn) {

		Book b = cqlTemplate.query("select * from book where isbn = ?", new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
				Book b = new Book();
				Row r = rs.one();
				if (r == null) {
					return null;
				}
				b.setIsbn(r.getString("isbn"));
				b.setTitle(r.getString("title"));
				b.setAuthor(r.getString("author"));
				b.setPages(r.getInt("pages"));
				return b;
			}
		});

		return b;

	}

	/**
	 * Get a Book from Cassandra for assertions, if the Book is not retruned then retry as needed. This is used for
	 * assertions after asynchronous insert/ingest to give the datastore time to catch up with the tests.
	 *
	 * @param isbn
	 * @param retryMillis
	 * @param numRetries
	 * @return
	 */
	private Book getBookWithRetry(final String isbn, final long retryMillis, final int numRetries) {

		Book b = getBook(isbn);

		for (int i = 1; i <= numRetries && b == null; i++) {
			log.info(String.format("SLEEP - Trying to get Book after Async Call Waiting [%s]ms, Retry [%s]", retryMillis, i));
			try {
				Thread.sleep(retryMillis);
			} catch (InterruptedException e) {
				throw new IllegalStateException("Failed to sleep for query retry", e);
			}
			b = getBook(isbn);
		}

		return b;
	}

	/**
	 * Get a Book from Cassandra for assertions, if the Book is not retruned then retry as needed. This is used for
	 * assertions after asynchronous insert/ingest to give the datastore time to catch up with the tests. Defaults to 5
	 * retries @ 200ms intervals
	 *
	 * @param isbn
	 * @return
	 */
	private Book getBookWithRetry(final String isbn) {
		return getBookWithRetry(isbn, 200, 5);
	}

	/**
	 * For testing a HostMapper Implementation
	 */
	public class MyHost {
		public String someName;
	}
}

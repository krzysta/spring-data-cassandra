package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public abstract class ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator extends
		AbstractSpringDataEmbeddedCassandraIntegrationTest {

	ForceQuotedCompositePrimaryKeyRepositoryIntegrationTests tests = new ForceQuotedCompositePrimaryKeyRepositoryIntegrationTests();

	@Autowired
	ImplicitRepository i;

	@Autowired
	ExplicitRepository e;

	@Autowired
	MultitableRepository m;

	@Autowired
	CassandraTemplate t;

	@Before
	public void before() {

		tests.i = i;
		tests.e = e;
		tests.t = t;
		tests.m = m;

		tests.before();
	}

	@Test
	public void testImplicit() {
		tests.testImplicit();
	}

	public void testExplicit(String tableName, String stringValueColumnName, String keyZeroColumnName,
			String keyOneColumnName) {

		tests.testExplicit(tableName, stringValueColumnName, keyZeroColumnName, keyOneColumnName);
	}

	public void testMultitable(String tableName, String discriminator, String stringValueColumnName, String keyZeroColumnName,
							   String keyOneColumnName) {
		tests.testMultitable(tableName, discriminator, stringValueColumnName, keyZeroColumnName, keyOneColumnName);
	}
}

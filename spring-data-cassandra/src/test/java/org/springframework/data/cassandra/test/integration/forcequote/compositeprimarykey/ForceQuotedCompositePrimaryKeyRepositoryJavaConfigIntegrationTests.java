package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.junit.Test;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedCompositePrimaryKeyRepositoryJavaConfigIntegrationTests extends
		ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ImplicitRepository.class)
	public static class Config extends IntegrationTestConfig {}

	@Test
	public void testExplicit() {
		testExplicit(String.format("\"%s\"", Explicit.TABLE_NAME),
				String.format("\"%s\"", Explicit.STRING_VALUE_COLUMN_NAME),
				String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ZERO), String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ONE));
	}

	@Test
	public void testMultitable1() {
		testMultitable("Table1");
		testMultitable("Table2");
	}

	private void testMultitable(String table1) {
		testMultitable(String.format("\"%s_%s\"", Multitable.TABLE_NAME, table1),
				table1,
				String.format("\"%s\"", Multitable.STRING_VALUE_COLUMN_NAME),
				String.format("\"%s\"", MultitableKey.MULTITABLE_KEY_ZERO), String.format("\"%s\"", MultitableKey.MULTITABLE_KEY_ONE));
	}
}

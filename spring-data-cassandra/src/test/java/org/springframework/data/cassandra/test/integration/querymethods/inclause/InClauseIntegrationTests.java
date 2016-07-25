package org.springframework.data.cassandra.test.integration.querymethods.inclause;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Tomek Adamczewski on 25/07/16.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class InClauseIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

    @Configuration
    @EnableCassandraRepositories(basePackageClasses = TestModel.class)
    public static class Config extends IntegrationTestConfig {
        @Override
        public String[] getEntityBasePackages() {
            return new String[]{TestModel.class.getPackage().getName()};
        }

        @Override
        public SchemaAction getSchemaAction() {
            return SchemaAction.RECREATE_DROP_UNUSED;
        }
    }

    @Autowired
    TestRepo repo;

    @Before
    public void setUp() {
        cleanupTable();
        createEntity("type1", "1", "value11");
        createEntity("type1", "2", "value12");
        createEntity("type2", "1", "value21");
        createEntity("type3", "", "value3");
    }

    @Test
    public void test() {
        // type 3 and no values
        assertTrue(repo.findModelsByTypeAndVersionsIn("type1", list()).isEmpty());

        // type 3 and multiple invalid values
        assertTrue(repo.findModelsByTypeAndVersionsIn("type3", list("1", "2")).isEmpty());

        // type 1 and multiple valid values
        assertEquals(2, repo.findModelsByTypeAndVersionsIn("type1", list("1", "2")).size());

        // type 2 and multiple (one valid, one not) values
        assertEquals(1, repo.findModelsByTypeAndVersionsIn("type2", list("1", "2")).size());

        // type 3 and no values
        assertTrue(repo.findModelsByTypeAndVersionsIn("type3", list()).isEmpty());

        // type 3 and valid (empty) value
        assertEquals(1, repo.findModelsByTypeAndVersionsIn("type3", list("")).size());
    }

    private List<String> list(String... values) {
        return Arrays.asList(values);
    }

    private void createEntity(String type, String version, String value) {
        TestModel e1 = new TestModel();
        e1.key = new TestModel.PK();
        e1.key.type = type;
        e1.key.version = version;
        e1.value = value;
        repo.save(e1);
    }

    private void cleanupTable() {
        repo.deleteAll();
    }
}

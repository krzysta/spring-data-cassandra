package org.springframework.data.cassandra.test.integration.querymethods.multitable;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MultiTableIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

    private Thing e1;
    private Thing e2;

    @Configuration
    @EnableCassandraRepositories(basePackageClasses = org.springframework.data.cassandra.test.integration.querymethods.multitable.ThingRepo.class)
    public static class Config extends IntegrationTestConfig {
        @Override
        public String[] getEntityBasePackages() {
            return new String[]{Thing.class.getPackage().getName()};
        }

        @Override
        public SchemaAction getSchemaAction() {
            return SchemaAction.RECREATE_DROP_UNUSED;
        }
    }

    @Autowired
    ThingRepo repo;

    @Before
    public void sutUp() {
        e1 = new Thing();
        e1.pk = new Thing.ThingKey();
        e1.pk.discriminator = "A";
        e1.pk.key = "keyA";
        e1.value = 11;
        e2 = new Thing();
        e2.pk = new Thing.ThingKey();
        e2.pk.discriminator = "B";
        e2.pk.key = "keyB";
        e2.value = 22;
        repo.deleteAll();
        repo.save(e1);
        repo.save(e2);
    }

    @Test
    public void testSimpleQuery() {
        Thing thing = repo.findThing("A", "keyA");

        assertNotNull(thing);
        assertEquals(e1.pk.discriminator, thing.pk.discriminator);
        assertEquals(e1.pk.key, thing.pk.key);
        assertEquals(e1.value, thing.value);
    }

    @Test(expected = MappingException.class)
    public void testInvalidDiscrimiantor() {
        Thing thing = repo.findThing("C", "keyA");
    }

    @Test(expected = NullPointerException.class)
    public void testNullDiscriminator() {
        Thing thing = repo.findThing(null, "keyA");
    }

    @Test
    public void testNotFound() {
        Thing thing = repo.findThing("B", "keyA");
        assertNull(thing);
    }
}

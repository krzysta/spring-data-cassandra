package org.springframework.data.cassandra.test.integration.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.HashSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.mapping.*;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Sets;

/**
 * Created by Lukasz Swiatek on 6/13/16.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class DiscriminableEnumCassandraPersistentEntityIntegrationTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

    public enum Discriminator {
        D1,
        D2,
        D3
    }


    @Table("T1_@discriminator")
    public static class Complex {
        @PrimaryKeyClass
        public static class ComplexKey implements Serializable {
            @TableDiscriminator
            Discriminator discriminator;
            @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
            String key;

        }

        @PrimaryKey
        ComplexKey key;
    }

    @Table("T2_@discriminator_magic")
    public static class Complex2 {
        @PrimaryKeyClass
        public static class ComplexKey implements Serializable {
            @TableDiscriminator
            Discriminator discriminator;
            @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
            String key;

        }

        @PrimaryKey
        ComplexKey key;
    }

    @Autowired
    BasicCassandraMappingContext ctx;

    @Test
    public void testMultitableEntityTableNames() {
        CassandraPersistentEntity<?> persistentEntity = ctx.getPersistentEntity(Complex.class);
        HashSet<String> names = Sets.newHashSet("t1_d1", "t1_d2", "t1_d3");
        for (CqlIdentifier cqlIdentifier : persistentEntity.getEntityDiscriminator().getTableNames()) {
            assertTrue(names.remove(cqlIdentifier.toCql()));
        }
        assertEquals(0, names.size());

    }

    @Test
    public void testTableNamesWithPattern() {
        CassandraPersistentEntity<?> persistentEntity = ctx.getPersistentEntity(Complex2.class);
        HashSet<String> names = Sets.newHashSet("t2_d1_magic", "t2_d2_magic", "t2_d3_magic");
        for (CqlIdentifier cqlIdentifier : persistentEntity.getEntityDiscriminator().getTableNames()) {
            assertTrue(names.remove(cqlIdentifier.toCql()));
        }
        assertEquals(0, names.size());
    }

    @Test
    public void testResolveName() {
        CassandraPersistentEntity<Complex> persistentEntity = (CassandraPersistentEntity<Complex>) ctx.getPersistentEntity(Complex.class);
        Complex type = new Complex();
        type.key = new Complex.ComplexKey();
        type.key.discriminator = Discriminator.D2;
        CqlIdentifier cqlIdentifier = persistentEntity.getEntityDiscriminator().getTableNameFor(type);
        assertEquals("t1_d2", cqlIdentifier.toCql());
    }

    @Test
    public void testResolveNameById() {
        CassandraPersistentEntity<?> persistentEntity = ctx.getPersistentEntity(Complex.class);

        Complex.ComplexKey key = new Complex.ComplexKey();
        key.discriminator = Discriminator.D2;
        CqlIdentifier cqlIdentifier = persistentEntity.getEntityDiscriminator().getTableNameForId(key);
        assertEquals("t1_d2", cqlIdentifier.toCql());
    }

}

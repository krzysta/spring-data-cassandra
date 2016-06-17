package org.springframework.data.cassandra.test.integration.repository.multitable;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Created by Lukasz Swiatek on 6/13/16.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MultitableRepositoryIntegrationTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {
    @Autowired
    DiscEntityRepository repo;
    @Autowired
    protected CassandraOperations template;

    private DiscEntity ent1;
    private DiscEntity ent2;
    private DiscEntity ent3;
    private DiscEntity ent4;

    @Before
    public void setUp() {
        LinkedList<Object> entities = new LinkedList<Object>();
        ent1 = new DiscEntity();
        ent1.key = new DiscEntity.DiscEntityKey();
        ent1.key.discriminator = "A";
        ent1.key.key = "key1";

        ent2 = new DiscEntity();
        ent2.key = new DiscEntity.DiscEntityKey();
        ent2.key.discriminator = "A";
        ent2.key.key = "key2";

        ent3 = new DiscEntity();
        ent3.key = new DiscEntity.DiscEntityKey();
        ent3.key.discriminator = "B";
        ent3.key.key = "key3";

        ent4 = new DiscEntity();
        ent4.key = new DiscEntity.DiscEntityKey();
        ent4.key.discriminator = "A";
        ent4.key.key = "key4";

        entities.add(ent1);
        entities.add(ent2);
        entities.add(ent3);
        entities.add(ent4);
        template.deleteAll(DiscEntity.class);
        template.insert(entities);
    }

    @Test
    public void testSave() {
        DiscEntity entity = new DiscEntity();
        entity.key = new DiscEntity.DiscEntityKey();
        entity.key.discriminator = "A";
        entity.key.key = "new";

        repo.save(entity);

        DiscEntity discEntity = template.selectOneById(DiscEntity.class, entity.key);
        assertNotNull(discEntity);

    }

    @Test
    public void testDelete() {
        repo.delete(ent2);

        DiscEntity discEntity = template.selectOneById(DiscEntity.class, ent2.key);
        assertNull(discEntity);
    }

    @Test
    public void testDeleteById() {
        repo.delete(ent2.key);

        DiscEntity discEntity = template.selectOneById(DiscEntity.class, ent2.key);
        assertNull(discEntity);
    }

    @Test
    public void testDeleteAll() {
        repo.deleteAll();
        long count = template.count(DiscEntity.class);
        assertEquals(0, count);
    }

    @Test
    public void testFind() {
        DiscEntity one = repo.findOne(ent3.key);
        assertNotNull(one);
        assertEquals(ent3, one);

    }

    @Test
    public void testFindAll() {
        Iterable<DiscEntity> all = repo.findAll();
        assertEquals(4, Iterables.size(all));
        HashSet<DiscEntity> ents = Sets.newHashSet(ent1, ent2, ent3, ent4);
        ents.removeAll(Lists.newArrayList(all));
        assertEquals(0, ents.size());
    }

    //right now cassandraTemplate does not support selectByComplexId
    @Test(expected = IllegalArgumentException.class)
    public void testFindAllById() {
        Iterable<DiscEntity> all = repo.findAll(Lists.newArrayList(ent1.key, ent3.key));
        assertEquals(2, Iterables.size(all));
        HashSet<DiscEntity> ents = Sets.newHashSet(ent1, ent3);
        ents.removeAll(Lists.newArrayList(all));
        assertEquals(0, ents.size());
    }

    @Test
    public void testCount() {
        long count = repo.count();
        assertEquals(4, count);
    }

    @Test
    public void testExists() {
        boolean exists = repo.exists(ent4.key);
        assertEquals(true, exists);

    }

    @Test
    public void testNotExists() {
        DiscEntity.DiscEntityKey id = new DiscEntity.DiscEntityKey();
        id.discriminator = "A";
        id.key = "notExists";
        boolean exists = repo.exists(id);
        assertEquals(false, exists);
    }

    @Test
    public void testBachSave() {
        DiscEntity e1 = new DiscEntity();
        e1.key = new DiscEntity.DiscEntityKey();
        e1.key.discriminator = "A";
        e1.key.key = "new1";
        DiscEntity e2 = new DiscEntity();
        e2.key = new DiscEntity.DiscEntityKey();
        e2.key.discriminator = "B";
        e2.key.key = "new2";
        repo.save(Lists.newArrayList(e1, e2));

        long count = template.count(DiscEntity.class);
        assertEquals(6, count);
    }

    @Test
    public void testBachDelete() {
        repo.delete(Lists.newArrayList(ent2, ent3));
        long count = template.count(DiscEntity.class);
        assertEquals(2, count);
    }

    @Test
    public void testSavesInProperTable() {
        Long aCount = template.queryForObject(QueryBuilder.select().countAll().from("entity_A"), Long.class);
        Long bCount = template.queryForObject(QueryBuilder.select().countAll().from("entity_B"), Long.class);
        assertEquals(3, (long)aCount);
        assertEquals(1, (long)bCount);
    }
}

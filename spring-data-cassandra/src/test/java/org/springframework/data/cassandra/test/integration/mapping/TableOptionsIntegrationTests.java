package org.springframework.data.cassandra.test.integration.mapping;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.mapping.*;
import org.springframework.data.util.ClassTypeInformation;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.cassandra.core.keyspace.TableOption.COMMENT;

/**
 * Created by Tomek Adamczewski on 15/04/16.
 */
public class TableOptionsIntegrationTests {


    CassandraMappingContext context;
    CassandraPersistentEntity<?> entity;

    @Before
    public void setup() {
        context = new BasicCassandraMappingContext();
        entity = context.getPersistentEntity(ClassTypeInformation.from(Entity.class));
    }

    @Test
    public void validateTableOptions() {
        CreateTableSpecification tableSpec = context.getCreateTableSpecificationFor(entity);
        assertEquals(
                "Table comment not present in CreateTableSpecification",
                "'" + Entity.class.getAnnotation(Table.class).comment() + "'",
                tableSpec.getOptions().get(COMMENT.getName()));
        assertTrue(
                "Table comment not present in CQL",
                new CreateTableCqlGenerator(tableSpec).toCql().endsWith("WITH comment = 'Entity table comment';"));
    }


    @Table(comment = "Entity table comment")
    static class Entity {

        @PrimaryKey
        UUID id;
    }

}
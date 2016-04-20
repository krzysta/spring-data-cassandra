package org.springframework.data.cassandra.test.unit;

import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.annotation.Transient;
import org.springframework.data.cassandra.mapping.*;
import org.springframework.data.mapping.PropertyHandler;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by Tomek Adamczewski on 18/04/16.
 */
public class BasicCassandraPropertyCommentsTest {

    private BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

    @Test
    public void example1() {
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("key", "Primary Key");
        expected.put("withcomment", "Column");
        expected.put("withoutcomment", null);

        processEntity(Entity1.class, expected);
    }

    @Test
    public void example2() {
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("foo", "Foo");
        expected.put("bar", "Bar");
        expected.put("value", "Column");

        processEntity(Entity2.class, expected);
    }

    @Test
    public void example3() {
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("key", null);
        expected.put("value", null);

        processEntity(Entity3.class, expected);
    }

    private void processEntity(Class<?> type, final Map<String, String> expected) {
        CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
        final Map<CqlIdentifier, String> actual = new HashMap<CqlIdentifier, String>();
        entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
            @Override
            public void doWithPersistentProperty(CassandraPersistentProperty prop) {
                actual.putAll(prop.getColumnComments());
            }
        });
        assertComments(expected, actual);
    }

    private void assertComments(Map<String, String> expected, Map<CqlIdentifier, String> actual) {
        for (String key : expected.keySet()) {
            assertEquals("Invalid comment for column " + key + ".", expected.get(key), commentFor(key, actual));
        }
    }

    static String commentFor(String key, Map<CqlIdentifier, String> comments) {
        for (CqlIdentifier cqlId : comments.keySet()) {
            if (cqlId.toCql().equals(key)) {
                return comments.get(cqlId);
            }
        }
        throw new AssertionError("No entry matching column " + key + " found. Collected comments: " + comments);
    }

    @Table
    static class Entity1 {

        @PrimaryKey(comment = "Primary Key")
        private String key;

        @Column(comment = "Column")
        private String withComment;

        private String withoutComment;
    }

    @Table
    static class Entity2 {

        @PrimaryKey(comment = "Primary Key")
        private ComposedPrimaryKey key;

        @Column(comment = "Column")
        private String value;

        @PrimaryKeyClass
        static class ComposedPrimaryKey implements Serializable {
            @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0, comment = "Foo")
            String foo;

            @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, comment = "Bar")
            String bar;

            @Transient
            String transientProperty;
        }
    }


    @Table
    static class Entity3 {

        @PrimaryKey
        private String key;

        @Column
        private String value;
    }
}

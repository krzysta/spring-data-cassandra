package org.springframework.data.cassandra.test.unit;

import org.junit.Test;
import org.springframework.cassandra.core.keyspace.TableOption;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Tomek Adamczewski on 15/04/16.
 */
public class BasicCassandraPersistentEntityTableOptionsTest {

    private BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

    @Test
    public void testTableOptions() {
        CassandraPersistentEntity<?> withComment = mappingContext.getPersistentEntity(EntityWithTableComment.class);
        assertEquals(
                "Table comment not extracted to persistent entity",
                EntityWithTableComment.class.getAnnotation(Table.class).comment(),
                withComment.getTableOptions().get(TableOption.COMMENT));

        CassandraPersistentEntity<?> withoutComment = mappingContext.getPersistentEntity(EntityWithoutTableComment.class);
        assertNull("Null table comment added to persistent entity",
                withoutComment.getTableOptions().get(TableOption.COMMENT));
    }

    @Table(comment = "Table Comment")
    static class EntityWithTableComment {

        @PrimaryKey
        private String key;
    }

    @Table
    static class EntityWithoutTableComment {

        @PrimaryKey
        private String key;
    }

}

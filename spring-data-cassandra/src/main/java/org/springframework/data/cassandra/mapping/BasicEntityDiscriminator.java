package org.springframework.data.cassandra.mapping;

import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.google.common.collect.Lists;

/**
 * Entity discriminator for {@link CassandraPersistentEntity} defined without {@link TableDiscriminator}.
 * It will return passed tableName.
 */
public class BasicEntityDiscriminator<T>  implements EntityDiscriminator<T> {
    private CqlIdentifier tableName;

    public BasicEntityDiscriminator(CqlIdentifier tableName) {
        this.tableName = tableName;
    }

    @Override
    public CqlIdentifier getTableNameFor(T entity) {
        return tableName;
    }

    @Override
    public CqlIdentifier getTableNameForId(Object id) {
        return tableName;
    }

    @Override
    public CqlIdentifier getTableNameForDiscriminator(Object discriminator) {
        return tableName;
    }

    @Override
    public void setDiscriminatorValue(T entity, Object value) {

    }

    @Override
    public Object parseTableName(String tableName) {
        return null;
    }

    @Override
    public List<CqlIdentifier> getTableNames() {
        return Lists.newArrayList(tableName);
    }

    @Override
    public boolean isMultitable() {
        return false;
    }
}

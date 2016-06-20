package org.springframework.data.cassandra.mapping;

import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

/**
 * Entity discriminator provides functionality to discriminate entities by resolving table name based on entity properties.
 */
public interface EntityDiscriminator<T> {
    CqlIdentifier getTableNameFor(T entity);

    CqlIdentifier getTableNameForId(Object id);

    CqlIdentifier getTableNameForDiscriminator(Object discriminator);

    void setDiscriminatorValue(T entity, Object value);

    Object parseTableName(String tableName);

    List<CqlIdentifier> getTableNames();
    boolean isMultitable();
}

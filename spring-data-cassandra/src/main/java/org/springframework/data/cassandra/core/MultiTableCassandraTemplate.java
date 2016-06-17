package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.*;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableListMultimap;

/**
 * Created by Lukasz Swiatek on 6/9/16.
 */
public class MultiTableCassandraTemplate extends CassandraTemplate {

    public MultiTableCassandraTemplate() {
        super();
    }

    public MultiTableCassandraTemplate(Session session) {
        super(session);
    }

    public MultiTableCassandraTemplate(Session session, CassandraConverter converter) {
        super(session, converter);
    }

    @Override
    protected <T> T doInsert(T entity, WriteOptions options) {
        Assert.notNull(entity);
        Insert insert = createInsertQuery(getTableName(entity).toCql(), entity, options, cassandraConverter);
        execute(insert);
        return entity;
    }

    @Override
    protected <T> List<T> doBatchWrite(List<T> entities, WriteOptions options, boolean insert) {
        if (entities == null || entities.size() == 0) {
            if (logger.isWarnEnabled()) {
                logger.warn("no-op due to given null or empty list");
            }
            return entities;
        }


        ImmutableListMultimap<String, T> tables = FluentIterable.from(entities).index(new Function<T, String>() {
            @Override
            public String apply(T input) {
                return getTableName(input).toCql();
            }
        });

        Batch b = QueryBuilder.batch();

        for (String table : tables.keySet()) {
            for (T entity : tables.get(table)) {
                b.add( insert ?
                        createInsertQuery(table, entity, options, cassandraConverter)
                         : createUpdateQuery(table, entity, options, cassandraConverter)

                );
            }
        }
        CqlTemplate.addQueryOptions(b, options);

        execute(b);

        return entities;
    }

    @Override
    public <T> T selectOneById(Class<T> type, Object id) {

        Assert.notNull(type);
        Assert.notNull(id);

        CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
        if (entity == null) {
            throw new IllegalArgumentException(String.format("unknown entity class [%s]", type.getName()));
        }

        Select select = QueryBuilder.select().all().from(getTableName(type, id).toCql());
        appendIdCriteria(select.where(), entity, id);

        return selectOne(select, type);
    }

    @Override
    public boolean exists(Class<?> type, Object id) {
        Assert.notNull(type);
        Assert.notNull(id);

        CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

        Select select = QueryBuilder.select().countAll().from(getTableName(type, id).toCql());
        appendIdCriteria(select.where(), entity, id);

        Long count = queryForObject(select, Long.class);

        return count != 0;
    }

    @Override
    public void deleteById(Class<?> type, Object id) {
        Assert.notNull(type);
        Assert.notNull(id);

        CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

        Delete delete = QueryBuilder.delete().from(getTableName(type, id).toCql());
        appendIdCriteria(delete.where(), entity, id);

        execute(delete);
    }


    protected <T> void doBatchDelete(List<T> entities, QueryOptions options) {
        if (entities == null || entities.size() == 0) {
            if (logger.isWarnEnabled()) {
                logger.warn("no-op due to given null or empty list");
            }
            return ;
        }

        ImmutableListMultimap<String, T> tables = FluentIterable.from(entities).index(new Function<T, String>() {
            @Override
            public String apply(T input) {
                return getTableName(input).toCql();
            }
        });

        Batch b = QueryBuilder.batch();
        for (String table : tables.keySet()) {
            for (T entity : tables.get(table)) {
                b.add( createDeleteQuery(table, entity, options, cassandraConverter));
            }
        }
        CqlTemplate.addQueryOptions(b, options);

        execute(b);
    }


    @Override
    public long count(Class<?> type) {
        List<CqlIdentifier> tableNames = mappingContext.getPersistentEntity(type).getEntityDiscriminator().getTableNames();
        long count=0;
        for (CqlIdentifier tableName : tableNames) {
            count+=count(tableName.toCql());
        }
        return count;
    }

    @Override
    public CqlIdentifier getTableName(Class<?> type) {
        if (mappingContext.getPersistentEntity(type).getEntityDiscriminator().isMultitable()){
            throw new UnsupportedOperationException();
        }
        return super.getTableName(type);
    }

    @Override
    public <T> List<T> selectAll(Class<T> type) {
        List<CqlIdentifier> tableNames = mappingContext.getPersistentEntity(type).getEntityDiscriminator().getTableNames();
        List<T> result = new ArrayList<T>();
        for (CqlIdentifier tableName : tableNames) {
            result.addAll(select(QueryBuilder.select().all().from(tableName.toCql()), type));
        }
        return result;
    }

    protected <T> void doDelete(T entity, QueryOptions options) {

        Assert.notNull(entity);
        Delete delete = createDeleteQuery(getTableName(entity).toCql(), entity, options, cassandraConverter);
        execute(delete);
    }

    protected <T> T doUpdate(T entity, WriteOptions options) {

        Assert.notNull(entity);
        Update update = createUpdateQuery(getTableName(entity).toCql(), entity, options, cassandraConverter);
        execute(update);
        return entity;
    }

    @Override
    public <T> void deleteAll(Class<T> clazz) {

        if (!mappingContext.contains(clazz)) {
            throw new IllegalArgumentException(String.format("unknown persistent entity class [%s]", clazz.getName()));
        }

        CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(clazz);
        for (CqlIdentifier tableName : persistentEntity.getEntityDiscriminator().getTableNames()) {
            truncate(tableName);
        }
    }

    private CqlIdentifier getTableName(Class<?> type, Object id) {
        CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
        return entity.getEntityDiscriminator().getTableNameForId(id);
    }

    private CqlIdentifier getTableName(Object type) {
        CassandraPersistentEntity<Object> entity = (CassandraPersistentEntity<Object>) mappingContext.getPersistentEntity(type.getClass());
        return entity.getEntityDiscriminator().getTableNameFor(type);
    }
}

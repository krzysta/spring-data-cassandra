package org.springframework.data.cassandra.mapping;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.StringUtils;

/**
 * Entity discriminator for entities with {@link TableDiscriminator}.
 * This will allow to resolve table name template by transforming value of annotated property.
 * If {@link TableDiscriminator} does not define list of allowed values, then for enum fields all enum names will be used.
 * For any other type converter defined in {@link TableDiscriminator} must provide list of discriminator names.
 */
public class MultitableEntityDiscriminator<T> implements EntityDiscriminator<T> {
    public static final String DISCRIMINATOR_PLACEHOLDER = "@discriminator";
    private DiscriminatorConverter<?> discriminatorConverter;
    private String tableNameTemplate;
    private TableDiscriminator discriminator;
    private List<CqlIdentifier> tableNames;
    private CassandraPersistentEntity<T> persistentEntity;
    private final Table table;

    public MultitableEntityDiscriminator(CassandraPersistentEntity<T> persistentEntity) {
        CassandraPersistentEntity<?> key = persistentEntity.getMappingContext().getPersistentEntity(persistentEntity.getIdProperty().getType());
        CassandraPersistentProperty persistentProperty = key.getPersistentProperty(TableDiscriminator.class);
        this.discriminator = persistentProperty.getField().getAnnotation(TableDiscriminator.class);
        this.table = persistentEntity.getType().getAnnotation(Table.class);
        this.persistentEntity = persistentEntity;
    }

    private String getTableNameTemplate() {
        if (tableNameTemplate == null) {
            if (table == null || !StringUtils.hasText(table.value())) {
                tableNameTemplate = persistentEntity.getType().getSimpleName();
            } else {
                tableNameTemplate = table.value();
            }
        }
        return tableNameTemplate;
    }

    private DiscriminatorConverter<?> getDiscriminatorConverter() {
        if (discriminatorConverter == null) {
            Class<?> type = getDiscriminatorProperty().getType();
            if (type.isEnum()) {
                this.discriminatorConverter = new EnumDiscriminatorConverter(type);
            } else if (String.class.isAssignableFrom(type)) {
                this.discriminatorConverter = StringDiscriminatorConverter.IT;
            } else {
                Class<? extends DiscriminatorConverter>[] classes = discriminator.converter();
                if (classes.length != 1) {
                    throw new MappingException("Exactly one discriminator converter is required when using non String/Enum discriminator type");
                }
                this.discriminatorConverter = persistentEntity.getApplicationContext().getBean(classes[0]);
            }
        }
        return discriminatorConverter;
    }

    private PersistentPropertyAccessor getIdentifierPropertyAccessor(T object) {
        IdentifierAccessor keyProp = persistentEntity.getIdentifierAccessor(object);
        CassandraPersistentEntity<?> keyEntity = getPersistentPrimaryKeyEntity();
        return keyEntity.getPropertyAccessor(keyProp.getIdentifier());
    }

    private CassandraPersistentProperty getDiscriminatorProperty() {
        CassandraPersistentEntity<?> keyEntity = getPersistentPrimaryKeyEntity();
        return keyEntity.getPersistentProperty(TableDiscriminator.class);
    }

    private CassandraPersistentEntity<?> getPersistentPrimaryKeyEntity() {
        return persistentEntity.getMappingContext().getPersistentEntity(persistentEntity.getIdProperty().getType());
    }

    @Override
    public CqlIdentifier getTableNameFor(T entity) {
        Object identifier = persistentEntity.getIdentifierAccessor(entity).getIdentifier();
        return getTableNameForId(identifier);
    }

    @Override
    public CqlIdentifier getTableNameForId(Object id) {
        CassandraPersistentEntity<?> keyEntity = persistentEntity.getMappingContext().getPersistentEntity(id.getClass());
        CassandraPersistentProperty discProp = keyEntity.getPersistentProperty(TableDiscriminator.class);
        Object property = keyEntity.getPropertyAccessor(id).getProperty(discProp);
        return getTableNameForDiscriminator(property);
    }

    @Override
    public CqlIdentifier getTableNameForDiscriminator(Object discriminator) {
        DiscriminatorConverter<Object> converter = (DiscriminatorConverter<Object>) getDiscriminatorConverter();
        CqlIdentifier tableName = evalTemplate(converter.convert(discriminator));
        if (!getTableNames().contains(tableName)) {
            throw new MappingException("Given discriminator is not in defined set");
        }
        return tableName;
    }

    @Override
    public void setDiscriminatorValue(T entity, Object value) {
        PersistentPropertyAccessor accessor = getIdentifierPropertyAccessor(entity);
        accessor.setProperty(getDiscriminatorProperty(), value);
    }

    private CqlIdentifier evalTemplate(String disc) {
        String result = getTableNameTemplate().replace(DISCRIMINATOR_PLACEHOLDER, disc);
        return cqlId(result, table.forceQuote());
    }

    @Override
    public Object parseTableName(String tableName) {
        String pattern = getTableNameTemplate().replace(DISCRIMINATOR_PLACEHOLDER, "(\\w+?)");
        Pattern compile = Pattern.compile(pattern, !table.forceQuote() ? Pattern.CASE_INSENSITIVE : 0);
        Matcher matcher = compile.matcher(tableName);
        if (matcher.find()) {
            String group = matcher.group(1);
            return getDiscriminatorConverter().fromString(group);
        }
        throw new MappingException("Could not parse table discriminator");
    }

    @Override
    public List<CqlIdentifier> getTableNames() {
        if (tableNames == null || tableNames.size() == 0) {
            tableNames = new ArrayList<CqlIdentifier>();
            List<String> allValues;
            if (discriminator.value().length > 0) {
                allValues = Arrays.asList(discriminator.value());
            } else {
                allValues = getDiscriminatorConverter().getAllValues();
            }

            for (String disc : allValues) {
                CqlIdentifier tableName = evalTemplate(disc);
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    @Override
    public boolean isMultitable() {
        return true;
    }

}

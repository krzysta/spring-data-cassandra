package org.springframework.data.cassandra.mapping;

import java.util.List;

/**
 * Converter for fields annotated with {@link TableDiscriminator}.
 * It allows to
 *      * convert discriminator type to string which will be used as part of table name
 *      * convert string extracted from table name back to discriminator object
 *      * list all values - used if {@link TableDiscriminator} does not specify discriminator values
 */
public interface DiscriminatorConverter<T> {
    List<String> getAllValues();
    String convert(T discriminator);
    T fromString(String tableName);
}

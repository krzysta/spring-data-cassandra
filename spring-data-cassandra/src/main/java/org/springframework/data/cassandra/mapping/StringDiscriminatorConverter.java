package org.springframework.data.cassandra.mapping;

import java.util.List;

/**
 * Created by Lukasz Swiatek on 6/13/16.
 */
public class StringDiscriminatorConverter implements DiscriminatorConverter<String> {
    public static final StringDiscriminatorConverter IT = new StringDiscriminatorConverter();
    @Override
    public List<String> getAllValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String convert(String discriminator) {
        return discriminator;
    }

    @Override
    public String fromString(String tableName) {
        return tableName;
    }
}

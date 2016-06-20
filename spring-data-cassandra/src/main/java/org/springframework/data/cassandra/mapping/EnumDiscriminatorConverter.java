package org.springframework.data.cassandra.mapping;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Lukasz Swiatek on 6/15/16.
 */
public class EnumDiscriminatorConverter<T extends Enum<T>> implements DiscriminatorConverter<T> {
    private Class<T> enumClass;

    public EnumDiscriminatorConverter(Class<T> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public List<String> getAllValues() {
        Enum[] enumConstants = enumClass.getEnumConstants();
        List<String> allValues = new ArrayList<String>(enumConstants.length);
        for (Enum en : enumConstants) {
            allValues.add(en.name());
        }
        return allValues;
    }

    @Override
    public String convert(T discriminator) {
        return discriminator.name();
    }

    @Override
    public T fromString(String tableNAme) {
        return Enum.valueOf(enumClass, tableNAme);
    }
}

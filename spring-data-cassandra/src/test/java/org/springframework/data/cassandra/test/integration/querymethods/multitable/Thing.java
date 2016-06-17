package org.springframework.data.cassandra.test.integration.querymethods.multitable;

import java.io.Serializable;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.*;

/**
 * Created by Lukasz Swiatek on 6/14/16.
 */
@Table("thing_@discriminator")
public class Thing {
    @PrimaryKeyClass
    public static class ThingKey implements Serializable{
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
        @TableDiscriminator({"A","B"})
        String discriminator;

        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
        String key;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ThingKey thingKey = (ThingKey) o;

            if (discriminator != null ? !discriminator.equals(thingKey.discriminator) : thingKey.discriminator != null)
                return false;
            return key != null ? key.equals(thingKey.key) : thingKey.key == null;

        }

        @Override
        public int hashCode() {
            int result = discriminator != null ? discriminator.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }
    }
    @PrimaryKey
    ThingKey pk;
    Integer value;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Thing thing = (Thing) o;

        if (pk != null ? !pk.equals(thing.pk) : thing.pk != null) return false;
        return value != null ? value.equals(thing.value) : thing.value == null;

    }

    @Override
    public int hashCode() {
        int result = pk != null ? pk.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}

package org.springframework.data.cassandra.test.integration.repository.multitable;

import java.io.Serializable;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.*;

/**
 * Created by Lukasz Swiatek on 6/13/16.
 */
@Table("entity_@discriminator")
public class DiscEntity {
    @PrimaryKeyClass
    public static class DiscEntityKey implements Serializable {
        @TableDiscriminator(value = {"A", "B"}, converter = StringDiscriminatorConverter.class)
        String discriminator;
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
        String key;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DiscEntityKey that = (DiscEntityKey) o;

            if (discriminator != null ? !discriminator.equals(that.discriminator) : that.discriminator != null)
                return false;
            return key != null ? key.equals(that.key) : that.key == null;

        }

        @Override
        public int hashCode() {
            int result = discriminator != null ? discriminator.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }
    }

    @PrimaryKey
    DiscEntityKey key;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiscEntity that = (DiscEntity) o;

        return key != null ? key.equals(that.key) : that.key == null;

    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }
}


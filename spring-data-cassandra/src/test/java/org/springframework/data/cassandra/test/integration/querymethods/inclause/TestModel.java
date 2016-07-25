package org.springframework.data.cassandra.test.integration.querymethods.inclause;

import org.springframework.data.annotation.AccessType;
import org.springframework.data.cassandra.mapping.*;

import java.io.Serializable;

import static com.datastax.driver.core.DataType.Name.TEXT;
import static org.springframework.cassandra.core.PrimaryKeyType.CLUSTERED;
import static org.springframework.cassandra.core.PrimaryKeyType.PARTITIONED;

/**
 * Created by Tomek Adamczewski on 25/07/16.
 */
@Table("models")
public class TestModel {

    @AccessType(AccessType.Type.PROPERTY)
    @PrimaryKeyClass
    public static class PK implements Serializable {

        @PrimaryKeyColumn(ordinal = 0, type= PARTITIONED)
        @CassandraType(type = TEXT)
        String type;

        @PrimaryKeyColumn(ordinal = 1, type = CLUSTERED)
        @CassandraType(type = TEXT)
        String version;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    @PrimaryKey
    PK key;

    String value;

    public PK getKey() {
        return key;
    }

    public void setKey(PK key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

/**
 * Created by Lukasz Swiatek on 6/15/16.
 */
@Table(forceQuote = true, value = Multitable.TABLE_NAME + "_@discriminator")
public class Multitable {
    public static final String TABLE_NAME = "JavaMultitableTable";
    public static final String STRING_VALUE_COLUMN_NAME = "JavaMultitableStringValue";

    @PrimaryKey
    MultitableKey primaryKey;
    @Column(value = STRING_VALUE_COLUMN_NAME, forceQuote = true)
    String stringValue = UUID.randomUUID().toString();


    public Multitable() {
    }

    public Multitable(MultitableKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Multitable(MultitableKey primaryKey, String stringValue) {
        this.primaryKey = primaryKey;
        this.stringValue = stringValue;
    }

    public String getStringValue() {
        return stringValue;
    }

    public MultitableKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(MultitableKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }
}

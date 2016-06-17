package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import java.io.Serializable;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.TableDiscriminator;

/**
 * Created by Lukasz Swiatek on 6/15/16.
 */
@PrimaryKeyClass
public class MultitableKey implements Serializable{
    public static final String MULTITABLE_KEY_ZERO = "JavaMultitableKeyZero";
    public static final String MULTITABLE_KEY_ONE = "JavaMultitableKeyOne";
    public static final String MULTITABLE_DISCRIMINATOR = "JavaMultitableDiscriminator";

    @PrimaryKeyColumn(ordinal = 2, forceQuote = true, name = MULTITABLE_DISCRIMINATOR)
    @TableDiscriminator({"Table1", "Table2"})
    String discriminator;
    @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, forceQuote = true, name = MULTITABLE_KEY_ZERO)
    String keyZero;

    @PrimaryKeyColumn(ordinal = 1, forceQuote = true, name = MULTITABLE_KEY_ONE)
    String keyOne;

    public MultitableKey(String discriminator, String keyZero, String keyOne) {
        this.discriminator = discriminator;
        this.keyZero = keyZero;
        this.keyOne = keyOne;
    }

    public String getDiscriminator() {
        return discriminator;
    }

    public void setDiscriminator(String discriminator) {
        this.discriminator = discriminator;
    }

    public String getKeyZero() {
        return keyZero;
    }

    public void setKeyZero(String keyZero) {
        this.keyZero = keyZero;
    }

    public String getKeyOne() {
        return keyOne;
    }

    public void setKeyOne(String keyOne) {
        this.keyOne = keyOne;
    }
}

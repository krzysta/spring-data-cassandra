package org.springframework.data.cassandra.mapping;

import java.lang.annotation.*;

/**
 * Created by Lukasz Swiatek on 6/8/16.
 */
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface TableDiscriminator {
    String[] value() default {};
    Class<? extends DiscriminatorConverter>[] converter() default {};
}

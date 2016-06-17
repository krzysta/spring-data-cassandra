package org.springframework.data.cassandra.test.integration.querymethods.multitable;

import org.springframework.data.cassandra.mapping.TableDiscriminator;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

/**
 * Created by Lukasz Swiatek on 6/14/16.
 */
public interface ThingRepo extends TypedIdCassandraRepository<Thing, Thing.ThingKey> {
    @Query("select * from @table where discriminator=?0 and key=?1")
    Thing findThing(@TableDiscriminator String discriminator, String key);
}

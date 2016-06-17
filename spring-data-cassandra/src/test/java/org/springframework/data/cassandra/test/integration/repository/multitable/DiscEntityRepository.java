package org.springframework.data.cassandra.test.integration.repository.multitable;

import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

/**
 * Created by Lukasz Swiatek on 6/13/16.
 */
public interface DiscEntityRepository extends TypedIdCassandraRepository<DiscEntity,DiscEntity.DiscEntityKey> {
}

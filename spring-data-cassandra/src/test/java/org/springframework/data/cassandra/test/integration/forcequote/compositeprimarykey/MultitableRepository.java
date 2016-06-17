package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

/**
 * Created by Lukasz Swiatek on 6/15/16.
 */
public interface MultitableRepository extends TypedIdCassandraRepository<Multitable, MultitableKey>{
}

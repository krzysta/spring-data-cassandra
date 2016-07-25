package org.springframework.data.cassandra.test.integration.querymethods.inclause;

import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

import java.util.List;

import static com.datastax.driver.core.DataType.Name.LIST;
import static com.datastax.driver.core.DataType.Name.TEXT;

/**
 * Created by Tomek Adamczewski on 25/07/16.
 */
public interface TestRepo extends TypedIdCassandraRepository<TestModel, TestModel.PK> {

    @Query("select * from models where type = ?0 and version in (?1) allow filtering")
    List<TestModel> findModelsByTypeAndVersionsIn(String type, @CassandraType(type= LIST, typeArguments = TEXT) List<String> versions);
}
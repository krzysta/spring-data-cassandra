/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.mappingcontext;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.TableSpecification;
import org.springframework.data.cassandra.mapping.*;

import com.google.common.collect.Sets;

public class MappingContextIntegrationTests {

	public static class Transient {}

	@Table
	public static class X {
		@PrimaryKey
		String key;
	}

	@Table
	public static class Y {
		@PrimaryKey
		String key;
	}

	@Table("T_@discriminator")
	public static class Complex {
		@PrimaryKeyClass
		public static class ComplexKey implements Serializable{
			@TableDiscriminator(value = {"A", "B"}, converter = StringDiscriminatorConverter.class)
			String discriminator;
			@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
			String key;

		}

		@PrimaryKey
		ComplexKey key;
	}


	BasicCassandraMappingContext ctx = new BasicCassandraMappingContext();

	@Test
	public void testGetPersistentEntityOfTransientType() {

		CassandraPersistentEntity<?> entity = ctx.getPersistentEntity(Transient.class);
		assertNull(entity);

	}

	@Test
	public void testGetExistingPersistentEntityHappyPath() {

		ctx.getPersistentEntity(X.class);

		assertTrue(ctx.contains(X.class));
		assertNotNull(ctx.getExistingPersistentEntity(X.class));
		assertFalse(ctx.contains(Y.class));
	}

	@Test
	public void testGetMultitableEntity(){
		CassandraPersistentEntity<?> entity = ctx.getPersistentEntity(Complex.class);
		assertTrue(entity.getEntityDiscriminator() instanceof MultitableEntityDiscriminator);
	}

	@Test
	public void testGetMultitableEntitySpec(){
		CassandraPersistentEntity<?> entity = ctx.getPersistentEntity(Complex.class);
		List<CreateTableSpecification> specs = ctx.getCreateTableSpecificationFor(entity);
		assertEquals(2, specs.size());
		Set<String> names = Sets.newHashSet("t_a", "t_b");
		for (TableSpecification spec : specs) {
			assertTrue(names.remove(spec.getName().toCql()));
		}
		assertEquals(0 , names.size());
	}

}

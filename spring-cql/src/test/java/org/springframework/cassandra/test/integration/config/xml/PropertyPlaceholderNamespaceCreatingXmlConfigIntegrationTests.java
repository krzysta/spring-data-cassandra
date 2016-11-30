/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.config.xml;

import com.datastax.driver.core.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for XML-based configuration using property placeholders.
 *
 * @author Mark Paluch
 * @author John Blum
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@SuppressWarnings("unused")
public class PropertyPlaceholderNamespaceCreatingXmlConfigIntegrationTests
		extends AbstractEmbeddedCassandraIntegrationTest {

	@Autowired private Cluster cassandraCluster;
	@Autowired private CqlOperations ops;
	@Autowired private Session session;

	@Test
	public void keyspaceExists() {

		IntegrationTestUtils.assertSession(session);
		IntegrationTestUtils.assertKeyspaceExists("ppncxct", session);

		assertNotNull(ops);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void localAndRemotePoolingOptionsWereConfiguredProperly() {

		PoolingOptions poolingOptions = cassandraCluster.getConfiguration().getPoolingOptions();

		assertThat(poolingOptions, is(notNullValue(PoolingOptions.class)));
		assertThat(poolingOptions.getHeartbeatIntervalSeconds(), is(equalTo(60)));
		assertThat(poolingOptions.getIdleTimeoutSeconds(), is(equalTo(180)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL), is(equalTo(4)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL), is(equalTo(8)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL), is(equalTo(20)));
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL), is(equalTo(10)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE), is(equalTo(2)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE), is(equalTo(4)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE), is(equalTo(10)));
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE), is(equalTo(5)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void socketOptionsWereConfiguredProperly() {

		SocketOptions socketOptions = cassandraCluster.getConfiguration().getSocketOptions();

		assertThat(socketOptions, is(notNullValue(SocketOptions.class)));
		assertThat(socketOptions.getKeepAlive(), is(true));
		assertThat(socketOptions.getConnectTimeoutMillis(), is(equalTo(15000)));
		assertThat(socketOptions.getReadTimeoutMillis(), is(equalTo(60000)));
		assertThat(socketOptions.getReceiveBufferSize(), is(equalTo(1024)));
		assertThat(socketOptions.getReuseAddress(), is(true));
		assertThat(socketOptions.getSendBufferSize(), is(equalTo(2048)));
		assertThat(socketOptions.getSoLinger(), is(equalTo(5)));
		assertThat(socketOptions.getTcpNoDelay(), is(false));
	}
}

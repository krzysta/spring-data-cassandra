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
package org.springframework.cassandra.config;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceActionSpecification;
import org.springframework.cassandra.core.util.CollectionUtils;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link org.springframework.beans.factory.FactoryBean} for configuring a Cassandra {@link Cluster}.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David Webb
 * @author Kirk Clemens
 * @author Jorge Davison
 * @author John Blum
 * @author Mark Paluch
 * @author Stefan Birkner
 * @see org.springframework.beans.factory.InitializingBean
 * @see org.springframework.beans.factory.DisposableBean
 * @see org.springframework.beans.factory.FactoryBean
 * @see com.datastax.driver.core.Cluster
 */
@SuppressWarnings("unused")
public class CassandraCqlClusterFactoryBean implements FactoryBean<Cluster>, InitializingBean, DisposableBean,
		BeanNameAware, PersistenceExceptionTranslator {

	public static final boolean DEFAULT_JMX_REPORTING_ENABLED = true;
	public static final boolean DEFAULT_METRICS_ENABLED = true;
	public static final boolean DEFAULT_SSL_ENABLED = false;

	public static final int DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS = 10;
	public static final int DEFAULT_PORT = 9042;

	public static final String DEFAULT_CONTACT_POINTS = "localhost";

	protected static final Logger log = LoggerFactory.getLogger(CassandraCqlClusterFactoryBean.class);

	private boolean jmxReportingEnabled = DEFAULT_JMX_REPORTING_ENABLED;
	private boolean metricsEnabled = DEFAULT_METRICS_ENABLED;
	private boolean sslEnabled = DEFAULT_SSL_ENABLED;

	private int maxSchemaAgreementWaitSeconds = DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS;
	private int port = DEFAULT_PORT;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private Cluster cluster;
	private ClusterBuilderConfigurer clusterBuilderConfigurer;

	private AddressTranslator addressTranslator;
	private AuthProvider authProvider;
	private CompressionType compressionType;
	private Host.StateListener hostStateListener;
	private LatencyTracker latencyTracker;

	private List<CreateKeyspaceSpecification> keyspaceCreations = new ArrayList<CreateKeyspaceSpecification>();
	private List<DropKeyspaceSpecification> keyspaceDrops = new ArrayList<DropKeyspaceSpecification>();
	private Set<KeyspaceActionSpecification<?>> keyspaceSpecifications = new HashSet<KeyspaceActionSpecification<?>>();

	private List<String> startupScripts = new ArrayList<String>();
	private List<String> shutdownScripts = new ArrayList<String>();

	private LoadBalancingPolicy loadBalancingPolicy;
	private NettyOptions nettyOptions;
	private PoolingOptions poolingOptions;
	private ProtocolVersion protocolVersion;
	private QueryOptions queryOptions;
	private ReconnectionPolicy reconnectionPolicy;
	private RetryPolicy retryPolicy;
	private SpeculativeExecutionPolicy speculativeExecutionPolicy;
	private SocketOptions socketOptions;
	private SSLOptions sslOptions;
	private TimestampGenerator timestampGenerator;

	private String beanName;
	private String clusterName;
	private String contactPoints = DEFAULT_CONTACT_POINTS;
	private String password;
	private String username;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.isTrue(StringUtils.hasText(contactPoints), "At least one server is required");

		Cluster.Builder clusterBuilder = newClusterBuilder();

		clusterBuilder.addContactPoints(StringUtils.commaDelimitedListToStringArray(contactPoints)).withPort(port);

		if (compressionType != null) {
			clusterBuilder.withCompression(convertCompressionType(compressionType));
		}

		if (poolingOptions != null) {
			clusterBuilder.withPoolingOptions(poolingOptions);
		}

		if (socketOptions != null) {
			clusterBuilder.withSocketOptions(socketOptions);
		}

		if (queryOptions != null) {
			clusterBuilder.withQueryOptions(queryOptions);
		}

		if (authProvider != null) {
			clusterBuilder.withAuthProvider(authProvider);
		} else if (username != null) {
			clusterBuilder.withCredentials(username, password);
		}

		if (nettyOptions != null) {
			clusterBuilder.withNettyOptions(nettyOptions);
		}

		if (loadBalancingPolicy != null) {
			clusterBuilder.withLoadBalancingPolicy(loadBalancingPolicy);
		}

		if (reconnectionPolicy != null) {
			clusterBuilder.withReconnectionPolicy(reconnectionPolicy);
		}

		if (retryPolicy != null) {
			clusterBuilder.withRetryPolicy(retryPolicy);
		}

		if (!metricsEnabled) {
			clusterBuilder.withoutMetrics();
		}

		if (!jmxReportingEnabled) {
			clusterBuilder.withoutJMXReporting();
		}

		if (sslEnabled) {
			if (sslOptions == null) {
				clusterBuilder.withSSL();
			} else {
				clusterBuilder.withSSL(sslOptions);
			}
		}

		if (protocolVersion != null) {
			clusterBuilder.withProtocolVersion(protocolVersion);
		}

		if (addressTranslator != null) {
			clusterBuilder.withAddressTranslator(addressTranslator);
		}

		String clusterName = resolveClusterName();

		if (StringUtils.hasText(clusterName)) {
			clusterBuilder.withClusterName(clusterName);
		}

		clusterBuilder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);

		if (speculativeExecutionPolicy != null) {
			clusterBuilder.withSpeculativeExecutionPolicy(speculativeExecutionPolicy);
		}

		if (timestampGenerator != null) {
			clusterBuilder.withTimestampGenerator(timestampGenerator);
		}

		if (clusterBuilderConfigurer != null) {
			clusterBuilderConfigurer.configure(clusterBuilder);
		}

		cluster = clusterBuilder.build();

		if (hostStateListener != null) {
			cluster.register(hostStateListener);
		}

		if (latencyTracker != null) {
			cluster.register(latencyTracker);
		}

		generateSpecificationsFromFactoryBeans();

		executeSpecsAndScripts(keyspaceCreations, startupScripts);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.Cluster#builder()
	 */
	Cluster.Builder newClusterBuilder() {
		return Cluster.builder();
	}

	private String resolveClusterName() {
		return (StringUtils.hasText(clusterName) ? clusterName : beanName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		executeSpecsAndScripts(keyspaceDrops, shutdownScripts);
		cluster.close();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public Cluster getObject() {
		return cluster;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<? extends Cluster> getObjectType() {
		return (cluster != null ? cluster.getClass() : Cluster.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/**
	 * Examines the contents of all the KeyspaceSpecificationFactoryBeans and generates the proper KeyspaceSpecification
	 * from them.
	 */
	private void generateSpecificationsFromFactoryBeans() {

		for (KeyspaceActionSpecification<?> keyspaceSpecification : keyspaceSpecifications) {

			if (keyspaceSpecification instanceof CreateKeyspaceSpecification) {
				keyspaceCreations.add((CreateKeyspaceSpecification) keyspaceSpecification);
			}

			if (keyspaceSpecification instanceof DropKeyspaceSpecification) {
				keyspaceDrops.add((DropKeyspaceSpecification) keyspaceSpecification);
			}
		}
	}

	protected void executeSpecsAndScripts(List<? extends KeyspaceActionSpecification<?>> kepspaceActionSpecifications,
										  List<String> scripts) {

		if (!CollectionUtils.isEmpty(kepspaceActionSpecifications) || !CollectionUtils.isEmpty(scripts)) {

			Session session = cluster.connect();

			try {
				CqlTemplate template = new CqlTemplate(session);

				for (KeyspaceActionSpecification<?> keyspaceActionSpecification : kepspaceActionSpecifications) {
					template.execute(toCql(keyspaceActionSpecification));
				}

				for (String script : scripts) {
					template.execute(script);
				}
			} finally {
				if (session != null) {
					session.close();
				}
			}
		}
	}

	private String toCql(KeyspaceActionSpecification<?> keyspaceActionSpecification) {

		return (keyspaceActionSpecification instanceof CreateKeyspaceSpecification
				? new CreateKeyspaceCqlGenerator((CreateKeyspaceSpecification) keyspaceActionSpecification).toCql()
				: new DropKeyspaceCqlGenerator((DropKeyspaceSpecification) keyspaceActionSpecification).toCql());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(String)
	 * @since 1.5
	 */
	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * Set a comma-delimited string of the contact points (hosts) to connect to. Default is {@code localhost};
	 * see {@link #DEFAULT_CONTACT_POINTS}.
	 *
	 * @param contactPoints the contact points used by the new cluster.
	 */
	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	/**
	 * Set the port for the contact points. Default is {@code 9042}, see {@link #DEFAULT_PORT}.
	 *
	 * @param port the port used by the new cluster.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Set the {@link CompressionType}. Default is uncompressed.
	 *
	 * @param compressionType the {@link CompressionType} used by the new cluster.
	 */
	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	/**
	 * Set the {@link PoolingOptions} to configure the connection pooling behavior.
	 *
	 * @param poolingOptions the {@link PoolingOptions} used by the new cluster.
	 */
	public void setPoolingOptions(PoolingOptions poolingOptions) {
		this.poolingOptions = poolingOptions;
	}

	/**
	 * Set the {@link ProtocolVersion}.
	 *
	 * @param protocolVersion the {@link ProtocolVersion} used by the new cluster.
	 * @since 1.4
	 */
	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	/**
	 * Set the {@link SocketOptions} containing low-level socket options.
	 *
	 * @param socketOptions the {@link SocketOptions} used by the new cluster.
	 */
	public void setSocketOptions(SocketOptions socketOptions) {
		this.socketOptions = socketOptions;
	}

	/**
	 * Set the {@link QueryOptions} to tune to defaults for individual queries.
	 *
	 * @param queryOptions the {@link QueryOptions} used by the new cluster.
	 */
	public void setQueryOptions(QueryOptions queryOptions) {
		this.queryOptions = queryOptions;
	}

	/**
	 * Set the {@link AuthProvider}. Default is unauthenticated.
	 *
	 * @param authProvider the {@link AuthProvider} used by the new cluster.
	 */
	public void setAuthProvider(AuthProvider authProvider) {
		this.authProvider = authProvider;
	}

	/**
	 * Set the {@link NettyOptions} used by a client to customize the driver's underlying Netty layer.
	 *
	 * @param nettyOptions the {@link NettyOptions} used by the new cluster.
	 * @since 1.5
	 */
	public void setNettyOptions(NettyOptions nettyOptions) {
		this.nettyOptions = nettyOptions;
	}

	/**
	 * Set the {@link LoadBalancingPolicy} that decides which Cassandra hosts to contact for each new query.
	 *
	 * @param loadBalancingPolicy the {@link LoadBalancingPolicy} used by the new cluster.
	 */
	public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	/**
	 * Set the {@link ReconnectionPolicy} that decides how often the reconnection to a dead node is attempted.
	 *
	 * @param reconnectionPolicy the {@link ReconnectionPolicy} used by the new cluster.
	 */
	public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}

	/**
	 * Set the {@link RetryPolicy} that defines a default behavior to adopt when a request fails.
	 *
	 * @param retryPolicy the {@link RetryPolicy} used by the new cluster.
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * Set whether metrics are enabled. Default is {@literal true}, see {@link #DEFAULT_METRICS_ENABLED}.
	 */
	public void setMetricsEnabled(boolean metricsEnabled) {
		this.metricsEnabled = metricsEnabled;
	}

	/**
	 * Set a {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications} that are executed when
	 * this factory is {@link #afterPropertiesSet() initialized}. {@link CreateKeyspaceSpecification Create keyspace
	 * specifications} are executed on a system session with no keyspace set, before executing
	 * {@link #setStartupScripts(List)}.
	 *
	 * @param specifications the {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications}.
	 */
	public void setKeyspaceCreations(List<CreateKeyspaceSpecification> specifications) {
		this.keyspaceCreations = specifications;
	}

	/**
	 * @return {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications}.
	 */
	public List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return keyspaceCreations;
	}

	/**
	 * Set a {@link List} of {@link DropKeyspaceSpecification drop keyspace specifications} that are executed when this
	 * factory is {@link #destroy() destroyed}. {@link DropKeyspaceSpecification Drop keyspace specifications} are
	 * executed on a system session with no keyspace set, before executing {@link #setShutdownScripts(List)}.
	 *
	 * @param specifications the {@link List} of {@link DropKeyspaceSpecification drop keyspace specifications}.
	 */
	public void setKeyspaceDrops(List<DropKeyspaceSpecification> specifications) {
		this.keyspaceDrops = specifications;
	}

	/**
	 * @return the {@link List} of {@link DropKeyspaceSpecification drop keyspace specifications}.
	 */
	public List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return keyspaceDrops;
	}

	/**
	 * Set a {@link List} of raw {@link String CQL statements} that are executed when this factory is
	 * {@link #afterPropertiesSet() initialized}. Scripts are executed on a system session with no keyspace set, after
	 * executing {@link #setKeyspaceCreations(List)}.
	 *
	 * @param scripts the scripts to execute on startup
	 */
	public void setStartupScripts(List<String> scripts) {
		this.startupScripts = scripts;
	}

	/**
	 *
	 * @return the startup scripts
	 */
	public List<String> getStartupScripts() {
		return startupScripts;
	}

	/**
	 * Set a {@link List} of raw {@link String CQL statements} that are executed when this factory is {@link #destroy()
	 * destroyed}. {@link DropKeyspaceSpecification Drop keyspace specifications} are executed on a system session with no
	 * keyspace set, after executing {@link #setKeyspaceDrops(List)}.
	 *
	 * @param scripts the scripts to execute on shutdown
	 */
	public void setShutdownScripts(List<String> scripts) {
		this.shutdownScripts = scripts;
	}

	/**
	 *
	 * @return the shutdown scripts
	 */
	public List<String> getShutdownScripts() {
		return shutdownScripts;
	}

	/**
	 * @param keyspaceSpecifications The {@link KeyspaceActionSpecification} to set.
	 */
	public void setKeyspaceSpecifications(Set<KeyspaceActionSpecification<?>> keyspaceSpecifications) {
		this.keyspaceSpecifications = keyspaceSpecifications;
	}

	/**
	 * @return the {@link KeyspaceActionSpecification} associated with this factory.
	 */
	public Set<KeyspaceActionSpecification<?>> getKeyspaceSpecifications() {
		return keyspaceSpecifications;
	}

	/**
	 * Set the username to use with {@link com.datastax.driver.core.PlainTextAuthProvider}.
	 *
	 * @param username The username to set.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Set the username to use with {@link com.datastax.driver.core.PlainTextAuthProvider}.
	 *
	 * @param password The password to set.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Set whether to use JMX reporting. Default is {@literal false}, see {@link #DEFAULT_JMX_REPORTING_ENABLED}.
	 *
	 * @param jmxReportingEnabled The jmxReportingEnabled to set.
	 */
	public void setJmxReportingEnabled(boolean jmxReportingEnabled) {
		this.jmxReportingEnabled = jmxReportingEnabled;
	}

	/**
	 * Set whether to use SSL. Default is plain, see {@link #DEFAULT_SSL_ENABLED}.
	 *
	 * @param sslEnabled The sslEnabled to set.
	 */
	public void setSslEnabled(boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	/**
	 * @param sslOptions The sslOptions to set.
	 */
	public void setSslOptions(SSLOptions sslOptions) {
		this.sslOptions = sslOptions;
	}

	/**
	 * @param hostStateListener The hostStateListener to set.
	 */
	public void setHostStateListener(Host.StateListener hostStateListener) {
		this.hostStateListener = hostStateListener;
	}

	/**
	 * @param latencyTracker The latencyTracker to set.
	 */
	public void setLatencyTracker(LatencyTracker latencyTracker) {
		this.latencyTracker = latencyTracker;
	}

	/**
	 * Configures the address translator used by the new cluster to translate IP addresses received
	 * from Cassandra nodes into locally query-able addresses.
	 *
	 * @param addressTranslator {@link AddressTranslator} used by the new cluster.
	 * @see com.datastax.driver.core.Cluster.Builder#withAddressTranslator(AddressTranslator)
	 * @see com.datastax.driver.core.policies.AddressTranslator
	 * @since 1.5
	 */
	public void setAddressTranslator(AddressTranslator addressTranslator) {
		this.addressTranslator = addressTranslator;
	}

	/**
	 * Sets the {@link ClusterBuilderConfigurer} used to apply additional configuration logic to the
	 * {@link com.datastax.driver.core.Cluster.Builder}. {@link ClusterBuilderConfigurer} is invoked after all provided
	 * options are configured. The factory will {@link Builder#build()} the {@link Cluster} after applying
	 * {@link ClusterBuilderConfigurer}.
	 *
	 * @param clusterBuilderConfigurer {@link ClusterBuilderConfigurer} used to configure the
	 *          {@link com.datastax.driver.core.Cluster.Builder}.
	 * @see org.springframework.cassandra.config.ClusterBuilderConfigurer
	 */
	public void setClusterBuilderConfigurer(ClusterBuilderConfigurer clusterBuilderConfigurer) {
		this.clusterBuilderConfigurer = clusterBuilderConfigurer;
	}

	/**
	 * An optional name for the cluster instance. This name appears in JMX metrics. Defaults to the bean name.
	 *
	 * @param clusterName optional name for the cluster.
	 * @see com.datastax.driver.core.Cluster.Builder#withClusterName(String)
	 * @since 1.5
	 */
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * Sets the maximum time to wait for schema agreement before returning from a DDL query.  The timeout is used
	 * to wait for all currently up hosts in the cluster to agree on the schema.
	 *
	 * @param seconds max schema agreement wait in seconds.
	 * @see com.datastax.driver.core.Cluster.Builder#withMaxSchemaAgreementWaitSeconds(int)
	 * @since 1.5
	 */
	public void setMaxSchemaAgreementWaitSeconds(int seconds) {
		this.maxSchemaAgreementWaitSeconds = seconds;
	}

	/**
	 * Configures the speculative execution policy to use for the new cluster.
	 *
	 * @param speculativeExecutionPolicy {@link SpeculativeExecutionPolicy} to use with the new cluster.
	 * @see com.datastax.driver.core.Cluster.Builder#withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy)
	 * @see com.datastax.driver.core.policies.SpeculativeExecutionPolicy
	 * @since 1.5
	 */
	public void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy speculativeExecutionPolicy) {
		this.speculativeExecutionPolicy = speculativeExecutionPolicy;
	}

	/**
	 * Configures the generator that will produce the client-side timestamp sent with each query.
	 *
	 * @param timestampGenerator {@link TimestampGenerator} used to produce a client-side timestamp
	 * sent with each query.
	 * @see com.datastax.driver.core.Cluster.Builder#withTimestampGenerator(TimestampGenerator)
	 * @see com.datastax.driver.core.TimestampGenerator
	 * @since 1.5
	 */
	public void setTimestampGenerator(TimestampGenerator timestampGenerator) {
		this.timestampGenerator = timestampGenerator;
	}

	private static Compression convertCompressionType(CompressionType type) {
		switch (type) {
			case NONE:
				return Compression.NONE;
			case SNAPPY:
				return Compression.SNAPPY;
		}

		throw new IllegalArgumentException(String.format("Unknown compression type [%s]", type));
	}
}

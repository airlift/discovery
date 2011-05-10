package com.proofpoint.discovery;

import com.proofpoint.node.NodeInfo;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class TestCassandraDynamicStore
        extends TestDynamicStore
{
    private final static AtomicLong counter = new AtomicLong(0);
    private CassandraDynamicStore cassandraStore;

    @Override
    protected DynamicStore initializeStore(DiscoveryConfig config, Provider<DateTime> timeProvider)
    {
        CassandraStoreConfig storeConfig = new CassandraStoreConfig()
                .setKeyspace("test_cassandra_dynamic_store" + counter.incrementAndGet());

        cassandraStore = new CassandraDynamicStore(storeConfig, CassandraServerSetup.getServerInfo(), config, new NodeInfo("testing"), timeProvider);
        cassandraStore.initialize();
        return cassandraStore;
    }

    @BeforeSuite
    public void setupCassandra()
            throws IOException, TTransportException, ConfigurationException, InterruptedException
    {
        CassandraServerSetup.tryInitialize();
    }

    @AfterSuite
    public void teardownCassandra()
            throws IOException
    {
        CassandraServerSetup.tryShutdown();
    }

    @AfterMethod
    public void teardown()
    {
        cassandraStore.shutdown();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Already initialized")
    public void testCannotBeInitalizedTwice()
    {
        cassandraStore.initialize();
    }
}

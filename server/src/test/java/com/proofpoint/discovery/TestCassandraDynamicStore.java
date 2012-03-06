package com.proofpoint.discovery;

import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.hector.api.Cluster;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TestCassandraDynamicStore
        extends TestDynamicStore
{
    private final static AtomicLong counter = new AtomicLong(0);
    private CassandraDynamicStore cassandraStore;
    private ClusterProvider clusterProvider;

    @Override
    protected DynamicStore initializeStore(DiscoveryConfig config, Provider<DateTime> timeProvider)
    {
        CassandraStoreConfig storeConfig = new CassandraStoreConfig()
                .setKeyspace("test_cassandra_dynamic_store" + counter.incrementAndGet());

        cassandraStore = new CassandraDynamicStore(storeConfig, config, timeProvider, clusterProvider.get());
        cassandraStore.initialize();

        return new DynamicStore()
        {
            @Override
            public boolean put(Id<Node> nodeId, DynamicAnnouncement announcement)
            {
                return cassandraStore.put(nodeId, announcement);
            }

            @Override
            public boolean delete(Id<Node> nodeId)
            {
                return cassandraStore.delete(nodeId);
            }

            @Override
            public Set<Service> getAll()
            {
                cassandraStore.reload();
                return cassandraStore.getAll();
            }

            @Override
            public Set<Service> get(String type)
            {
                cassandraStore.reload();
                return cassandraStore.get(type);
            }

            @Override
            public Set<Service> get(String type, String pool)
            {
                cassandraStore.reload();
                return cassandraStore.get(type, pool);
            }
        };
    }

    @BeforeSuite
    public void setupCassandra()
            throws IOException, TTransportException, ConfigurationException, InterruptedException
    {
        CassandraServerSetup.tryInitialize();
        clusterProvider = new ClusterProvider(CassandraServerSetup.getServerInfo(), new NodeInfo("testing"));

    }

    @AfterSuite
    public void teardownCassandra()
            throws IOException
    {
        clusterProvider.stop();
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

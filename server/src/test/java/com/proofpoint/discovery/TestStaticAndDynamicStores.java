package com.proofpoint.discovery;

import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.hector.api.Cluster;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertTrue;

public class TestStaticAndDynamicStores
{
    private final static AtomicLong counter = new AtomicLong(0);
    private ClusterProvider clusterProvider;

    @Test
    public void testBothInitializeProperly()
    {
        CassandraStoreConfig storeConfig = new CassandraStoreConfig()
                .setKeyspace("test_static_and_dynamic_stores" + counter.incrementAndGet());

        Cluster cluster = clusterProvider.get();

        CassandraStaticStore staticStore = new CassandraStaticStore(storeConfig, cluster, new TestingTimeProvider());
        CassandraDynamicStore dynamicStore = new CassandraDynamicStore(storeConfig, new DiscoveryConfig(), new TestingTimeProvider(), cluster);
        dynamicStore.initialize();

        assertTrue(staticStore.getAll().isEmpty());
        assertTrue(dynamicStore.getAll().isEmpty());
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
}

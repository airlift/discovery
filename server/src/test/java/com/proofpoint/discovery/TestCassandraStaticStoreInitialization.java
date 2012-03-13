package com.proofpoint.discovery;

import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestCassandraStaticStoreInitialization
{
    private final static AtomicLong counter = new AtomicLong(0);
    private ClusterProvider clusterProvider;

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

    @Test
    public void testUpdateReplicationFactor()
    {
        String keyspace = "test_cassandra_static_store_initialization" + counter.incrementAndGet();

        CassandraStaticStore store = new CassandraStaticStore(new CassandraStoreConfig().setStaticKeyspace(keyspace).setReplicationFactor(1),
                clusterProvider.get(),
                new TestingTimeProvider());
        store.initialize();
        store.shutdown();

        CassandraStaticStore store2 = new CassandraStaticStore(new CassandraStoreConfig().setStaticKeyspace(keyspace).setReplicationFactor(2),
                clusterProvider.get(),
                new TestingTimeProvider());
        store2.initialize();
        store2.shutdown();

        assertEquals(clusterProvider.get().describeKeyspace(keyspace).getReplicationFactor(), 2);
    }
}

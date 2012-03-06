package com.proofpoint.discovery;

import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.hector.api.Cluster;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TestCassandraStaticStore
    extends TestStaticStore
{
    private final static AtomicLong counter = new AtomicLong(0);
    private ClusterProvider clusterProvider;

    @Override
    protected StaticStore initializeStore()
    {
        CassandraStoreConfig storeConfig = new CassandraStoreConfig()
                .setKeyspace("test_cassandra_static_store" + counter.incrementAndGet());

        Cluster cluster = clusterProvider.get();

        final CassandraStaticStore store = new CassandraStaticStore(storeConfig, cluster, new TestingTimeProvider());

        return new StaticStore()
        {
            @Override
            public void put(Service service)
            {
                store.put(service);
            }

            @Override
            public void delete(Id<Service> nodeId)
            {
                store.delete(nodeId);
            }

            @Override
            public Set<Service> getAll()
            {
                store.reload();
                return store.getAll();
            }

            @Override
            public Set<Service> get(String type)
            {
                store.reload();
                return store.get(type);
            }

            @Override
            public Set<Service> get(String type, String pool)
            {
                store.reload();
                return store.get(type, pool);
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
        CassandraServerSetup.tryShutdown();
        clusterProvider.stop();
    }
}

package com.proofpoint.discovery;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Iterables.find;
import static com.proofpoint.discovery.ColumnFamilies.named;
import static org.testng.Assert.assertEquals;

public class TestCassandraDynamicStoreInitialization
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
    public void testUpdatesGCGraceSeconds()
    {
        String keyspace = "test_cassandra_dynamic_store_initialization" + counter.incrementAndGet();

        Cluster cluster = clusterProvider.get();
        cluster.addKeyspace(new ThriftKsDef(keyspace));

        ThriftCfDef columnFamily = new ThriftCfDef(keyspace, CassandraDynamicStore.COLUMN_FAMILY);
        columnFamily.setGcGraceSeconds(100);
        cluster.addColumnFamily(columnFamily);

        CassandraDynamicStore store = new CassandraDynamicStore(new CassandraStoreConfig().setKeyspace(keyspace),
                                                                new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES)),
                                                                new TestingTimeProvider(),
                                                                cluster);
        store.initialize();
        store.shutdown();

        ColumnFamilyDefinition columnFamilyDefinition = find(cluster.describeKeyspace(keyspace).getCfDefs(), named(CassandraDynamicStore.COLUMN_FAMILY));
        assertEquals(columnFamilyDefinition.getGcGraceSeconds(), 0);
    }
}

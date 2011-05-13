package com.proofpoint.discovery;

import com.proofpoint.cassandra.testing.CassandraServerSetup;
import com.proofpoint.node.NodeInfo;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class TestCassandraStaticStore
    extends TestStaticStore
{
    private final static AtomicLong counter = new AtomicLong(0);

    @Override
    protected StaticStore initializeStore()
    {
        CassandraStoreConfig storeConfig = new CassandraStoreConfig()
                .setKeyspace("test_cassandra_static_store" + counter.incrementAndGet());

        return new CassandraStaticStore(storeConfig, CassandraServerSetup.getServerInfo(), new NodeInfo("testing"));
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
}

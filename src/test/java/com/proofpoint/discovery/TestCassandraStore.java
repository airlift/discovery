package com.proofpoint.discovery;

import com.google.common.io.Files;
import com.proofpoint.node.NodeInfo;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicLong;

public class TestCassandraStore
    extends TestStore
{
    private final static AtomicLong counter = new AtomicLong(0);
    private File tempDir;
    private EmbeddedCassandraServer server;
    private int rpcPort;

    @BeforeClass
    public void setupServer()
            throws IOException, TTransportException, ConfigurationException, InterruptedException
    {
        rpcPort = findUnusedPort();
        tempDir = Files.createTempDir();
        CassandraServerConfig config = new CassandraServerConfig()
                .setSeeds("localhost")
                .setStoragePort(findUnusedPort())
                .setRpcPort(rpcPort)
                .setClusterName("discovery")
                .setDirectory(tempDir);

        NodeInfo nodeInfo = new NodeInfo("testing");

        server = new EmbeddedCassandraServer(config, nodeInfo);
        server.start();
    }

    @AfterClass
    public void teardownServer()
            throws IOException
    {
        server.stop();
        try {
            Files.deleteRecursively(tempDir);
        }
        catch (IOException e) {
            // ignore
        }
    }

    @BeforeMethod
    public void setup()
    {
        CassandraStoreConfig config = new CassandraStoreConfig()
                .setKeyspace("keyspace" + counter.incrementAndGet());

        store = new CassandraStore(config, new CassandraServerInfo(rpcPort), new DiscoveryConfig(), new NodeInfo("testing"));
    }

    private int findUnusedPort()
            throws IOException
    {
        ServerSocket socket = new ServerSocket();
        try {
            socket.bind(new InetSocketAddress(0));
            return socket.getLocalPort();
        }
        finally {
            socket.close();
        }
    }
}

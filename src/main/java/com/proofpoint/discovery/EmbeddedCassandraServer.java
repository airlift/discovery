package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import com.proofpoint.node.NodeConfig;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class EmbeddedCassandraServer
{
    private final CassandraDaemon cassandra;
    private final Thread thread;
    private final int rpcPort;

    @Inject
    public EmbeddedCassandraServer(CassandraServerConfig config, NodeInfo nodeInfo)
            throws TTransportException, IOException, InterruptedException, ConfigurationException
    {
        File directory = config.getDirectory();

        if (!directory.mkdirs() && !directory.exists()) {
            throw new IllegalStateException(format("Directory %s does not exist and cannot be created", directory));
        }

        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("cluster_name", config.getClusterName())
                .put("auto_bootstrap", "false")
                .put("hinted_handoff_enabled", "true")
                .put("partitioner", "org.apache.cassandra.dht.RandomPartitioner") // TODO: make configurable
                .put("data_file_directories", asList(new File(directory, "data").getAbsolutePath()))
                .put("commitlog_directory", new File(directory, "commitlog").getAbsolutePath())
                .put("saved_caches_directory", new File(directory, "saved_caches").getAbsolutePath())
                .put("commitlog_sync", "periodic") // TODO: make configurable
                .put("commitlog_sync_period_in_ms", "10000") // TODO: make configurable
                .put("seeds", asList(config.getSeeds()))
                .put("disk_access_mode", "auto")
                .put("storage_port", config.getStoragePort())
                .put("listen_address", InetAddresses.toUriString(nodeInfo.getPublicIp()))
                .put("rpc_address", InetAddresses.toUriString(nodeInfo.getBindIp()))
                .put("rpc_port", config.getRpcPort())
                .put("endpoint_snitch", "org.apache.cassandra.locator.SimpleSnitch") // TODO: make configurable
                .put("request_scheduler", "org.apache.cassandra.scheduler.NoScheduler")
                .put("in_memory_compaction_limit_in_mb", 64)
                .build();

        File configFile = new File(directory, "config.yaml");

        Files.write(new Yaml().dump(map), configFile, Charsets.UTF_8);
        System.setProperty("cassandra.config", configFile.toURI().toString());

        rpcPort = config.getRpcPort();

        cassandra = new CassandraDaemon();
        cassandra.init(null);

        thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                cassandra.start();
            }
        });
        thread.setDaemon(true);
    }

    @PostConstruct
    public void start()
    {
        thread.start();
    }

    @PreDestroy
    public void stop()
    {
        thread.interrupt();
        cassandra.stop();
    }

    public int getRpcPort()
    {
        return rpcPort;
    }
}

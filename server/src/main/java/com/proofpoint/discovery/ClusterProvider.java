package com.proofpoint.discovery;

import com.google.common.net.InetAddresses;
import com.proofpoint.cassandra.CassandraServerInfo;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.clock.MillisecondsClockResolution;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;

import static java.lang.String.format;

public class ClusterProvider
    implements Provider<Cluster>
{
    private final static Logger log = Logger.get(ClusterProvider.class);

    private final Cluster cluster;

    @Inject
    public ClusterProvider(CassandraServerInfo cassandraInfo, NodeInfo nodeInfo)
    {
        CassandraHostConfigurator configurator = new CassandraHostConfigurator(format("%s:%s",
                InetAddresses.toUriString(nodeInfo.getInternalIp()),
                cassandraInfo.getRpcPort()));
        configurator.setClockResolution(new MillisecondsClockResolution());

        cluster = HFactory.getOrCreateCluster("discovery", configurator);
    }

    @Override
    public Cluster get()
    {
        return cluster;
    }

    @PreDestroy
    public void stop()
    {
        HFactory.shutdownCluster(cluster);
    }
}

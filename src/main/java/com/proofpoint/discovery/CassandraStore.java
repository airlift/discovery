package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.proofpoint.experimental.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.lang.String.format;

public class CassandraStore
        implements Store
{
    private final static Logger log = Logger.get(CassandraStore.class);

    private static final String CLUSTER = "discovery";
    private final static String COLUMN_FAMILY = "announcements";
    private static final String SERVICES_COLUMN = "services";
    private static final String TIMESTAMP_COLUMN = "timestamp";

    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);
    private final ScheduledExecutorService reaper = new ScheduledThreadPoolExecutor(1);

    private final Keyspace keyspace;

    private final Duration maxAge;

    @Inject
    public CassandraStore(CassandraStoreConfig config, CassandraServerInfo cassandraInfo, DiscoveryConfig discoveryConfig, NodeInfo nodeInfo)
    {
        maxAge = discoveryConfig.getMaxAge();

        Cluster cluster = HFactory.getOrCreateCluster(CLUSTER, format("%s:%s",
                                                                      InetAddresses.toUriString(nodeInfo.getPublicIp()),
                                                                      cassandraInfo.getRpcPort()));

        String keyspaceName = config.getKeyspace();
        KeyspaceDefinition definition = cluster.describeKeyspace(keyspaceName);
        if (definition == null) {
            cluster.addKeyspace(new ThriftKsDef(keyspaceName));
        }

        if (cluster.describeKeyspace(keyspaceName).getCfDefs().isEmpty()) {
            cluster.addColumnFamily(new ThriftCfDef(keyspaceName, COLUMN_FAMILY));
        }

        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        keyspace.setConsistencyLevelPolicy(new AllOneConsistencyLevelPolicy());
    }

    @PostConstruct
    public void initialize()
    {
        reaper.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    removeExpired();
                }
                catch (Throwable e) {
                    log.error(e);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        reaper.shutdown();
    }

    private void removeExpired()
    {
        List<Row<String, String, Long>> result = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKeys("", "")
                .setColumnNames(TIMESTAMP_COLUMN)
                .execute()
                .get()
                .getList();

        ImmutableSet.Builder<String> toDelete = new ImmutableSet.Builder<String>();
        for (Row<String, String, Long> row : result) {
            HColumn<String, Long> column = row.getColumnSlice().getColumnByName(TIMESTAMP_COLUMN);
            if (column != null) {
                Long timestamp = column.getValue();

                if (System.currentTimeMillis() - timestamp > maxAge.toMillis()) {
                    toDelete.add(row.getKey());
                }
            }
        }

        // TODO: race

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        for (String key : toDelete.build()) {
            mutator.addDeletion(key, COLUMN_FAMILY);
        }
        mutator.execute();
    }

    @Override
    public boolean put(UUID nodeId, Set<Service> descriptors)
    {
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        mutator.addInsertion(nodeId.toString(), COLUMN_FAMILY, HFactory.createStringColumn(SERVICES_COLUMN, codec.toJson(ImmutableList.copyOf(descriptors))));
        mutator.addInsertion(nodeId.toString(), COLUMN_FAMILY, HFactory.createColumn(TIMESTAMP_COLUMN, System.currentTimeMillis(), StringSerializer.get(), LongSerializer.get()));
        mutator.execute();

        // TODO: return true/false depending on whether key already existed

        return false;
    }

    @Override
    public boolean delete(UUID nodeId)
    {
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(nodeId.toString(), COLUMN_FAMILY);
        mutator.execute();

        // TODO: return true if key already existed

        return false;
    }

    public Set<Service> getAll()
    {
        List<Row<String, String, String>> result = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKeys("", "")
                .setColumnNames(SERVICES_COLUMN)
                .execute()
                .get()
                .getList();

        ImmutableSet.Builder<Service> builder = new ImmutableSet.Builder<Service>();
        for (Row<String, String, String> row : result) {
            HColumn column = row.getColumnSlice().getColumnByName(SERVICES_COLUMN);
            if (column != null) {
                builder.addAll(codec.fromJson(column.getValue().toString()));
            }
        }

        return builder.build();
    }

    @Override
    public Set<Service> get(String type)
    {
        return ImmutableSet.copyOf(filter(getAll(), matchesType(type)));
    }

    @Override
    public Set<Service> get(String type, String pool)
    {
        return ImmutableSet.copyOf(filter(getAll(), and(matchesType(type), matchesPool(pool))));
    }
}

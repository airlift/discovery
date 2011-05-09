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
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.joda.time.DateTime;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.lang.String.format;

public class CassandraDynamicStore
        implements DynamicStore
{
    private final static Logger log = Logger.get(CassandraDynamicStore.class);

    private static final String CLUSTER = "discovery";
    private final static String COLUMN_FAMILY = "announcements";

    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);
    private final ScheduledExecutorService reaper = new ScheduledThreadPoolExecutor(1);

    private final Keyspace keyspace;

    private final Duration maxAge;

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Provider<DateTime> currentTime;

    @Inject
    public CassandraDynamicStore(
            CassandraStoreConfig config,
            CassandraServerInfo cassandraInfo,
            DiscoveryConfig discoveryConfig,
            NodeInfo nodeInfo,
            Provider<DateTime> currentTime)
    {
        this.currentTime = currentTime;
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
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Already initialized");
        }

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
        }, 0, 1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void shutdown()
    {
        reaper.shutdown();
    }

    @Override
    public boolean put(UUID nodeId, Set<Service> descriptors)
    {
        boolean exists = exists(nodeId);

        // TODO: race condition here....

        String value = codec.toJson(ImmutableList.copyOf(descriptors));

        HFactory.createMutator(keyspace, StringSerializer.get())
                .addInsertion(nodeId.toString(), COLUMN_FAMILY, HFactory.createColumn(currentTime.get().getMillis(), value, LongSerializer.get(), StringSerializer.get()))
                .execute();

        return !exists;
    }

    @Override
    public boolean delete(UUID nodeId)
    {
        boolean exists = exists(nodeId);

        // TODO: race condition here....

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(nodeId.toString(), COLUMN_FAMILY);
        mutator.execute();

        return exists;
    }

    public Set<Service> getAll()
    {
        List<Row<String, Long, String>> result = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), LongSerializer.get(), StringSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKeys("", "")
                .setRange(Long.MAX_VALUE, expirationCutoff().getMillis(), true, 1)
                .execute()
                .get()
                .getList();

        ImmutableSet.Builder<Service> builder = new ImmutableSet.Builder<Service>();
        for (Row<String, Long, String> row : result) {
            List<HColumn<Long, String>> columns = row.getColumnSlice().getColumns();
            if (!columns.isEmpty()) { // TODO: can this ever be empty? or will cassandra suppress rows that have no columns in the specified range?
                builder.addAll(codec.fromJson(columns.iterator().next().getValue()));
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

    private boolean exists(UUID nodeId)
    {
        ColumnSlice<Long, String> slice = HFactory.createSliceQuery(keyspace, StringSerializer.get(), LongSerializer.get(), StringSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKey(nodeId.toString())
                .setRange(expirationCutoff().getMillis(), Long.MAX_VALUE, false, 1)
                .execute()
                .get();

        return !slice.getColumns().isEmpty();
    }

    private void removeExpired()
    {
        OrderedRows<String, Long, String> rows = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), LongSerializer.get(), StringSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKeys("", "")
                .setRange(0L, expirationCutoff().getMillis(), false, Integer.MAX_VALUE)
                .execute()
                .get();

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        for (Row<String, Long, String> row : rows) {
            for (HColumn<Long, String> column : row.getColumnSlice().getColumns()) {
                mutator.addDeletion(row.getKey(), COLUMN_FAMILY, column.getName());
            }
        }
        mutator.execute();
    }

    private DateTime expirationCutoff()
    {
        return currentTime.get().minusMillis((int) maxAge.toMillis());
    }
}

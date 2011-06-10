package com.proofpoint.discovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Iterables.getFirst;
import static com.proofpoint.discovery.CassandraPaginator.paginate;
import static com.proofpoint.discovery.ColumnFamilies.named;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

public class CassandraDynamicStore
        implements DynamicStore
{
    private final static Logger log = Logger.get(CassandraDynamicStore.class);

    public final static String COLUMN_FAMILY = "dynamic_announcements";
    private final static String COLUMN = "dynamic_announcement";
    private static final int PAGE_SIZE = 1000;
    private static final int GC_GRACE_SECONDS = 0;  // don't care about columns being brought back from the dead

    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);
    private final ScheduledExecutorService loader = new ScheduledThreadPoolExecutor(1);

    private final Keyspace keyspace;

    private final Duration maxAge;

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Provider<DateTime> currentTime;

    private final AtomicReference<Set<Service>> services = new AtomicReference<Set<Service>>(ImmutableSet.<Service>of());

    @Inject
    public CassandraDynamicStore(
            CassandraStoreConfig config,
            DiscoveryConfig discoveryConfig,
            Provider<DateTime> currentTime,
            Cluster cluster)
    {
        this.currentTime = currentTime;
        maxAge = discoveryConfig.getMaxAge();

        String keyspaceName = config.getKeyspace();
        KeyspaceDefinition definition = cluster.describeKeyspace(keyspaceName);
        if (definition == null) {
            cluster.addKeyspace(new ThriftKsDef(keyspaceName));
        }

        ColumnFamilyDefinition existing = find(cluster.describeKeyspace(keyspaceName).getCfDefs(), named(COLUMN_FAMILY), null);
        if (existing == null) {
            cluster.addColumnFamily(withDefaults(new ThriftCfDef(keyspaceName, COLUMN_FAMILY)));
        }
        else if (needsUpdate(existing)) {
            cluster.updateColumnFamily(withDefaults(existing));
        }

        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        keyspace.setConsistencyLevelPolicy(new AllOneConsistencyLevelPolicy());
    }

    private boolean needsUpdate(ColumnFamilyDefinition definition)
    {
        return definition.getGcGraceSeconds() != GC_GRACE_SECONDS;
    }

    private ColumnFamilyDefinition withDefaults(ColumnFamilyDefinition original)
    {
        ThriftCfDef updated = new ThriftCfDef(original);
        updated.setGcGraceSeconds(GC_GRACE_SECONDS);
        return updated;
    }

    @PostConstruct
    public void initialize()
    {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Already initialized");
        }

        cleanup();

        loader.scheduleWithFixedDelay(new Runnable()
                                      {
                                          @Override
                                          public void run()
                                          {
                                              try {
                                                  reload();
                                              }
                                              catch (Throwable e) {
                                                  log.error(e);
                                              }
                                          }
                                      }, 0, 1, TimeUnit.SECONDS);
    }

    private void cleanup()
    {
        // delete all existing columns. Needed to clean up legacy columns (timestamps as names)
        CassandraPaginator.PageQuery<String, String, String> query = new CassandraPaginator.PageQuery<String, String, String>()
        {
            @Override
            public Iterable<Row<String, String, String>> query(String start, int count)
            {
                return HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                        .setColumnFamily(COLUMN_FAMILY)
                        .setKeys(start, null)
                        .setReturnKeysOnly()
                        .setRowCount(count)
                        .execute()
                        .get();
            }
        };

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        for (Row<String, String, String> row : paginate(query, null, PAGE_SIZE)) {
            mutator.addDeletion(row.getKey(), COLUMN_FAMILY, currentTime.get().getMillis());
        }
        mutator.execute();
    }

    @PreDestroy
    public void shutdown()
    {
        loader.shutdown();
    }

    @Override
    public boolean put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(announcement, "announcement is null");

        List<Service> services = copyOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));
        String value = codec.toJson(services);

        DateTime now = currentTime.get();

        HColumn<String, String> column = HFactory.createColumn(COLUMN, value, now.getMillis(), StringSerializer.get(), StringSerializer.get())
                .setTtl((int) maxAge.convertTo(TimeUnit.SECONDS));

        HFactory.createMutator(keyspace, StringSerializer.get())
                .addInsertion(nodeId.toString(), COLUMN_FAMILY, column)
                .execute();

        return true;
    }

    @Override
    public boolean delete(Id<Node> nodeId)
    {
        HColumn<String, String> column = HFactory.createColumnQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKey(nodeId.toString())
                .setName(COLUMN)
                .execute()
                .get();

        boolean exists = column != null && column.getClock() > expirationCutoff().getMillis();

        // TODO: race condition here....

        HFactory.createMutator(keyspace, StringSerializer.get())
                .addDeletion(nodeId.toString(), COLUMN_FAMILY, currentTime.get().getMillis())
                .execute();

        return exists;
    }

    public Set<Service> getAll()
    {
        return services.get();
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

    @VisibleForTesting
    void reload()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();

        CassandraPaginator.PageQuery<String, String, String> query = new CassandraPaginator.PageQuery<String, String, String>()
        {
            public Iterable<Row<String, String, String>> query(String start, int count)
            {
                return HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                        .setColumnFamily(COLUMN_FAMILY)
                        .setKeys(start, null)
                        .setRange(COLUMN, COLUMN, false, 1)
                        .setRowCount(count)
                        .execute()
                        .get();
            }
        };

        for (Row<String, String, String> row : paginate(query, null, PAGE_SIZE)) {
            HColumn<String, String> column = getFirst(row.getColumnSlice().getColumns(), null);
            if (column != null) {
                if(column.getClock() > expirationCutoff().getMillis()) {
                    builder.addAll(codec.fromJson(column.getValue()));
                }
            }
        }

        services.set(builder.build());
    }


    private DateTime expirationCutoff()
    {
        return currentTime.get().minusMillis((int) maxAge.toMillis());
    }
}

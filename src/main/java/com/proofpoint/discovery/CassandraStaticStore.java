package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
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
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.find;
import static com.proofpoint.discovery.CassandraPaginator.paginate;
import static com.proofpoint.discovery.ColumnFamilies.named;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

public class CassandraStaticStore
        implements StaticStore
{
    private final static Logger log = Logger.get(CassandraDynamicStore.class);

    private final static String COLUMN_FAMILY = "static_announcements";
    private static final String COLUMN_NAME = "static";
    private static final int PAGE_SIZE = 1000;

    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);

    private final Keyspace keyspace;
    private final Provider<DateTime> currentTime;

    private final AtomicReference<Set<Service>> services = new AtomicReference<Set<Service>>(ImmutableSet.<Service>of());
    private final ScheduledExecutorService loader = new ScheduledThreadPoolExecutor(1);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @Inject
    public CassandraStaticStore(CassandraStoreConfig config, Cluster cluster, Provider<DateTime> currentTime)
    {
        this.currentTime = currentTime;

        String keyspaceName = config.getKeyspace();
        KeyspaceDefinition definition = cluster.describeKeyspace(keyspaceName);
        if (definition == null) {
            cluster.addKeyspace(new ThriftKsDef(keyspaceName));
        }

        ColumnFamilyDefinition existing = find(cluster.describeKeyspace(keyspaceName).getCfDefs(), named(COLUMN_FAMILY), null);
        if (existing == null) {
            cluster.addColumnFamily(new ThriftCfDef(keyspaceName, COLUMN_FAMILY));
        }

        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        keyspace.setConsistencyLevelPolicy(new QuorumAllConsistencyLevelPolicy());
    }

    @PostConstruct
    public void initialize()
    {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Already initialized");
        }

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
                                      }, 0, 5, TimeUnit.SECONDS);

    }

    @PreDestroy
    public void shutdown()
    {
        loader.shutdown();
    }

    @Override
    public void put(Service service)
    {
        String value = codec.toJson(ImmutableList.of(service));

        HFactory.createMutator(keyspace, StringSerializer.get())
                .addInsertion(service.getId().toString(), COLUMN_FAMILY, HFactory.createColumn(COLUMN_NAME, value, currentTime.get().getMillis(), StringSerializer.get(), StringSerializer.get()))
                .execute();
    }

    @Override
    public void delete(Id<Service> id)
    {
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(id.toString(), COLUMN_FAMILY, currentTime.get().getMillis());
        mutator.execute();
    }

    @Override
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

    void reload()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();

        CassandraPaginator.PageQuery<String, String, String> query = new CassandraPaginator.PageQuery<String, String, String>()
        {
            @Override
            public Iterable<Row<String, String, String>> query(String start, int count)
            {
                return HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                        .setColumnFamily(COLUMN_FAMILY)
                        .setKeys(start, null)
                        .setColumnNames(COLUMN_NAME)
                        .setRowCount(count)
                        .execute()
                        .get();
            }
        };

        for (Row<String, String, String> row : paginate(query, null, PAGE_SIZE)) {
            HColumn<String, String> column = row.getColumnSlice().getColumnByName(COLUMN_NAME);
            if (column != null) {
                builder.addAll(codec.fromJson(column.getValue()));
            }
        }

        services.set(builder.build());
    }
}

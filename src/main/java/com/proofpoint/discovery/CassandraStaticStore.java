package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.json.JsonCodec;
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

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

public class CassandraStaticStore
    implements StaticStore
{
    private final static String COLUMN_FAMILY = "static_announcements";
    private static final String COLUMN_NAME = "static";
    private static final int PAGE_SIZE = 1000;

    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);

    private final Keyspace keyspace;
    private final Provider<DateTime> currentTime;

    @Inject
    public CassandraStaticStore(CassandraStoreConfig config, Cluster cluster, Provider<DateTime> currentTime)
    {
        this.currentTime = currentTime;

        String keyspaceName = config.getKeyspace();
        KeyspaceDefinition definition = cluster.describeKeyspace(keyspaceName);
        if (definition == null) {
            cluster.addKeyspace(new ThriftKsDef(keyspaceName));
        }

        boolean exists = false;
        for (ColumnFamilyDefinition columnFamily : cluster.describeKeyspace(keyspaceName).getCfDefs()) {
            if (columnFamily.getName().equals(COLUMN_FAMILY)) {
                exists = true;
                break;
            }
        }

        if (!exists) {
            cluster.addColumnFamily(new ThriftCfDef(keyspaceName, COLUMN_FAMILY));
        }

        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        keyspace.setConsistencyLevelPolicy(new QuorumAllConsistencyLevelPolicy());
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
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();

        OrderedRows<String, String, String> rows;
        String start = null;
        do {
            rows = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                    .setColumnFamily(COLUMN_FAMILY)
                    .setKeys(start, null)
                    .setColumnNames(COLUMN_NAME)
                    .setRowCount(PAGE_SIZE)
                    .execute()
                    .get();

            for (Row<String, String, String> row : rows) {
                HColumn<String, String> column = row.getColumnSlice().getColumnByName(COLUMN_NAME);
                if (column != null) {
                    builder.addAll(codec.fromJson(column.getValue()));
                }
            }
            if (rows.getCount() > 0) {
                start = rows.peekLast().getKey();
            }
        }
        while (rows.getCount() == PAGE_SIZE);

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

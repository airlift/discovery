package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.proofpoint.experimental.json.JsonCodec;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
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

import javax.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.lang.String.format;

public class CassandraStaticStore
    implements StaticStore
{
    private static final String CLUSTER = "discovery";
    private final static String COLUMN_FAMILY = "static_announcements";

    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);

    private final Keyspace keyspace;

    @Inject
    public CassandraStaticStore(CassandraStoreConfig config, CassandraServerInfo cassandraInfo, NodeInfo nodeInfo)
    {
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
        keyspace.setConsistencyLevelPolicy(new QuorumAllConsistencyLevelPolicy());
    }

    @Override
    public void put(Service service)
    {
        String value = codec.toJson(ImmutableList.of(service));

        HFactory.createMutator(keyspace, StringSerializer.get())
                .addInsertion(service.getId().toString(), COLUMN_FAMILY, HFactory.createColumn("static", value, StringSerializer.get(), StringSerializer.get()))
                .execute();
    }

    @Override
    public void delete(UUID nodeId)
    {
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(nodeId.toString(), COLUMN_FAMILY);
        mutator.execute();
    }

    @Override
    public Set<Service> getAll()
    {
        List<Row<String, String, String>> result = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                .setColumnFamily(COLUMN_FAMILY)
                .setKeys("", "")
                .setColumnNames("static")
                .execute()
                .get()
                .getList();

        ImmutableSet.Builder<Service> builder = new ImmutableSet.Builder<Service>();
        for (Row<String, String, String> row : result) {
            HColumn<String, String> column = row.getColumnSlice().getColumnByName("static");
            if (column != null) {
                builder.addAll(codec.fromJson(column.getValue()));
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

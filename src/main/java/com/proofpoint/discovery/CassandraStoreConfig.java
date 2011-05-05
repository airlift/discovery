package com.proofpoint.discovery;

import com.proofpoint.configuration.Config;

import javax.validation.constraints.NotNull;

public class CassandraStoreConfig
{
    private String keyspace = "announcements";

    @NotNull
    public String getKeyspace()
    {
        return keyspace;
    }

    @Config("store.cassandra.keyspace")
    public CassandraStoreConfig setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
        return this;
    }
}

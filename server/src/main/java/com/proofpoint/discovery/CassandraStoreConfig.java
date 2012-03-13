package com.proofpoint.discovery;

import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

public class CassandraStoreConfig
{
    private String staticKeyspace = "announcements"; // keep old value for backward compatibility
    private String dynamicKeyspace = "dynamic_announcements";

    @NotNull
    public String getStaticKeyspace()
    {
        return staticKeyspace;
    }

    @Config("static-store.keyspace")
    @LegacyConfig("store.cassandra.keyspace")
    public CassandraStoreConfig setStaticKeyspace(String keyspace)
    {
        this.staticKeyspace = keyspace;
        return this;
    }

    @NotNull
    public String getDynamicKeyspace()
    {
        return dynamicKeyspace;
    }

    @Config("dynamic-store.keyspace")
    public CassandraStoreConfig setDynamicKeyspace(String dynamicKeyspace)
    {
        this.dynamicKeyspace = dynamicKeyspace;
        return this;
    }
}

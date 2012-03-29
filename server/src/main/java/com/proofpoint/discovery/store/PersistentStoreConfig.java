package com.proofpoint.discovery.store;

import com.proofpoint.configuration.Config;

import javax.validation.constraints.NotNull;
import java.io.File;

public class PersistentStoreConfig
{
    private File location = new File("db");

    @NotNull
    public File getLocation()
    {
        return location;
    }

    @Config("db.location")
    public PersistentStoreConfig setLocation(File location)
    {
        this.location = location;
        return this;
    }
}

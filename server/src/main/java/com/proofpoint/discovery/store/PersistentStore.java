package com.proofpoint.discovery.store;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;

public class PersistentStore
    implements LocalStore
{
    private final DB db;
    private final ObjectMapper mapper = new ObjectMapper(new SmileFactory());

    @Inject
    public PersistentStore(PersistentStoreConfig config)
            throws IOException
    {
        db = Iq80DBFactory.factory.open(config.getLocation(), new Options().createIfMissing(true));
    }

    @Override
    public void put(Entry entry)
    {
        byte[] dbEntry;
        try {
            dbEntry = mapper.writeValueAsBytes(entry);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        db.put(entry.getKey(), dbEntry);
    }

    @Override
    public Entry get(byte[] key)
    {
        try {
            return mapper.readValue(db.get(key), Entry.class);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void delete(byte[] key, Version version)
    {
        db.delete(key);
    }

    @Override
    public Iterable<Entry> getAll()
    {
        return Iterables.transform(db, new Function<Map.Entry<byte[], byte[]>, Entry>()
        {
            @Override
            public Entry apply(Map.Entry<byte[], byte[]> dbEntry)
            {
                try {
                    return mapper.readValue(dbEntry.getValue(), Entry.class);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }
}

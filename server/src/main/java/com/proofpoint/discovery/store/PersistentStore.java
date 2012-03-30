package com.proofpoint.discovery.store;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.proofpoint.log.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Predicates.notNull;

public class PersistentStore
    implements LocalStore
{
    private static final Logger log = Logger.get(PersistentStore.class);
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
        return Iterables.filter(Iterables.transform(db, new Function<Map.Entry<byte[], byte[]>, Entry>()
        {
            @Override
            public Entry apply(Map.Entry<byte[], byte[]> dbEntry)
            {
                try {
                    return mapper.readValue(dbEntry.getValue(), Entry.class);
                }
                catch (IOException e) {
                    byte[] key = dbEntry.getKey();
                    log.error(e, "Corrupt entry " + Arrays.toString(key));

                    // delete the corrupt entry... if another node has a non-corrupt version it will be replicated
                    db.delete(key);

                    // null if filtered below
                    return null;
                }
            }
        }), notNull());
    }
}

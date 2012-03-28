package com.proofpoint.discovery.store;

public interface LocalStore
{
    void put(Entry entry);
    Entry get(byte[] key);
    void delete(byte[] key, Version version);
    Iterable<Entry> getAll();
}

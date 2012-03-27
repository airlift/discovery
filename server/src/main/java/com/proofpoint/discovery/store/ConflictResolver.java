package com.proofpoint.discovery.store;

public class ConflictResolver
{
    public Entry resolve(Entry a, Entry b)
    {
        switch (a.getVersion().compare(b.getVersion())) {
            case BEFORE:
                return b;
            case AFTER:
                return a;
        }

        return a; // arbitrary
    }
}

package com.proofpoint.discovery;

import java.util.Set;
import java.util.UUID;

public interface StaticStore
{
    void put(Service service);
    void delete(UUID nodeId);

    Set<Service> getAll();
    Set<Service> get(String type);
    Set<Service> get(String type, String pool);
}

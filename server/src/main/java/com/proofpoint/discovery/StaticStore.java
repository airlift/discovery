package com.proofpoint.discovery;

import java.util.Set;

public interface StaticStore
{
    void put(Service service);
    void delete(Id<Service> id);

    Set<Service> getAll();
    Set<Service> get(String type);
    Set<Service> get(String type, String pool);
}

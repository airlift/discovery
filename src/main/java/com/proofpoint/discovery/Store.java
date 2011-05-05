package com.proofpoint.discovery;

import java.util.Set;
import java.util.UUID;

public interface Store
{
    boolean put(UUID nodeId, Set<Service> descriptors);
    boolean delete(UUID nodeId);
    Set<Service> getAll();
    Set<Service> get(String type);
    Set<Service> get(String type, String pool);
}

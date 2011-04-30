package com.proofpoint.discovery;

import java.util.Set;
import java.util.UUID;

public interface Store
{
    boolean put(UUID nodeId, Set<Service> descriptors);
    Set<Service> delete(UUID nodeId);
    Set<Service> get(String type);
    Set<Service> get(String type, String pool);
}

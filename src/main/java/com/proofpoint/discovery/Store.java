package com.proofpoint.discovery;

import java.util.Set;
import java.util.UUID;

public interface Store
{
    void put(UUID nodeId, Set<ServiceDescriptor> descriptors);
    Set<ServiceDescriptor> delete(UUID nodeId);
    Set<ServiceDescriptor> get(String type);
    Set<ServiceDescriptor> get(String type, String pool);
}

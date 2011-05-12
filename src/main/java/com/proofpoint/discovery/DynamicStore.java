package com.proofpoint.discovery;

import java.util.Set;

public interface DynamicStore
{
    boolean put(Id<Node> nodeId, DynamicAnnouncement announcement);
    boolean delete(Id<Node> nodeId);

    Set<Service> getAll();
    Set<Service> get(String type);
    Set<Service> get(String type, String pool);
}

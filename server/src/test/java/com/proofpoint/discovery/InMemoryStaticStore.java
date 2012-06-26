/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

public class InMemoryStaticStore
    implements StaticStore
{
    private final Map<Id<Service>, Service> services = Maps.newHashMap();

    @Override
    public synchronized void put(Service service)
    {
        Preconditions.checkNotNull(service, "service is null");
        Preconditions.checkArgument(service.getNodeId() == null, "service.nodeId should be null");

        services.put(service.getId(), service);
    }

    @Override
    public synchronized void delete(Id<Service> id)
    {
        services.remove(id);
    }

    @Override
    public synchronized Set<Service> getAll()
    {
        return ImmutableSet.copyOf(services.values());
    }

    @Override
    public synchronized Set<Service> get(String type)
    {
        return ImmutableSet.copyOf(filter(getAll(), matchesType(type)));
    }

    @Override
    public synchronized Set<Service> get(String type, String pool)
    {
        return ImmutableSet.copyOf(filter(getAll(), and(matchesType(type), matchesPool(pool))));
    }
}

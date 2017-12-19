/*
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
package io.airlift.discovery.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class DiscoveryServiceSelector
        implements ServiceSelector
{
    private final NodeInfo nodeInfo;
    private final ServiceInventory inventory;

    @Inject
    public DiscoveryServiceSelector(NodeInfo nodeInfo, ServiceInventory inventory)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.inventory = requireNonNull(inventory, "inventory is null");
    }

    @Override
    public String getType()
    {
        return "discovery";
    }

    @Override
    public String getPool()
    {
        return nodeInfo.getPool();
    }

    @Override
    public List<ServiceDescriptor> selectAllServices()
    {
        return ImmutableList.copyOf(inventory.getServiceDescriptors(getType()));
    }

    @Override
    public ListenableFuture<List<ServiceDescriptor>> refresh()
    {
        // this should be async, but it is never used
        inventory.updateServiceInventory();
        return immediateFuture(selectAllServices());
    }
}
